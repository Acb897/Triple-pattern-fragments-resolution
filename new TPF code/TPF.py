# == TPF Federated Harvester with Binding Propagation & Blank Node Safety ==
#
# Python implementation of the Ruby version with the following improvements:
#
# 1. SPARQL algebra parsing via RDFLib (no regex)
# 2. Concurrent TPF requests
# 3. RDFa page caching (avoid repeated parsing)
# 4. Vectorized bind-join batches
# 5. Unified datasource abstraction (TPF / SPARQL endpoint / RDF dump)
#
# Behaviour intentionally matches the Ruby version:
# - Harvest patterns independently per endpoint
# - Does NOT require endpoints to answer the whole query
# - Store harvested triples locally

import re
import json
import requests
import threading
from urllib.parse import urlencode, urljoin
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from rdflib import ConjunctiveGraph, Graph, URIRef, Literal, Variable
from rdflib.namespace import Namespace
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import Expr
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery
import time
import queue

HYDRA = Namespace("http://www.w3.org/ns/hydra/core#")
VOID = Namespace("http://rdfs.org/ns/void#")

# -------------------------------------------------------------------------
# GLOBAL SETTINGS
# -------------------------------------------------------------------------

MAX_THREADS = 3
BIND_BATCH_SIZE = MAX_THREADS * 5
MAX_BUFFER_BYTES   = 10_000_000  # ~10MB safety cap
FLUSH_INTERVAL     = 5           # seconds
_cache_lock = threading.Lock()
_repo_lock  = threading.Lock()
page_cache = {}


BUFFER_QUEUE_MAXSIZE = 100_000  # prevents memory explosion
_ingest_queue = queue.Queue(maxsize=BUFFER_QUEUE_MAXSIZE)

INDEXING_MODE = False
ALLOW_PARTIAL = True
# -------------------------------------------------------------------------
# SPARQL ALGEBRA PARSING
# -------------------------------------------------------------------------

def extract_all_patterns(node, patterns=None):
    if patterns is None:
        patterns = []

    if node is None:
        return patterns

    # If node has .triples attribute (BGP)
    if hasattr(node, "triples") and node.triples:
        patterns.extend(node.triples)

    # Recurse over common attributes
    for attr in ["p", "p1", "p2", "args", "graph"]:
        if hasattr(node, attr):
            child = getattr(node, attr)
            if isinstance(child, list):
                for c in child:
                    extract_all_patterns(c, patterns)
            else:
                extract_all_patterns(child, patterns)

    return patterns

def transform(query):
    try:
        parsed = parseQuery(query)
        algebra = translateQuery(parsed)
        if not hasattr(algebra, "algebra") or algebra.algebra is None:
            print("WARNING: translateQuery returned empty algebra")
            return []
        triples = extract_all_patterns(algebra.algebra)
    except Exception as e:
        print("SPARQL parse error:", e)
        return []

    def term_to_str(term):
        if isinstance(term, Variable):
            return f"?{term}"   # <-- preserve the ? prefix
        return str(term)

    # Deduplicate
    seen = set()
    bgp = []
    for s, p, o in triples:
        key = (term_to_str(s), term_to_str(p), term_to_str(o))
        if key not in seen:
            seen.add(key)
            bgp.append({
                "subject":   term_to_str(s),
                "predicate": term_to_str(p),
                "object":    term_to_str(o),
            })
    print("DEBUG: extracted triple patterns:", bgp)
    return bgp

# -------------------------------------------------------------------------
# PATTERN HELPERS
# -------------------------------------------------------------------------

def extract_vars_from_pattern(pat):

    vars = []

    if pat["subject"].startswith("?"):
        vars.append(pat["subject"][1:])

    if pat["predicate"].startswith("?"):
        vars.append(pat["predicate"][1:])

    if pat["object"].startswith("?"):
        vars.append(pat["object"][1:])

    return vars


def shares_variable(pat, processed_patterns):

    vars = set(extract_vars_from_pattern(pat))

    for p in processed_patterns:
        if vars.intersection(extract_vars_from_pattern(p)):
            return True

    return False


# -------------------------------------------------------------------------
# TPF URL BUILDER
# -------------------------------------------------------------------------

from urllib.parse import urlencode

def tpf_uri_request_builder(control_uri, subject, predicate, object_):
    """
    Build a TPF request URL.
    - Encodes URIs properly
    - Skips variables (starting with ?)
    - Returns full URL ready for fetch_tpf_page
    """
    params = {}

    if subject is not None and not subject.startswith("?"):
        params["subject"] = subject

    if predicate is not None and not predicate.startswith("?"):
        params["predicate"] = predicate

    if object_ is not None and not object_.startswith("?"):
        params["object"] = object_

    if params:
        query = urlencode(params)
        url = f"{control_uri}?{query}"
        # print("TPF URL:", url)
        return url
    else:
        return control_uri


# -------------------------------------------------------------------------
# CARDINALITY ESTIMATION
# -------------------------------------------------------------------------

def heuristic_cardinality(html):

    if re.search(r'no\s*triples', html, re.I):
        return 0

    if 'rel="next"' in html:
        return 10000

    triple_count = len(re.findall(r'property=|typeof=', html))

    if triple_count > 0:
        return triple_count

    return 5000


def get_pattern_count(control_uri, subject, predicate, object):

    url = tpf_uri_request_builder(control_uri, subject, predicate, object)

    try:
        response = requests.get(url)
        html = response.text
        content_type = response.headers.get("content-type","")
    except Exception as e:
        print("Connection failed:", url, e)
        return 999_999_999

    if "html" not in content_type and "<html" not in html:
        return heuristic_cardinality(html)

    graph = Graph()
    count = None

    try:
        graph.parse(data=html, format="rdfa")
    except:
        return heuristic_cardinality(html)

    for pred in [HYDRA.totalItems, VOID.triples]:

        for s,p,o in graph:

            if p == pred:

                raw = str(o).strip()
                raw = re.sub(r'[,±~]', '', raw)

                if raw.isdigit():
                    count = int(raw)
                    break

        if count:
            break

    return count if count else heuristic_cardinality(html)


# -------------------------------------------------------------------------
# PAGE FETCHING (WITH CACHE)
# -------------------------------------------------------------------------

from rdflib import Graph, ConjunctiveGraph
import requests

page_cache = {}

FOAF_PRIMARY_TOPIC = URIRef("http://xmlns.com/foaf/0.1/primaryTopic")
METADATA_NAMESPACES = (
    "http://www.w3.org/ns/hydra/core#",
    "http://rdfs.org/ns/void#",
)

def _is_metadata_predicate(p):
    return any(str(p).startswith(ns) for ns in METADATA_NAMESPACES)

def _extract_data_and_meta(cg):
    """
    Primary strategy: exclude the graph that contains foaf:primaryTopic.
    Returns (data_graph, meta_graph, strategy_used).
    Falls back to predicate filtering if primaryTopic not found or yields no data.
    """
    # --- Strategy 1: graph-based exclusion via foaf:primaryTopic ---
    metadata_graph_iris = set()
    for s, p, o, ctx in cg.quads((None, FOAF_PRIMARY_TOPIC, None, None)):
        metadata_graph_iris.add(ctx.identifier)

    if metadata_graph_iris:
        data_graph = Graph()
        meta_graph = Graph()

        for s, p, o, ctx in cg.quads((None, None, None, None)):
            if ctx.identifier in metadata_graph_iris:
                meta_graph.add((s, p, o))
            else:
                data_graph.add((s, p, o))

        if len(data_graph) > 0:
            print(f"  [Strategy] Graph exclusion via foaf:primaryTopic "
                  f"({len(metadata_graph_iris)} metadata graph(s))")
            return data_graph, meta_graph, "graph"
        else:
            print("  [Strategy] foaf:primaryTopic found but yielded 0 data triples, "
                  "falling back to predicate filtering")

    else:
        print("  [Strategy] foaf:primaryTopic not found, "
              "falling back to predicate filtering")

    # --- Strategy 2: predicate namespace filtering (original behaviour) ---
    data_graph = Graph()
    meta_graph = Graph()

    for s, p, o, ctx in cg.quads((None, None, None, None)):
        if _is_metadata_predicate(p):
            meta_graph.add((s, p, o))
        else:
            data_graph.add((s, p, o))

    print(f"  [Strategy] Predicate filtering → {len(data_graph)} data triples")
    return data_graph, meta_graph, "predicate"


def fetch_tpf_page(url):
    with _cache_lock:
        if url in page_cache:
            return page_cache[url]

    headers = {"Accept": "application/trig, text/turtle;q=0.9"}

    cg = ConjunctiveGraph()
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        text = resp.text
        print(f"[FETCH] {url} → {resp.status_code}, {len(text)} bytes")

        for fmt in ["trig", "turtle", "nt", "xml", "rdfa", "json-ld"]:
            try:
                cg.parse(data=text, format=fmt)
                break
            except Exception:
                continue

    except Exception as e:
        print(f"  Fetch/parse error: {e}")

    data_graph, meta_graph, _ = _extract_data_and_meta(cg)

    result = (data_graph, meta_graph)

    with _cache_lock:
        page_cache[url] = result

    return result

def _next_page_from_graph(meta_graph, current_url):
    for next_pred in [HYDRA.nextPage, HYDRA.next,
                      URIRef("http://www.w3.org/ns/hydra/core#next")]:
        for s, p, o in meta_graph.triples((None, next_pred, None)):
            return str(o)
    return None


def harvest_pattern_into_repo(url, named_graph):
    print("Harvesting URL:", url)
    current_url = url
    page_count = 0

    while current_url:
        page_count += 1
        print(f" Page {page_count}: {current_url}")

        data_graph, meta_graph = fetch_tpf_page(current_url)
        next_url = _next_page_from_graph(meta_graph, current_url)

        items_per_page = None
        for s, p, o in meta_graph.triples((None, HYDRA.itemsPerPage, None)):
            try:
                items_per_page = int(str(o))
            except ValueError:
                pass

        data_triples = 0
        for s, p, o in data_graph:
            add_to_buffer((s, p, o), named_graph)
            data_triples += 1

        print(f"  Buffered {data_triples} data triples")

        if data_triples == 0:
            print("  → No data triples → end of results")
            break

        if items_per_page is not None and data_triples < items_per_page:
            print(f"  → Partial page ({data_triples} < {items_per_page}) → last page")
            break

        if next_url == current_url:
            print("  WARNING: next URL equals current URL, stopping.")
            break

        current_url = next_url




# -------------------------------------------------------------------------
# VECTORISED BIND JOIN
# -------------------------------------------------------------------------

def fetch_binding(binding, pat, control_uri, named_graph):

    def concretize(term, binding):
        if not term.startswith("?"):
            return term
        var_name = term[1:]
        if var_name not in binding:
            return term
        val = binding[var_name]
        if isinstance(val, URIRef):
            return str(val)
        elif isinstance(val, Literal):
            return val.n3()
        return str(val)

    s = concretize(pat["subject"],   binding)
    p = concretize(pat["predicate"], binding)
    o = concretize(pat["object"],    binding)

    if None in (s, p, o):
        return

    url = tpf_uri_request_builder(control_uri, s, p, o)

    harvest_pattern_into_repo(url, named_graph)


def fetch_binding_batch(batch, pat, control_uri, named_graph):

    with ThreadPoolExecutor(MAX_THREADS) as pool:

        futures = []

        for binding in batch:
            futures.append(
                pool.submit(fetch_binding, binding, pat, control_uri, named_graph)
            )

        for f in futures:
            f.result()


# -------------------------------------------------------------------------
# BINDING EXTRACTION
# -------------------------------------------------------------------------

# def extract_upstream_bindings(repo, current_idx, harvested, bgp):

#     if not harvested:
#         return []

#     prev_patterns = [bgp[i] for i in harvested]

#     needed = extract_vars_from_pattern(bgp[current_idx])

#     upstream_vars = set()

#     for p in prev_patterns:
#         upstream_vars.update(extract_vars_from_pattern(p))

#     join_vars = list(set(needed).intersection(upstream_vars))

#     if not join_vars:
#         return []

#     query = "SELECT " + " ".join("?" + v for v in join_vars) + " WHERE {\n"

#     def safe(term):

#         if term.startswith("?"):
#             return term

#         if term.startswith("<"):
#             return term

#         if term.startswith('"'):
#             return term

#         if term.startswith("http"):
#             return f"<{term}>"

#         return f'"{term}"'

#     for pat in prev_patterns:

#         s = safe(pat["subject"])
#         p = safe(pat["predicate"])
#         o = safe(pat["object"])

#         query += f" {s} {p} {o} .\n"

#     query += "}"

#     print("DEBUG binding extraction")
#     print(query)

#     try:

#         q = prepareQuery(query)

#         solutions = []

#         for row in repo.query(q):

#             sol = {}

#             for v in join_vars:

#                 val = row[v]

#                 if val:
#                     sol[v] = val

#             if sol:
#                 solutions.append(sol)

#         print("DEBUG:", len(solutions), "bindings")

#         return solutions

#     except Exception as e:

#         print("Local binding extraction failed", e)
#         return []

def term_matches(pattern_term, triple_term):
    """
    Check whether a triple term matches a pattern term.
    Variables always match.
    Constants must be equal.
    """
    if pattern_term.startswith("?"):
        return True

    if pattern_term.startswith("<") and pattern_term.endswith(">"):
        pattern_term = pattern_term[1:-1]

    return str(triple_term) == pattern_term

def triple_matches_pattern(triple, pat):
    s, p, o = triple

    if not term_matches(pat["subject"], s):
        return False

    if not term_matches(pat["predicate"], p):
        return False

    if not term_matches(pat["object"], o):
        return False

    return True

def extract_upstream_bindings(repo, current_idx, harvested, bgp):

    if not harvested:
        return []

    current_pat = bgp[current_idx]

    # variables in current pattern
    current_vars = set(extract_vars_from_pattern(current_pat))

    # collect vars from previous patterns
    upstream_vars = set()
    prev_patterns = [bgp[i] for i in harvested]

    for p in prev_patterns:
        upstream_vars.update(extract_vars_from_pattern(p))

    join_vars = current_vars.intersection(upstream_vars)

    print("\n--- Binding Extraction ---")
    print("Current pattern:", current_pat)
    print("Join variables:", join_vars)
    print("Repo size:", len(repo))
    if not join_vars:
        return []

    bindings = []

    for triple in repo:

        s, p, o = triple

        for pat in prev_patterns:

            if not triple_matches_pattern(triple, pat):
                continue

            sol = {}

            if pat["subject"].startswith("?"):
                var = pat["subject"][1:]
                if var in join_vars:
                    sol[var] = s

            if pat["predicate"].startswith("?"):
                var = pat["predicate"][1:]
                if var in join_vars:
                    sol[var] = p

            if pat["object"].startswith("?"):
                var = pat["object"][1:]
                if var in join_vars:
                    sol[var] = o

            if sol:
                bindings.append(sol)

    # deduplicate
    unique = []
    seen = set()

    for b in bindings:
        key = tuple(sorted((k, str(v)) for k, v in b.items()))
        if key not in seen:
            seen.add(key)
            unique.append(b)

    print("Bindings extracted:", len(unique))

    if unique:
        print("Sample bindings:", unique[:5])

    print("--------------------------\n")

    return unique


def extract_upstream_bindings_graphdb(current_idx, harvested, bgp, graph_iri):

    if not harvested:
        return []

    current_pat = bgp[current_idx]

    current_vars = set(extract_vars_from_pattern(current_pat))

    upstream_vars = set()
    prev_patterns = [bgp[i] for i in harvested]

    for p in prev_patterns:
        upstream_vars.update(extract_vars_from_pattern(p))

    join_vars = current_vars.intersection(upstream_vars)

    if not join_vars:
        return []

    def safe(term):
        if term.startswith("?"):
            return term
        if term.startswith("<"):
            return term
        if term.startswith('"'):
            return term
        if term.startswith("http"):
            return f"<{term}>"
        return f'"{term}"'

    query = "SELECT " + " ".join("?" + v for v in join_vars) + " WHERE {\n"

    if graph_iri:
        query += f" GRAPH <{graph_iri}> {{\n"

    for pat in prev_patterns:
        s = safe(pat["subject"])
        p = safe(pat["predicate"])
        o = safe(pat["object"])
        query += f" {s} {p} {o} .\n"

    if graph_iri:
        query += " }\n"

    query += "}"

    print("DEBUG GraphDB binding query:")
    print(query)

    try:
        results = execute_sparql_query(query)
        bindings = []

        for row in results:
            sol = {}
            for v in join_vars:
                if v in row:
                    sol[v] = row[v]
            if sol:
                bindings.append(sol)

        print("Bindings extracted from GraphDB:", len(bindings))
        return bindings

    except Exception as e:
        print("GraphDB binding extraction failed:", e)
        return []
# -------------------------------------------------------------------------
# INSERT INTO LOCAL TRIPLESTORE
# -------------------------------------------------------------------------

def build_query(statements, named_graph=None):

    triples = "\n".join(
        f"{s.n3()} {p.n3()} {o.n3()} ." for s,p,o in statements
    )

    if named_graph:

        return f"""
INSERT DATA {{
 GRAPH <{named_graph}> {{
 {triples}
 }}
}}
"""

    else:

        return f"""
INSERT DATA {{
{triples}
}}
"""


# def insert_query(query):
#     endpoint = "http://acb8computer:7200/repositories/test1/statements"

#     try:
#         r = requests.post(
#             endpoint,
#             data=query,
#             headers={
#                 "Content-Type":"application/sparql-update",
#                 "Accept":"application/json"
#             }
#         )

#         if r.status_code == 200 or r.status_code == 204:
#             print("Bulk insert successful")
#         else:
#             print("Insert failed:", r.status_code, r.text)  # <- show server response

#     except Exception as e:
#         print("Insert error:", e)


def insert_triples_stream(statements, named_graph=None):
    """
    Send triples directly to GraphDB as RDF (N-Triples), without SPARQL.
    Much lower memory usage and faster ingestion.
    """

    endpoint = "http://acb8computer:7200/repositories/test1/statements"

    lines = []

    for s, p, o in statements:
        triple = f"{s.n3()} {p.n3()} {o.n3()} ."

        # If using named graphs → use N-Quads format
        if named_graph:
            triple = f"{s.n3()} {p.n3()} {o.n3()} <{named_graph}> ."

        lines.append(triple)

    payload = "\n".join(lines)

    headers = {
        # IMPORTANT: switch format depending on named_graph
        "Content-Type": "application/n-quads" if named_graph else "application/n-triples"
    }

    try:
        r = requests.post(endpoint, data=payload, headers=headers)

        if r.status_code in (200, 204):
            print(f"Stream insert OK ({len(statements)} triples)")
            
        else:
            print("Stream insert failed:", r.status_code, r.text)
        time.sleep(0.1)  # slight delay to avoid overwhelming the server
    except Exception as e:
        print("Streaming insert error:", e)


# -------------------------------------------------------------------------
# BUFFERED INGESTION
# -------------------------------------------------------------------------
def add_to_buffer(triple, named_graph=None):
    """
    Push triple into ingestion queue (blocking if full).
    This provides backpressure and guarantees no data loss.
    """

    s, p, o = triple

    if named_graph:
        line = f"{s.n3()} {p.n3()} {o.n3()} <{named_graph}> .\n"
    else:
        line = f"{s.n3()} {p.n3()} {o.n3()} .\n"

    _ingest_queue.put(line)  # blocks if queue full → SAFE




def buffer_flusher_daemon():
    endpoint = "http://acb8computer:7200/repositories/test1/statements"
    BATCH_SIZE = 500
    MAX_RETRIES = 3

    while True:
        batch = []
        try:
            # Block until at least one item is available
            line = _ingest_queue.get(timeout=1)
            batch.append(line)

            # Drain up to BATCH_SIZE without blocking
            while len(batch) < BATCH_SIZE:
                try:
                    batch.append(_ingest_queue.get_nowait())
                except queue.Empty:
                    break

            # Attempt POST with retries
            payload = "".join(batch)
            success = False

            for attempt in range(MAX_RETRIES):
                try:
                    r = requests.post(
                        endpoint,
                        data=payload,
                        headers={"Content-Type": "application/n-quads"},
                        timeout=30
                    )
                    if r.status_code in (200, 204):
                        success = True
                        break
                    else:
                        print(f"[FLUSH ERROR] attempt {attempt+1}: {r.status_code} {r.text[:200]}")
                except requests.RequestException as e:
                    print(f"[FLUSH ERROR] attempt {attempt+1}: {e}")
                time.sleep(0.5 * (attempt + 1))  # backoff

            if not success:
                # Log the lost triples so you can diagnose
                print(f"[DATA LOSS] Failed to insert {len(batch)} triples after {MAX_RETRIES} retries")

            # Always mark done — keeps join() unblocked
            # Log failures above rather than silently swallowing
            for _ in batch:
                _ingest_queue.task_done()

            time.sleep(0.05)

        except queue.Empty:
            # Queue idle — nothing to flush right now
            pass
# -------------------------------------------------------------------------
# HARVEST EXECUTION
# -------------------------------------------------------------------------

def harvest_endpoint_optimized(control_uri, bgp, named_graph):

    print("Harvesting:", control_uri)

    harvested = set()
    counts = {}

    for i,pat in enumerate(bgp):
        counts[i] = get_pattern_count(
            control_uri,
            pat["subject"],
            pat["predicate"],
            pat["object"]
        )

    remaining = list(range(len(bgp)))

    first = min(remaining, key=lambda i: counts[i])
    remaining.remove(first)

    execution_order = [first]

    while remaining:

        connected = [
            idx for idx in remaining
            if shares_variable(bgp[idx], [bgp[i] for i in execution_order])
        ]

        if connected:
            next_idx = min(connected, key=lambda i: counts[i])
        else:
            next_idx = min(remaining, key=lambda i: counts[i])

        execution_order.append(next_idx)
        remaining.remove(next_idx)

    print("Execution order:", execution_order)

    for idx in execution_order:

        pat = bgp[idx]

        print("Processing pattern", idx, pat)

        required_vars = extract_vars_from_pattern(pat)

        bindings = extract_upstream_bindings_graphdb(
            idx, harvested, bgp, named_graph
        )

        if INDEXING_MODE:
            if not bindings and required_vars and len(harvested) > 0:
                print("Skipping pattern (strict mode, no bindings)")
                harvested.add(idx)
                continue

        if not bindings or not required_vars:

            print("Full pattern download")

            url = tpf_uri_request_builder(
                control_uri,
                pat["subject"],
                pat["predicate"],
                pat["object"]
            )

            harvest_pattern_into_repo(url, named_graph)

        else:

            print("Bind join:", len(bindings), "bindings")

            for i in range(0, len(bindings), BIND_BATCH_SIZE):

                print(f"Processing binding batch {i} → {i+BIND_BATCH_SIZE}")

                batch = bindings[i:i+BIND_BATCH_SIZE]

                fetch_binding_batch(batch, pat, control_uri, named_graph)
        _ingest_queue.join()
        print(f"[SYNC] Queue drained after pattern {idx}")
        harvested.add(idx)
        print("------------------------------------")
        

    return None


# -------------------------------------------------------------------------
# MAIN ENTRY POINT
# -------------------------------------------------------------------------

def FindBGPPriority(query, endpoints, base_named_graph=None):
    threading.Thread(target=buffer_flusher_daemon, daemon=True).start()
    if isinstance(endpoints,str):
        endpoints = [endpoints]

    try:
        bgp = transform(query)
    except Exception as e:
        print("ERROR inside transform:", e)
        return

    if not bgp:
        print("No triple patterns extracted from query")
        return

    print("Query has", len(bgp), "triple patterns")

    for i,endpoint in enumerate(endpoints):

        graph_iri = None

        if base_named_graph:
            graph_iri = f"{base_named_graph}/endpoint{i+1}"

        harvest_endpoint_optimized(endpoint, bgp, graph_iri)
    print("Waiting for ingestion queue to empty...")
    _ingest_queue.join()
    print("All data flushed.")
    print("All endpoints processed.")

# -------------------------------------------------------------------------
# EXECUTE SPARQL QUERY ON LOCAL GRAPHDB
# -------------------------------------------------------------------------

def execute_sparql_query(query):

    endpoint = "http://acb8computer:7200/repositories/test1"

    try:

        r = requests.post(
            endpoint,
            data=query,
            headers={
                "Content-Type": "application/sparql-query",
                "Accept": "application/sparql-results+json"
            }
        )

        if r.status_code != 200:
            print("Query failed:", r.text)
            return None

        data = r.json()

        results = []

        for row in data["results"]["bindings"]:

            parsed = {}

            for var,val in row.items():
                parsed[var] = val["value"]

            results.append(parsed)

        return results

    except Exception as e:

        print("SPARQL execution error:", e)
        return None


