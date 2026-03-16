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
from urllib.parse import urlencode, urljoin
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from rdflib import ConjunctiveGraph, Graph, URIRef, Literal, Variable
from rdflib.namespace import Namespace
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql.parserutils import Expr
from rdflib.plugins.sparql.parser import parseQuery
from rdflib.plugins.sparql.algebra import translateQuery

HYDRA = Namespace("http://www.w3.org/ns/hydra/core#")
VOID = Namespace("http://rdfs.org/ns/void#")

# -------------------------------------------------------------------------
# GLOBAL SETTINGS
# -------------------------------------------------------------------------

MAX_THREADS = 8
BIND_BATCH_SIZE = 20

page_cache = {}

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
        print("TPF URL:", url)
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

def fetch_tpf_page(url):
    """
    Universal TPF page fetcher:
    - Tries multiple RDF formats (TriG, Turtle, N-Triples, RDF/XML, RDFa, JSON-LD)
    - Handles named graphs automatically
    - Flattens all triples into a single rdflib.Graph
    """
    if url in page_cache:
        return page_cache[url]

    headers = {
        "Accept": "text/turtle;q=1.0, application/trig;q=0.9, application/rdf+xml;q=0.8, text/n3;q=0.7, application/ld+json;q=0.6",
        "User-Agent": "TPF-Harvester/1.0 (Python; universal parser)",
    }

    cg = ConjunctiveGraph()

    try:
        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()
        text = resp.text
        print(f"[FETCH] URL: {url} → Status: {resp.status_code}, length={len(text)}")
        print("RESPONSE:")
        print(text)

        # Try parsing using multiple formats
        formats = ["trig", "turtle", "nt", "xml", "rdfa", "json-ld"]
        parsed = False

        for fmt in formats:
            try:
                cg.parse(data=text, format=fmt)
                parsed = True
                # print(f"  → Parsed {len(cg)} triples (including named graphs) using format: {fmt}")
                break
            except Exception:
                continue

        if not parsed:
            print("  → WARNING: Failed to parse RDF from this page")
            page_cache[url] = Graph()
            return page_cache[url]

        # Flatten all quads into a single Graph
        flattened = Graph()
        for s, p, o, ctx in cg.quads((None, None, None, None)):
            flattened.add((s, p, o))

        page_cache[url] = flattened
        # print(f"  → Flattened triples: {len(flattened)}")
        return flattened

    except Exception as e:
        print(f"  → Fetch/parse error: {e}")
        page_cache[url] = Graph()
        return page_cache[url]



def harvest_pattern_into_repo(url, repo):
    print("Harvesting URL:", url)
    current_url = url
    page_count = 0
    while current_url:
        page_count += 1
        print(f"  Page {page_count}: {current_url}")
        try:
            html = requests.get(current_url).text
            # print(f"    Content length: {len(html)} bytes")
            # print(f"    Starts with: {html[:80]!r}")

            soup = BeautifulSoup(html, "html.parser")
            next_link = soup.select_one('link[rel="next"], a[rel="next"]')
            next_url = None
            if next_link and next_link.get("href"):
                next_url = urljoin(current_url, next_link["href"])
                # print("    Next page:", next_url)

            # ─── Critical part ─────────────────────────────────────
            g = fetch_tpf_page(current_url)
            # print("    Parsed triples:", len(g))

            if len(g) == 0:
                print("    WARNING: empty graph after RDFa parse!")

            added = 0
            for triple in g:
                print("Adding triples:")
                print(triple)
                repo.add(triple)
                added += 1
            print(f"    Added {added} new triples → total now {len(repo)}")
            # ───────────────────────────────────────────────────────

            current_url = next_url
        except Exception as e:
            print("    Harvest error:", e)
            current_url = None


# -------------------------------------------------------------------------
# VECTORISED BIND JOIN
# -------------------------------------------------------------------------

def fetch_binding(binding, pat, control_uri, repo):
    """
    Fetch triples for a single binding using proper concretization.
    Only constructs a TPF URL when at least one position is bound to a concrete value.
    """
    def concretize(term, binding):
        if not term.startswith("?"):
            return term  # already concrete (URI, literal, etc.)
        
        var_name = term[1:]
        if var_name not in binding:
            return term  # unbound variable → keep as-is (will be skipped in URL builder)
        
        val = binding[var_name]
        if isinstance(val, URIRef):
            return str(val)           # proper URI string for TPF parameter
        elif isinstance(val, Literal):
            return val.n3()
        else:
            return str(val)           # fallback (bnode, etc.)

    s = concretize(pat["subject"], binding)
    p = concretize(pat["predicate"], binding)
    o = concretize(pat["object"], binding)

    # If any concretization returned None (e.g. literal we don't support), skip
    if None in (s, p, o):
        return

    # Only fetch if at least one position is concrete (otherwise it's too broad)
    if all(x.startswith("?") for x in (s, p, o)):
        # fallback to full pattern harvest
        url = tpf_uri_request_builder(control_uri, s, p, o)
        harvest_pattern_into_repo(url, repo)
        return

    url = tpf_uri_request_builder(control_uri, s, p, o)

    before = len(repo)

    print(f"Bind-join fetching: {url}")

    harvest_pattern_into_repo(url, repo)

    after = len(repo)

    if after == before:
        print("⚠️  Bind produced NO triples:", url)


def fetch_binding_batch(batch, pat, control_uri, repo):

    with ThreadPoolExecutor(MAX_THREADS) as pool:

        futures = []

        for binding in batch:
            futures.append(
                pool.submit(fetch_binding, binding, pat, control_uri, repo)
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


def insert_query(query):
    endpoint = "http://acb8computer:7200/repositories/test1/statements"

    try:
        r = requests.post(
            endpoint,
            data=query,
            headers={
                "Content-Type":"application/sparql-update",
                "Accept":"application/json"
            }
        )

        if r.status_code == 200 or r.status_code == 204:
            print("Bulk insert successful")
        else:
            print("Insert failed:", r.status_code, r.text)  # <- show server response

    except Exception as e:
        print("Insert error:", e)


# -------------------------------------------------------------------------
# HARVEST EXECUTION
# -------------------------------------------------------------------------

def harvest_endpoint_optimized(control_uri, bgp, named_graph):

    print("Harvesting:", control_uri)

    local_repo = Graph()
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

        bindings = extract_upstream_bindings(local_repo, idx, harvested, bgp)

        if not bindings or not required_vars:

            print("Full pattern download")

            url = tpf_uri_request_builder(
                control_uri,
                pat["subject"],
                pat["predicate"],
                pat["object"]
            )

            harvest_pattern_into_repo(url, local_repo)

        else:

            print("Bind join:", len(bindings), "bindings")

            for i in range(0, len(bindings), BIND_BATCH_SIZE):
                print(f"Processing binding batch {i} → {i+BIND_BATCH_SIZE}")

                batch = bindings[i:i+BIND_BATCH_SIZE]

                fetch_binding_batch(batch, pat, control_uri, local_repo)

        harvested.add(idx)
        print("Repo size after pattern", idx, ":", len(local_repo))
        print("------------------------------------")

    if len(local_repo) > 0:

        unique = set(local_repo)

        print("Inserting", len(unique), "triples")

        query = build_query(unique, named_graph)
        xEnzyme = URIRef("http://bio2rdf.org/ns/kegg#xEnzyme")
        equation_pred = URIRef("http://bio2rdf.org/ns/kegg#equation")
        reactions_with_enzyme = set(s for s,p,o in local_repo if p == xEnzyme)
        reactions_with_equation = set(s for s,p,o in local_repo if p == equation_pred)
        print(f"Reactions with xEnzyme triples: {len(reactions_with_enzyme)}")
        print(f"Reactions with equation triples: {len(reactions_with_equation)}")
        print(f"Reactions with BOTH (= your result count): {len(reactions_with_enzyme & reactions_with_equation)}")
        missing_equation = reactions_with_enzyme - reactions_with_equation
        missing_enzyme = reactions_with_equation - reactions_with_enzyme

        print("Missing equation triples:", len(missing_equation))
        print("Missing enzyme triples:", len(missing_enzyme))

        print("Example missing equation:", list(missing_equation)[:5])
        print("Example missing enzyme:", list(missing_enzyme)[:5])

        insert_query(query)

    else:

        print("No triples harvested")


# -------------------------------------------------------------------------
# MAIN ENTRY POINT
# -------------------------------------------------------------------------

def FindBGPPriority(query, endpoints, base_named_graph=None):

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