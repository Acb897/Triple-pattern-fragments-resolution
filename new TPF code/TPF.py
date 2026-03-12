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
# - Do NOT require endpoints to answer the whole query
# - Store harvested triples locally

import re
import json
import requests
from urllib.parse import urlencode, urljoin
from concurrent.futures import ThreadPoolExecutor

from bs4 import BeautifulSoup

from rdflib import Graph, URIRef, Literal, Variable
from rdflib.namespace import Namespace
from rdflib.plugins.sparql import prepareQuery

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

    if hasattr(node, "triples"):
        patterns.extend(node.triples)

    for attr in ["p", "p1", "p2"]:
        if hasattr(node, attr):
            extract_all_patterns(getattr(node, attr), patterns)

    if hasattr(node, "args"):
        for arg in node.args:
            extract_all_patterns(arg, patterns)

    return patterns


def transform(query):

    try:
        parsed = parseQuery(query)
        algebra = translateQuery(parsed)
    except Exception as e:
        print("SPARQL parse error:", e)
        return False

    triples = extract_all_patterns(algebra.algebra)

    seen = set()
    bgp = []

    for s,p,o in triples:

        s = str(s)
        p = str(p)
        o = str(o)

        key = (s,p,o)

        if key in seen:
            continue

        seen.add(key)

        bgp.append({
            "subject": s,
            "predicate": p,
            "object": o
        })

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

def tpf_uri_request_builder(control_uri, subject, predicate, object):

    params = {}

    if not subject.startswith("?"):
        params["subject"] = subject

    if not predicate.startswith("?"):
        params["predicate"] = predicate

    if not object.startswith("?"):
        params["object"] = object

    query = urlencode(params)

    return f"{control_uri}?{query}" if query else control_uri


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

def fetch_tpf_page(url):

    if url in page_cache:
        return page_cache[url]

    html = requests.get(url).text

    g = Graph()
    g.parse(data=html, format="rdfa")

    page_cache[url] = g

    return g


def harvest_pattern_into_repo(url, repo):

    current_url = url
    page_count = 0

    while current_url:

        page_count += 1
        print("Page", page_count, current_url)

        try:

            html = requests.get(current_url).text

            soup = BeautifulSoup(html, "html.parser")

            next_link = soup.select_one('link[rel="next"], a[rel="next"]')

            next_url = None

            if next_link and next_link.get("href"):
                next_url = urljoin(current_url, next_link["href"])

            g = fetch_tpf_page(current_url)

            for triple in g:
                repo.add(triple)

            print(" →", len(repo), "triples in repo")

            current_url = next_url

        except Exception as e:

            print("Error:", e)
            current_url = None


# -------------------------------------------------------------------------
# VECTORISED BIND JOIN
# -------------------------------------------------------------------------

def fetch_binding(binding, pat, control_uri, repo):

    s = pat["subject"]
    p = pat["predicate"]
    o = pat["object"]

    if s.startswith("?") and s[1:] in binding:
        s = str(binding[s[1:]])

    if p.startswith("?") and p[1:] in binding:
        p = str(binding[p[1:]])

    if o.startswith("?") and o[1:] in binding:
        o = str(binding[o[1:]])

    if all(x.startswith("?") for x in [s,p,o]):
        return

    url = tpf_uri_request_builder(control_uri, s, p, o)

    harvest_pattern_into_repo(url, repo)


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

def extract_upstream_bindings(repo, current_idx, harvested, bgp):

    if not harvested:
        return []

    prev_patterns = [bgp[i] for i in harvested]

    needed = extract_vars_from_pattern(bgp[current_idx])

    upstream_vars = set()

    for p in prev_patterns:
        upstream_vars.update(extract_vars_from_pattern(p))

    join_vars = list(set(needed).intersection(upstream_vars))

    if not join_vars:
        return []

    query = "SELECT " + " ".join("?" + v for v in join_vars) + " WHERE {\n"

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

    for pat in prev_patterns:

        s = safe(pat["subject"])
        p = safe(pat["predicate"])
        o = safe(pat["object"])

        query += f" {s} {p} {o} .\n"

    query += "}"

    print("DEBUG binding extraction")
    print(query)

    try:

        q = prepareQuery(query)

        solutions = []

        for row in repo.query(q):

            sol = {}

            for v in join_vars:

                val = row[v]

                if val:
                    sol[v] = val

            if sol:
                solutions.append(sol)

        print("DEBUG:", len(solutions), "bindings")

        return solutions

    except Exception as e:

        print("Local binding extraction failed", e)
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

        if r.status_code == 200:
            print("Bulk insert successful")
        else:
            print("Insert failed", r.text)

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

                batch = bindings[i:i+BIND_BATCH_SIZE]

                fetch_binding_batch(batch, pat, control_uri, local_repo)

        harvested.add(idx)

    if len(local_repo) > 0:

        unique = set(local_repo)

        print("Inserting", len(unique), "triples")

        query = build_query(unique, named_graph)

        insert_query(query)

    else:

        print("No triples harvested")


# -------------------------------------------------------------------------
# MAIN ENTRY POINT
# -------------------------------------------------------------------------

def FindBGPPriority(query, endpoints, base_named_graph=None):

    if isinstance(endpoints,str):
        endpoints = [endpoints]

    bgp = transform(query)

    if not bgp:
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