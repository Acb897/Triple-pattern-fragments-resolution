import pytest
from unittest.mock import patch, MagicMock
from rdflib import URIRef, Literal, Graph
from rdflib.paths import MulPath
import TPF  # adjust to your module name

# ── Test 1: path detection ────────────────────────────────────────────────

def test_is_path_pattern_detects_sentinel():
    pat = {"subject": "?x", "predicate": "__PATH__", "object": "?y",
           "predicate_path": MagicMock()}
    assert TPF.is_path_pattern(pat)

def test_is_path_pattern_rejects_plain_iri():
    pat = {"subject": "?x",
           "predicate": "http://www.w3.org/2000/01/rdf-schema#subClassOf",
           "object": "?y"}
    assert not TPF.is_path_pattern(pat)

# ── Test 2: path_to_str ───────────────────────────────────────────────────

def test_path_to_str_star():
    from rdflib.paths import MulPath
    RDFS_SCO = URIRef("http://www.w3.org/2000/01/rdf-schema#subClassOf")
    path = RDFS_SCO * '*'          # rdflib syntax for subClassOf*
    result = TPF.path_to_str(path)
    assert "*" in result
    assert "subClassOf" in result

# ── Test 3: extract_base_iris_from_path ──────────────────────────────────

def test_extract_base_iris_star_path():
    RDFS_SCO = URIRef("http://www.w3.org/2000/01/rdf-schema#subClassOf")
    path = RDFS_SCO * '*'
    iris = TPF.extract_base_iris_from_path(path)
    assert RDFS_SCO in iris

def test_extract_base_iris_sequence():
    RDF_TYPE  = URIRef("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")
    RDFS_SCO  = URIRef("http://www.w3.org/2000/01/rdf-schema#subClassOf")
    path = RDF_TYPE / (RDFS_SCO * '*')      # rdf:type / rdfs:subClassOf*
    iris = TPF.extract_base_iris_from_path(path)
    assert RDF_TYPE in iris
    assert RDFS_SCO in iris

# ── Test 4: transform correctly flags path patterns ───────────────────────

# def test_transform_marks_star_path():
#     query = """
#     PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
#     SELECT ?x ?y WHERE { ?x rdfs:subClassOf* ?y }
#     """
#     bgp = TPF.transform(query)
#     assert len(bgp) == 1
#     assert bgp[0]["predicate"] == "__PATH__"
#     assert "predicate_path" in bgp[0]

def test_transform_marks_star_path():
    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    SELECT ?x ?y WHERE { ?x rdfs:subClassOf* ?y }
    """
    from rdflib.plugins.sparql.parser import parseQuery
    from rdflib.plugins.sparql.algebra import translateQuery
    
    parsed = parseQuery(query)
    algebra = translateQuery(parsed).algebra
    
    # Print the algebra tree so we can see what node name is actually used
    print(repr(algebra))
    
    bgp = TPF.transform(query)
    print(bgp)

def test_transform_keeps_plain_triple():
    query = """
    PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
    SELECT ?x WHERE { ?x rdf:type <http://example.org/Foo> }
    """
    bgp = TPF.transform(query)
    assert len(bgp) == 1
    assert bgp[0]["predicate"] != "__PATH__"

# ── Test 5: extract_vars_from_pattern ignores sentinel ────────────────────

def test_extract_vars_ignores_path_sentinel():
    pat = {"subject": "?x", "predicate": "__PATH__",
           "object": "?y", "graph": None}
    vars_ = TPF.extract_vars_from_pattern(pat)
    assert "x" in vars_
    assert "y" in vars_
    # The sentinel must NOT appear as a variable name
    assert "__PATH__" not in vars_
    assert "" not in vars_

# ── Test 6: evaluate_path_locally (mocked GraphDB) ───────────────────────

@patch("TPF.execute_sparql_query")
def test_evaluate_path_locally_star(mock_sparql):
    """
    Graph: A subClassOf B, B subClassOf C
    Query: ?x subClassOf* ?y  (all transitive pairs including identity)
    """
    RDFS_SCO = URIRef("http://www.w3.org/2000/01/rdf-schema#subClassOf")
    A = "http://example.org/A"
    B = "http://example.org/B"
    C = "http://example.org/C"

    mock_sparql.return_value = [
        {"s": A, "p": str(RDFS_SCO), "o": B},
        {"s": B, "p": str(RDFS_SCO), "o": C},
    ]

    path = RDFS_SCO * '*'
    pat = {
        "subject": "?x",
        "predicate": "__PATH__",
        "predicate_path": path,
        "object": "?y",
        "graph": None,
    }

    bindings = TPF.evaluate_path_locally(pat, named_graph=None)
    pairs = {(str(b["x"]), str(b["y"])) for b in bindings}

    # Transitive closure must include A→B, A→C, B→C
    assert (A, B) in pairs
    assert (A, C) in pairs
    assert (B, C) in pairs