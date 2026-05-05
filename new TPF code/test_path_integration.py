import pytest
from rdflib import URIRef, Graph
from rdflib.paths import MulPath

# Change this import to match your actual module filename
import TPF

GRAPH_IRI = "urn:test:path"
RDFS_SCO  = URIRef("http://www.w3.org/2000/01/rdf-schema#subClassOf")

EX = "http://example.org/"
A  = EX + "A"
B  = EX + "B"
C  = EX + "C"
D  = EX + "D"


def make_star_pat(subj="?x", obj="?y"):
    """Helper: build a __PATH__ pattern dict for subClassOf*"""
    path = RDFS_SCO * "*"
    return {
        "subject":        subj,
        "predicate":      "__PATH__",
        "predicate_path": path,
        "object":         obj,
        "graph":          None,
    }


# ── Test 1: both variables ────────────────────────────────────────────────

def test_both_vars():
    """
    ?x subClassOf* ?y over A→B→C→D.
    Must return all transitive pairs AND reflexive pairs.
    """
    pat      = make_star_pat("?x", "?y")
    bindings = TPF.evaluate_path_locally(pat, named_graph=GRAPH_IRI)
    pairs    = {(str(b["x"]), str(b["y"])) for b in bindings}

    # Direct hops
    assert (A, B) in pairs, "Missing A→B"
    assert (B, C) in pairs, "Missing B→C"
    assert (C, D) in pairs, "Missing C→D"

    # Transitive hops
    assert (A, C) in pairs, "Missing transitive A→C"
    assert (A, D) in pairs, "Missing transitive A→D"
    assert (B, D) in pairs, "Missing transitive B→D"

    # Reflexive (zero hops) — subClassOf* must include identity
    assert (A, A) in pairs, "Missing reflexive A→A"
    assert (B, B) in pairs, "Missing reflexive B→B"
    assert (C, C) in pairs, "Missing reflexive C→C"
    assert (D, D) in pairs, "Missing reflexive D→D"

    print(f"  test_both_vars: {len(pairs)} pairs")


# ── Test 2: bound subject ─────────────────────────────────────────────────

def test_bound_subject():
    """
    ex:A subClassOf* ?y — should return B, C, D and A itself.
    """
    pat      = make_star_pat(subj=A, obj="?y")
    bindings = TPF.evaluate_path_locally(pat, named_graph=GRAPH_IRI)
    objects  = {str(b["y"]) for b in bindings}

    assert B in objects, "Missing A→B"
    assert C in objects, "Missing A→C (transitive)"
    assert D in objects, "Missing A→D (transitive)"
    assert A in objects, "Missing reflexive A→A"

    # D has no outgoing subClassOf so nothing beyond D
    assert len(objects) == 4, f"Expected 4 objects, got {len(objects)}: {objects}"
    print(f"  test_bound_subject: {objects}")


# ── Test 3: bound object ──────────────────────────────────────────────────

def test_bound_object():
    """
    ?x subClassOf* ex:D — should return A, B, C and D itself.
    """
    pat      = make_star_pat(subj="?x", obj=D)
    bindings = TPF.evaluate_path_locally(pat, named_graph=GRAPH_IRI)
    subjects = {str(b["x"]) for b in bindings}

    assert A in subjects, "Missing A→D"
    assert B in subjects, "Missing B→D (transitive)"
    assert C in subjects, "Missing C→D"
    assert D in subjects, "Missing reflexive D→D"

    assert len(subjects) == 4, f"Expected 4 subjects, got {len(subjects)}: {subjects}"
    print(f"  test_bound_object: {subjects}")


# ── Test 4: nonexistent node ──────────────────────────────────────────────

def test_nonexistent_node():
    """
    A node that isn't in the graph at all should return only itself
    (reflexive) and nothing else.
    """
    GHOST    = EX + "GHOST"
    pat      = make_star_pat(subj=GHOST, obj="?y")
    bindings = TPF.evaluate_path_locally(pat, named_graph=GRAPH_IRI)
    objects  = {str(b["y"]) for b in bindings}

    # subClassOf* is reflexive so GHOST→GHOST must hold
    assert GHOST in objects, "Missing reflexive GHOST→GHOST"
    assert len(objects) == 1, f"Expected only GHOST, got: {objects}"
    print(f"  test_nonexistent_node: {objects}")


# ── Test 5: wrong graph returns nothing useful ────────────────────────────

def test_wrong_graph_is_empty():
    """
    Querying against a graph IRI that doesn't exist should produce
    only reflexive results (the local graph will be empty, so only
    zero-hop identity pairs can be generated — and only if the subject
    is concrete).
    """
    pat      = make_star_pat(subj=A, obj="?y")
    bindings = TPF.evaluate_path_locally(pat, named_graph="urn:does:not:exist")
    objects  = {str(b["y"]) for b in bindings}

    # With an empty local graph, subClassOf* still gives A→A (reflexive)
    assert objects == {A}, f"Expected only reflexive A, got: {objects}"
    print(f"  test_wrong_graph_is_empty: {objects}")