# test_integration.py  — run manually, requires network
import pytest
from rdflib import URIRef

DBPEDIA_TPF = "https://fragments.dbpedia.org/2016-04/en"

@pytest.mark.integration
def test_star_path_over_dbpedia(tmp_path):
    """
    Fetch the rdfs:subClassOf closure above dbo:Person.
    Expect at least one transitive ancestor.
    """
    import TPF

    query = """
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX dbo:  <http://dbpedia.org/ontology/>
    SELECT ?ancestor WHERE {
        dbo:Person rdfs:subClassOf* ?ancestor .
    }
    """

    # Use an in-process local GraphDB substitute:
    # override the insert endpoint to a local rdflib graph for this test
    results = TPF.run_query_strict(query, [DBPEDIA_TPF])
    ancestors = [r[2] for r in results]  # object position

    assert len(ancestors) > 0, "Expected at least one ancestor via subClassOf*"
    # dbo:Person should be its own ancestor (zero-hop)
    assert any("Person" in a for a in ancestors)