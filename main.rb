require_relative "documented_functions.rb"

# Define a SPARQL query to be used in the example
query = <<~SPARQL
PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
PREFIX dbpprop: <http://dbpedia.org/property/>
PREFIX dc: <http://purl.org/dc/terms/>
PREFIX dbpedia: <http://dbpedia.org/resource/Category:>

SELECT ?person ?city WHERE {
  ?person a dbpedia-owl:Architect .
  ?person dbpprop:birthPlace ?city .
  ?city dc:subject dbpedia:Capitals_in_Europe .
}
SPARQL


# query = <<~SPARQL
# PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
# PREFIX dbpprop: <http://dbpedia.org/property/>
# PREFIX dc: <http://purl.org/dc/terms/>
# PREFIX dbpedia: <http://dbpedia.org/resource/Category:>

# SELECT ?person ?city WHERE {
#   ?city dc:subject dbpedia:Capitals_in_Europe .
# }
# SPARQL


# query = "
# PREFIX dbpedia: <http://dbpedia.org/resource/Category:> 
# PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> 
# SELECT ?person WHERE  {
#   ?person a dbpedia-owl:Architect . 
# }"


@named_graph_iri = "http://example.org/graphs/my-new-graph1"
# Define the tpf URI
tpf_server = 'https://fragments.dbpedia.org/2016-04/en'

# Execute the function to process the query
FindBGPPriority(query, tpf_server)


results = execute_sparql_query(query)

puts results