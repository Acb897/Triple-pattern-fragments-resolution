require_relative "functions.rb"




# Define a SPARQL query to be used in the example
# query = "
# PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> 
# PREFIX dbpprop: <http://dbpedia.org/property/> 
# PREFIX dc: <http://purl.org/dc/terms/> 
# PREFIX dbpedia: <http://dbpedia.org/resource/Category:> 
# SELECT ?person ?city WHERE  {
#   ?person a dbpedia-owl:Architect . 
#   ?person dbpprop:birthPlace ?city. 
#   ?city dc:subject dbpedia:Capitals_in_Europe. 
# }"

query = "
PREFIX dbpedia: <http://dbpedia.org/resource/Category:> 
SELECT ?person WHERE  {
  ?person a dbpedia-owl:Architect . 
}"



# Define the control URI
control = 'https://fragments.dbpedia.org/2015/en'

# Execute the function to process the query
FindBGPPriority(query, control)
execute_sparql_query(query)