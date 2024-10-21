require_relative "./Transform.rb"
require "SPARQL"

# @sparql = "SELECT ?person ?city WHERE { OPTIONAL {?person a dbpedia-owl:Architect} ?person dbpprop:birthPlace ?city. ?city dc:subject dbpedia:Category:Capitals_in_Europe. } LIMIT 100"
# @sparql = "SELECT ?person ?city WHERE  {?person a dbpedia-owl:Architect . ?person dbpprop:birthPlace ?city. ?city dc:subject dbpedia:Category:Capitals_in_Europe. } LIMIT 100"

@sparql = "
PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> 
PREFIX dbpprop: <http://dbpedia.org/property/> 
PREFIX dc: <http://purl.org/dc/terms/> 
PREFIX dbpedia: <http://dbpedia.org/resource/Category:> 
SELECT ?person ?city WHERE  {
?person a dbpedia-owl:Architect . 
?person dbpprop:birthPlace ?city. 
?city dc:subject dbpedia:Capitals_in_Europe. 
} LIMIT 100"



# @sparql = "PREFIX info:    <http://somewhere/peopleInfo#>
# PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>

# SELECT ?name ?age
# WHERE
# {
#     ?person vcard:FN  ?name .
#     OPTIONAL { ?person info:age ?age }
# }"
transform(@sparql)

print @bgp
