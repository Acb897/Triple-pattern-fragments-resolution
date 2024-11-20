require_relative "./Transform.rb"
require "SPARQL"
require "uri"
# @sparql = "SELECT ?person ?city WHERE { OPTIONAL {?person a dbpedia-owl:Architect} ?person dbpprop:birthPlace ?city. ?city dc:subject dbpedia:Category:Capitals_in_Europe. } LIMIT 100"
# @sparql = "SELECT ?person ?city WHERE  {?person a dbpedia-owl:Architect . ?person dbpprop:birthPlace ?city. ?city dc:subject dbpedia:Category:Capitals_in_Europe. } LIMIT 100"

@sparql = "
PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> 
PREFIX dbpprop: <http://dbpedia.org/property/> 
PREFIX dc: <http://purl.org/dc/terms/> 
PREFIX dbpedia: <http://dbpedia.org/resource/Category:> 
SELECT ?person ?city WHERE  {
?person a dbpedia-owl:Architect . 
OPTIONAL {?person dbpprop:birthPlace ?city}. 
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


testing = transform(@sparql)

puts testing


# def decode_url(encoded_url)
#   URI.decode_www_form_component(encoded_url)
# end

# encoded_url = "https://fragments.dbpedia.org/2016-04/en?subject=&amp;predicate=rdf%3Atype&amp;object=http%3A%2F%2Fdbpedia.org%2Fontology%2FArchitect&amp;page=2"
# decoded_url = decode_url(encoded_url)

# puts decoded_url
