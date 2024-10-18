require_relative "./Transform.rb"
require "SPARQL"

# @sparql = "SELECT ?person ?city WHERE { OPTIONAL {?person a dbpedia-owl:Architect} ?person dbpprop:birthPlace ?city. ?city dc:subject dbpedia:Category:Capitals_in_Europe. } LIMIT 100"
@sparql = "SELECT ?person ?city WHERE  {?person a dbpedia-owl:Architect . ?person dbpprop:birthPlace ?city. ?city dc:subject dbpedia:Category:Capitals_in_Europe. } LIMIT 100"



# @sparql = "PREFIX info:    <http://somewhere/peopleInfo#>
# PREFIX vcard:   <http://www.w3.org/2001/vcard-rdf/3.0#>

# SELECT ?name ?age
# WHERE
# {
#     ?person vcard:FN  ?name .
#     OPTIONAL { ?person info:age ?age }
# }"
transform

