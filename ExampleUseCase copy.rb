require 'open-uri'
require 'nokogiri'

# Assuming the previous classes are defined in the same file or required appropriately
# RootIterator, TriplePatternIterator, BasicGraphPatternIterator
require_relative "./Compare.rb"
require_relative "./RootIterator.rb"
require_relative "./TriplePatternIterator2.rb"


# Define the control URL of the TPF server (DBpedia TPF endpoint for the correct dataset)
control_url = 'https://fragments.dbpedia.org/2016-04/en' # Corrected URL

# Initialize the RootIterator (starting point)
root_iterator = RootIterator.new


# Define the Basic Graph Pattern (BGP) representing the SPARQL query
bgp = [
  { subject: '?person', predicate: 'rdf:type', object: 'http://dbpedia.org/ontology/Architect' }, # ?person a dbpedia-owl:Architect
  { subject: '?person', predicate: 'http://dbpedia.org/property/birthPlace', object: '?city' } # ?person dbpprop:birthPlace ?city
]

# Initialize the BasicGraphPatternIterator with the BGP and the control URL
bgp_iterator = BasicGraphPatternIterator.new(root_iterator, bgp, control_url)

# Fetch and print all results (solution mappings)
puts "Results:"

loop do
  result = bgp_iterator.get_next
  break if result.nil? # Stop if no more results are available

  # Print each result (solution mapping)
  puts result
end
