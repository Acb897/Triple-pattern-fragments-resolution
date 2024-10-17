require 'uri'
require 'net/http'
require 'json'

# Import necessary classes (assuming they are defined in the same file or required separately)
# RootIterator, TriplePatternIterator, BasicGraphPatternIterator
require_relative "./Compare.rb"
require_relative "./RootIterator.rb"
require_relative "./TriplePatternIterator.rb"
# Define the control URL of the TPF server (DBpedia TPF endpoint for the correct dataset)
control_url = 'https://fragments.dbpedia.org/2015/en'

# Initialize the RootIterator (starting point)
root_iterator = RootIterator.new

# Define the Basic Graph Pattern (BGP) representing the SPARQL query
# In this case, we want to find people who are architects and their birthplaces
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

# Fetching method to handle TPF queries
def fetch_tpf_page(control_url, mapping)
  subject = mapping[:subject] ? URI.encode_www_form_component(mapping[:subject]) : ''
  predicate = mapping[:predicate] ? URI.encode_www_form_component(mapping[:predicate]) : ''
  object = mapping[:object] ? URI.encode_www_form_component(mapping[:object]) : ''
  
  # Construct the TPF query URL
  tpf_query_url = "#{control_url}?subject=#{subject}&predicate=#{predicate}&object=#{object}"
  
  # Fetch the page from the TPF server
  response = Net::HTTP.get(URI(tpf_query_url))
  
  # Parse and return the response JSON
  JSON.parse(response)
end
