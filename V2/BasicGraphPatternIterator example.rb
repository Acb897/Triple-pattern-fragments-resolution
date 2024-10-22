# Example usage of BasicGraphPatternIterator
# require_relative 'Compare.rb'
# require_relative 'RootIterator.rb'
require_relative "testPage_extractor.rb"
require_relative "./Transform.rb"
require "SPARQL"
require 'net/http'
require 'open-uri'
require 'nokogiri'

def FindBGPPriority (query, control)
  # Extracts the BGP from the query
  transform(query)
  count_hash = Hash.new

  # Fetch and parse the TPF page as HTML
  # Iterates over the triple patterns inside of the Basic Graph Pattern to create TPF server request URIs that match them.
  @bgp.each do |triple_pattern| 
    tpf_url = tpf_uri_request_builder(control, triple_pattern[:subject], triple_pattern[:predicate], triple_pattern[:object])
    # Fetch the page from the TPF server and return the HTML content
    html_content = URI.open(tpf_url).read
    doc = Nokogiri::HTML(html_content)    
    # Find the count in the HTML content
    total_items_span = doc.at_css('span[property="void:triples hydra:totalItems"]')
    # Extract the value of the "content" attribute and convert it to an integer
    count = total_items_span['content'].to_i if total_items_span
    #Create a hash of the triple pattern and its count of answers
    count_hash[triple_pattern] = count
  end

  # Select the Triple pattern with the lowest amount of answers
  min_pattern = count_hash.min_by { |pattern, count| count }.first


  # Removes it from the hash
  count_hash.delete(min_pattern)

  puts "Min pattern"
  puts min_pattern
  puts puts puts

  min_pattern_url = tpf_uri_request_builder(control, min_pattern[:subject], min_pattern[:predicate], min_pattern[:object])

  # Harvests the triples for the min triple pattern, and stores the bound variables in an array of hashes that follow this structure: {variable (e.g. ?city) => bound variable (e.g. http://dbpedia.org/resource/Amsterdam)}
  parse_html_to_json(min_pattern_url)
  
  mappings_array = Array.new

  @list_of_solutions.each do |solution|
    min_pattern[:variables].each do |variable|
      mappings = Hash.new
      variable_value = variable.keys.join("")
      mappings[variable_value.to_sym] = solution[variable_value]
      mappings_array.append mappings
    end
  end
  
# puts mappings_array

  # To optimize the algorithm, it will check for other triple patterns that share a variable with the first TP that was used. If there is more than one TP that shares a variable, it will choose the one with fewer answers.
  matching = Array.new
  min_pattern_vars = Array.new

  min_pattern[:variables].each do |hash|
    min_pattern_vars.append hash.values.join("")
  end
puts min_pattern_vars
  count_hash.each do |tp, count|
    tp[:variables].each do |hash|
      if min_pattern_vars.include? hash.values.join("")
        matching.append ({tp => count})
      end
    end

  end

  # puts matching
  if matching.length > 1
    min_pattern = matching.min_by { |pattern, count| count }.first
  else matching.length == 1
    min_pattern = matching[0]
  end
  min_pattern = min_pattern.keys.join("")
  puts "Min pattern"
  puts min_pattern
end

def tpf_uri_request_builder (controlURI, subject, predicate, object)
  urisubject = subject ? URI.encode_www_form_component(subject) : ''
  uripredicate = predicate ? URI.encode_www_form_component(predicate) : ''
  uriobject = object ? URI.encode_www_form_component(object) : ''
  # Construct the TPF query URL
  tpf_query_url = "#{controlURI}?subject=#{urisubject}&predicate=#{uripredicate}&object=#{uriobject}"
  return tpf_query_url
end







# Define a SPARQL query
query = "
PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> 
PREFIX dbpprop: <http://dbpedia.org/property/> 
PREFIX dc: <http://purl.org/dc/terms/> 
PREFIX dbpedia: <http://dbpedia.org/resource/Category:> 
SELECT ?person ?city WHERE  {
?person a dbpedia-owl:Architect . 
?person dbpprop:birthPlace ?city. 
?city dc:subject dbpedia:Capitals_in_Europe. 
} LIMIT 100"

# Control URI
control = 'https://fragments.dbpedia.org/2015/en'
FindBGPPriority(query, control)
# TODO: Make this a loop that does the comprobations automatically.