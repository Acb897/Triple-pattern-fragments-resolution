# require 'uri'
# require 'net/http'
# require 'json'
# require "nokogiri"
# require 'open-uri'

# url = "https://fragments.dbpedia.org/2015/en?subject=&predicate=rdf%3Atype&object=http%3A%2F%2Fdbpedia.org%2Fontology%2FArchitect"
# html_content = URI.open(url).read


# # Parse the HTML content using Nokogiri
# doc = Nokogiri::HTML(html_content)

# # Find the <span> element with property="void:triples hydra:totalItems"
# total_items_span = doc.at_css('span[property="void:triples hydra:totalItems"]')

# # Extract the value of the "content" attribute and convert it to an integer
# total_items_content = total_items_span['content'].to_i if total_items_span

# # Output the numerical value only
# puts total_items_content # => 2355


require 'open-uri'
require 'nokogiri'
require_relative "./Compare.rb"
require_relative "./RootIterator.rb"
require_relative "./TriplePatternIterator2.rb"
# Example usage
source_iterator = RootIterator.new
triple_pattern = {:subject=>"?person", :predicate=>"rdf:type", :object=>"http://dbpedia.org/ontology/Architect"}# Define the triple pattern here
control_url = 'https://fragments.dbpedia.org/2015/en' # Ensure this is a valid URL

triple_iterator = TriplePatternIterator.new(source_iterator, triple_pattern, control_url)

# Fetch and print all matching triples
puts "Matching Triples:"
loop do
  result = triple_iterator.get_next
  break if result.nil? # Stop if no more results are available
  puts result
end
