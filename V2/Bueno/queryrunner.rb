require 'net/http'
require 'uri'
require 'json'

# GraphDB SPARQL endpoint
ENDPOINT_URL = 'http://osboxes:7200/repositories/test'

# SPARQL query (puedes cambiar la consulta a cualquier otra que necesites)
query = <<-SPARQL
PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> 
PREFIX dbpprop: <http://dbpedia.org/property/> 
PREFIX dc: <http://purl.org/dc/terms/> 
PREFIX dbpedia: <http://dbpedia.org/resource/Category:> 
SELECT ?person ?city 
WHERE  {
?person a dbpedia-owl:Architect . 
?person dbpprop:birthPlace ?city. 
?city dc:subject dbpedia:Capitals_in_Europe. 
}
SPARQL

# HTTP headers
headers = {
  'Content-Type' => 'application/x-www-form-urlencoded',
  'Accept' => 'application/sparql-results+json'
}

# Send the request
uri = URI.parse(ENDPOINT_URL)
http = Net::HTTP.new(uri.host, uri.port)
request = Net::HTTP::Post.new(uri.request_uri, headers)
request.body = "query=#{URI.encode_www_form_component(query)}"

response = http.request(request)

# Parse and display results
if response.code.to_i == 200
  results = JSON.parse(response.body)
  puts results
  results['results']['bindings'].each do |binding|
    binding.each do |key, value|
      puts "#{key.capitalize}: #{value['value']}"
    end
    puts "-" * 40  # Separator for readability
  end
else
  puts "Error: #{response.code} - #{response.message}"
  puts response.body
end
require 'net/http'
require 'uri'
require 'json'

# GraphDB SPARQL endpoint
ENDPOINT_URL = 'http://osboxes:7200/repositories/test'

# SPARQL query (puedes cambiar la consulta a cualquier otra que necesites)
query = <<-SPARQL
PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> 
PREFIX dbpprop: <http://dbpedia.org/property/> 
PREFIX dc: <http://purl.org/dc/terms/> 
PREFIX dbpedia: <http://dbpedia.org/resource/Category:> 
SELECT ?person ?city 
WHERE  {
?person a dbpedia-owl:Architect . 
?person dbpprop:birthPlace ?city. 
?city dc:subject dbpedia:Capitals_in_Europe. 
}
SPARQL

# HTTP headers
headers = {
  'Content-Type' => 'application/x-www-form-urlencoded',
  'Accept' => 'application/sparql-results+json'
}

# Send the request
uri = URI.parse(ENDPOINT_URL)
http = Net::HTTP.new(uri.host, uri.port)
request = Net::HTTP::Post.new(uri.request_uri, headers)
request.body = "query=#{URI.encode_www_form_component(query)}"

response = http.request(request)

# Parse and display results
if response.code.to_i == 200
  results = JSON.parse(response.body)
  puts results
  results['results']['bindings'].each do |binding|
    binding.each do |key, value|
      puts "#{key.capitalize}: #{value['value']}"
    end
    puts "-" * 40  # Separator for readability
  end
else
  puts "Error: #{response.code} - #{response.message}"
  puts response.body
end
