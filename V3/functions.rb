# Required dependencies
require 'rest-client'
require 'open-uri'
require 'nokogiri'
require "sparql"
require 'net/http'
# require 'open-uri'
# require 'nokogiri'
# require_relative "./Query_builder.rb"
# require_relative "testPage_extractor.rb"
# require_relative "./Transform.rb"

# Transforms a SPARQL query into a structure containing RDF triples and associated metadata
# 
# @param sparql [String] the SPARQL query string to parse
# @return [Array<Hash>] an array of hash objects representing the parsed triple patterns and metadata
#                        or false in case of parsing failure.
def transform(sparql)
  begin
    # Parse the SPARQL query. This method can return various types of objects.
    parsed = SPARQL.parse(sparql)
  rescue => e
    # Log the error and return false in case of parsing failure
    $stderr.puts e.to_s
    return false
  end

  # Initialization of variables
  select = false
  distinct = false
  vars = ''
  prefixes = []
  rdf_query = nil
  optional_patterns = []

  # Handle the parsed object, which could be an RDF::Query or a collection of elements
  if parsed.is_a?(RDF::Query)
    rdf_query = parsed
  else
    parsed.each do |c|
      optional_patterns << c if c.is_a?(SPARQL::Algebra::Operator::LeftJoin) # Capture OPTIONAL patterns
      rdf_query = c if c.is_a?(RDF::Query)                                  # Capture the main RDF::Query object
      select ||= c.is_a?(SPARQL::Algebra::Operator::Project)                # Check if it's a SELECT query
      distinct ||= c.is_a?(SPARQL::Algebra::Operator::Project)              # Check if DISTINCT is used
      vars += " #{c}" if c.is_a?(RDF::Query::Variable)                      # Collect variable names
      next if c.is_a?(Array) && c.first.is_a?(RDF::Query::Variable)
      prefixes << c if c.is_a?(Array) && !c.first.is_a?(Array)              # Collect PREFIX definitions
    end
  end

  # Construct the SPARQL query string
  query_string = ""
  prefixes.each { |prefix| query_string += "PREFIX #{prefix[0]} <#{prefix[1]}>\n" }
  query_string += "SELECT "
  query_string += "DISTINCT " if distinct
  query_string += vars.empty? ? "*" : vars
  query_string += " WHERE {\n"

  # Get the triple patterns from the main query object
  patterns = rdf_query&.patterns || []

  # Append patterns from optional clauses
  optional_patterns.each do |optional_list|
    optional_list.each do |optional_pattern|
      next unless optional_pattern.is_a?(RDF::Query)
      patterns.concat(optional_pattern.patterns)
    end
  end

  # Process the triple patterns into a structured format
  bgp = [] # Basic Graph Patterns
  patterns.each do |pattern|
    triple = {
      subject: pattern.subject.to_s,
      predicate: pattern.predicate.to_s,
      object: pattern.object.to_s
    }
    bgp << triple unless bgp.include?(triple) # Avoid duplicate patterns
  end

  # Return the basic graph patterns
  bgp
end

# Generates a SPARQL INSERT DATA query to insert RDF triples
#
# @param triples [Array<Hash>] the RDF triples to insert, each containing a :subject, :predicate, and :object
# @param named_graph [String, nil] optional, the IRI of the named graph to insert into
# @return [String] the generated SPARQL query string
def build_query(triples, named_graph = nil)
  # Generate the GRAPH clause if a named graph is provided
  graph_clause = named_graph ? "GRAPH <#{named_graph}> {" : ""

  # Generate the triples clause by formatting each triple as a SPARQL statement
  triples_clause = triples.map do |triple|
    subject = "<#{triple["subject"]}>"
    predicate = "<#{triple["predicate"]}>"
    object = triple["object"].start_with?("http://") ? "<#{triple["object"]}>" : "\"#{triple["object"]}\""
    "    #{subject} #{predicate} #{object} ."
  end.join("\n")

  # Return the full SPARQL query as a string using the Heredoc syntax
  <<~SPARQL
    INSERT DATA {
      #{graph_clause}
      #{triples_clause}
      #{graph_clause.empty? ? '' : '}'}
    }
  SPARQL
end

# Sends the SPARQL query to the endpoint to insert RDF triples
#
# @param query [String] the SPARQL query to execute
def insert_query(query)
  sparql_endpoint = "http://localhost:7200/repositories/test/statements"
  
  begin
    # Send the SPARQL UPDATE query to the server using REST client
    response = RestClient.post(
      sparql_endpoint,
      query,
      { content_type: 'application/sparql-update', accept: 'application/json' }
    )
    # Output success message if the query was executed successfully
    puts "Named graph created successfully with response: #{response.code}"
  rescue RestClient::ExceptionWithResponse => e
    # Output the error message if the query fails
    puts "Failed to create named graph: #{e.response}"
  end
end

# Function to parse TPF (Triple Pattern Fragments) response from a given URL and insert into a named graph
#
# @param url [String] the URL of the TPF endpoint to parse
# @return [void]
def parse_tpf_response(url)
  begin
    puts "URL: #{url}"

    # Initialize instance variables if not already done
    nextpage = nil

    # Open the URL and read the HTML content
    html_content = URI.open(url).read

    # Parse the HTML using Nokogiri
    doc = Nokogiri::HTML(html_content)

    # Array to hold solutions for the current page
    list_of_solutions_to_write = []

    # Iterate over all 'a' tags to find relevant data
    doc.css('a').each do |line|
      line = line.to_s

      # Check if the line contains the 'next' page link
      if line.include?("hydra:next")
        nextpage = line.match(/href="(.*)" rel="next"/)[1]
        nextpage = nextpage.gsub("&amp;", "&")

      # Parse subject, predicate, and object from the 'href' attributes
      elsif line.include?('href="?subject')
        @solution_mapping = {}
        answsubject = line.match(/href="\?subject.*title="(.*)">/)
        @solution_mapping["subject"] = answsubject[1] if answsubject

      elsif line.include?('href="?predicate')
        answpredicate = line.match(/href="\?predicate.*title="(.*)">/)
        @solution_mapping["predicate"] = answpredicate[1] if answpredicate

      elsif line.include?('href="?object')
        answobject = line.match(/href="\?object=(.*?)" resource="/)
        answobject = CGI.unescape(answobject[1]) if answobject
        @solution_mapping["object"] = answobject.gsub('"', "'") if answobject

        # Add the solution mapping to the list after finding all components (subject, predicate, object)
        list_of_solutions_to_write << @solution_mapping
      end
    end

    # Construct SPARQL query to insert the parsed data
    puts @named_graph_iri
    query = build_query(list_of_solutions_to_write, @named_graph_iri)
    puts query
    insert_query(query)

    # If there is a next page, recursively call the function to parse the next page
    parse_tpf_response(nextpage) unless nextpage.nil?

  rescue OpenURI::HTTPError => e
    puts "Failed to retrieve the URL: #{e.message}"
  rescue StandardError => e
    puts "An error occurred: #{e.message}"
  end
end

# Function to extract and process Basic Graph Patterns (BGP) from a query
#
# @param query [String] the SPARQL query containing the Basic Graph Pattern (BGP)
# @param control [String] the base URL for the TPF server
# @return [void]
def FindBGPPriority(query, control)
  # Extracts the BGP from the query using the transform method
  bgp = transform(query)
  
  # Iterate over each triple pattern in the BGP
  bgp.each do |triple_pattern|
    subject = triple_pattern[:subject]
    predicate = triple_pattern[:predicate]
    object = triple_pattern[:object]

    # Build the TPF URI request for the current triple pattern
    current_pattern_url = tpf_uri_request_builder(control, subject, predicate, object)
    puts current_pattern_url

    # Parse the TPF response for the current triple pattern
    parse_tpf_response(current_pattern_url)
  end
end

# Function to build the TPF request URL
#
# @param controlURI [String] the base URL for the TPF server
# @param subject [String] the subject in the triple pattern
# @param predicate [String] the predicate in the triple pattern
# @param object [String] the object in the triple pattern
# @param mapping [Hash, optional] a mapping to update the triple pattern values
# @return [String] the constructed TPF query URL
def tpf_uri_request_builder(controlURI, subject, predicate, object, mapping = nil)
  urisubject = subject ? URI.encode_www_form_component(subject) : ''
  uripredicate = predicate ? URI.encode_www_form_component(predicate) : ''
  uriobject = object ? URI.encode_www_form_component(object) : ''
  
  # Construct the TPF query URL
  tpf_query_url = "#{controlURI}?subject=#{urisubject}&predicate=#{uripredicate}&object=#{uriobject}"
  return tpf_query_url
end

# Function to find the next triple pattern with the lowest count from a hash of triple patterns
#
# @param count_hash [Hash] a hash of triple patterns and their associated counts
# @param mapping [Hash, optional] a mapping to adjust how the patterns are selected
# @return [Array] the selected minimum pattern, updated count hash, and matched variables
def find_next_TP(count_hash, mapping = nil)
  if mapping.nil?
    # Select the triple pattern with the lowest count
    min_pattern = count_hash.min_by { |pattern, count| count }.first
    count_hash.delete(min_pattern)
  else
    matching = []
    min_pattern_vars = @previous_min_pattern[:variables].map { |hash| hash.values.join("") }
    
    count_hash.each do |tp, count|
      tp[:variables].each do |hash|
        if min_pattern_vars.include? hash.values.join("")
          matching.append({tp => count})
        end
      end
    end

    # If multiple patterns match, select the one with the least count
    if matching.length > 1
      min_pattern = matching.min_by { |pattern, count| count }.first[0].keys[0]
    else
      min_pattern = matching[0].keys[0]
    end
    count_hash.delete(min_pattern)
  end

  return [min_pattern, count_hash, matching]
end

# Function to harvest TPF responses for the minimum pattern and its mappings
#
# @param min_pattern [Hash] the triple pattern with the lowest count
# @param mapping [Hash, optional] a mapping to adjust values in the pattern
# @param matched [Array, optional] the matched variables for the pattern
# @return [void]
def harvest_tpf(min_pattern, mapping = nil, matched = nil)
  if mapping.nil?
    min_pattern_url = tpf_uri_request_builder(@control, min_pattern[:subject], min_pattern[:predicate], min_pattern[:object])
    parse_tpf_response(min_pattern_url, "Harvested triples.txt", "w")
  else
    # Process the mappings for harvesting triples
    min_pattern[:variables].each do |array|
      array.each do |minK, minV|
        mapping.each do |maparray|
          maparray.each do |mapK, mapV|
            # Update subject, predicate, and object based on mappings
            subject = mapV if mapK == minV && minK == :subject
            predicate = mapV if mapK == minV && minK == :predicate
            object = mapV if mapK == minV && minK == :object

            # Build the TPF URI and parse the response
            if mapK == minV
              min_pattern_url = tpf_uri_request_builder(@control, subject, predicate, object)
              parse_tpf_response(min_pattern_url, "Harvested triples.txt", "a")
            end
          end
        end
      end
    end
  end
end

# Function to iterate over the BGP and harvest the triples
#
# @param bgp [Array] the list of triple patterns
# @param mapping [Hash, optional] a mapping for variable substitution
# @return [void]
def bgp_iterator(bgp, mapping = nil)
  min_pattern, bgp, matched_vars = find_next_TP(bgp, mapping)
  @previous_min_pattern = min_pattern

  # Harvest triples for the minimum pattern
  harvest_tpf(min_pattern, mapping, matched_vars)

  # Recursively continue iterating over the BGP if there are more patterns
  bgp_iterator(bgp, nil) unless bgp.empty?
end

# Function to execute a SPARQL query and return the results
#
# @param query [String] the SPARQL query to be executed
# @return [void] outputs the query results to the console
#
# Example usage:
# query = "SELECT ?person ?city WHERE { ?person a dbpedia-owl:Architect . }"
# execute_sparql_query(query)
def execute_sparql_query(query)
  # Define the SPARQL endpoint URL
  # This is the endpoint where the SPARQL query will be sent
  endpoint_url = 'http://osboxes:7200/repositories/test'

  # Set up HTTP headers for the request
  # 'Content-Type' specifies the format of the request body
  # 'Accept' specifies that we want the response in JSON format
  headers = {
    'Content-Type' => 'application/x-www-form-urlencoded',
    'Accept' => 'application/sparql-results+json'
  }

  # Create a URI object from the endpoint URL
  uri = URI.parse(endpoint_url)

  # Create an HTTP client to send the request
  http = Net::HTTP.new(uri.host, uri.port)

  # Prepare a POST request with the SPARQL query as the body
  request = Net::HTTP::Post.new(uri.request_uri, headers)
  request.body = "query=#{URI.encode_www_form_component(query)}"

  # Send the HTTP request and capture the response
  response = http.request(request)

  # Check if the response was successful (HTTP status code 200)
  if response.code.to_i == 200
    results = JSON.parse(response.body) 
    results['results']['bindings'].each do |binding| 
      binding.each do |key, value| 
        puts "#{key.capitalize}: #{value['value']}" 
      end 
      puts "-" * 40 # Separator for readability 
    end 
  else 
    puts "Error: #{response.code} - #{response.message}" 
    puts response.body 
  end    
end



