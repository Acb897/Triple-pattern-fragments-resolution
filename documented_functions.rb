# Required dependencies
require 'rest-client'
require 'open-uri'
require 'nokogiri'
require "sparql"
require 'net/http'

# Transforms a SPARQL query into a structure containing the triple patterns in the form of subject, predicate and object.
# The query is parsed and extracted into a set of subject-predicate-object triple pattern fragments.
# @param sparql [String] The SPARQL query to be transformed.
# @return [Array] An array of hashes representing RDF triples in the form of subject, predicate, and object.
def transform(sparql)
  begin
    parsed = SPARQL.parse(sparql)
  rescue => e
    $stderr.puts e.to_s
    return false
  end
  
  select = false
  distinct = false
  vars = ''
  prefixes = Array.new
  rdf_query = ''
  optional_patterns = Array.new
  
  # Process the parsed query.
  if parsed.is_a?(RDF::Query)
    rdf_query = parsed
  else
    parsed.each do |c|
      optional_patterns.append c if c.is_a? (SPARQL::Algebra::Operator::LeftJoin)
      rdf_query = c if c.is_a?(RDF::Query)
      select = true if c.is_a? SPARQL::Algebra::Operator::Project
      distinct = true if c.is_a? SPARQL::Algebra::Operator::Project
      vars += " #{c.to_s}" if c.is_a? RDF::Query::Variable
      next if c.is_a? Array and c.first.is_a? RDF::Query::Variable
      prefixes << c if (c.is_a? Array and !(c.first.is_a? Array))
    end
  end
  
  # Construct the SPARQL query with prefixes, select, and distinct clauses.
  qs = ""
  prefixes.each {|e| qs += "PREFIX #{e[0].to_s} <#{e[1].to_s}>\n"}
  qs += "SELECT " if select
  qs += "DISTINCT " if distinct
  qs += vars
  qs += " WHERE { \n"
  
  patterns = rdf_query.patterns

  optional_patterns.each do |optionals_list|
    optionals_list.each do |optional_pattern|
      next unless optional_pattern.is_a? (RDF::Query)
      patterns.append optional_pattern.patterns[0]
    end
  end
  
  # Process the RDF patterns to create the final BGP (Basic Graph Pattern).
  bgp = Array.new
  patterns.each do |pattern|
    pat = Hash.new
    subject = pattern.subject.to_s
    predicate = pattern.predicate.to_s
    object = pattern.object.to_s
    pat[:subject] = subject
    pat[:predicate] = predicate
    pat[:object] = object
    bgp.append pat unless bgp.include? pat
  end
  return bgp
end

# Builds a SPARQL INSERT DATA query to insert RDF triples.
# @param triples [Array<Hash>] An array of triples to be inserted.
# @param named_graph [String, nil] The URI of the named graph (optional).
# @return [String] A formatted SPARQL query to insert the triples.
def build_query(triples, named_graph = nil)
  graph_clause = named_graph ? "GRAPH <#{named_graph}> {" : ""
  triples_clause = triples.map do |triple|
    subject = "<#{triple["subject"]}>"
    predicate = "<#{triple["predicate"]}>"
    object = triple["object"].start_with?("http://") ? "<#{triple["object"]}>" : "\"#{triple["object"]}\""
    "    #{subject} #{predicate} #{object} ."
  end.join("\n")

  <<~SPARQL
    INSERT DATA {
      #{graph_clause}
      #{triples_clause}
      #{graph_clause.empty? ? '' : '}'}
    }
  SPARQL
end

# Sends a SPARQL query to a specified endpoint to insert data into a GraphDB repository.
# @param query [String] The SPARQL query to be executed.
def insert_query(query)
  sparql_endpoint = "http://localhost:7200/repositories/test1/statements"
  begin
    response = RestClient.post(
      sparql_endpoint,
      query,
      { content_type: 'application/sparql-update', accept: 'application/json' }
    )
    puts "Named graph created successfully with response: #{response.code}"
  rescue RestClient::ExceptionWithResponse => e
    puts "Failed to create named graph: #{e.response}"
  end
end

# Parses a response from a Triple Pattern Fragment (TPF) service and extracts relevant data.
# @param url [String] The URL to the TPF service.
def parse_tpf_response(url)
  begin
    puts "URL: #{url}"
    @complete_list_of_solutions ||= []  
    @nextpage = nil

    html_content = URI.open(url).read
    doc = Nokogiri::HTML(html_content)

    total_items_span = doc.at_css('span[property="void:triples hydra:totalItems"]')
    total_items_content = total_items_span['content'].to_i if total_items_span

    @list_of_solutions_to_write = []

    doc.css('a').each do |line| 
      line = line.to_s
      if line.include? "hydra:next"
        @nextpage = line.match(/href="(.*)" rel="next"/)[1]
        @nextpage = @nextpage.gsub("&amp;", "&")
      elsif line.include? 'href="?subject'
        @solution_mapping = {}
        answsubject = line.match(/href="\?subject.*title="(.*)">/)
        @solution_mapping["subject"] = answsubject[1] if answsubject
      elsif line.include? 'href="?predicate'
        answpredicate = line.match(/href="\?predicate.*title="(.*)">/)
        @solution_mapping["predicate"] = answpredicate[1] if answpredicate
      elsif line.include? 'href="?object'
        answobject = line.match(/href="\?object=(.*?)" resource="/)
        answobject = CGI.unescape(answobject[1])
        @solution_mapping["object"] = answobject.gsub('"', "'") if answobject

        @list_of_solutions_to_write << @solution_mapping
      end
    end

    puts @named_graph_iri
    query = build_query(@list_of_solutions_to_write, @named_graph_iri)
    puts query
    insert_query(query)

    parse_tpf_response(@nextpage) unless @nextpage.nil?

  rescue OpenURI::HTTPError => e
    puts "Failed to retrieve the URL: #{e.message}"
  rescue StandardError => e
    puts "An error occurred: #{e.message}"
  end
end

# Constructs a URL to request triple patterns from a TPF service based on subject, predicate, and object.
# @param controlURI [String] The base URL of the TPF service.
# @param subject [String] The subject for the triple pattern (optional).
# @param predicate [String] The predicate for the triple pattern (optional).
# @param object [String] The object for the triple pattern (optional).
# @param mapping [Hash, nil] Optional mapping to modify the parameters.
# @return [String] A URL to query the TPF service.
def tpf_uri_request_builder(controlURI, subject, predicate, object, mapping = nil)
  urisubject = subject ? URI.encode_www_form_component(subject) : ''
  uripredicate = predicate ? URI.encode_www_form_component(predicate) : ''
  uriobject = object ? URI.encode_www_form_component(object) : ''
  tpf_query_url = "#{controlURI}?subject=#{urisubject}&predicate=#{uripredicate}&object=#{uriobject}"
  return tpf_query_url
end

# Determines the next Triple Pattern to request based on the least populated pattern in the query.
# @param count_hash [Hash] A hash of triple patterns with their respective counts.
# @param mapping [Hash, nil] An optional mapping for more complex variable matching.
# @return [Array] The selected pattern, updated hash, and matched variables.
def find_next_TP(count_hash, mapping = nil)
  if mapping.nil?
    min_pattern = count_hash.min_by { |pattern, count| count }.first
    count_hash.delete(min_pattern)
  else
    matching = Array.new
    min_pattern_vars = Array.new

    @previous_min_pattern[:variables].each do |hash|
      min_pattern_vars.append hash.values.join("")
    end
    count_hash.each do |tp, count|
      tp[:variables].each do |hash|
        @matched = []
        if min_pattern_vars.include? hash.values.join("")
          @matched.append hash.values.join("")
          matching.append ({tp => count})
        end
      end
    end

    if matching.length > 1
      min_pattern = matching.min_by { |pattern, count| count }.first[0].keys[0]
    else 
      min_pattern = matching[0].keys[0]
    end
    count_hash.delete(min_pattern)
  end
  return [min_pattern, count_hash, @matched]
end

# Initiates the process of harvesting triples from a TPF service by sending requests based on the least populated pattern.
# @param min_pattern [Hash] The minimum pattern to start harvesting from.
# @param mapping [Hash, nil] Optional mapping for more complex variable matching.
# @param matched [Array, nil] Previously matched variables.
def harvest_tpf(min_pattern, mapping = nil, matched = nil)
  if mapping.nil?
    min_pattern_url = tpf_uri_request_builder(@control, min_pattern[:subject], min_pattern[:predicate], min_pattern[:object])
    parse_tpf_response(min_pattern_url)
  else
    subject = min_pattern[:subject]
    predicate = min_pattern[:predicate]
    object = min_pattern[:object]
    min_pattern[:variables].each do |array|
      array.each do |minK, minV|
        mapping.each do |maparray|
          maparray.each do |mapK, mapV|
            subject = mapV if mapK == minV && minK == :subject
            predicate = mapV if mapK == minV && minK == :predicate
            object = mapV if mapK == minV && minK == :object
            if mapK == minV
              min_pattern_url = tpf_uri_request_builder(@control, subject, predicate, object)
              parse_tpf_response(min_pattern_url)
            end
          end
        end
      end
    end
  end
end

# Iterates through the Basic Graph Pattern (BGP) and processes each triple pattern.
# @param bgp [Array] An array of RDF triple patterns.
# @param mapping [Hash, nil] Optional mapping for more complex variable matching.
def bgp_iterator(bgp, mapping = nil)
  min_pattern, bgp, matched_vars = find_next_TP(bgp, mapping)
  @previous_min_pattern = min_pattern
  harvest_tpf(min_pattern, mapping, matched_vars)
  bgp_iterator(bgp, nil) unless bgp.empty?
end

# Executes a SPARQL query on a specified endpoint and returns the results.
# @param query [String] The SPARQL query to be executed.
# @return [Array] The results of the query.
def execute_sparql_query(query)
  endpoint_url = 'http://osboxes:7200/repositories/test1'
  headers = {
    'Content-Type' => 'application/x-www-form-urlencoded',
    'Accept' => 'application/sparql-results+json'
  }

  uri = URI.parse(endpoint_url)
  http = Net::HTTP.new(uri.host, uri.port)
  request = Net::HTTP::Post.new(uri.request_uri, headers)
  request.body = "query=#{URI.encode_www_form_component(query)}"
  response = http.request(request)

  if response.code.to_i == 200
    json = JSON.parse(response.body)
    results = json['results']['bindings']
    return results
  else
    raise "Error: Unable to execute SPARQL query. HTTP Status: #{response.code}"
  end
end

# Main method to initiate the process of analyzing and harvesting triple patterns from a SPARQL query.
# @param query [String] The SPARQL query to be executed.
# @param control [String] The control URI for the TPF service.
def FindBGPPriority(query, control)
  @control = control
  # Extracts the BGP from the query
  bgp = transform(query)

  bgp.each do |triple_pattern|
    subject = triple_pattern[:subject]
    predicate = triple_pattern[:predicate]
    object = triple_pattern[:object]
    current_pattern_url = tpf_uri_request_builder(@control, subject, predicate, object)
    puts current_pattern_url
    parse_tpf_response(current_pattern_url)
  end
end
