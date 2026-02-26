# Required dependencies
require 'rest-client'
require 'open-uri'
require 'nokogiri'
require "sparql"
require 'net/http'
require 'rdf'
require 'rdf/rdfa'
require 'set'




# Recursively extracts EVERY triple pattern from any SPARQL algebra tree
# (handles BGP, OPTIONAL/LeftJoin, UNION, sub-queries, GRAPH, etc.)
def extract_all_patterns(operator, patterns = [])
  case operator
  when RDF::Query, SPARQL::Algebra::Operator::BGP
    patterns.concat(operator.patterns) if operator.respond_to?(:patterns)
  when SPARQL::Algebra::Operator
    operator.operands.each { |op| extract_all_patterns(op, patterns) } if operator.respond_to?(:operands)
  when Array
    operator.each { |op| extract_all_patterns(op, patterns) }
  end
  patterns
end 

# Transforms a SPARQL query into a structure containing the triple patterns in the form of subject, predicate and object.
# The query is parsed and extracted into a set of subject-predicate-object triple pattern fragments.
# @param sparql [String] The SPARQL query to be transformed.
# @return [Array] An array of hashes representing RDF triples in the form of subject, predicate, and object.
def transform(sparql)
  begin
    parsed = SPARQL.parse(sparql)
  rescue => e
    $stderr.puts "SPARQL parse error: #{e.message}"
    return false
  end

  raw_patterns = extract_all_patterns(parsed)

  bgp = []
  seen = Set.new
  raw_patterns.each do |pat|
    s = pat.subject.to_s
    p = pat.predicate.to_s
    o = pat.object.to_s
    key = [s, p, o]
    next if seen.include?(key)
    seen << key

    bgp << { subject: s, predicate: p, object: o }
  end

  bgp
end

# Builds a SPARQL INSERT DATA query to insert RDF triples.
# @param triples [Array<Hash>] An array of triples to be inserted.
# @param named_graph [String, nil] The URI of the named graph (optional).
# @return [String] A formatted SPARQL query to insert the triples.
def build_query(statements, named_graph = nil)
  graph_open  = named_graph ? "GRAPH <#{named_graph}> {\n" : ""
  graph_close = named_graph ? "\n}" : ""

  triples_clause = statements.map do |stmt|
    "#{stmt.subject.to_ntriples} #{stmt.predicate.to_ntriples} #{stmt.object.to_ntriples} ."
  end.join("\n")

  <<~SPARQL
    INSERT DATA {
      #{graph_open}#{triples_clause}#{graph_close}
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
    puts "Fetching TPF page: #{url}"

    # 1. Load the document (can also use RestClient if you prefer)
    html_content = URI.open(url).read
    doc = Nokogiri::HTML(html_content)

    # 2. Parse RDFa — this is the important change
    graph = RDF::Graph.new
    reader = RDF::RDFa::Reader.new(doc, base_uri: url)

    reader.each_statement do |statement|
      graph << statement
    end

    # Optional: also show how many triples were found
    puts "Extracted #{graph.count} triples from RDFa"

    # 3. Convert to your expected array-of-hashes format
    triples_to_insert = []
    graph.each_statement do |stmt|
      triple = {
        "subject"   => stmt.subject.to_s,
        "predicate" => stmt.predicate.to_s,
        "object"    => stmt.object.to_s   # will be URI or literal string
      }
      triples_to_insert << triple
    end

    # Handle next page (still using your current logic — could also be improved)
    next_link = doc.at_css('link[rel="next"]')&.[](:href) ||
                doc.at_css('a[rel="next"]')&.[](:href)

    if next_link
      @nextpage = URI.join(url, next_link).to_s
      @nextpage.gsub!("&amp;", "&") if @nextpage
    else
      @nextpage = nil
    end

    # 4. Insert what we found
    unless triples_to_insert.empty?
      query = build_query(triples_to_insert, @named_graph_iri)
      puts "Inserting batch of #{triples_to_insert.size} triples"
      # puts query   # uncomment for debugging
      insert_query(query)
    end

    # 5. Recurse to next page
    parse_tpf_response(@nextpage) if @nextpage

  rescue OpenURI::HTTPError => e
    puts "HTTP error fetching #{url}: #{e.message}"
  rescue RDF::FormatError, RDF::ReaderError => e
    puts "RDFa parsing failed for #{url}: #{e.message.inspect}"
  rescue StandardError => e
    puts "Unexpected error in parse_tpf_response(#{url}): #{e.message}"
    puts e.backtrace.first(8)
  end
end

# Constructs a URL to request triple patterns from a TPF service based on subject, predicate, and object.
# @param controlURI [String] The base URL of the TPF service.
# @param subject [String] The subject for the triple pattern (optional).
# @param predicate [String] The predicate for the triple pattern (optional).
# @param object [String] The object for the triple pattern (optional).
# @param mapping [Hash, nil] Optional mapping to modify the parameters.
# @return [String] A URL to query the TPF service.
def tpf_uri_request_builder(controlURI, subject, predicate, object)
  params = {}
  # Only add the parameter if the position is BOUND (not a variable)
  params[:subject]   = URI.encode_www_form_component(subject)   unless subject.to_s.start_with?('?') || subject.to_s.empty?
  params[:predicate] = URI.encode_www_form_component(predicate) unless predicate.to_s.start_with?('?') || predicate.to_s.empty?
  params[:object]    = URI.encode_www_form_component(object)    unless object.to_s.start_with?('?') || object.to_s.empty?

  query_string = params.map { |k, v| "#{k}=#{v}" }.join('&')
  "#{controlURI}?#{query_string}"
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
  endpoint_url = 'http://acb8computer:7200/repositories/test1'
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
def FindBGPPriority(query, control, named_graph_iri = nil)
  @control = control
  @named_graph_iri = named_graph_iri

  bgp = transform(query)
  if bgp == false || bgp.empty?
    puts "No triple patterns found in query (or parse error)."
    return
  end

  puts "Query contains #{bgp.size} unique triple pattern(s). Starting harvest..."

  bgp.each do |pat|
    url = tpf_uri_request_builder(@control, pat[:subject], pat[:predicate], pat[:object])
    puts "  Pattern: #{pat}"
    puts "  URL: #{url}"
    parse_tpf_response(url)
  end

  puts "\n✅ Harvesting finished for the query."
  puts "   You can now run the original SPARQL query against http://localhost:7200/repositories/test1"
end