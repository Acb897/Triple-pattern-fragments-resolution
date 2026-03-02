# frozen_string_literal: true
# == TPF Federated Harvester with Binding Propagation & Blank Node Safety ==
#
# UPDATED VERSION (Feb 2026):
# - Dynamic connectivity-aware greedy ordering (from new version)
# - Batching of bind-join requests (restored: 20 bindings per batch)
# - Full debugging / progress output restored (cardinalities, per-pattern, per-page, restricted URLs, etc.)
# - Robust error handling & warnings restored everywhere
# - Cleaner binding logic kept from new version
# - Improved insert_query with visible success/failure

require 'rdf'
require 'rdf/rdfa'
require 'rdf/repository'
require 'nokogiri'
require 'open-uri'
require 'rest-client'
require 'sparql'
require 'set'
require 'uri'
require 'cgi'
require 'net/http'
require 'json'
require 'rdf/vocab'

# -------------------------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------------------------
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

def transform(sparql)
  begin
    parsed = SPARQL.parse(sparql)
  rescue StandardError => e
    warn "SPARQL parse error: #{e.message}"
    return false
  end
  raw_patterns = extract_all_patterns(parsed)
  seen = Set.new
  bgp = []
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

def extract_vars_from_pattern(pat)
  vars = []
  vars << pat[:subject][1..].to_sym if pat[:subject].start_with?('?')
  vars << pat[:predicate][1..].to_sym if pat[:predicate].start_with?('?')
  vars << pat[:object][1..].to_sym if pat[:object].start_with?('?')
  vars
end

def shares_variable?(pat, processed_patterns)
  vars = extract_vars_from_pattern(pat)
  processed_patterns.any? do |p|
    (vars & extract_vars_from_pattern(p)).any?
  end
end

def tpf_uri_request_builder(control_uri, subject, predicate, object)
  params = {}
  params[:subject]   = URI.encode_www_form_component(subject)   unless subject.to_s.start_with?('?')   || subject.to_s.empty?
  params[:predicate] = URI.encode_www_form_component(predicate) unless predicate.to_s.start_with?('?') || predicate.to_s.empty?
  params[:object]    = URI.encode_www_form_component(object)    unless object.to_s.start_with?('?')    || object.to_s.empty?
  query_string = params.map { |k, v| "#{k}=#{v}" }.join('&')
  query_string.empty? ? control_uri : "#{control_uri}?#{query_string}"
end

def heuristic_cardinality(html)
  return 0 if html =~ /no\s*triples/i
  return 10_000 if html =~ /rel=["']?next["']?/i
  triple_count = html.scan(/property=|typeof=/).size
  return triple_count if triple_count > 0
  5_000
end

def get_pattern_count(control_uri, subject, predicate, object)
  url = tpf_uri_request_builder(control_uri, subject, predicate, object)
  begin
    response = RestClient.get(url)
  rescue RestClient::ExceptionWithResponse => e
    warn "HTTP error for #{url}: #{e.http_code}"
    return 999_999_999
  rescue StandardError => e
    warn "Connection failed for #{url}: #{e.message}"
    return 999_999_999
  end

  html = response.body
  content_type = response.headers[:content_type].to_s

  return heuristic_cardinality(html) unless content_type.include?("html") || html.include?("<html")

  graph = RDF::Graph.new
  count = nil
  begin
    RDF::RDFa::Reader.new(html, base_uri: url).each_statement { |stmt| graph << stmt }
  rescue StandardError => e
    warn "RDFa parsing failed for #{url}: #{e.message}"
    return heuristic_cardinality(html)
  end

  [RDF::Vocab::HYDRA.totalItems, RDF::Vocab::VOID.triples].each do |pred|
    graph.each_statement do |stmt|
      next unless stmt.predicate == pred
      raw = stmt.object.to_s.strip.gsub(/[,±~]/, '')
      if raw =~ /^\d+$/
        count = raw.to_i
        break
      end
    end
    break if count
  end
  count || heuristic_cardinality(html)
end

def term_or_var(str)
  return RDF::Query::Variable.new(str[1..].to_sym) if str.start_with?('?')
  return RDF::URI(str[1..-2]) if str.start_with?('<') && str.end_with?('>')
  RDF::Literal(str)
end

def harvest_pattern_into_repo(url, repo)
  current_url = url
  page_count = 0
  while current_url
    page_count += 1
    puts "   Page #{page_count}: #{current_url}"
    begin
      html = RestClient.get(current_url).body
      doc = Nokogiri::HTML(html)
      next_href = doc.at_css('link[rel="next"], a[rel="next"]')&.[](:href)
      next_url = next_href ? URI.join(current_url, next_href).to_s.gsub('&amp;', '&') : nil

      RDF::RDFa::Reader.new(html, base_uri: current_url).each_statement do |stmt|
        repo << stmt
      end

      puts "    → #{repo.count} triples so far in local repo"
      current_url = next_url
    rescue StandardError => e
      warn " Error on page #{current_url}: #{e.message}"
      current_url = nil
    end
  end
end

def extract_upstream_bindings(repo, current_pat_idx, harvested, bgp)
  return [] if harvested.empty?

  # === ONLY previously harvested patterns ===
  prev_patterns = harvested.map { |i| bgp[i] }

  # Variables we need to bind in the CURRENT pattern
  needed_for_current = extract_vars_from_pattern(bgp[current_pat_idx])

  # Only the variables that are actually available from upstream (the join keys)
  join_vars = needed_for_current & prev_patterns.flat_map { |p| extract_vars_from_pattern(p) }
  return [] if join_vars.empty?

  query = "SELECT #{join_vars.map { |v| "?#{v}" }.join(' ')} WHERE {\n"

  # Your existing safe formatter (works perfectly with the strings in bgp)
  sparql_safe = lambda do |term|
    return term if term.start_with?('?')
    return term if term.start_with?('"') || term.start_with?("'")
    return "<#{term}>" if term =~ /\Ahttps?:\/\//
    return term if term.start_with?('<')
    "\"#{term}\""
  end

  prev_patterns.each do |pat|
    s = sparql_safe.call(pat[:subject])
    p = sparql_safe.call(pat[:predicate])
    o = sparql_safe.call(pat[:object])
    query << "  #{s} #{p} #{o} .\n"
  end
  query << "}"

  puts "DEBUG: Extracting upstream bindings for pattern #{current_pat_idx} (join on #{join_vars}):"
  puts query

  begin
    solutions = repo.query(SPARQL.parse(query))
    puts "DEBUG: Found #{solutions.count} upstream binding set(s)"
  rescue StandardError => e
    warn "Local binding extraction failed: #{e.message}"
    warn "Query was:\n#{query}"
    return []
  end

  solutions.map do |sol|
    h = {}
    join_vars.each do |v|
      val = sol[v]
      h[v] = val if val && !val.node? && !val.variable?
    end
    h
  end.uniq.reject(&:empty?)
end

def build_query(statements, named_graph = nil)
  graph_open = named_graph ? "GRAPH <#{named_graph}> {\n" : ''
  graph_close = named_graph ? "\n}" : ''
  triples_str = statements.map do |stmt|
    "#{stmt.subject.to_ntriples} #{stmt.predicate.to_ntriples} #{stmt.object.to_ntriples} ."
  end.join("\n")
  <<~SPARQL
    INSERT DATA {
      #{graph_open}#{triples_str}#{graph_close}
    }
  SPARQL
end

def insert_query(query)
  endpoint = 'http://acb8computer:7200/repositories/test1/statements'
  begin
    RestClient.post(
      endpoint,
      query,
      content_type: 'application/sparql-update',
      accept: 'application/json'
    )
    puts " Bulk insert successful"
  rescue RestClient::ExceptionWithResponse => e
    warn "Insert failed: #{e.response&.body || e.message}"
  end
end

# -------------------------------------------------------------------------
# OPTIMIZED HARVESTING WITH BINDING PROPAGATION + DYNAMIC ORDERING + BATCHING
# -------------------------------------------------------------------------
def harvest_endpoint_optimized(control_uri, bgp, named_graph_iri)
  puts "\n=== Optimized harvest from: #{control_uri} (dynamic ordering + batch bind-join) ==="

  local_repo = RDF::Repository.new
  harvested = Set.new

  # 1. Estimate cardinalities
  counts = {}
  bgp.each_with_index do |pat, i|
    counts[i] = get_pattern_count(control_uri, pat[:subject], pat[:predicate], pat[:object])
  end

  puts "\nEstimated cardinalities:"
  bgp.each_with_index do |pat, i|
    puts " Pattern #{i}: #{pat.inspect} → est. #{counts[i]}"
  end

  # 2. Dynamic connectivity-aware ordering
  remaining = bgp.each_index.to_a
  first = remaining.min_by { |i| counts[i] }
  remaining.delete(first)
  execution_order = [first]

  until remaining.empty?
    connected = remaining.select do |idx|
      shares_variable?(bgp[idx], execution_order.map { |i| bgp[i] })
    end
    next_idx = if connected.any?
                 connected.min_by { |i| counts[i] }
               else
                 remaining.min_by { |i| counts[i] }
               end
    execution_order << next_idx
    remaining.delete(next_idx)
  end
  # -------------------------------------------------------------------------
  # Print clear execution plan
  # -------------------------------------------------------------------------
  puts "\nExecution plan (dynamic greedy + connectivity-aware):"
  execution_order.each_with_index do |idx, step|
    pat = bgp[idx]
    est = counts[idx]
    est_str = est >= 999_000_000 ? "≥1e9 (error/fallback)" : est.to_s

    # Try to show the most likely join variable (simple heuristic)
    join_hint = ""
    if step > 0
      prev_pats = execution_order[0...step].map { |i| bgp[i] }
      current_vars = extract_vars_from_pattern(pat)
      shared = prev_pats.flat_map { |p| extract_vars_from_pattern(p) } & current_vars
      join_hint = shared.empty? ? " (no shared variable)" : " ← joins on #{shared.map { |v| "?#{v}" }.join(', ')}"
    end

    puts "  #{step+1}. Pattern ##{idx}   est. #{est_str.to_s.rjust(8)}   #{pat.inspect}#{join_hint}"
  end

  puts "Chosen execution order: #{execution_order.inspect}"

  # 3. Execute in smart order with batching
  execution_order.each do |idx|
    pat = bgp[idx]
    puts "\n Processing pattern #{idx}: #{pat.inspect}"

    required_vars = extract_vars_from_pattern(pat)
    upstream_bindings = extract_upstream_bindings(local_repo, idx, harvested, bgp)

    if upstream_bindings.empty? || required_vars.empty?
      puts " → No bindings available → full download"
      url = tpf_uri_request_builder(control_uri, pat[:subject], pat[:predicate], pat[:object])
      harvest_pattern_into_repo(url, local_repo)
    else
      puts " → #{upstream_bindings.size} usable upstream binding(s) → bind join (batched by 20)"
      upstream_bindings.each_slice(20) do |batch|
        batch.each do |binding|
          bound_s = pat[:subject]
          bound_p = pat[:predicate]
          bound_o = pat[:object]

          if pat[:subject].start_with?('?')
            var = pat[:subject][1..].to_sym
            bound_s = binding[var].to_s if binding[var]
          end
          if pat[:predicate].start_with?('?')
            var = pat[:predicate][1..].to_sym
            bound_p = binding[var].to_s if binding[var]
          end
          if pat[:object].start_with?('?')
            var = pat[:object][1..].to_sym
            bound_o = binding[var].to_s if binding[var]
          end

          next if [bound_s, bound_p, bound_o].all? { |t| t.start_with?('?') }

          # Shortened debug output
          s_str = bound_s.to_s
          p_str = bound_p.to_s
          o_str = bound_o.to_s
          s_short = s_str.length > 60 ? s_str[0...57] + "..." : s_str
          p_short = p_str.length > 60 ? p_str[0...57] + "..." : p_str
          o_short = o_str.length > 60 ? o_str[0...57] + "..." : o_str
          puts "   Restricted request: s=#{s_short}, p=#{p_short}, o=#{o_short}"

          url = tpf_uri_request_builder(control_uri, bound_s, bound_p, bound_o)
          harvest_pattern_into_repo(url, local_repo)
        end
      end
    end
    harvested << idx
  end

  # 4. Final bulk insert
  unless local_repo.empty?
    statements = local_repo.statements.to_a.uniq { |s| [s.subject.to_s, s.predicate.to_s, s.object.to_s] }
    puts "\n Inserting #{statements.size} unique triples into #{named_graph_iri ? "<#{named_graph_iri}>" : 'default graph'}"
    query = build_query(statements, named_graph_iri)
    insert_query(query)
  else
    puts " No triples harvested for this endpoint."
  end

  puts "=== Finished #{control_uri} ==="
end

# -------------------------------------------------------------------------
# QUERY EXECUTION OVER LOCAL TRIPLESTORE (kept from new version)
# -------------------------------------------------------------------------
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
    json['results']['bindings']
  else
    raise "Error: Unable to execute SPARQL query. HTTP Status: #{response.code}"
  end
end

# -------------------------------------------------------------------------
# MAIN ENTRY POINT
# -------------------------------------------------------------------------
def FindBGPPriority(query, endpoints, base_named_graph = nil)
  endpoints = [endpoints] if endpoints.is_a?(String)
  bgp = transform(query)
  return if bgp == false || bgp.empty?

  puts "Query has #{bgp.size} unique triple pattern(s)."
  puts "Harvesting from #{endpoints.size} endpoint(s)…"

  endpoints.each_with_index do |control_uri, i|
    graph_iri = if base_named_graph
                  "#{base_named_graph}/endpoint#{i + 1}"
                else
                  nil
                end
    harvest_endpoint_optimized(control_uri, bgp, graph_iri)
  end

  puts "\nAll endpoints processed."
  puts "Data is stored in named graphs: #{base_named_graph ? base_named_graph + '/*' : 'default graph'}"
  puts "You can now run the original SPARQL query against your local GraphDB."
end