# require 'sparql'
require 'rest-client'

# # Define the hash containing RDF triples
# triples = [
#   { subject: "http://example.org/subject1", predicate: "http://example.org/predicate1", object: "http://example.org/Object1" },
#   { subject: "http://example.org/subject2", predicate: "http://example.org/predicate2", object: "Object2" }
# ]

# # Named graph (optional)
# NAMED_GRAPH_IRI = "http://example.org/graphs/my-new-graph"

# Generate the SPARQL INSERT DATA query
def build_query(triples, named_graph = nil)
  # puts triples
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

def insert_query(query)
  sparql_endpoint = "http://localhost:7200/repositories/test/statements"
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

# Construct the SPARQL query
# sparql_query = build_insert_query(triples, NAMED_GRAPH_IRI)

# puts "Generated SPARQL Query:"
# puts sparql_query