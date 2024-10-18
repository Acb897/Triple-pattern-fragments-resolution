def transform
  begin
    parsed = SPARQL.parse(@sparql)  # this is a nightmare method, that returns a wide variety of things! LOL!
  rescue => e
    $stderr.puts e.to_s
    return false
  end
  
  select = false
  distinct = false
  vars = ''
  prefixes = Array.new
  rdf_query= ''
  
  if parsed.is_a?(RDF::Query)  # we need to get the RDF:Query object out of the list of things returned from the parse
    rdf_query = parsed
  else
    parsed.each do |c|
      rdf_query = c if c.is_a?(RDF::Query)
      select = true if c.is_a? SPARQL::Algebra::Operator::Project
      distinct = true if c.is_a? SPARQL::Algebra::Operator::Project
      vars += " #{c.to_s}" if c.is_a? RDF::Query::Variable
      next if c.is_a? Array and c.first.is_a? RDF::Query::Variable
      prefixes << c if (c.is_a? Array and !(c.first.is_a? Array))
    end
  end
  
  qs = ""
  prefixes.each {|e| qs += "PREFIX #{e[0].to_s} <#{e[1].to_s}>\n"}
  if select
    qs += "SELECT "
  else
    qs += "SELECT *"
  end
  
  qs += "DISTINCT " if distinct
  qs += vars
  qs += " WHERE { \n"
  
  patterns = rdf_query.patterns  # returns the triple patterns in the query
  @bgp = Array.new
  patterns.each do |pattern|
    pat = Hash.new
    pat[:subject] = pattern.subject.to_s
    pat[:predicate] = pattern.predicate.to_s
    pat[:object] = pattern.object.to_s
    @bgp.append pat
  end
end


