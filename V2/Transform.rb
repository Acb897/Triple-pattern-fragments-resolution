def transform (sparql)
  begin
    parsed = SPARQL.parse(sparql)  # this is a nightmare method, that returns a wide variety of things! LOL!
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
    subject = pattern.subject.to_s
    predicate = pattern.predicate.to_s
    object = pattern.object.to_s
    pat[:subject] = subject
    pat[:predicate] = predicate
    pat[:object] = object
    # Adds which parts of the triple are the variables (i.e. subject, predicate, or object)
    variables = pattern.variables.to_h.values.to_sparql
    spovariables = Array.new
    spovariables.append "subject" if variables.include? subject
    spovariables.append "predicate" if variables.include? predicate
    spovariables.append "object" if variables.include? object
    pat[:variables] = spovariables
    @bgp.append pat
  end
end


