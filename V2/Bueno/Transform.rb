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
  optional_patterns = Array.new
  
  if parsed.is_a?(RDF::Query)  # we need to get the RDF:Query object out of the list of things returned from the parse
    rdf_query = parsed
  else
    parsed.each do |c|
      # if c.is_a?(SPARQL::Algebra::Operator::LeftJoin)
      #   puts c.operands[1].patterns
      # end
      optional_patterns.append c if c.is_a? (SPARQL::Algebra::Operator::LeftJoin)
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

  optional_patterns.each do |optionals_list|
    optionals_list.each do |optional_pattern|
      next unless optional_pattern.is_a? (RDF::Query)
      patterns.append optional_pattern.patterns[0]
    end
  end
  
  bgp = Array.new
  patterns.each do |pattern|
    pat = Hash.new
    subject = pattern.subject.to_s
    predicate = pattern.predicate.to_s
    object = pattern.object.to_s
    pat[:subject] = subject
    pat[:predicate] = predicate
    pat[:object] = object
    # print pat
    # puts
    # Adds which parts of the triple are the variables (i.e. subject, predicate, or object)
    # variables = pattern.variables.to_h.values.to_sparql.split(" ")
    # spovariables = Array.new
    
    # # puts variables
    # # puts puts
    # #FIX this
    # variables.each do |var|
    #   pat.each do |k,v|
    #     variable_hash = Hash.new
    #     if var == v
    #       variable_hash[k] = v 
    #       spovariables.append variable_hash
    #     end
    #   end 
    # end  
    # spovariables.append variable_hash["subject"] = subject if variables.include? subject
    # spovariables.append variable_hash["predicate"] = predicate if variables.include? predicate
    # spovariables.append variable_hash["object"] = object if variables.include? object
    # pat[:variables] = spovariables
    bgp.append pat unless bgp.include? pat
  end
  return bgp
end


