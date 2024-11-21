# Example usage of BasicGraphPatternIterator
# require_relative 'Compare.rb'
# require_relative 'RootIterator.rb'
require_relative "testPage_extractor.rb"
require_relative "./Transform.rb"
require "SPARQL"
require 'net/http'
require 'open-uri'
require 'nokogiri'

def FindBGPPriority (query, control)
  @control = control
  # Extracts the BGP from the query
  bgp = transform(query)
  # # puts bgp
  # @count_hash = Hash.new
  # # Iterates over the triple patterns inside of the Basic Graph Pattern to create TPF server request URIs that match them.
  # bgp.each do |triple_pattern|
  #   # puts triple_pattern
  #   tpf_url = tpf_uri_request_builder(@control, triple_pattern[:subject], triple_pattern[:predicate], triple_pattern[:object])
  #   # puts tpf_url
  #   # Fetch the page from the TPF server and return the HTML content
  #   html_content = URI.open(tpf_url).read
  #   doc = Nokogiri::HTML(html_content)    
  #   # Find the count in the HTML content
  #   total_items_span = doc.at_css('span[property="void:triples hydra:totalItems"]')
  #   # Extract the value of the "content" attribute and convert it to an integer
  #   count = total_items_span['content'].to_i if total_items_span
  #   # puts count
  #   #Create a hash of the triple pattern and its count of answers
  #   @count_hash[triple_pattern] = count
  # end
  # mappings = nil
  # bgp_iterator(@count_hash, mappings)
  bgp.each do |triple_pattern|
    # puts triple_pattern
    subject = triple_pattern[:subject]
    predicate = triple_pattern[:predicate]
    object = triple_pattern[:object]
    current_pattern_url = tpf_uri_request_builder(@control, subject, predicate, object)
    puts current_pattern_url
    parse_tpf_response(current_pattern_url)

    # min_pattern_url = tpf_uri_request_builder(@control, subject, predicate, object) 
    #           puts "Ojo, min pattern url nuevo" 
    #           puts min_pattern_url
    #           parse_tpf_response(min_pattern_url, "Harvested triples.txt", "a")
  end

 
  
  
  
  
# puts mappings_array

  # puts "Min pattern"
  # puts min_pattern
end

def tpf_uri_request_builder (controlURI, subject, predicate, object, mapping = nil)
  # puts "----------BUILDING TPF URI"

  urisubject = subject ? URI.encode_www_form_component(subject) : ''
  uripredicate = predicate ? URI.encode_www_form_component(predicate) : ''
  uriobject = object ? URI.encode_www_form_component(object) : ''
  # Construct the TPF query URL
  tpf_query_url = "#{controlURI}?subject=#{urisubject}&predicate=#{uripredicate}&object=#{uriobject}"
  # puts tpf_query_url
  return tpf_query_url
end

def find_next_TP (count_hash, mapping = nil)
  # puts "-----------FINDING NEXT TP"
  # puts "BGPantes: #{count_hash}"
  # puts "Mappings: #{mapping}"
  if mapping.nil?
    # Select the Triple pattern with the lowest amount of answers
    min_pattern = count_hash.min_by { |pattern, count| count }.first
    # Removes it from the hash
    count_hash.delete(min_pattern)
  else
    # To optimize the algorithm, it will check for other triple patterns that share a variable with the first TP that was used. If there is more than one TP that shares a variable, it will choose the one with fewer answers.
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
    # puts "MATCHING: #{matching}"
    if matching.length > 1
      min_pattern = matching.min_by { |pattern, count| count }.first[0].keys[0]
    else matching.length == 1
      min_pattern = matching[0].keys[0]
      # puts "MIN PATTERN EN EL ELSE: #{min_pattern}"
    end
    # min_pattern = min_pattern.keys.join("")
    count_hash.delete(min_pattern)
    
  end
  # puts "Min pattern: #{min_pattern}"
  # puts "BGPdespues: #{count_hash}"
  # puts "Matched: #{@matched}"
  return [min_pattern, count_hash, @matched]
end

def harvest_tpf (min_pattern, mapping = nil, matched = nil)
    # puts "--------HARVESTING"
    # puts "Min pattern: #{min_pattern}"
    # puts "Mappings: #{mapping}"
    # puts "Matched harvest = #{matched}"
  if mapping.nil?
    min_pattern_url = tpf_uri_request_builder(@control, min_pattern[:subject], min_pattern[:predicate], min_pattern[:object])
    # Harvests the triples for the min triple pattern, and stores the bound variables in an array of hashes that follow this structure: {variable (e.g. ?city) => bound variable (e.g. http://dbpedia.org/resource/Amsterdam)}
    parse_tpf_response(min_pattern_url, "Harvested triples.txt", "w")
    
  else
    puts "Harvesting with mappings"
    subject = min_pattern[:subject]
    predicate = min_pattern[:predicate]
    object = min_pattern[:object]
    puts "min pattern: #{min_pattern}"
    min_pattern[:variables].each do |array|
      # puts "Min pattern: #{subject}; #{predicate}; #{object}"
      # puts "ARRAY"
      puts array
      array.each do|minK, minV|
        mapping.each do |maparray|
          maparray.each do|mapK, mapV|
            subject = mapV if mapK == minV && minK == :subject
            predicate = mapV if mapK == minV && minK == :predicate
            object = mapV if mapK == minV && minK == :object
            if mapK == minV 
              min_pattern_url = tpf_uri_request_builder(@control, subject, predicate, object) 
              puts "Ojo, min pattern url nuevo" 
              puts min_pattern_url
              parse_tpf_response(min_pattern_url, "Harvested triples.txt", "a")
            end
        end
      end
    end
  end
end
  
end

def bgp_iterator(bgp, mapping = nil)
  # puts "--------- ITERATING"
  # puts "BGP = #{bgp}"
  # puts "Mapping = #{mapping}"

  # Fetch and parse the TPF page as HTML
  min_pattern, bgp, matched_vars = find_next_TP(bgp, mapping)
  @previous_min_pattern = min_pattern
  puts "Min pattern: #{min_pattern}"
  puts "BGP: #{bgp}"
  # puts "-------- Harvesting"
  # puts "Min pattern: #{min_pattern}"
  # puts "Mapping: #{mapping}"
  harvest_tpf(min_pattern, mapping, matched_vars)


  puts @complete_list_of_solutions
  puts puts

  # mappings_array = Array.new
  # # puts "ESTA ES LA SIGUIENTE PATTERN: #{min_pattern}"
  # # puts "Clase de la pattern: #{min_pattern[:subject]}"   #Ojo, aqui esta poniendo la case de variables como Nil, asi que tengo que comprobar como devuelve min_pattern el find_next_tp
  # puts puts
  # @complete_list_of_solutions.each do |solution|
  #   min_pattern[:variables].each do |variable|
  #     mappings = Hash.new
  #     variable_value = variable.keys.join("")
  #     # puts "Variable value: #{variable.values}"
  #     # puts "Solution: #{solution}"
  #     mappings[variable.values.join("")] = solution[variable_value]
  #     mappings_array.append mappings
  #   end
  # end
  # puts "MAPPING: #{mappings_array}"
  mappings_array = nil
  bgp_iterator(bgp, mappings_array) unless bgp.empty?
end

# Define a SPARQL query
query = "
PREFIX dbpedia-owl: <http://dbpedia.org/ontology/> 
PREFIX dbpprop: <http://dbpedia.org/property/> 
PREFIX dc: <http://purl.org/dc/terms/> 
PREFIX dbpedia: <http://dbpedia.org/resource/Category:> 
SELECT ?person ?city WHERE  {
?person a dbpedia-owl:Architect . 
?person dbpprop:birthPlace ?city. 
?city dc:subject dbpedia:Capitals_in_Europe. 
}"

# Control URI
control = 'https://fragments.dbpedia.org/2015/en'
FindBGPPriority(query, control)
# TODO: Make this a loop that does the comprobations automatically.