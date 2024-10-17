# Example usage of TriplePatternIterator
require_relative 'TriplePatternIterator.rb'
require_relative 'RootIterator.rb'

# Define the base URL for the TPF server (e.g., DBpedia)
base_url = 'http://fragments.dbpedia.org/2016-04/en'

# Create the Control object with the base URL
control = Control.new(base_url)

# Define a triple pattern (can be any RDF property and subject)
triple_pattern = {
  subject: '?person',
  predicate: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type',
  object: 'http://dbpedia.org/ontology/Architect' # The object is a variable, meaning we want to know what Einstein is known for
}

# Use the RootIterator as the source iterator (assuming you've implemented RootIterator)
root_iterator = RootIterator.new

# Create the TriplePatternIterator
iterator = TriplePatternIterator.new(root_iterator, triple_pattern, control)

# Fetch results one by one
