# Example usage of BasicGraphPatternIterator
require_relative 'basic_graph_pattern_iterator'
require_relative 'root_iterator'

# Define a Basic Graph Pattern (BGP) as an array of triple patterns
bgp = [
  { subject: '?person', predicate: 'http://example.org/type', object: 'http://example.org/Person' },
  { subject: '?person', predicate: 'http://example.org/name', object: '?name' }
]

# Placeholder control object (needs to be implemented to actually fetch from a TPF server)
control = Control.new(base_url)

# Use the RootIterator as the source iterator
root_iterator = RootIterator.new

# Create the BasicGraphPatternIterator
iterator = BasicGraphPatternIterator.new(root_iterator, bgp, control)

# Fetch results one by one
while (solution_mapping = iterator.get_next)
  puts "Solution Mapping: #{solution_mapping.inspect}"
end
