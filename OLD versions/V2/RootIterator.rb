# The RootIterator class serves as the starting point for query execution.
# It returns a single, empty solution mapping that acts as the initial input
# for subsequent iterators. Once the empty mapping is returned, it returns nil.
#
# @example Usage
#   root_iterator = RootIterator.new
#   iterator = TriplePatternIterator.new(root_iterator, triple_pattern, control)
#
#   while (result = iterator.get_next)
#     puts "Solution mapping: #{result.inspect}"
#   end
class RootIterator
  def initialize
    @has_returned = false # Ensures it only returns one empty mapping
  end

  # Returns the next solution mapping. The first call returns an empty solution mapping,
  # and subsequent calls return nil.
  #
  # @return [Hash, nil] An empty solution mapping on the first call, and nil on subsequent calls
  def get_next
    unless @has_returned
      @has_returned = true
      {} # Return an empty solution mapping
    else
      nil # Return nil after the first empty mapping
    end
  end
end
