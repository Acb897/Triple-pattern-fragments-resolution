# The BasicGraphPatternIterator class handles query execution for Basic Graph
# Patterns (BGPs) by recursively iterating over Triple Pattern Iterators and
# combining solution mappings. It processes BGPs and resolves variable bindings
# incrementally across multiple triple patterns.
#
# @example Usage
#   source_iterator = RootIterator.new
#   bgp = [
#     { subject: '?person', predicate: 'http://example.org/type', object: 'http://example.org/Person' },
#     { subject: '?person', predicate: 'http://example.org/name', object: '?name' }
#   ]
#   control = some_control_object
#   iterator = BasicGraphPatternIterator.new(source_iterator, bgp, control)
#
#   while (result = iterator.get_next)
#     puts "Solution mapping: #{result.inspect}"
#   end
class BasicGraphPatternIterator
  # @param [Object] source_iterator The source iterator, typically another iterator
  # @param [Array<Hash>] bgp An array of triple patterns that form the Basic Graph Pattern (BGP)
  # @param [Object] control An object responsible for handling the TPF requests and page fetching
  def initialize(source_iterator, bgp, control)
    @source_iterator = source_iterator
    @bgp = bgp
    @control = control
    @current_iterator = nil
    @current_mapping = nil
  end

  # Retrieves the next solution mapping that satisfies the BGP.
  #
  # This method resolves the triple patterns one by one and returns the combined
  # solution mappings for the BGP incrementally.
  #
  # @return [Hash, nil] The next solution mapping, or nil if no more mappings are available
  def get_next
    result = nil
    loop do
      while @current_iterator.nil?
        @current_mapping = @source_iterator.get_next
        return nil unless @current_mapping

        best_tp = select_best_triple_pattern(@bgp)
        next_iterator = TriplePatternIterator.new(RootIterator.new, best_tp, @control)
        remaining_bgp = @bgp.reject { |tp| tp == best_tp }
        @current_iterator = BasicGraphPatternIterator.new(next_iterator, remaining_bgp, @control)
      end

      result = @current_iterator.get_next
      @current_iterator = nil if result.nil?
      return result.merge(@current_mapping) if result
    end
  end

  private

  # Selects the best triple pattern from the BGP to resolve next.
  #
  # This method selects the triple pattern with the smallest estimated result set.
  #
  # @param [Array<Hash>] bgp The BGP to select from
  # @return [Hash] The selected triple pattern
  def select_best_triple_pattern(bgp)
    bgp.min_by { |tp| estimated_matches(tp) }
  end

  # Estimates the number of matches for a given triple pattern.
  #
  # @param [Hash] triple_pattern The triple pattern to estimate matches for
  # @return [Integer] An estimated number of matching triples
  def estimated_matches(triple_pattern)
    100 # Placeholder for actual logic to estimate matches
  end
end
