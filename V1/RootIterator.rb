# RootIterator generates an empty mapping μ∅ exactly once
class RootIterator
  # Initializes the iterator, setting the `@finished` flag to false.
  def initialize
    @finished = false
  end

  # Returns the next mapping.
  # @return [Hash, NilClass] an empty hash {} on the first call, then `nil`.
  def get_next
    return nil if @finished
    @finished = true
    {} # empty mapping μ∅
  end
end
