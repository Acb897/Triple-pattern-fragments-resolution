# BasicGraphPatternIterator evaluates a Basic Graph Pattern (BGP) incrementally
# by delegating each triple pattern to a TriplePatternIterator.
class BasicGraphPatternIterator
  # Initializes the iterator.
  #
  # @param source_iterator [Object] The iterator providing initial mappings.
  # @param bgp [Array] An array of triple patterns representing the BGP.
  # @param control [String] The control URL for the TPF server.
  def initialize(source_iterator, bgp, control)
    @source_iterator = source_iterator
    @bgp = bgp
    @control = control
    @current_iterator = nil
  end

  # Fetches the next set of solution mappings for the BGP.
  #
  # @return [Hash, NilClass] A solution mapping, or nil if no more solutions are available.
  def get_next
    loop do
      if @current_iterator.nil?
        mapping = @source_iterator.get_next
        return nil if mapping.nil?

        # Estimate triple counts for all triple patterns in the BGP
        triple_counts = @bgp.map do |triple_pattern|
          page = fetch_tpf_page(triple_pattern, mapping)
          [triple_pattern, extract_count(page)]
        end.to_h

        # Select the triple pattern with the smallest estimated count
        min_pattern = triple_counts.min_by { |_pattern, count| count }.first

        # Create a new TriplePatternIterator for the selected triple pattern
        triple_iter = TriplePatternIterator.new(RootIterator.new, min_pattern, @control)
        @current_iterator = BasicGraphPatternIterator.new(triple_iter, @bgp - [min_pattern], @control)
      end

      result = @current_iterator.get_next
      if result.nil?
        @current_iterator = nil
      else
        return result
      end
    end
  end

  private

  # Fetches the first page of a TPF for a given triple pattern and mapping.
  #
  # @param triple_pattern [String] The triple pattern to be matched.
  # @param mapping [Hash] The current solution mapping.
  # @return [Hash] A JSON-parsed response containing triples and metadata.
  def fetch_tpf_page(triple_pattern, mapping)
    url = "#{@control}/#{mapping[triple_pattern]}"
    fetch_page(url)
  end

  # Fetches a page from a given URL.
  #
  # @param url [String] The URL to request.
  # @return [Hash] A JSON-parsed response containing triples and metadata.
  def fetch_page(url)
    response = Net::HTTP.get(URI(url))
    JSON.parse(response)
  end

  # Extracts the triple count from the page metadata.
  #
  # @param page [Hash] A JSON-parsed page from the TPF server.
  # @return [Integer] The estimated number of triples matching the pattern.
  def extract_count(page)
    page['metadata']['count']
  end
end
