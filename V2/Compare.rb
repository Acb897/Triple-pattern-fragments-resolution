# Takes the BGP, and the control. Fist, it splits the BGP into individual TPFs, and asks the server to reply with the amount of matches for each of the TPFs. Then, it selects the one with the lowest amount of matches, binds the variable and runs the others with it. 
require_relative "./Transform.rb"

class BasicGraphPatternIterator
  # Initializes the iterator.
  #
  # @param source_iterator [Object] The iterator providing initial mappings.
  # @param bgp [Array] An array of triple patterns representing the BGP.
  # @param control [String] The control URL for the TPF server.
  def initialize(source_iterator, query, control)
    @source_iterator = source_iterator
    @sparql = query
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
        puts @sparql
        puts @control
        transform
        # Fetch and parse the TPF page as HTML
        @bgp.each do |tp| 
          page = fetch_tpf_page(triple_pattern, mapping)
        end
        
        
      
        count = parse_html_for_count(page) # Ensure this returns an Integer
        @count_hash = [triple_pattern, count] # Create a pair for the hash
      end
      print count_hash
        # # Convert the array of pairs to a hash
        # triple_counts = Hash[triple_counts]
        # # Select the triple pattern with the smallest estimated count
        # min_pattern = triple_counts.min_by { |_pattern, count| count }.first
        # # Create a new TriplePatternIterator for the selected pattern
        # triple_iter = TriplePatternIterator.new(RootIterator.new, min_pattern, @control)
        # @current_iterator = BasicGraphPatternIterator.new(triple_iter, @bgp - [min_pattern], @control)
      # end

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
  # @return [String] The HTML content from the TPF server.
  def fetch_tpf_page(triple_pattern, mapping)
    subject = triple_pattern[:subject] ? URI.encode_www_form_component(triple_pattern[:subject]) : ''
    predicate = triple_pattern[:predicate] ? URI.encode_www_form_component(triple_pattern[:predicate]) : ''
    object = triple_pattern[:object] ? URI.encode_www_form_component(triple_pattern[:object]) : ''
    # Construct the TPF query URL
    tpf_query_url = "#{@control}?subject=#{subject}&predicate=#{predicate}&object=#{object}"

    # Fetch the page from the TPF server and return the HTML content
    URI.open(tpf_query_url).read
  end

  # Parses the HTML content to extract the estimated count of triples.
  #
  # @param html_content [String] The HTML content to parse.
  # @return [Integer] The estimated count of triples, or 0 if not found.
  def parse_html_for_count(html_content)
    doc = Nokogiri::HTML(html_content)    
    # Find the count in the HTML content; adjust the CSS selectors as necessary
    total_items_span = doc.at_css('span[property="void:triples hydra:totalItems"]')

    # Extract the value of the "content" attribute and convert it to an integer
    total_items_content = total_items_span['content'].to_i if total_items_span
    # return total_items_content ? total_items_content['content'].to_i : 0
  end
end
