require 'open-uri'
require 'json'
require 'nokogiri'

class TriplePatternIterator
  # Initializes the iterator.
  #
  # @param source_iterator [Object] The iterator providing initial mappings.
  # @param triple_pattern [String] The triple pattern to be matched.
  # @param control [String] The control URL for the TPF server.
  def initialize(source_iterator, triple_pattern, control)
    @source_iterator = source_iterator
    @triple_pattern = triple_pattern
    @control = control
    @current_page = nil
    @current_triples = []
    @finished = false
  end

  # Fetches the next matching triple.
  #
  # @return [Hash, NilClass] A mapping of the triple pattern to a matching triple, or nil if no more triples are available.
  def get_next
    while @current_triples.empty? && !@finished
      if @current_page.nil?
        mapping = @source_iterator.get_next
        return nil if mapping.nil?

        # Construct the TPF query URL based on the triple pattern and the current mapping
        tpf_query_url = construct_tpf_url(mapping)

        # Fetch the first page of triples for the current mapping
        @current_page = fetch_tpf_page(tpf_query_url)
        @current_triples = extract_triples(@current_page)
      else
        # Handle pagination to fetch the next page
        next_page_url = @current_page.dig('controls', 'next')
        if next_page_url
          @current_page = fetch_page(next_page_url)
          @current_triples = extract_triples(@current_page)
        else
          @finished = true # No more pages to fetch
        end
      end
    end

    # Return the next triple and mapping
    triple = @current_triples.shift
    return { @triple_pattern => triple }
  end

  private

  # Constructs the TPF query URL based on the mapping and the triple pattern.
  #
  # @param mapping [Hash] The current solution mapping.
  # @return [String] The constructed TPF query URL.
  def construct_tpf_url(mapping)
    puts mapping
    subject = mapping[:subject] ? URI.encode_www_form_component(mapping[:subject]) : ''
    predicate = URI.encode_www_form_component(@triple_pattern.split.first) # Use the first part as the predicate
    object = URI.encode_www_form_component(mapping[:object]) ? mapping[:object] : ''

    "#{@control}?subject=#{subject}&predicate=#{predicate}&object=#{object}"
  end

  # Fetches a TPF page for a given query URL.
  #
  # @param tpf_query_url [String] The TPF query URL to fetch.
  # @return [Hash] The parsed JSON response containing triples and metadata.
  def fetch_tpf_page(tpf_query_url)
    response = URI.open(tpf_query_url).read
    JSON.parse(response) # Assuming the TPF server returns JSON
  end

  # Fetches a page from a given URL.
  #
  # @param url [String] The URL to request.
  # @return [Hash] A parsed JSON response containing triples and metadata.
  def fetch_page(url)
    response = URI.open(url).read
    JSON.parse(response) # Assuming the pagination also returns JSON
  end

  # Extracts triples from the page content.
  #
  # @param page [Hash] The JSON page to extract triples from.
  # @return [Array] An array of triples extracted from the page.
  def extract_triples(page)
    page['triples'] || [] # Adjust based on actual structure of the JSON response
  end
end
