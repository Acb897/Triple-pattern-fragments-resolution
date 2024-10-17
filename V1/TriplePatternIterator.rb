require 'net/http'
require 'json'

# TriplePatternIterator fetches triples matching a given triple pattern
# from a Triple Pattern Fragment (TPF) server incrementally.
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
        puts mapping
        return nil if mapping.nil?

        # Fetch the first page of triples from the TPF server
        @current_page = fetch_tpf_page(mapping)
        if @current_page['triples'].empty?
          @finished = true
          return nil
        end
      else
        # Fetch subsequent pages if available
        next_page_url = @current_page.dig('controls', 'next')
        if next_page_url
          @current_page = fetch_page(next_page_url)
        else
          @finished = true
          return nil
        end
      end
      @current_triples = @current_page['triples']
    end

    # Return the next triple and mapping
    triple = @current_triples.shift
    { @triple_pattern => triple }
  end

  private

  # Fetches the first page of a TPF for a given mapping.
  #
  # @param mapping [Hash] The current solution mapping.
  # @return [Hash] A JSON-parsed response containing triples and metadata.
  def fetch_tpf_page(mapping)
    puts mapping
    subject = mapping[:subject] ? URI.encode_www_form_component(mapping[:subject]) : ''
    predicate = mapping[:predicate] ? URI.encode_www_form_component(mapping[:predicate]) : ''
    object = mapping[:object] ? URI.encode_www_form_component(mapping[:object]) : ''
    # Construct the TPF query URL
    tpf_query_url = "#{@control}?subject=#{subject}&predicate=#{predicate}&object=#{object}"
    puts tpf_query_url
    # Fetch the page from the TPF server and return the HTML content
    URI.open(tpf_query_url).read
  end

  # Fetches a page from a given URL.
  #
  # @param url [String] The URL to request.
  # @return [Hash] A JSON-parsed response containing triples and metadata.
  def fetch_page(url)
    response = Net::HTTP.get(URI(url))
    JSON.parse(response)
  end
end
