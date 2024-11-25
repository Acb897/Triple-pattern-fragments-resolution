require 'uri'
require 'net/http'
require 'nokogiri'
require 'json'

# Control class to handle the URL construction for Triple Pattern Fragment (TPF) queries.
class Control
  def initialize(base_url, triple_pattern)
    @base_url = base_url
  end

  # Constructs the URL for a given triple pattern
  #
  # @param [Hash] triple_pattern A hash representing the triple pattern (subject, predicate, object)
  # @return [String] The constructed URL for the TPF request
  def construct_url(triple_pattern)
    s = triple_pattern[:subject] || ''
    p = triple_pattern[:predicate] || ''
    o = triple_pattern[:object] || ''
    
    # Construct the URL with proper URL encoding for subject, predicate, and object
    "#{@base_url}?subject=#{URI.encode_www_form_component(s)}&predicate=#{URI.encode_www_form_component(p)}&object=#{URI.encode_www_form_component(o)}"
  end
end



# Page class to handle fetching triples and pagination from a Triple Pattern Fragment (TPF) server.
class Page
  attr_reader :triples, :next_page_control

  # Initializes the Page object with triples and control for the next page
  #
  # @param [String] url The URL from which to fetch the page
  def initialize(url)
    # Fetch triples and the next page control using the given TPF server URL
    @triples, @next_page_control = fetch_triples_and_next_control(url)
    @current_index = 0
  end

  # Returns true if there are unread triples on the current page
  #
  # @return [Boolean] True if there are more triples to read
  def has_unread_triples?
    @current_index < @triples.length
  end

  # Returns the next triple from the page
  #
  # @return [Array, nil] The next triple, or nil if there are no more triples
  def next_triple
    return nil unless has_unread_triples?
    triple = @triples[@current_index]
    @current_index += 1
    triple
  end

  private

  # Fetches triples and next page control from the TPF server
  #
  # @param [String] url The URL of the TPF page
  # @return [Array<Array>, String] An array of triples and the next page URL (or nil if no next page)
  def fetch_triples_and_next_control(url)
    uri = URI(url)
    response = Net::HTTP.get(uri)
    json = JSON.parse(response)

    # Extract triples from the JSON-LD response
    triples = extract_triples(json)

    # Find the URL for the next page from the Hydra controls (if available)
    next_page = json.dig('hydra:next', '@id')

    [triples, next_page]
  end

  # Extracts triples from the JSON-LD response
  #
  # @param [Hash] json The parsed JSON-LD response from the TPF server
  # @return [Array<Array>] An array of triples
  def extract_triples(json)
    triples = []
    if json['@graph']
      json['@graph'].each do |triple|
        subject = triple['@id']
        triple.each do |predicate, objects|
          next if predicate == '@id' # Skip the subject itself
          objects.each do |object|
            triples << [subject, predicate, object['@value'] || object['@id']]
          end if objects.is_a?(Array)
        end
      end
    end
    triples
  end
end



# The TriplePatternIterator class is responsible for iterating over triple pattern
# matches from a Triple Pattern Fragment (TPF) collection. It retrieves
# pages of triples that match a given triple pattern and returns solution mappings incrementally.
class TriplePatternIterator
  def initialize(source_iterator, triple_pattern, control)
    @source_iterator = source_iterator
    @triple_pattern = triple_pattern
    @control = control
    @page = nil
    @current_mapping = nil
  end

  # Retrieves the next solution mapping that matches the triple pattern.
  #
  # This method fetches pages from the TPF interface and processes triples incrementally.
  # It will return solution mappings one by one.
  #
  # @return [Hash, nil] The next solution mapping, or nil if no more mappings are available
  def get_next
    @page ||= fetch_page(nil) # Initialize the page if not already done

    loop do
      if @page && @page.has_unread_triples?
        # Read the next triple from the current page
        triple = @page.next_triple
        return combine_with_source_mapping(triple) if triple
      end

      # Fetch the next page if available, or read the next mapping from the source iterator
      if @page&.next_page_control
        @page = fetch_page(@page.next_page_control)
      else
        @current_mapping = @source_iterator.get_next
        return nil unless @current_mapping
        @page = fetch_page(@control.construct_url(@triple_pattern))
      end
    end
  end

  private

  # Fetches a new page of triples from the TPF server
  #
  # @param [String] url The URL to fetch the page from
  # @return [Page] A new page object
  def fetch_page(url)
    Page.new(url || @control.construct_url(@triple_pattern))
  end

  # Combines the current source mapping with a triple to produce a solution mapping.
  #
  # @param [Array] triple A triple from the TPF page
  # @return [Hash] A solution mapping based on the triple and the current source mapping
  def combine_with_source_mapping(triple)
    solution_mapping = {
      'subject' => triple[0],
      'predicate' => triple[1],
      'object' => triple[2]
    }
    solution_mapping.merge!(@current_mapping) if @current_mapping
    solution_mapping
    puts solution_mapping
  end
end
