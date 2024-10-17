require 'open-uri'
require 'nokogiri'
require 'json'

# Function to parse HTML from a given URL and convert to JSON
def parse_html_to_json(url)
  begin
    # Open the URL and read the HTML content
    html_content = URI.open(url).read

    # Parse the HTML using Nokogiri
    doc = Nokogiri::HTML(html_content)

    # Create a hash to store the parsed data
    result = {}

    # Extract the title of the page
    result[:title] = doc.title

    # Extract all headers (h1, h2, h3, etc.)
    puts result[:headers]
    result[:headers] = {}
    (1..6).each do |i|
      result[:headers]["h#{i}".to_sym] = doc.css("h#{i}").map(&:text)
    end
    result[:span] = doc.css("span").map(&:text)
    # Extract all links from the page
    result[:links] = doc.css('a').map { |link| { text: link.text.strip, href: link['href'] } }

    # Extract paragraphs
    result[:paragraphs] = doc.css('p').map(&:text)

    # Convert the result hash to JSON format
    json_result = JSON.pretty_generate(result)

    File.open("output.json", "w") do |f|
      f.write(json_result)
    end
    
    # Print the JSON result
    puts json_result

    # Return the JSON result
    json_result
  rescue OpenURI::HTTPError => e
    puts "Failed to retrieve the URL: #{e.message}"
  rescue StandardError => e
    puts "An error occurred: #{e.message}"
  end
end

# URL to parse (you can replace this with any valid URL)
url = 'https://fragments.dbpedia.org/2015/en?subject=&predicate=rdf%3Atype&object=http%3A%2F%2Fdbpedia.org%2Fontology%2FArchitect'

# Call the function to parse the HTML and convert it to JSON
parse_html_to_json(url)

