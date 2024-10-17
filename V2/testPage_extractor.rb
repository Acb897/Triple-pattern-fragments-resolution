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

    # # Extract the title of the page
    # result[:title] = doc.title

    # # Extract all headers (h1, h2, h3, etc.)
    # puts result[:headers]
    # result[:headers] = {}
    # (1..4).each do |i|
    #   result[:headers]["h#{i}".to_sym] = doc.css("h#{i}").map(&:text)
    # end



    # Find the count in the HTML content; adjust the CSS selectors as necessary
    total_items_span = doc.at_css('span[property="void:triples hydra:totalItems"]')

    # Extract the value of the "content" attribute and convert it to an integer
    total_items_content = total_items_span['content'].to_i if total_items_span

    puts total_items_content                                                                               #counts
    # # Extract all links from the page
    # result[:links] = doc.css('a').map { |link| { text: link.text.strip, href: link['href'] } }
    # puts doc.css('a').map { |link| { text: link.text.strip, href: link['href'] } }
    
    @list_of_solutions = Array.new
    
    doc.css('a').each { |line| 
      line = line.to_s    
      if line.include? "hydra:next"
        @nextpage = line.match(/href="(.*)" rel/)[1]
        puts "Next page: #{@nextpage}"
      elsif line.include? "?subject"
        @solution_mapping = {"subject" => "", "predicate" => "", "object" => ""}
        answsubject = line.match(/title="(.*)"/)
        @solution_mapping["subject"] = answsubject[1]
      
      elsif line.include? "?predicate"
        answpredicate = line.match(/title="(.*)"/)
        @solution_mapping["predicate"] = answpredicate[1]

      elsif line.include? "?object"
        answobject = line.match(/title="(.*)" property/)
        @solution_mapping["object"] = answobject[1]
        @list_of_solutions << @solution_mapping
      end
      }
      puts @nextpage
      print @list_of_solutions
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

