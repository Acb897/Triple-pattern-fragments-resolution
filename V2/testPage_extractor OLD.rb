require 'open-uri'
require 'nokogiri'


# Function to parse HTML from a given URL and convert to JSON
def parse_tpf_response(url, output_file_name, writing_mode)
  begin
    @complete_list_of_solutions = Array.new
    @nextpage = nil
    # Open the URL and read the HTML content
    puts "URL: #{url}"
    html_content = URI.open(url).read
    # Parse the HTML using Nokogiri
    doc = Nokogiri::HTML(html_content)

    # Find the count in the HTML content; adjust the CSS selectors as necessary
    total_items_span = doc.at_css('span[property="void:triples hydra:totalItems"]')

    # Extract the value of the "content" attribute and convert it to an integer
    total_items_content = total_items_span['content'].to_i if total_items_span
    puts total_items_content
    # puts total_items_content                                                                               #counts

    # # Extract all links from the page
    # result[:links] = doc.css('a').map { |link| { text: link.text.strip, href: link['href'] } }
    # puts doc.css('a').map { |link| { text: link.text.strip, href: link['href'] } }
    
    @list_of_solutions_to_write = Array.new
    
    doc.css('a').each { |line| 
      line = line.to_s
      if line.include? "hydra:next"
        @nextpage = line.match(/href="(.*)" rel="next"/)[1]
        @nextpage = @nextpage.gsub("&amp;", "&")
        puts "Next page: #{@nextpage}"
      elsif line.include? 'href="?subject'
        # @solution_mapping = {"subject" => "", "predicate" => "", "object" => ""}
        @solution_mapping = Hash.new
        answsubject = line.match(/href="\?subject.*title="(.*)">/)
        @solution_mapping["subject"] = answsubject[1]
      
      elsif line.include? 'href="?predicate'
        answpredicate = line.match(/href="\?predicate.*title="(.*)">/)
        @solution_mapping["predicate"] = answpredicate[1]

      elsif line.include? 'href="?object'
        answobject = line.match(/href="\?object.*resource="(.*)">/)
        @solution_mapping["object"] = answobject[1]
        @list_of_solutions_to_write << @solution_mapping
      end
      }


      
      # Open a file to write the contents
      File.open(output_file_name, writing_mode) do |file|
        # Iterate over each element in the array
        @list_of_solutions_to_write.each do |solution|
          # Write the element to the file followed by a newline
          file.puts solution
        end
      end
    puts
    if !(@nextpage.nil?)
      parse_tpf_response(@nextpage, output_file_name, "a")
    end


  rescue OpenURI::HTTPError => e
    puts "Failed to retrieve the URL: #{e.message}"
  rescue StandardError => e
    puts "An error occurred: #{e.message}"
  end
end

# # # URL to parse (you can replace this with any valid URL)

url = 'https://fragments.dbpedia.org/2015/en?subject=&predicate=rdf%3Atype&object=http%3A%2F%2Fdbpedia.org%2Fontology%2FArchitect'
# url = "https://fragments.dbpedia.org/2015/en?subject=&predicate=rdf%3Atype&object=http%3A%2F%2Fdbpedia.org%2Fontology%2FArchitect&page=24"
# url = "http://fairdata.systems:5000/?subject=&predicate=rdf%3Atype&object="


# # # Call the function to parse the HTML and convert it to JSON
parse_tpf_response(url, "Harvested triples.txt", "w")
print @complete_list_of_solutions
