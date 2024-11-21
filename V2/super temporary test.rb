require 'open-uri'
require 'nokogiri'
require_relative "./Query_builder.rb"
# require_relative "./GraphDBInteraction.rb"
require 'cgi'

# Function to parse HTML from a given URL and convert to JSON
def parse_tpf_response(url)
  begin
    puts "URL: #{url}"
    @complete_list_of_solutions ||= []  # Initialize if not already done
    @nextpage = nil

    # Open the URL and read the HTML content
    # puts "URL: #{url}"
    html_content = URI.open(url).read

    # Parse the HTML using Nokogiri
    doc = Nokogiri::HTML(html_content)

    # Find the count in the HTML content; adjust the CSS selectors as necessary
    total_items_span = doc.at_css('span[property="void:triples hydra:totalItems"]')

    # Extract the value of the "content" attribute and convert it to an integer
    total_items_content = total_items_span['content'].to_i if total_items_span
    # puts total_items_content  # Outputs total number of items

    # Array to hold solutions for the current page
    @list_of_solutions_to_write = []

    # Iterate over all 'a' tags to find relevant data
    doc.css('a').each do |line| 
      line = line.to_s
      if line.include? "hydra:next"
        @nextpage = line.match(/href="(.*)" rel="next"/)[1]
        @nextpage = @nextpage.gsub("&amp;", "&")
        # puts "Next page: #{@nextpage}"
      elsif line.include? 'href="?subject'
        # Create a new solution mapping for subject, predicate, and object
        @solution_mapping = {}
        answsubject = line.match(/href="\?subject.*title="(.*)">/)
        @solution_mapping["subject"] = answsubject[1] if answsubject

      elsif line.include? 'href="?predicate'
        answpredicate = line.match(/href="\?predicate.*title="(.*)">/)
        @solution_mapping["predicate"] = answpredicate[1] if answpredicate

      elsif line.include? 'href="?object'
        # puts line
        answobject = line.match(/href="\?object=(.*?)" resource="/)
        # puts answobject[1]
        answobject = CGI.unescape(answobject[1])

        
        if answobject 
            print answobject
            puts answobject.include? "\\"
            puts CGI.unescape(answobject).inspect
            @solution_mapping["object"] = answobject
        end

        # Add the solution to the list after 'object' is found
        puts @solution_mapping
        puts puts puts
        @list_of_solutions_to_write << @solution_mapping
      end
    end

    # Append the solutions from the current page to the complete list
    # @complete_list_of_solutions.concat(@list_of_solutions_to_write)

    # # Open a file to append the contents
    # File.open(output_file_name, writing_mode) do |file|
    #   @list_of_solutions_to_write.each do |solution|
    #     file.puts solution
    #   end
    # end
    # puts "Successfully written to file"
    # puts @complete_list_of_solutions
    # print @list_of_solutions_to_write
    # print @list_of_solutions_to_write
    # puts put
end
end

# # URL to parse
url = 'https://fragments.dbpedia.org/2015/en?subject=%3Fperson&predicate=http%3A%2F%2Fdbpedia.org%2Fproperty%2FbirthPlace&object=%3Fcity'

# # Call the function to parse the HTML and convert it to JSON
parse_tpf_response(url)

# # Output the complete list of solutions
# puts @complete_list_of_solutions
