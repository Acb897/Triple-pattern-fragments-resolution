# Triple pattern fragments resolultion - Project Overview

This project consists of a set of functions for transforming a SPARQL query into Triple Pattern Fragments (TPF) server requests. The functions are designed to transform a SPARQL query into TPF request URLs, interact with a TPF service to fetch data, insert the retrieved triples into a specified GraphDB named graph, execute the query in that named graph's SPARQL endpoint and returning the results.

The key operations include:

- Extracting Basic Graph Patterns (BGP) from a SPARQL query.
- Harvesting triples from a TPF service.
- Building and executing SPARQL queries to insert or retrieve data.

## Functions

#### transform(sparql)

Transforms a SPARQL query into a structure containing RDF triples (subject, predicate, object). This is useful for extracting the BGP from a query and processing the resulting triples.

_Parameters_:

> - **sparql**: A string representing a SPARQL query.

_Returns_:

> An array of hashes, each representing a triple.

#### build_query(triples, named_graph = nil)

Builds a SPARQL INSERT DATA query to insert RDF triples into a graph.

_Parameters_:

> - **triples**: An array of triples to be inserted.
> - **named_graph**: (Optional) URI of the named graph where the data will be inserted.

_Returns_:

> A string representing the SPARQL INSERT DATA query.

#### insert_query(query)

Sends the SPARQL query to the specified endpoint to insert the triples into the RDF store.

_Parameters_:

> - **query**: The SPARQL query to be executed.

#### parse_tpf_response(url)

Parses the response from a Triple Pattern Fragment (TPF) service and extracts relevant data to insert as triples into a graph.

_Parameters_:

> - **url**: The URL to the TPF service.

#### tpf_uri_request_builder(controlURI, subject, predicate, object, mapping = nil)

Constructs a URL to request triple patterns from a TPF service.

_Parameters_:

> - **controlURI**: The base URL of the TPF service.
> - **subject**: The subject of the triple pattern.
> - **predicate**: The predicate of the triple pattern.
> - **object**: The object of the triple pattern.
> - **mapping**: (Optional) A mapping for more complex variable matching.

_Returns_:

> A string representing the URL to query the TPF service.

#### find_next_TP(count_hash, mapping = nil)

Identifies the next least populated triple pattern to request based on the counts of previous triples.

_Parameters_:

> - **count_hash**: A hash containing triple patterns and their respective counts.
> - **mapping**: (Optional) A mapping for variable matching.

_Returns_:

> An array containing the selected pattern, updated count hash, and matched variables.

#### harvest_tpf(min_pattern, mapping = nil, matched = nil)

Harvests triples from a TPF service based on the specified triple pattern.

_Parameters_:

> - **min_pattern**: The minimum pattern to start harvesting from.
> - **mapping**: (Optional) A mapping for more complex variable matching.
> - **matched**: (Optional) Previously matched variables.

#### bgp_iterator(bgp, mapping = nil)

Iterates over the Basic Graph Pattern (BGP) and processes each triple pattern by sending requests to a TPF service.

_Parameters_:

> - **bgp**: An array of RDF triple patterns.
> - **mapping**: (Optional) A mapping for more complex variable matching.

#### execute_sparql_query(query)

Executes a SPARQL query against a specified endpoint and returns the results.

_Parameters_:

> - **query**: The SPARQL query to be executed.

_Returns_:

> An array of results from the SPARQL query.

#### FindBGPPriority(query, control)

Main function that initiates the process of analyzing a SPARQL query, extracting the BGP, and harvesting triples from a TPF service based on the least populated pattern.

_Parameters_:

> - **query**: The SPARQL query to be processed.
> - **control**: The control URI for the TPF service.

## Example Usage

Below is an example of how to use the functions in this project:

    require_relative "functions.rb"

    #Define a SPARQL query to be used in the example

    query = "
    PREFIX dbpedia: <http://dbpedia.org/resource/Category:>
    PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>
    SELECT ?person WHERE {
    ?person a dbpedia-owl:Architect .
    }"

    @named_graph_iri = "http://example.org/graphs/my-new-graph1"

    #Define the control URI for the TPF service

    control = 'https://fragments.dbpedia.org/2015/en'

    #Execute the function to process the query and harvest triples

    FindBGPPriority(query, control)

    #Execute the SPARQL query and get results

    results = execute_sparql_query(query)

    #Output the results

    puts results

Explanation of the Example:

- Defining the SPARQL query: A query is defined that selects ?person who is an architect from DBpedia.

- Setting up the named graph URI: A URI for the named graph where the triples will be inserted.

- Specifying the TPF control URI: The control URI for the DBpedia TPF service.

- Processing the query: FindBGPPriority is called to process the query, extract the Basic Graph Patterns (BGP), and harvest triples from the TPF service.
- Executing the SPARQL query: The query is executed using execute_sparql_query, and the results are output to the console.

Requirements

To run this project, you'll need the following Ruby gems:

    rest-client
    open-uri
    nokogiri
    sparql
    net/http

You can install these dependencies by running:

    gem install rest-client nokogiri sparql


<h3>Funding</h3>

<b>TED2021-130788B-I00</b>
<br />
Proyecto TED2021-130788B-I00 financed by
<br />
MCIN/AEI /10.13039/501100011033 and by the European Union Next Generation EU/ PRTR.
<br />
<img width="300" src="./VP/images/ted-logo.png" />
<br />
<br />
<br />
The FLAIR-GG computational infrastructure is supported by the
<br />"Severo Ochoa Program for Centres of Excellence in R&D 2022-2025" from the
Agencia Estatal de Investigación of Spain, awarded to the
<br />Center for Plant Biotechnology and Genomics (CBGP UPM-INIA/CSIC).
<br />
<br />
<br />
<br />
<b>Principal Investigators:</b>
<br />
<br />
Moreno Vazquez, Santiago<br />
UPM grupo: Biodiversidad y conservación de recursos fitogenéticos
<br /><br />
Wilkinson, Mark Denis <br />
CBGP UPM-INIA/CSIC. <br />
UPM grupo: Abordajes Multidisciplinares en la Interfaz Planta-Microorganismo<br />
