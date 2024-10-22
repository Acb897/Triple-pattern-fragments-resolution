This folder contains the RootIterator, TriplePatternIterator, BasicGraphPattern iterator, the testPage_extractor and some examples

For now, the good version of the TriplePatternIterator is Compare.rb, and I'm using TriplePatternIterator example.rb to test it
The testPage_extractor.rb contains the algorithm to extract the triples and next page for a given TPF server query. Note that I will probably have to add a check for the last page of the results, which doesn't have a next page, to indicate that this is the end of the iteration.

The idea is to merge the extractor algorithm with the TriplePatternIterator, by replacing the non-functioning parse functions.

Then, I will test that TriplePatternIterator works and can extract the current page

Then, BasicGraphPatternIterator has to be able to check which of the TPF of the query has less answers, run it using TriplePatternIterator, until all results have been consumed. For each iteration, it will bind the variable for the variable of the TPF with less answers, and use it to find the next TPF to run from the BGP.

Transform.rb contains the transform class, that parses SPARQL queries and returns a Hash with each Triple pattern divided into subject, predicate and object.

BasicGraphPatternIterator example.rb contains the proof of concept for the algorithm that will decide the order in which the triple patterns will be sent to the TPF server.

TODO: Change the query parser to fix it not recognizing OPTIONAL statements

ONGOING: Make the algorithm prioritization of the Triple patterns a recursive iterator that does it all automatically
    TODO: Store the harvested triples from each TPF request into a .ttl file. 
    TODO: Right now, it gets the triple pattern with the least solutions, harvests the triples, uses the solutions to create a list of instantiated variables, chooses the triple pattern fragment that shares a variable with the minimal pattern. I need to make it so that it calls the request uri creator function with the instantiated variables, stores the solutions, and does the selection of the next tpf. RESUMEN: tengo que hacer la llamada al server TPF, recopilar los datos en un archivo, y hacer que la seleccion del siguiente tpf y su resolucion sean automaticas (reescribir lo que tengo en basicgraphpatterniterator example.rb para que sea una funcion que recorre todo el Basic graph pattern, prioriza las triple patterns, las ejectuta, y usa los resultados con las siguientes tp hasta que se acabe el bgp.)


    Fix ln 54-57 Transform.rb (make it a loop)
    #FIX this
    spovariables.append variable_hash[:subject] = subject if variables.include? subject
    spovariables.append variable_hash[:predicate] = predicate if variables.include? predicate
    spovariables.append variable_hash[:object] = object if variables.include? object