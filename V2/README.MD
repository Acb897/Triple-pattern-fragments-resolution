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
    TODO: Right now, the transform function returns TPFs for a given query, and the variables from each TPF (see example). Make the FindBGPPriority function in BasicGraphPatternIterator example.rb take those variables, create a mappings hash with key = variable value (e.g. ?city), and values = each the corresponding values after the request to the TPF server (e.g. http://dbpedia.org/resource/Amsterdam, http://dbpedia.org/resource/Athens etc). Note that there could be multiple variables in a single TPF. Then, the findbgppriority function will find the TPF that shares a variable with the minimal one [THIS IS ALREADY DONE], iterate over the mappings, instantiate all possible variables, and resolve. This would happen iteratively.