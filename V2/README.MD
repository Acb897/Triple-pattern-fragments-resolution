This folder contains the RootIterator, TriplePatternIterator, BasicGraphPattern iterator, the testPage_extractor and some examples

For now, the good version of the TriplePatternIterator is Compare.rb, and I'm using TriplePatternIterator example.rb to test it
The testPage_extractor.rb contains the algorithm to extract the triples and next page for a given TPF server query. Note that I will probably have to add a check for the last page of the results, which doesn't have a next page, to indicate that this is the end of the iteration.

The idea is to merge the extractor algorithm with the TriplePatternIterator, by replacing the non-functioning parse functions.

Then, I will test that TriplePatternIterator works and can extract the current page

Then, BasicGraphPatternIterator has to be able to check which of the TPF of the query has less answers, run it using TriplePatternIterator, until all results have been consumed. For each iteration, it will bind the variable for the variable of the TPF with less answers, and use it to find the next TPF to run from the BGP.
