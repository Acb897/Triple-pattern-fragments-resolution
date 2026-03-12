from TPF import fetch_tpf_page

url = "http://localhost:3000/kegg-sparql?predicate=http%3A%2F%2Fwww.w3.org%2F1999%2F02%2F22-rdf-syntax-ns%23type&object=http%3A%2F%2Fbio2rdf.org/ns/kegg%23Enzyme"
g = fetch_tpf_page(url)
for s,p,o in g:
    print(s,p,o)