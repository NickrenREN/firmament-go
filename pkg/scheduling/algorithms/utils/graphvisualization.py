import igraph

print(igraph.__version__)
g = igraph.Graph()
g.add_vertices(3)
g.add_edges([(0,1),(1,2)])
g.add_edges([(2,0)])
g.add_vertices(3)
g.add_edges([(2,3),(3,4),(4,5),(5,3)])

print(g)

dimacs = igraph.Graph()
dimacs = dimacs.Read_DIMACS('./dimacs.txt', False)

print(dimacs)

layout = dimacs.layout("kk")
igraph.plot(dimacs, layout = layout)