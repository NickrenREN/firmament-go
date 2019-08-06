package mcmf

import (
	"math"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
)

// Successive shortest path algorithm which use D-Esopo-Pape algorithm to find shortest path in each iteration
func SuccessiveShortestPathWithDEP(graph *flowgraph.Graph, src, dst flowgraph.NodeID) (uint64, int64) {
	var maxFlow uint64
	var minCost int64

	distance, parent := DEsopoPape(graph, src, dst)
	for distance[dst] != math.MaxInt64 {
		minFlow := retrieveMinflow(graph, parent, dst)

		maxFlow += minFlow
		minCost += distance[dst] * int64(minFlow)
		child := dst
		for father := parent[child]; father != 0; father = parent[child] {
			arc := graph.GetArcByIds(father, child)
			arc.CapUpperBound -= minFlow
			reverseArc := graph.GetArcByIds(child, father)
			if reverseArc == nil {
				reverseArc = graph.AddArc(graph.Node(child), graph.Node(father))
				reverseArc.CapUpperBound = minFlow
				reverseArc.Cost = -arc.Cost
			} else {
				reverseArc.CapUpperBound += minFlow
			}
			child = father
		}
		distance, parent = DEsopoPape(graph, src, dst)
	}

	return maxFlow, minCost
}

func retrieveMinflowAndPathCost(graph *flowgraph.Graph, parent []flowgraph.NodeID,
	dst flowgraph.NodeID) (uint64, int64) {
	child := dst
	var minFlow uint64 = math.MaxUint64
	var pathCost int64 = 0

	for father := parent[child]; father != 0; father = parent[child] {
		arc := graph.GetArcByIds(father, child)
		if arc != nil && arc.CapUpperBound < minFlow {
			minFlow = arc.CapUpperBound
		}
		pathCost += arc.Cost
		child = father
	}
	return minFlow, pathCost
}

func SuccessiveShortestPathWithDijkstra(graph *flowgraph.Graph, src, dst flowgraph.NodeID) (uint64, int64) {
	var maxFlow uint64
	var minCost int64
	var visitCount uint32 = 1

	distance, parent := Dijkstra(graph, src, dst, visitCount)
	for distance[dst] != math.MaxInt64 {
		minFlow, pathCost := retrieveMinflowAndPathCost(graph, parent, dst)

		maxFlow += minFlow
		minCost += pathCost * int64(minFlow)
		child := dst
		for father := parent[child]; father != 0; father = parent[child] {
			arc := graph.GetArcByIds(father, child)
			arc.CapUpperBound -= minFlow
			reverseArc := graph.GetArcByIds(child, father)
			if reverseArc == nil {
				reverseArc = graph.AddArc(graph.Node(child), graph.Node(father))
				reverseArc.CapUpperBound = minFlow
				reverseArc.Cost = -arc.Cost
			} else {
				reverseArc.CapUpperBound += minFlow
			}
			child = father
		}
		for id, node := range graph.NodeMap {
			if node.Visited == visitCount {
				node.Potential -= distance[int(id)]
			} else {
				node.Potential -= distance[int(dst)]
			}
		}
		visitCount++
		distance, parent = Dijkstra(graph, src, dst, visitCount)
	}

	return maxFlow, minCost
}
