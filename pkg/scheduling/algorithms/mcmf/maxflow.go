package mcmf

import (
	"log"
	"math"
	"nickren/firmament-go/pkg/scheduling/algorithms/datastructure"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
)

/**
	methods in this file are max-flow algorithms which can accept constraint flag:
	Each task in the graph has a resource request in term of resource slot, each slot is
	a unit of flow in MCMF. However we simply cannot scheduler one task with multiple request slots
	to different machine. Therefore we need to add some constraints to the MCMF algorithms.

	algorithms in this file also has some pre-assumption about the topology of graph, so they are
	pretty ad-hoc.

	However this is extremely hard to implement and contradict to MCMF's philosophy
*/

func DFS(graph *flowgraph.Graph, src, dst flowgraph.NodeID, parent []flowgraph.NodeID,
	dfsCount uint32, dfs bool, constraint bool) bool {
	deque := datastructure.NewDeque(10)
	srcNode := graph.Node(src)
	if srcNode == nil {
		log.Fatalf("srcNode id %v does not exists", src)
	}

	deque.PushEnd(srcNode)
	srcNode.Visited = dfsCount
	parent[src] = 0
	var current *flowgraph.Node
/*	var requestedSupply uint64*/

	for !deque.IsEmpty() {
		if dfs {
			current = deque.PopEnd().(*flowgraph.Node)
		} else {
			current = deque.PopFront().(*flowgraph.Node)
		}

		/*if current.IsTaskNode() {
			requestedSupply = uint64(current.Excess)
		}*/

/*		OUTER:*/
		for id, arc := range current.OutgoingArcMap {
			dstNode := graph.Node(id)
			if dstNode != nil && dstNode.Visited != dfsCount && arc.CapUpperBound > 0 {
				/*if constraint && dstNode.IsResourceNode() {
					for _, outArc := range dstNode.OutgoingArcMap {
						if outArc.CapUpperBound < requestedSupply {
							continue OUTER
						}
					}
				}*/
				dstNode.Visited = dfsCount
				parent[id] = current.ID
				if id == dst {
					return true
				}
				deque.PushEnd(dstNode)
			}
		}
	}

	return false
}

func retrieveMinflow(graph *flowgraph.Graph, parent []flowgraph.NodeID,
	dst flowgraph.NodeID) uint64 {
	child := dst
	var minFlow uint64 = math.MaxUint64
	for father := parent[child]; father != 0; father = parent[child] {
		arc := graph.GetArcByIds(father, child)
		if arc != nil && arc.CapUpperBound < minFlow {
			minFlow = arc.CapUpperBound
		}
		child = father
	}
	return minFlow
}

func EdmondsKarp(graph *flowgraph.Graph, src, dst flowgraph.NodeID, dfs bool, constraint bool) uint64 {
	var flow uint64 = 0
	var dfsCount uint32 = 1
	parent := make([]flowgraph.NodeID, len(graph.NodeMap) + 1)
	for DFS(graph, src, dst, parent, dfsCount, dfs, constraint) {
		dfsCount++
		minFlow := retrieveMinflow(graph, parent, dst)

		flow += minFlow
		child := dst
		for father := parent[child]; father != 0; father = parent[child] {
			arc := graph.GetArcByIds(father, child)
			arc.CapUpperBound -= minFlow
			reverseArc := graph.GetArcByIds(child, father)
			if reverseArc == nil {
				reverseArc = graph.AddArc(graph.Node(child), graph.Node(father))
				reverseArc.CapUpperBound = minFlow
			} else {
				reverseArc.CapUpperBound += minFlow
			}
			child = father
		}
	}

	return flow
}
