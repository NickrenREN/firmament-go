package mcmf

import (
	"math"
	"nickren/firmament-go/pkg/scheduling/algorithms/datastructure"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
)

/**
	This file contains shortest-path algorithms which will be used to solve MCMF problems
	Some of these algorithms do not reply on potentials, some do
 */

 // D-Esopo-Pape algorithm can tolerate the negative cost edge.
 // Despite it cannot tolerate negative cost cycle, in the successive shortest path algorithms
 // we will not have negative cost cycle
 func DEsopoPapeWithSlice(graph *flowgraph.Graph, src, dst flowgraph.NodeID) ([]int64, []flowgraph.NodeID) {
	distance := make([]int64, len(graph.NodeMap) + 1)
	parent := make([]flowgraph.NodeID, len(graph.NodeMap) + 1)
	m := make([]int, len(graph.NodeMap) + 1)
	for i := 1; i < len(parent); i++ {
		distance[i] = math.MaxInt64
		parent[i] = 0
		m[i] = 2
	}
	distance[int(src)] = 0
	deque := datastructure.NewDeque(len(graph.NodeMap))
	deque.PushEnd(src)

	for !deque.IsEmpty() {
		current := deque.PopFront().(flowgraph.NodeID)
		m[int(current)] = 0
		for nextId, arc := range graph.Node(current).OutgoingArcMap {
			if arc.CapUpperBound > 0 && distance[int(nextId)] > distance[int(current)] + arc.Cost {
				distance[int(nextId)] = distance[int(current)] + arc.Cost
				parent[int(nextId)] = current
				if m[int(nextId)] == 2 {
					m[int(nextId)] = 1
					deque.PushEnd(nextId)
				} else if m[int(nextId)] == 0 {
					m[int(nextId)] = 1
					deque.PushFront(nextId)
				}
			}
		}
	}

	return distance, parent
 }

func DijkstraWithSlice(graph *flowgraph.Graph, src, dst flowgraph.NodeID, visiteCount uint32) ([]int64, []flowgraph.NodeID) {
	distance := make([]int64, len(graph.NodeMap) + 1)
	parent := make([]flowgraph.NodeID, len(graph.NodeMap) + 1)
	fh := datastructure.NewFibHeap()
	for i := 1; i < len(parent); i++ {
		distance[i] = math.MaxInt64
		parent[i] = 0
	}
	fh.Insert(int64(0), &datastructure.Distance{uint64(src), 0})
	distance[int(src)] = 0

	for fh.Len() > 0 {
		current := fh.ExtractMin().Value.(*datastructure.Distance)
		currentNode := graph.Node(flowgraph.NodeID(current.NodeId))
		currentNode.Visited = visiteCount

		if flowgraph.NodeID(current.NodeId) == dst {
			return distance, parent
		}

		for nextId, arc := range currentNode.OutgoingArcMap {
			nextNode := graph.Node(nextId)
			if nextNode.Visited < visiteCount && arc.CapUpperBound > 0 {
				arcCost := arc.Cost - currentNode.Potential + nextNode.Potential
				updatedCost := current.Distance + arcCost
				if updatedCost < distance[int(nextId)] {
					distance[int(nextId)] = updatedCost
					parent[int(nextId)] = flowgraph.NodeID(current.NodeId)
					fh.Insert(updatedCost, &datastructure.Distance{uint64(nextId), updatedCost})
				}
			}
		}
	}

	return distance, parent
}

 func Dijkstra(graph *flowgraph.Graph, src, dst flowgraph.NodeID, visitCount uint32) (map[flowgraph.NodeID]int64, map[flowgraph.NodeID]flowgraph.NodeID) {
	 distance := make(map[flowgraph.NodeID]int64)
	 parent := make(map[flowgraph.NodeID]flowgraph.NodeID)
	 fh := datastructure.NewFibHeap()

	 for id, _ := range graph.NodeMap {
	 	distance[id] = math.MaxInt64
	 	parent[id] = 0
	 }
	 fh.Insert(int64(0), &datastructure.Distance{uint64(src), 0})
	 distance[src] = 0

	 for fh.Len() > 0 {
	 	current := fh.ExtractMin().Value.(*datastructure.Distance)
	 	currentNode := graph.Node(flowgraph.NodeID(current.NodeId))
	 	currentNode.Visited = visitCount

	 	if flowgraph.NodeID(current.NodeId) == dst {
	 		return distance, parent
		}

	 	for nextId, arc := range currentNode.OutgoingArcMap {
	 		nextNode := graph.Node(nextId)
	 		if nextNode.Visited < visitCount && arc.CapUpperBound > 0 {
	 			arcCost := arc.Cost - currentNode.Potential + nextNode.Potential
	 			updatedCost := current.Distance + arcCost
	 			if updatedCost < distance[nextId] {
					distance[nextId] = updatedCost
					parent[nextId] = flowgraph.NodeID(current.NodeId)
					fh.Insert(updatedCost, &datastructure.Distance{uint64(nextId), updatedCost})
				}
			}
		}
	 }

	 return distance, parent
 }

