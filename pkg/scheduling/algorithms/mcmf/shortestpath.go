package mcmf

import (
	"container/heap"
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
 func DEsopoPape(graph *flowgraph.Graph, src, dst flowgraph.NodeID) (int64, []flowgraph.NodeID) {
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

	return distance[dst], parent
 }

 func Dijkstra(graph *flowgraph.Graph, src, dst flowgraph.NodeID, visiteCount uint32) ([]int64, []flowgraph.NodeID) {
	 distance := make([]int64, len(graph.NodeMap) + 1)
	 parent := make([]flowgraph.NodeID, len(graph.NodeMap) + 1)
	 pq := make(datastructure.BinaryMinHeap, 0)
	 for i := 1; i < len(parent); i++ {
		 distance[i] = math.MaxInt64
		 parent[i] = 0
	 }
	 heap.Init(&pq)
	 heap.Push(&pq, &datastructure.Distance{src, 0})
	 distance[int(src)] = 0

	 for pq.Len() > 0 {
	 	current := heap.Pop(&pq).(*datastructure.Distance)
	 	currentNode := graph.Node(current.NodeId)
	 	currentNode.Visited = visiteCount

	 	if current.NodeId == dst {
	 		return distance, parent
		}

	 	for nextId, arc := range currentNode.OutgoingArcMap {
	 		nextNode := graph.Node(nextId)
	 		if nextNode.Visited < visiteCount && arc.CapUpperBound > 0 {
	 			arcCost := arc.Cost - currentNode.Potential + nextNode.Potential
	 			updatedCost := current.Distance + arcCost
	 			if updatedCost < distance[int(nextId)] {
					distance[int(nextId)] = updatedCost
					parent[int(nextId)] = current.NodeId
					heap.Push(&pq, &datastructure.Distance{nextId, updatedCost})
				}
			}
		}
	 }

	 return distance, parent
 }

