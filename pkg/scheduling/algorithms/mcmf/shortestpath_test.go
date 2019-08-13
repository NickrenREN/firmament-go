package mcmf

import (
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	"testing"
)

func generateGraphWithCost() *flowgraph.Graph {
	graph := flowgraph.NewGraph(false)
	nodes := make([]*flowgraph.Node, 7, 7)
	for i := 0; i < 7; i++ {
		nodes[i] = graph.AddNode()
	}

	graph.SourceID = 1
	graph.SinkID = 7

	graph.AddArcById(1, 2).Cost = 0
	graph.AddArcById(1, 3).Cost = 0
	graph.AddArcById(1, 4).Cost = 0
	graph.AddArcById(2, 5).Cost = 5
	graph.AddArcById(5, 2).Cost = -5
	graph.AddArcById(3, 5).Cost = 6
	graph.AddArcById(5, 3).Cost = -6
	graph.AddArcById(3, 6).Cost = 7
	graph.AddArcById(6, 3).Cost = -7
	graph.AddArcById(4, 6).Cost = 8
	graph.AddArcById(6, 4).Cost = -8
	graph.AddArcById(5, 7).Cost = 0
	graph.AddArcById(6, 7).Cost = 0
	for arc, _ := range graph.ArcSet {
		arc.CapUpperBound = 1
	}

	return graph
}

func generateGraphWithPositiveCost() *flowgraph.Graph {
	graph := flowgraph.NewGraph(false)
	nodes := make([]*flowgraph.Node, 7, 7)
	for i := 0; i < 7; i++ {
		nodes[i] = graph.AddNode()
	}

	graph.SourceID = 1
	graph.SinkID = 7

	graph.AddArcById(1, 2).Cost = 0
	graph.AddArcById(1, 3).Cost = 0
	graph.AddArcById(1, 4).Cost = 0
	graph.AddArcById(2, 5).Cost = 5
	graph.AddArcById(3, 5).Cost = 6
	graph.AddArcById(3, 6).Cost = 7
	graph.AddArcById(4, 6).Cost = 8
	graph.AddArcById(5, 7).Cost = 0
	graph.AddArcById(6, 7).Cost = 0
	for arc, _ := range graph.ArcSet {
		arc.CapUpperBound = 1
	}

	return graph
}

func TestDEsopoPape(t *testing.T) {
	graph := generateGraphWithCost()
	distance, parent := DEsopoPapeWithSlice(graph, 1, 7)

	if distance[7] != 5 || parent[7] != 5 {
		t.Errorf("something is wrong")
	}
}

func TestDijkstra(t *testing.T) {
	graph := generateGraphWithPositiveCost()
	distance, parent := Dijkstra(graph, 1, 7, 1)
	if distance[7] != 5 || parent[7] != 5 {
		t.Errorf("something is wrong")
	}
}
