// Copyright 2016 The ksched Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package flowgraph

import (
	"fmt"
	"log"
	"math/rand"
	"nickren/firmament-go/pkg/scheduling/algorithms/datastructure"
	"runtime"
	"time"

	"nickren/firmament-go/pkg/scheduling/utility/queue"
)

type NodeID uint64

type Graph struct {
	// Next node id to use
	NextID NodeID
	// Unordered set of arcs in graph
	ArcSet map[*Arc]struct{}

	// Unordered set of tasks in graph
	TaskSet map[*Node]struct{}

	// Unordered set of resources in graph
	ResourceSet map[*Node]struct{}

	// node id of sink node
	SinkID NodeID

	// node id of source node
	SourceID NodeID

	// Map of nodes keyed by nodeID
	// TODO consider change it to array and make nextId comply
	NodeMap map[NodeID]*Node
	// Queue storing the ids of the nodes we've previously removed.
	UnusedIDs queue.FIFO

	// Behaviour flag - set as struct field rather than global static variable
	//                  since we will have only one instance of the FlowGraph.
	// If true then the flow graph will not generate node ids in order
	RandomizeNodeIDs bool
}

func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func ModifyGraphFromTotalToIncremental(graph *Graph) *Graph {
	PrintMemUsage()
	incrementalGraph := CopyGraph(graph, true)
	PrintMemUsage()
	src := incrementalGraph.AddNode()
	for id, node := range incrementalGraph.NodeMap {
		node.Visited = 0
		if node.Type == NodeTypeUnscheduledTask {
			var request uint64
			for _, arc := range node.OutgoingArcMap {
				request = arc.CapUpperBound
				break
			}
			incrementalGraph.AddArcWithCapAndCost(src.ID, id, request, 0)
			incrementalGraph.TaskSet[node] = struct{}{}
		}

		if node.Type == NodeTypeMachine {
			incrementalGraph.ResourceSet[node] = struct{}{}
		}
	}
	incrementalGraph.SourceID = src.ID
	incrementalGraph.SinkID = 1
	return incrementalGraph
}


func CopyGraph(graph *Graph, modify bool) *Graph {
	fg := &Graph{
		ArcSet:  make(map[*Arc]struct{}),
		NodeMap: make(map[NodeID]*Node),
		TaskSet: make(map[*Node]struct{}),
		ResourceSet: make(map[*Node]struct{}),
	}
	fg.SinkID = graph.SinkID
	fg.SourceID = graph.SourceID
	fg.NextID = graph.NextID

	s1 := time.Now()
	for id, val := range graph.NodeMap {
		node := &Node{
			ID:             id,
			IncomingArcMap: make(map[NodeID]*Arc),
			OutgoingArcMap: make(map[NodeID]*Arc),
			Visited: val.Visited,
			Type: val.Type,
			Excess: val.Excess,
			Potential: val.Potential,
		}
		fg.NodeMap[id] = node
	}
	fmt.Printf("copy nodes took %v\n", time.Since(s1))

	for node, val := range graph.TaskSet {
		fg.TaskSet[fg.NodeMap[node.ID]] = val
	}

	for node, val := range graph.ResourceSet {
		fg.ResourceSet[fg.NodeMap[node.ID]] = val
	}

	s2 := time.Now()
	for arc, _ := range graph.ArcSet {
		fg.AddArcWithCapAndCost(arc.Src, arc.Dst, arc.CapUpperBound, arc.Cost)
		//fg.ArcSet[copyArc] = val
		arc.ReverseArc = nil

	}
	fmt.Printf("copy arcs took %v\n", time.Since(s2))
	fg.UnusedIDs = queue.NewFIFO()

	if modify {
		for _, node := range fg.NodeMap {
			node.Visited = 0
		}
		var visitCount uint32 = 1
		for id, node := range graph.NodeMap {
			if node.IsScheduled() {
				nodeToDelete := fg.Node(id)
				DFSDeleteNodeFromOriginGraph(fg, nodeToDelete, visitCount)
				visitCount++
			}
		}
	}

	return fg
}

func DFSDeleteNodeFromOriginGraph(graph *Graph, nodeToDelete *Node, visitCount uint32) {
	outArc := nodeToDelete.GetRandomArc()
	deque := datastructure.NewDeque(5)
	deque.PushEnd(nodeToDelete)

	for !deque.IsEmpty() {
		current := deque.PopEnd().(*Node)
		for _, arc := range current.OutgoingArcMap {
			arc.CapUpperBound -= outArc.CapUpperBound
			if arc.DstNode.Visited < visitCount {
				arc.DstNode.Visited = visitCount
				deque.PushEnd(arc.DstNode)
			}
		}
	}

	graph.DeleteArc(outArc)
	graph.DeleteNode(nodeToDelete)
}

// Constructor equivalent in Go
// Must specify RandomizeNodeIDs flag
func NewGraph(randomizeNodeIDs bool) *Graph {
	fg := &Graph{
		ArcSet:  make(map[*Arc]struct{}),
		NodeMap: make(map[NodeID]*Node),
		TaskSet: make(map[*Node]struct{}),
		ResourceSet: make(map[*Node]struct{}),
	}
	fg.NextID = 1
	fg.UnusedIDs = queue.NewFIFO()
	if randomizeNodeIDs {
		fg.RandomizeNodeIDs = true
		fg.PopulateUnusedIds(50)
	}
	return fg
}

// Adds an arc based on references to the src and dst nodes
func (fg *Graph) AddArc(src, dst *Node) *Arc {
	return fg.AddArcById(src.ID, dst.ID)
}

func (fg *Graph) AddArcById(src, dst NodeID) *Arc {
	srcNode := fg.NodeMap[src]
	if srcNode == nil {
		log.Fatalf("graph: AddArc error, src node with id:%d not found\n", src)
	}
	dstNode := fg.NodeMap[dst]
	if dstNode == nil {
		log.Fatalf("graph: AddArc error, dst node with id:%d not found\n", dst)
	}

	arc := NewArc(srcNode, dstNode)
	fg.ArcSet[arc] = struct{}{}
	srcNode.AddArc(arc)
	return arc
}

func (fg *Graph) AddArcWithCapAndCost(src, dst NodeID, cap uint64, cost int64) *Arc {
	arc := fg.AddArcById(src, dst)
	if arc != nil {
		arc.Cost = cost
		arc.CapUpperBound = cap
	}
	return arc
}

func (fg *Graph) ChangeArc(arc *Arc, l, u uint64, c int64) {
	if l == 0 && u == 0 {
		delete(fg.ArcSet, arc)
	}
	arc.CapLowerBound = l
	arc.CapUpperBound = u
	arc.Cost = c
}

func (fg *Graph) AddNode() *Node {
	id := fg.NextId()
	// fmt.Printf("AddNode called for id:%v\n", id)
	node := &Node{
		ID:             id,
		IncomingArcMap: make(map[NodeID]*Arc),
		OutgoingArcMap: make(map[NodeID]*Arc),
	}
	// Insert into NodeMap, must not already be present
	_, ok := fg.NodeMap[id]
	if ok {
		log.Fatalf("graph: AddNode error, node with id:%d already present in NodeMap\n", id)
	}
	fg.NodeMap[id] = node
	return node
}

func (fg *Graph) DeleteArc(arc *Arc) {
	delete(arc.SrcNode.OutgoingArcMap, arc.DstNode.ID)
	delete(arc.DstNode.IncomingArcMap, arc.SrcNode.ID)
	delete(fg.ArcSet, arc)
}

func (fg *Graph) NumArcs() int {
	return len(fg.ArcSet)
}

func (fg *Graph) Arcs() map[*Arc]struct{} {
	// TODO: we should return a copy? Only after concurrency pattern is known.
	return fg.ArcSet
}

func (fg *Graph) Node(id NodeID) *Node {
	return fg.NodeMap[id]
}

func (fg *Graph) NumNodes() int {
	return len(fg.NodeMap)
}

func (fg *Graph) Nodes() map[NodeID]*Node {
	// TODO: we should return a copy? Only after concurrency pattern is known.
	return fg.NodeMap
}

func (fg *Graph) DeleteNode(node *Node) {
	// Reuse this ID for later
	fg.UnusedIDs.Push(node.ID)
	// fmt.Printf("DeleteNode called for id:%v\n", node.ID)
	// First remove all outgoing arcs
	for dstID, arc := range node.OutgoingArcMap {
		if dstID != arc.Dst {
			log.Fatalf("graph: DeleteNode error, dstID:%d != arc.Dst:%d\n", dstID, arc.Dst)
		}
		if node.ID != arc.Src {
			log.Fatalf("graph: DeleteNode error, node.ID:%d != arc.Src:%d\n", node.ID, arc.Src)
		}
		delete(arc.DstNode.IncomingArcMap, arc.Src)
		fg.DeleteArc(arc)
	}
	// Remove all incoming arcs
	for srcID, arc := range node.IncomingArcMap {
		if srcID != arc.Dst {
			log.Fatalf("graph: DeleteNode error, srcID:%d != arc.Src:%d\n", srcID, arc.Src)
		}
		if node.ID != arc.Dst {
			log.Fatalf("graph: DeleteNode error, node.ID:%d != arc.Dst:%d\n", node.ID, arc.Dst)
		}
		delete(arc.SrcNode.OutgoingArcMap, arc.Dst)
		fg.DeleteArc(arc)
	}
	// Remove node from NodeMap
	// log.Printf("Deleting nodeID:%v from NodeMap\n", node.ID)
	delete(fg.NodeMap, node.ID)

}

// Returns nil if arc not found
func (fg *Graph) GetArc(src, dst *Node) *Arc {
	return src.OutgoingArcMap[dst.ID]
}

func (fg *Graph) GetArcByIds(src, dst NodeID) *Arc {
	if fg.NodeMap[src] == nil {
		return nil
	}
	return fg.NodeMap[src].OutgoingArcMap[dst]
}

// Returns the NextID to assign to a node
func (fg *Graph) NextId() NodeID {
	if fg.RandomizeNodeIDs {
		if fg.UnusedIDs.IsEmpty() {
			fg.PopulateUnusedIds(fg.NextID * 2)
		}
		return fg.UnusedIDs.Pop().(NodeID)
	}
	if fg.UnusedIDs.IsEmpty() {
		newID := fg.NextID
		fg.NextID++
		return newID
	}
	return fg.UnusedIDs.Pop().(NodeID)
}

// Called if fg.RandomizeNodeIDs is true to generate a random shuffle of ids
func (fg *Graph) PopulateUnusedIds(newNextID NodeID) {
	t := time.Now().UnixNano()
	r := rand.New(rand.NewSource(t))
	ids := make([]NodeID, 0)
	for i := fg.NextID; i < newNextID; i++ {
		ids = append(ids, i)
	}
	// Fisher-Yates shuffle
	for i := range ids {
		j := r.Intn(i + 1)
		ids[i], ids[j] = ids[j], ids[i]
	}
	for i := range ids {
		fg.UnusedIDs.Push(ids[i])
	}
	fg.NextID = newNextID
}
