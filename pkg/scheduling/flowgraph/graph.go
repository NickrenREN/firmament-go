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
	"github.com/aybabtme/uniplot/histogram"
	"log"
	"math/rand"
	"nickren/firmament-go/pkg/scheduling/algorithms/datastructure"
	"os"
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

	// mcmf solve's copied graph will have different node id compare with the original graph to achieve better performance
	OriginalIdToCopyIdMap map[NodeID]NodeID
	CopyIdToOriginalIdMap map[NodeID]NodeID

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
	var totalRequest uint64 = 0
	for id, node := range incrementalGraph.NodeMap {
		node.Visited = 0
		if node.Type == NodeTypeUnscheduledTask {
			var request uint64
			for _, arc := range node.OutgoingArcMap {
				if arc.CapUpperBound == 0 {
					continue
				}
				request = arc.CapUpperBound
				break
			}
			node.Excess = int64(request)
			totalRequest += request
			incrementalGraph.AddArcWithCapAndCost(src.ID, id, request, 0)
			incrementalGraph.TaskSet[node] = struct{}{}
		}

		if node.Type == NodeTypeMachine {
			incrementalGraph.ResourceSet[node] = struct{}{}
		}
	}
	incrementalGraph.SourceID = src.ID
	//incrementalGraph.SinkID = 1
	fmt.Printf("before mcmf, total request: %v", totalRequest)
	return incrementalGraph
}


func CopyGraph(graph *Graph, modify bool) *Graph {
	fg := &Graph{
		ArcSet:  make(map[*Arc]struct{}),
		NodeMap: make(map[NodeID]*Node),
		TaskSet: make(map[*Node]struct{}),
		ResourceSet: make(map[*Node]struct{}),
		OriginalIdToCopyIdMap: make(map[NodeID]NodeID),
		CopyIdToOriginalIdMap: make(map[NodeID]NodeID),
	}
	//fg.SinkID = graph.SinkID
	//fg.SourceID = graph.SourceID
	//fg.NextID = graph.NextID

	var totalRequest uint64 = 0
	var index int = 1
	scheduledList := make([]*Node, 0)
	for id, val := range graph.NodeMap {
		if id == 1 {
			fg.SinkID = NodeID(index)
		}

		if val.Type == NodeTypeUnscheduledTask {
			var request uint64
			for _, arc := range val.OutgoingArcMap {
				if arc.CapUpperBound == 0 {
					continue
				}
				request = arc.CapUpperBound
				break
			}
			totalRequest += request
		}

		if val.IsScheduled() {
			scheduledList = append(scheduledList, val)
			continue
		}

		node := &Node{
			ID:             NodeID(index),
			IncomingArcMap: make(map[NodeID]*Arc),
			OutgoingArcMap: make(map[NodeID]*Arc),
			Visited: val.Visited,
			Type: val.Type,
			Excess: val.Excess,
			Potential: val.Potential,
			JobID: val.JobID,
		}
		fg.NodeMap[node.ID] = node
		fg.OriginalIdToCopyIdMap[id] = node.ID
		fg.CopyIdToOriginalIdMap[node.ID] = id
		index++
	}
	fg.NextID = NodeID(index)
	fmt.Printf("The original graph has %v total task requests\n", totalRequest)

	for _, val := range scheduledList {
		node := &Node{
			ID:             NodeID(index),
			IncomingArcMap: make(map[NodeID]*Arc),
			OutgoingArcMap: make(map[NodeID]*Arc),
			Visited: val.Visited,
			Type: val.Type,
			Excess: val.Excess,
			Potential: val.Potential,
			JobID: val.JobID,
		}
		fg.NodeMap[node.ID] = node
		fg.OriginalIdToCopyIdMap[val.ID] = node.ID
		fg.CopyIdToOriginalIdMap[node.ID] = val.ID
		index++
	}

	costMap := make(map[int64]int)
	costArr := make([]float64, 0)
	for arc, _ := range graph.ArcSet {
		if arc.CapUpperBound == 0 {
			continue
		}
		if arc.Cost > 0 && arc.Cost < 10001 {
			costArr = append(costArr, float64(arc.Cost))
		}
		fg.AddArcWithCapAndCost(fg.OriginalIdToCopyIdMap[arc.Src], fg.OriginalIdToCopyIdMap[arc.Dst],
			arc.CapUpperBound, arc.Cost)
		if _, ok := costMap[arc.Cost]; ok {
			costMap[arc.Cost]++
		} else {
			costMap[arc.Cost] = 1
		}
	}

	for cost, count := range costMap {
		fmt.Printf("cost: %v, count: %v\n", cost, count)
	}
	hist := histogram.Hist(20, costArr)
	histogram.Fprint(os.Stdout, hist, histogram.Linear(5))
	fg.UnusedIDs = queue.NewFIFO()

	// TODO might rethink about the implementation here
	if modify {
		for _, node := range fg.NodeMap {
			node.Visited = 0
		}
		var visitCount uint32 = 1
		for id, node := range fg.NodeMap {
			if node.IsScheduled() {
				fmt.Printf("delete node and it's path %v\n", id)
				nodeToDelete := node
				DFSDeleteNodeFromOriginGraph(fg, nodeToDelete, visitCount)
				visitCount++
			}
		}
	}
	fg.UnusedIDs = queue.NewFIFO()
	return fg
}

func DFSDeleteNodeFromOriginGraph(graph *Graph, nodeToDelete *Node, visitCount uint32) {
	outArc := nodeToDelete.GetRandomArc()
	request := outArc.CapUpperBound
	deque := datastructure.NewDeque(5)
	deque.PushEnd(nodeToDelete)

	for !deque.IsEmpty() {
		current := deque.PopEnd().(*Node)
		for _, arc := range current.OutgoingArcMap {
			arc.CapUpperBound -= request
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
