package mcmf

import (
	"bufio"
	"fmt"
	"log"
	"nickren/firmament-go/pkg/scheduling/algorithms/utils"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	"os"
	"strconv"
	"strings"
	"testing"
)

func generateGraph() *flowgraph.Graph {
	graph := flowgraph.NewGraph(false)
	nodes := make([]*flowgraph.Node, 6, 6)
	for i := 0; i < 6; i++ {
		nodes[i] = graph.AddNode()
	}

	graph.SourceID = 1
	graph.SinkID = 6

	graph.AddArcById(1, 2).CapUpperBound = 16
	graph.AddArcById(1, 3).CapUpperBound = 13
	graph.AddArcById(2, 3).CapUpperBound = 10
	graph.AddArcById(3, 2).CapUpperBound = 4
	graph.AddArcById(2, 4).CapUpperBound = 12
	graph.AddArcById(4, 3).CapUpperBound = 9
	graph.AddArcById(3, 5).CapUpperBound = 14
	graph.AddArcById(5, 4).CapUpperBound = 7
	graph.AddArcById(4, 6).CapUpperBound = 20
	graph.AddArcById(5, 6).CapUpperBound = 4

	return graph
}

func generateAdhocGraph() *flowgraph.Graph {
	graph := flowgraph.NewGraph(false)
	nodes := make([]*flowgraph.Node, 7, 7)
	for i := 0; i < 7; i++ {
		nodes[i] = graph.AddNode()
	}

	graph.SourceID = 1
	graph.SinkID = 7
	nodes[1].Excess = 5
	nodes[1].Type = flowgraph.NodeTypeUnscheduledTask
	graph.TaskSet[nodes[1]] = struct{}{}
	nodes[2].Excess = 5
	nodes[2].Type = flowgraph.NodeTypeUnscheduledTask
	graph.TaskSet[nodes[2]] = struct{}{}
	nodes[3].Excess = 5
	nodes[3].Type = flowgraph.NodeTypeUnscheduledTask
	graph.TaskSet[nodes[3]] = struct{}{}
	nodes[4].Type = flowgraph.NodeTypeMachine
	graph.ResourceSet[nodes[4]] = struct{}{}
	nodes[5].Type = flowgraph.NodeTypeMachine
	graph.ResourceSet[nodes[5]] = struct{}{}

	graph.AddArcById(1, 2).CapUpperBound = 5
	graph.AddArcById(1, 3).CapUpperBound = 5
	graph.AddArcById(1, 4).CapUpperBound = 5
	graph.AddArcById(2, 5).CapUpperBound = 5
	graph.AddArcById(2, 6).CapUpperBound = 5
	graph.AddArcById(3, 5).CapUpperBound = 5
	graph.AddArcById(3, 6).CapUpperBound = 5
	graph.AddArcById(4, 5).CapUpperBound = 5
	graph.AddArcById(4, 6).CapUpperBound = 5
	graph.AddArcById(5, 7).CapUpperBound = 8
	graph.AddArcById(6, 7).CapUpperBound = 8

	return graph
}

func generateAdhocGraph1() *flowgraph.Graph {
	graph := flowgraph.NewGraph(false)
	nodes := make([]*flowgraph.Node, 7, 7)
	for i := 0; i < 7; i++ {
		nodes[i] = graph.AddNode()
	}

	graph.SourceID = 1
	graph.SinkID = 7
	nodes[1].Excess = 5
	nodes[1].Type = flowgraph.NodeTypeUnscheduledTask
	graph.TaskSet[nodes[1]] = struct{}{}
	nodes[2].Excess = 5
	nodes[2].Type = flowgraph.NodeTypeUnscheduledTask
	graph.TaskSet[nodes[2]] = struct{}{}
	nodes[3].Excess = 4
	nodes[3].Type = flowgraph.NodeTypeUnscheduledTask
	graph.TaskSet[nodes[3]] = struct{}{}
	nodes[4].Type = flowgraph.NodeTypeMachine
	graph.ResourceSet[nodes[4]] = struct{}{}
	nodes[5].Type = flowgraph.NodeTypeMachine
	graph.ResourceSet[nodes[5]] = struct{}{}

	graph.AddArcById(1, 2).CapUpperBound = 5
	graph.AddArcById(1, 3).CapUpperBound = 5
	graph.AddArcById(1, 4).CapUpperBound = 4
	graph.AddArcById(2, 5).CapUpperBound = 5
	graph.AddArcById(2, 6).CapUpperBound = 5
	graph.AddArcById(3, 5).CapUpperBound = 5
	graph.AddArcById(3, 6).CapUpperBound = 5
	graph.AddArcById(4, 5).CapUpperBound = 4
	graph.AddArcById(4, 6).CapUpperBound = 4
	graph.AddArcById(5, 7).CapUpperBound = 8
	graph.AddArcById(6, 7).CapUpperBound = 9

	return graph
}

func generateGraphFromFile(filePath string) *flowgraph.Graph {
	file, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	line := 0
	var graph *flowgraph.Graph
	for scanner.Scan() {
		if line == 0 {
			n, _ := strconv.ParseUint(scanner.Text(), 0, 64)
			graph = flowgraph.NewGraph(false)
			for i := 0; i < int(n); i++ {
				graph.AddNode()
			}
			graph.SourceID = 1
			graph.SinkID = flowgraph.NodeID(n)
		} else {
			stringSlice := strings.Fields(scanner.Text())
			for index, flow := range stringSlice {
				index++
				flowInt, _ := strconv.ParseUint(flow, 0, 64)
				if flowInt != 0 {
					graph.AddArcById(flowgraph.NodeID(line), flowgraph.NodeID(index)).CapUpperBound = uint64(flowInt)
				}
			}
		}
		line++
	}

	return graph
}

func TestEdmondsKarp(t *testing.T) {
	graph := generateGraph()
	maxflow := EdmondsKarp(graph, graph.SourceID, graph.SinkID, true, false)
	if maxflow != 23 {
		t.Errorf("max flow should be 23 but is %v\n", maxflow)
	} else {
		fmt.Printf("maximum flow should be 23, result is %v\n", maxflow)
	}

	graph = generateGraph()
	maxflow = EdmondsKarp(graph, graph.SourceID, graph.SinkID, false, false)
	if maxflow != 23 {
		t.Errorf("max flow should be 23 but is %v\n", maxflow)
	} else {
		fmt.Printf("maximum flow should be 23, result is %v\n", maxflow)
	}
}

func TestEdmondsKarpLargeCase(t *testing.T) {
	graph := generateGraphFromFile("/Users/weitao92/Documents/go/src/nickren/firmament-go/pkg/scheduling/algorithms/testfiles/test0.txt")
	maxflow := EdmondsKarp(graph, graph.SourceID, graph.SinkID, true, false)
	if maxflow != 256 {
		t.Errorf("max flow should be 256 but is %v\n", maxflow)
	}

	graph = generateGraphFromFile("/Users/weitao92/Documents/go/src/nickren/firmament-go/pkg/scheduling/algorithms/testfiles/test1.txt")
	maxflow = EdmondsKarp(graph, graph.SourceID, graph.SinkID, true, false)
	if maxflow != 2789 {
		t.Errorf("max flow should be 2789 but is %v\n", maxflow)
	}
}

func TestEdmondsKarpWithConstraint(t *testing.T) {
	graph := generateAdhocGraph()
	maxflow := EdmondsKarp(graph, graph.SourceID, graph.SinkID, false, false)
	if maxflow != 15 {
		t.Errorf("max flow should be 15 but is %v\n", maxflow)
	} else {
		fmt.Printf("maximum flow should be 15, result is %v\n", maxflow)
	}
	scheduleResult := utils.ExtractScheduleResult(graph, 1)
	for mapping, flow := range scheduleResult {
		if flow != 0 {
			fmt.Printf("task %v flow %v to machine %v\n", mapping.TaskId, flow, mapping.ResourceId)
		} else {
			if mapping.ResourceId == 0 {
				fmt.Printf("task %v is unscheduled\n", mapping.TaskId)
			}
		}
	}
	fmt.Println("")
	scheduleResult, repairCount := utils.GreedyRepairFlow(graph, scheduleResult, 7)
	fmt.Printf("After the greedy repair, %v tasks got repaired", repairCount)

	for mapping, flow := range scheduleResult {
		if flow != 0 {
			fmt.Printf("task %v flow %v to machine %v\n", mapping.TaskId, flow, mapping.ResourceId)
		} else {
			if mapping.ResourceId == 0 {
				fmt.Printf("task %v is unscheduled\n", mapping.TaskId)
			}
		}
	}

	graph = generateAdhocGraph()
	maxflow = EdmondsKarp(graph, graph.SourceID, graph.SinkID, true, false)
	if maxflow != 15 {
		t.Errorf("max flow should be 15 but is %v\n", maxflow)
	} else {
		fmt.Printf("maximum flow should be 15, result is %v\n", maxflow)
	}
	scheduleResult = utils.ExtractScheduleResult(graph, 1)
	for mapping, flow := range scheduleResult {
		if flow != 0 {
			fmt.Printf("task %v flow %v to machine %v\n", mapping.TaskId, flow, mapping.ResourceId)
		} else {
			if mapping.ResourceId == 0 {
				fmt.Printf("task %v is unscheduled\n", mapping.TaskId)
			}
		}
	}
	fmt.Println("")
	scheduleResult, repairCount = utils.GreedyRepairFlow(graph, scheduleResult, 7)
	fmt.Printf("After the greedy repair, %v tasks got repaired", repairCount)

	for mapping, flow := range scheduleResult {
		if flow != 0 {
			fmt.Printf("task %v flow %v to machine %v\n", mapping.TaskId, flow, mapping.ResourceId)
		} else {
			if mapping.ResourceId == 0 {
				fmt.Printf("task %v is unscheduled\n", mapping.TaskId)
			}
		}
	}

	graph = generateAdhocGraph1()
	maxflow = EdmondsKarp(graph, graph.SourceID, graph.SinkID, false, false)
	if maxflow != 14 {
		t.Errorf("max flow should be 14 but is %v\n", maxflow)
	} else {
		fmt.Printf("maximum flow should be 14, result is %v\n", maxflow)
	}
	scheduleResult = utils.ExtractScheduleResult(graph, 1)
	for mapping, flow := range scheduleResult {
		if flow != 0 {
			fmt.Printf("task %v flow %v to machine %v\n", mapping.TaskId, flow, mapping.ResourceId)
		} else {
			if mapping.ResourceId == 0 {
				fmt.Printf("task %v is unscheduled\n", mapping.TaskId)
			}
		}
	}
	fmt.Println("")
	scheduleResult, repairCount = utils.GreedyRepairFlow(graph, scheduleResult, 7)
	fmt.Printf("After the greedy repair, %v tasks got repaired", repairCount)

	for mapping, flow := range scheduleResult {
		if flow != 0 {
			fmt.Printf("task %v flow %v to machine %v\n", mapping.TaskId, flow, mapping.ResourceId)
		} else {
			if mapping.ResourceId == 0 {
				fmt.Printf("task %v is unscheduled\n", mapping.TaskId)
			}
		}
	}

	graph = generateAdhocGraph1()
	maxflow = EdmondsKarp(graph, graph.SourceID, graph.SinkID, true, false)
	if maxflow != 14 {
		t.Errorf("max flow should be 14 but is %v\n", maxflow)
	} else {
		fmt.Printf("maximum flow should be 14, result is %v\n", maxflow)
	}
	scheduleResult = utils.ExtractScheduleResult(graph, 1)
	for mapping, flow := range scheduleResult {
		if flow != 0 {
			fmt.Printf("task %v flow %v to machine %v\n", mapping.TaskId, flow, mapping.ResourceId)
		} else {
			if mapping.ResourceId == 0 {
				fmt.Printf("task %v is unscheduled\n", mapping.TaskId)
			}
		}
	}
	fmt.Println("")
	scheduleResult, repairCount = utils.GreedyRepairFlow(graph, scheduleResult, 7)
	fmt.Printf("After the greedy repair, %v tasks got repaired", repairCount)

	for mapping, flow := range scheduleResult {
		if flow != 0 {
			fmt.Printf("task %v flow %v to machine %v\n", mapping.TaskId, flow, mapping.ResourceId)
		} else {
			if mapping.ResourceId == 0 {
				fmt.Printf("task %v is unscheduled\n", mapping.TaskId)
			}
		}
	}
}

func TestEdmondsKarpWithLargeGraph(t *testing.T) {
	graph := generateRandomGraph(100, 10000, 5, 100)
	maxFlow := EdmondsKarp(graph, 1, 10102, false, false)
	fmt.Printf("maxflow %v", maxFlow)
}


