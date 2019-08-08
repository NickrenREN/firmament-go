package mcmf

import (
	"fmt"
	"nickren/firmament-go/pkg/scheduling/algorithms/utils"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	"testing"
)

func generateRandomGraph(taskNum, machineNum, request, machineCap int) *flowgraph.Graph {
	graph := flowgraph.NewGraph(false)
	for i := 0; i < taskNum + machineNum + 2; i++ {
		graph.AddNode()
	}
	graph.SourceID = 1
	graph.SinkID = flowgraph.NodeID(taskNum + machineNum + 2)
	for i := 2; i <= 1 + taskNum; i++ {
		graph.AddArcWithCapAndCost(1, flowgraph.NodeID(i), uint64(request), 0)
		graph.Node(flowgraph.NodeID(i)).Excess = int64(request)
		graph.Node(flowgraph.NodeID(i)).Type = flowgraph.NodeTypeUnscheduledTask
		graph.TaskSet[graph.Node(flowgraph.NodeID(i))] = struct{}{}
	}
	for i := 2 + taskNum; i < taskNum + machineNum + 2; i++ {
		graph.AddArcWithCapAndCost(flowgraph.NodeID(i), flowgraph.NodeID(taskNum + machineNum + 2), uint64(machineCap), 0)
		graph.Node(flowgraph.NodeID(i)).Type = flowgraph.NodeTypeMachine
	}
	for i := 2; i <= 1 + taskNum; i++ {
		for j := 2 + taskNum; j < taskNum + machineNum + 2; j++ {
			graph.AddArcWithCapAndCost(flowgraph.NodeID(i), flowgraph.NodeID(j), uint64(request), 5)
		}
	}

	return graph
}

func genetaeRandomOptimizedGraph(taskNum, machineNum, request, machineCap int) *flowgraph.Graph {
	graph := flowgraph.NewGraph(false)
	intermediate := 100
	for i := 0; i < taskNum + machineNum + intermediate + 2; i++ {
		graph.AddNode()
	}
	graph.SourceID = 1
	graph.SinkID = flowgraph.NodeID(taskNum + machineNum + intermediate + 2)
	for i := 2; i <= 1 + taskNum; i++ {
		graph.AddArcWithCapAndCost(1, flowgraph.NodeID(i), uint64(request), 0)
	}
	for i := 2; i <= 1 + taskNum; i++ {
		graph.AddArcWithCapAndCost(flowgraph.NodeID(i), flowgraph.NodeID(2 + taskNum + (i % 100)),
			uint64(request), 0)
		graph.Node(flowgraph.NodeID(2 + taskNum + (i % 100))).Excess += int64(request)
	}
	for i := 2 + taskNum + intermediate; i < taskNum + machineNum + intermediate + 2; i++ {
		graph.AddArcWithCapAndCost(flowgraph.NodeID(i), flowgraph.NodeID(taskNum + machineNum + intermediate + 2),
			uint64(machineCap), 0)

	}

	var cost int64 = 1
	for i := 2 + taskNum; i <= 1 + taskNum + intermediate; i++ {
		for j := 2 + taskNum + intermediate; j < taskNum + machineNum + intermediate + 2; j++ {
			graph.AddArcWithCapAndCost(flowgraph.NodeID(i), flowgraph.NodeID(j), uint64(graph.Node(flowgraph.NodeID(i)).Excess),
				5)
			cost++
		}
	}

	return graph
}

func generateGraphWithCostAndCapacity() *flowgraph.Graph {
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


	graph.AddArcWithCapAndCost(1, 2, 5, 0)
	graph.AddArcWithCapAndCost(1, 3, 5, 0)
	graph.AddArcWithCapAndCost(1, 4, 5, 0)
	graph.AddArcWithCapAndCost(2, 5, 5, 5)
	graph.AddArcWithCapAndCost(2, 6, 5, 9)
	graph.AddArcWithCapAndCost(3, 5, 5, 7)
	graph.AddArcWithCapAndCost(3, 6, 5, 8)
	graph.AddArcWithCapAndCost(4, 5, 5, 9)
	graph.AddArcWithCapAndCost(4, 6, 5, 5)
	graph.AddArcWithCapAndCost(5, 7, 8, 0)
	graph.AddArcWithCapAndCost(6, 7, 8, 0)

	return graph
}

func TestBenchSuccessiveShortestPathWithDijkstra(b *testing.T) {
	graph := generateRandomGraph(1000, 10000, 5, 100)
	maxFlow, minCost := SuccessiveShortestPathWithDijkstra(graph, 1, 11002)
	fmt.Printf("maxflow %v, mincost %v\n", maxFlow, minCost)
}

func TestSuccessiveShortestPathWithDEP(t *testing.T) {
	graph := generateGraphWithCostAndCapacity()
	maxFlow, minCost := SuccessiveShortestPathWithDEP(graph, 1, 7)
	if maxFlow != 15 || minCost != 87 {
		t.Errorf("something is wrong")
	}

	graph = generateRandomGraph(100, 10000, 5, 100)
	maxFlow, minCost = SuccessiveShortestPathWithDEP(graph, 1, 10102)
	fmt.Printf("maxflow %v, mincost %v\n", maxFlow, minCost)
}

func TestSuccessiveShortesPathWithDijkstra(t *testing.T) {
	graph := generateGraphWithCostAndCapacity()
	maxFlow, minCost := SuccessiveShortestPathWithDijkstra(graph, 1, 7)
	if maxFlow != 15 || minCost != 87 {
		t.Errorf("something is wrong, maxflow %v, mincost %v", maxFlow, minCost)
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
	scheduleResult = utils.GreedyRepairFlow(graph, scheduleResult, 7)
	for mapping, flow := range scheduleResult {
		if flow != 0 {
			fmt.Printf("task %v flow %v to machine %v\n", mapping.TaskId, flow, mapping.ResourceId)
		} else {
			if mapping.ResourceId == 0 {
				fmt.Printf("task %v is unscheduled\n", mapping.TaskId)
			}
		}
	}
	graph = generateRandomGraph(100, 10000, 5, 100)
	maxFlow, minCost = SuccessiveShortestPathWithDijkstra(graph, 1, 10102)
	fmt.Printf("maxflow %v, mincost %v\n", maxFlow, minCost)
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

	fmt.Println("After the repair")
	scheduleResult = utils.GreedyRepairFlow(graph, scheduleResult, 10102)
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

func TestOptimizedRandomGraph(t *testing.T) {
	graph := genetaeRandomOptimizedGraph(1000, 1000, 5, 100)
	maxFlow, minCost := SuccessiveShortestPathWithDEP(graph, 1, 2002)
	fmt.Printf("maxflow %v, mincost %v\n", maxFlow, minCost)
}

func TestOptimizedRandomGraphWithDijkstra(t *testing.T) {
	graph := genetaeRandomOptimizedGraph(100000, 10000, 5, 100)
	//g1 := flowgraph.CopyGraph(graph)
	maxFlow, minCost := SuccessiveShortestPathWithDijkstra(graph, 1, 110002)
	fmt.Printf("maxflow %v, mincost %v\n", maxFlow, minCost)

	//maxFlow, minCost = SuccessiveShortestPathWithDijkstra(g1, 1, 20002)
	//fmt.Printf("maxflow %v, mincost %v\n", maxFlow, minCost)
}

