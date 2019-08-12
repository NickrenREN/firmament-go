package utils

import (
	"container/heap"
	"fmt"
	"github.com/aybabtme/uniplot/histogram"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	"nickren/firmament-go/pkg/scheduling/utility"
	"os"
	"sort"
)

type Mapping struct {
	TaskId     flowgraph.NodeID
	ResourceId flowgraph.NodeID
}

type TaskStruct struct {
	TaskId flowgraph.NodeID
	Flow   uint64
}

type MachineStruct struct {
	MachineId flowgraph.NodeID
	Residual  uint64
}

type BinaryMinHeap []*MachineStruct

func (pq BinaryMinHeap) Len() int { return len(pq) }

func (pq BinaryMinHeap) Less(i, j int) bool {
	return pq[i].Residual > pq[j].Residual
}

func (pq BinaryMinHeap) Swap(i, j int) {
	if i < 0 || j < 0 {
		return
	}
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *BinaryMinHeap) Push(x interface{}) {
	*pq = append(*pq, x.(*MachineStruct))
}

func (pq *BinaryMinHeap) Pop() interface{} {
	old := *pq
	n := len(old)
	if n == 0 {
		return nil
	}
	x := old[n-1]
	*pq = old[0 : n-1]
	return x
}

func ExtractScheduleResult(graph *flowgraph.Graph, sourceId flowgraph.NodeID) map[Mapping]uint64 {
	scheduleResult := make(map[Mapping]uint64)

	for task, _ := range graph.TaskSet {
		for id, arc := range task.IncomingArcMap {
			if id == sourceId {
				continue
			}
			m := Mapping{arc.DstNode.ID, arc.SrcNode.ID}
			if _, ok := scheduleResult[m]; ok {
				scheduleResult[m] += arc.CapUpperBound
			} else {
				scheduleResult[m] = arc.CapUpperBound
			}
		}
	}

	return scheduleResult
}

func GreedyRepairFlow(graph *flowgraph.Graph, scheduleResult map[Mapping]uint64, sinkId flowgraph.NodeID) (map[Mapping]uint64, int) {
	machineResidual := make(map[flowgraph.NodeID]uint64)
	for machine, _ := range graph.ResourceSet {
		machineResidual[machine.ID] = machine.GetResidualy(sinkId)
	}

	jobIdToUnMap := make(map[utility.JobID]*flowgraph.Node)
	for _, node := range graph.NodeMap {
		if node.Type == flowgraph.NodeTypeJobAggregator {
			jobIdToUnMap[node.JobID] = node
		}
	}

	whatever := make(map[flowgraph.NodeID][]flowgraph.NodeID)
	rescheduleMap := make(map[flowgraph.NodeID]uint64)
	for mapping, _ := range scheduleResult {
		if _, ok := whatever[mapping.TaskId]; ok {
			whatever[mapping.TaskId] = append(whatever[mapping.TaskId], mapping.ResourceId)
		} else {
			whatever[mapping.TaskId] = make([]flowgraph.NodeID, 0)
			whatever[mapping.TaskId] = append(whatever[mapping.TaskId], mapping.ResourceId)
		}
	}

	repairCount := 0
	for taskId, list := range whatever {
		if len(list) > 1 {
			repairCount++
			rescheduleMap[taskId] = 0
			for _, machineId := range list {
				m := Mapping{taskId, machineId}
				flow := scheduleResult[m]
				machineResidual[machineId] += flow
				scheduleResult[m] = 0
				rescheduleMap[taskId] += flow
			}
		}
	}

	taskSlice := make([]TaskStruct, len(rescheduleMap))
	index := 0
	for taskId, flow := range rescheduleMap {
		taskSlice[index] = TaskStruct{taskId, flow}
		index++
	}
	sort.Slice(taskSlice, func(i, j int) bool {
		return taskSlice[i].Flow > taskSlice[j].Flow
	})

	pq := make(BinaryMinHeap, len(machineResidual))
	i := 0
	for machineId, residual := range machineResidual {
		pq[i] = &MachineStruct{machineId, residual}
		i++
	}
	heap.Init(&pq)

	for _, rescheduleTask := range taskSlice {
		machineWithLargestResidual := heap.Pop(&pq).(*MachineStruct)
		if rescheduleTask.Flow <= machineWithLargestResidual.Residual {
			scheduleResult[Mapping{rescheduleTask.TaskId,
				machineWithLargestResidual.MachineId}] = rescheduleTask.Flow
			heap.Push(&pq, &MachineStruct{machineWithLargestResidual.MachineId,
				machineWithLargestResidual.Residual - rescheduleTask.Flow})
		} else {
			heap.Push(&pq, machineWithLargestResidual)
			scheduleResult[Mapping{rescheduleTask.TaskId,
				jobIdToUnMap[graph.Node(rescheduleTask.TaskId).JobID].ID}] = rescheduleTask.Flow

			fmt.Printf("task %v is scheduled to UN node %v, flow is %v", rescheduleTask.TaskId,
				jobIdToUnMap[graph.Node(rescheduleTask.TaskId).JobID].ID, rescheduleTask.Flow)
		}
	}

	return scheduleResult, repairCount
}

func ExamCostModel(graph *flowgraph.Graph, tm map[flowgraph.NodeID]flowgraph.NodeID) {
	capacityMap := make(map[flowgraph.NodeID]uint64)
	usageMap := make(map[flowgraph.NodeID]uint64)
	var totalFreeSlots uint64 = 0
	var totalUnScheduledSlots uint64 = 0
	for node, _ := range graph.ResourceSet {
		outArc := graph.GetArcByIds(node.ID, graph.SinkID)
		inArc := graph.GetArcByIds(graph.SinkID, node.ID)
		var machineCapacity uint64
		if outArc != nil {
			machineCapacity += outArc.CapUpperBound
		}
		if inArc != nil {
			machineCapacity += inArc.CapUpperBound
		}
		capacityMap[node.ID] = machineCapacity
		totalFreeSlots += machineCapacity
	}
	machineTotalCapacity := totalFreeSlots

	var totalTaskRequest uint64 = 0
	for taskId, machineId := range tm {
		taskId = graph.OriginalIdToCopyIdMap[taskId]
		machineId = graph.OriginalIdToCopyIdMap[machineId]
		srcNode := graph.Node(taskId)
		dstNode := graph.Node(machineId)
		totalTaskRequest += uint64(srcNode.Excess)
		if dstNode.Type == flowgraph.NodeTypeJobAggregator {
			totalUnScheduledSlots += uint64(srcNode.Excess)
		} else {
			totalFreeSlots -= uint64(srcNode.Excess)
			if _, ok := usageMap[machineId]; ok {
				usageMap[machineId] += uint64(srcNode.Excess)
			} else {
				usageMap[machineId] = uint64(srcNode.Excess)
			}
		}
	}

	fmt.Printf("machine total capacity %v, task total request %v, After the MCMF schedule, there are %v unscheduled slots and %v free slots, unscheduled percentage is %v\n",
		machineTotalCapacity, totalTaskRequest, totalUnScheduledSlots, totalFreeSlots, float64(totalUnScheduledSlots)/float64(totalTaskRequest))

	usagePercentage := make([]float64, len(capacityMap))
	index := 0
	for id, capacity := range capacityMap {
		if usage, ok := usageMap[id]; !ok {
			usagePercentage[index] = 0
		} else {
			usagePercentage[index] = float64(usage) / float64(capacity)
		}
		index++
	}

	hist := histogram.Hist(10, usagePercentage)
	histogram.Fprint(os.Stdout, hist, histogram.Linear(5))
}
