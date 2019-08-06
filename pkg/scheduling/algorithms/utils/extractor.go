package utils

import (
	"container/heap"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	"sort"
)

type Mapping struct {
	TaskId flowgraph.NodeID
	ResourceId flowgraph.NodeID
}

type TaskStruct struct {
	TaskId flowgraph.NodeID
	Flow uint64
}

type MachineStruct struct {
	MachineId flowgraph.NodeID
	Residual uint64
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

func GreedyRepairFlow(graph *flowgraph.Graph, scheduleResult map[Mapping]uint64, sinkId flowgraph.NodeID) map[Mapping]uint64 {
	machineResidual := make(map[flowgraph.NodeID]uint64)
	for machine, _ := range graph.ResourceSet {
		machineResidual[machine.ID] = machine.GetResidualy(sinkId)
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

	for taskId, list := range whatever {
		if len(list) > 1 {
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
			scheduleResult[Mapping{rescheduleTask.TaskId, 0}] = 0
		}
	}

	return scheduleResult
}

