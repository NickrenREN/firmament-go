package costmodel

import (
	"log"
	pb "nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	util "nickren/firmament-go/pkg/scheduling/utility"
)

var _ CostModeler = new(directMappingCostModel)

type directMappingCostModel struct {
	resourceMap              *util.ResourceMap
	taskMap                  *util.TaskMap
	leafResIDset             map[util.ResourceID]struct{}
	machineToResTopo         map[util.ResourceID]*pb.ResourceTopologyNodeDescriptor
	maxTasksPerMachine       uint64
	taskToRequestSlots       map[util.TaskID]RequestSlots
	jobToRequestSlots        map[util.JobID]RequestSlots
	machineToResourceSlots   map[util.ResourceID]MachineResourceSlots
	sumTaskRequestSlots      RequestSlots
	sumMachineCapacitySlots  RequestSlots
	sumMachineAvailableSlots RequestSlots
}

const (
	Unschedule_Factor uint64 = 10
	baseDelta         int64  = 10001
	maxCapacity       int64  = 100
)

// NewDirectMapping return a new direct-mapping cost model
func NewCostModel(resourceMap *util.ResourceMap, taskMap *util.TaskMap, leafResIDset map[util.ResourceID]struct{}, maxTasksPerMachine uint64) *directMappingCostModel {
	return &directMappingCostModel{
		resourceMap:              resourceMap,
		taskMap:                  taskMap,
		leafResIDset:             leafResIDset,
		machineToResTopo:         make(map[util.ResourceID]*pb.ResourceTopologyNodeDescriptor),
		maxTasksPerMachine:       maxTasksPerMachine,
		taskToRequestSlots:       make(map[util.TaskID]RequestSlots),
		jobToRequestSlots:        make(map[util.JobID]RequestSlots),
		machineToResourceSlots:   make(map[util.ResourceID]MachineResourceSlots),
		sumTaskRequestSlots:      RequestSlots(0),
		sumMachineCapacitySlots:  RequestSlots(0),
		sumMachineAvailableSlots: RequestSlots(0),
	}
}

func (dmc *directMappingCostModel) TaskToUnscheduledAgg(taskID util.TaskID) ArcDescriptor {
	taskDescriptor := dmc.taskMap.FindPtrOrNull(taskID)
	if taskDescriptor == nil {
		log.Panicf("get taskID %v failed ", taskID)
	}
	// TODO: check unscheduled time is valid
	waitTime := taskDescriptor.TotalUnscheduledTime
	capacity := dmc.getSlotsByTaskID(taskID)
	return NewArcDescriptor(int64(waitTime*Unschedule_Factor)+baseDelta, uint64(capacity), 0)
}

func (dmc *directMappingCostModel) UnscheduledAggToSink(id util.JobID) ArcDescriptor {
	//fmt.Printf("debug unschedule : %v; %v; capacity %d\n", id, dmc.jobToRequestSlots, dmc.jobToRequestSlots[id])
	capacity := dmc.jobToRequestSlots[id]
	return NewArcDescriptor(0, uint64(capacity), 0)
}

func (dmc *directMappingCostModel) TaskToResourceNode(taskID util.TaskID, resourceID util.ResourceID) ArcDescriptor {
	requestSlots := dmc.getSlotsByTaskID(taskID)
	machineResourceSlots := dmc.getSlotsByMachineID(resourceID)
	capacity := machineResourceSlots.CapacitySlots
	usage := machineResourceSlots.UsedSlots
	availableSlots := capacity - usage
	if requestSlots > availableSlots {
		return NewArcDescriptor(0, 0, 0)
	}
	// TODO: not implement balanced slots x because the performance of x is bad
	//x := dmc.getBalancedSlots()
	x := 1.0

	var factor int64 = 1
	expectCapacity := float64(capacity) * x
	cost := float64(maxCapacity*maxCapacity) / ((expectCapacity - float64(usage)) * float64(requestSlots))
	//log.Printf("resourceID %d 's capacity is %d, expectCapacity is %d, usage is %d, requestSlots is %d "+
	//	"and cost is %d\n", resourceID, capacity, expectCapacity, usage, requestSlots, cost)
	cost = normalizeCost(cost, 1, 10000, 1, 100)
	return NewArcDescriptor(int64(cost)*factor, uint64(requestSlots), 0)
}

func (dmc *directMappingCostModel) ResourceNodeToResourceNode(source, destination *pb.ResourceDescriptor) ArcDescriptor {
	// No need to implement
	return NewArcDescriptor(0, 0, 0)
}

func (dmc *directMappingCostModel) LeafResourceNodeToSink(resourceID util.ResourceID) ArcDescriptor {
	capacity := dmc.getSlotsByMachineID(resourceID)
	return NewArcDescriptor(0, uint64(capacity.CapacitySlots), 0)
}

func (dmc *directMappingCostModel) TaskContinuation(id util.TaskID) ArcDescriptor {
	capacity := dmc.getSlotsByTaskID(id)
	return NewArcDescriptor(0, uint64(capacity), 0)
}

func (dmc *directMappingCostModel) TaskPreemption(util.TaskID) ArcDescriptor {
	return NewArcDescriptor(0, 0, 0)
}

func (dmc *directMappingCostModel) TaskToEquivClassAggregator(util.TaskID, util.EquivClass) ArcDescriptor {
	// No need to implement
	return NewArcDescriptor(0, 0, 0)
}

func (dmc *directMappingCostModel) EquivClassToResourceNode(util.EquivClass, util.ResourceID) ArcDescriptor {
	// No need to implement
	return NewArcDescriptor(0, 0, 0)
}

func (dmc *directMappingCostModel) EquivClassToEquivClass(tec1, tec2 util.EquivClass) ArcDescriptor {
	// No need to implement
	return NewArcDescriptor(0, 0, 0)
}

func (dmc *directMappingCostModel) GetTaskEquivClasses(util.TaskID) []util.EquivClass {
	return nil
}

func (dmc *directMappingCostModel) GetOutgoingEquivClassPrefArcs(ec util.EquivClass) []util.ResourceID {
	return nil
}

func (dmc *directMappingCostModel) GetTaskPreferenceArcs(util.TaskID) []util.ResourceID {
	// TODO: get machine list directly
	resourceIDs := make([]util.ResourceID, 0)
	for resourceID, rtnd := range dmc.machineToResTopo {
		if rtnd.ResourceDesc.Type == pb.ResourceDescriptor_RESOURCE_MACHINE {
			resourceIDs = append(resourceIDs, resourceID)
		}
	}
	return resourceIDs
}

func (dmc *directMappingCostModel) GetEquivClassToEquivClassesArcs(util.EquivClass) []util.EquivClass {
	return nil
}

func (dmc *directMappingCostModel) AddMachine(r *pb.ResourceTopologyNodeDescriptor) {
	id, err := util.ResourceIDFromString(r.ResourceDesc.Uuid)
	if err != nil {
		log.Panicln(err)
	}
	if _, ok := dmc.machineToResTopo[id]; !ok {
		dmc.machineToResTopo[id] = r
	}
	capacity := dmc.getSlotsByMachineID(id)
	r.ResourceDesc.NumSlotsBelow = uint64(capacity.CapacitySlots)
	return
}

func (dmc *directMappingCostModel) AddTask(id util.TaskID) {
	_ = dmc.getSlotsByTaskID(id)
	return
}

func (dmc *directMappingCostModel) RemoveMachine(id util.ResourceID) {
	if _, ok := dmc.machineToResTopo[id]; ok {
		delete(dmc.machineToResTopo, id)
	} else {
		log.Panicf("resource id %d has been deleted or not existed", id)
	}
	if _, ok := dmc.machineToResourceSlots[id]; ok {
		dmc.sumMachineCapacitySlots -= dmc.machineToResourceSlots[id].CapacitySlots
		delete(dmc.machineToResourceSlots, id)
	} else {
		log.Panicf("resource id %d has been deleted or not existed", id)
	}
	return
}

func (dmc *directMappingCostModel) RemoveTask(id util.TaskID) {
	if _, ok := dmc.taskToRequestSlots[id]; ok {
		dmc.sumTaskRequestSlots -= dmc.taskToRequestSlots[id]
		if td := dmc.taskMap.FindPtrOrNull(id); td != nil {
			jobId := util.MustJobIDFromString(td.GetJobId())
			dmc.jobToRequestSlots[jobId] -= dmc.taskToRequestSlots[id]
		} else {
			log.Panicf("")
		}
		delete(dmc.taskToRequestSlots, id)
	} else {
		log.Panicf("")
	}
	return
}

func (dmc *directMappingCostModel) GatherStats(accumulator, other *flowgraph.Node) *flowgraph.Node {
	if !accumulator.IsResourceNode() {
		return accumulator
	}
	if !other.IsResourceNode() {
		if other.Type == flowgraph.NodeTypeSink {
			accumulator.ResourceDescriptor.NumRunningTasksBelow = dmc.getUsedSlotsByMachineID(accumulator)
			dmc.updateResourceSlots(accumulator)
		}
		return accumulator
	}
	if other.ResourceDescriptor == nil {
		log.Panicf("the resourcedescriptor of node (%d) is nil", other.ID)
	}
	return accumulator
}

func (dmc *directMappingCostModel) PrepareStats(accumulator *flowgraph.Node) {
	return
}

func (dmc *directMappingCostModel) UpdateStats(accumulator, other *flowgraph.Node) *flowgraph.Node {
	return accumulator
}

func (dmc *directMappingCostModel) DebugInfo() string {
	return "debug"
}

func (dmc *directMappingCostModel) DebugInfoCSV() string {
	return "debug"
}

func (dmc *directMappingCostModel) getBalancedSlots() float64 {
	usage := dmc.sumMachineCapacitySlots - dmc.sumMachineAvailableSlots
	balancedScores := float64(usage+dmc.sumTaskRequestSlots) / float64(dmc.sumMachineCapacitySlots)
	//log.Printf("balacned slots number : %d", balancedScores)
	if balancedScores >= 1.0 {
		balancedScores = 1.0
	}
	return balancedScores
}

func (dmc *directMappingCostModel) getSlotsByTaskID(id util.TaskID) RequestSlots {
	if _, ok := dmc.taskToRequestSlots[id]; !ok {
		if tdPtr := dmc.taskMap.FindPtrOrNull(id); tdPtr != nil {
			requestSlots := NewRequestSlots(tdPtr.ResourceRequest)
			dmc.taskToRequestSlots[id] = requestSlots
			jobID := util.MustJobIDFromString(tdPtr.GetJobId())
			dmc.jobToRequestSlots[jobID] += requestSlots
			dmc.sumTaskRequestSlots += requestSlots
			return requestSlots
		} else {
			log.Panicf("Can not get task descriptor for task id %d", id)
		}
	}
	return dmc.taskToRequestSlots[id]
}

func (dmc *directMappingCostModel) getSlotsByMachineID(id util.ResourceID) MachineResourceSlots {
	if _, ok := dmc.machineToResourceSlots[id]; !ok {
		if rtndPtr, ok := dmc.machineToResTopo[id]; ok {
			capacitySlots := NewRequestSlots(rtndPtr.ResourceDesc.ResourceCapacity)
			usedSlots := RequestSlots(0)
			machineResourceSlots := NewMachineResourceSlots(capacitySlots, usedSlots)
			dmc.sumMachineCapacitySlots += capacitySlots
			dmc.sumMachineAvailableSlots += capacitySlots - usedSlots
			dmc.machineToResourceSlots[id] = machineResourceSlots
			return machineResourceSlots
		} else {
			log.Panicf("Can not get rtnd for resource id %d", id)
		}
	}
	return dmc.machineToResourceSlots[id]
}

func (dmc *directMappingCostModel) getUsedSlotsByMachineID(node *flowgraph.Node) uint64 {
	if len(node.ResourceDescriptor.CurrentRunningTasks) == 0 {
		return 0
	}
	var sum uint64 = 0
	for _, taskID := range node.ResourceDescriptor.CurrentRunningTasks {
		sum += uint64(dmc.getSlotsByTaskID(util.TaskID(taskID)))
	}
	return sum
}

func (dmc *directMappingCostModel) updateResourceSlots(node *flowgraph.Node) {
	oldResourceSlots := dmc.getSlotsByMachineID(node.ResourceID)
	newUsedSlots := RequestSlots((node.ResourceDescriptor.NumRunningTasksBelow))
	dmc.sumMachineAvailableSlots += oldResourceSlots.UsedSlots
	dmc.sumMachineAvailableSlots -= newUsedSlots
	newResourceSlots := NewMachineResourceSlots(oldResourceSlots.CapacitySlots, newUsedSlots)

	dmc.machineToResourceSlots[node.ResourceID] = newResourceSlots
}
func normalizeCost(cost, minBefore, maxBefore, minAfter, maxAfter float64) float64 {
	return (maxAfter-minAfter)*((cost-minBefore)/(maxBefore-minBefore)) + minAfter
}
