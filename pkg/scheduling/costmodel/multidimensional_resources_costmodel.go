package costmodel

import (
	pb "nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	//"nickren/firmament-go/pkg/scheduling/flowmanager"
	util "nickren/firmament-go/pkg/scheduling/utility"
)

type multidimensionalResourcesCostModel struct {
	resourceMap *util.ResourceMap
	taskMap *util.TaskMap
	// leafResIDset is passed in and maintained by user.
	leafResIDset map[util.ResourceID]struct{}

	// TODO: double check if this is needed
	// graphManager flowmanager.GraphManager

}

func NewCostModel(resourceMap *util.ResourceMap, taskMap *util.TaskMap, leafResIDset map[util.ResourceID]struct{}) CostModeler {
	return &multidimensionalResourcesCostModel{
		resourceMap: resourceMap,
		taskMap: taskMap,
		leafResIDset: leafResIDset,
	}
}

/*func (mrc *multidimensionalResourcesCostModel) SetFlowGraphManager(manager flowmanager.GraphManager) {
	// mrc.graphManager = manager
}*/

func (mrc *multidimensionalResourcesCostModel) TaskToUnscheduledAgg(util.TaskID) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) UnscheduledAggToSink(util.JobID) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) TaskToResourceNode(util.TaskID, util.ResourceID) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) ResourceNodeToResourceNode(source, destination *pb.ResourceDescriptor) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) LeafResourceNodeToSink(util.ResourceID) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) TaskContinuation(util.TaskID) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) TaskPreemption(util.TaskID) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) TaskToEquivClassAggregator(util.TaskID, util.EquivClass) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) EquivClassToResourceNode(util.EquivClass, util.ResourceID) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) EquivClassToEquivClass(tec1, tec2 util.EquivClass) ArcDescriptor {
	return ArcDescriptor{}
}

func (mrc *multidimensionalResourcesCostModel) GetTaskEquivClasses(util.TaskID) []util.EquivClass {
	return nil
}

func (mrc *multidimensionalResourcesCostModel) GetOutgoingEquivClassPrefArcs(ec util.EquivClass) []util.ResourceID {
	return nil
}

func (mrc *multidimensionalResourcesCostModel) GetTaskPreferenceArcs(util.TaskID) []util.ResourceID {
	return nil
}

func (mrc *multidimensionalResourcesCostModel) GetEquivClassToEquivClassesArcs(util.EquivClass) []util.EquivClass {
	return nil
}

func (mrc *multidimensionalResourcesCostModel) AddMachine(*pb.ResourceTopologyNodeDescriptor) {

}

func (mrc *multidimensionalResourcesCostModel) AddTask(util.TaskID) {

}

func (mrc *multidimensionalResourcesCostModel) RemoveMachine(util.ResourceID) {

}

func (mrc *multidimensionalResourcesCostModel) RemoveTask(util.TaskID) {

}

func (mrc *multidimensionalResourcesCostModel) GatherStats(accumulator, other *flowgraph.Node) *flowgraph.Node {
	return nil
}

func (mrc *multidimensionalResourcesCostModel) PrepareStats(accumulator *flowgraph.Node) {

}

func (mrc *multidimensionalResourcesCostModel) UpdateStats(accumulator, other *flowgraph.Node) *flowgraph.Node {
	return nil
}

func (mrc *multidimensionalResourcesCostModel) DebugInfo() string {
	return "Debug Info"
}

func (mrc *multidimensionalResourcesCostModel) DebugInfoCSV() string {
	return "Debug Info CSV"
}
