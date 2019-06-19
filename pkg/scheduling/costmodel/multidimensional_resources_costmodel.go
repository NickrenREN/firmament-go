package costmodel

import (
	util "nickren/firmament-go/pkg/scheduling/utility"
	pb "nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	"nickren/firmament-go/pkg/scheduling/flowmanager"
)

type multidimensionalResourcesCostModel struct {
	graphManager flowmanager.GraphManager
}

func NewCostModel(graphManager flowmanager.GraphManager) CostModeler {
	return &multidimensionalResourcesCostModel{
		graphManager:graphManager,
	}
}

func (mrc *multidimensionalResourcesCostModel) TaskToUnscheduledAgg(util.TaskID) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) UnscheduledAggToSink(util.JobID) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) TaskToResourceNode(util.TaskID, util.ResourceID) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) ResourceNodeToResourceNode(source, destination *pb.ResourceDescriptor) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) LeafResourceNodeToSink(util.ResourceID) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) TaskContinuation(util.TaskID) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) TaskPreemption(util.TaskID) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) TaskToEquivClassAggregator(util.TaskID, util.EquivClass) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) EquivClassToResourceNode(util.EquivClass, util.ResourceID) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) EquivClassToEquivClass(tec1, tec2 util.EquivClass) ArcDescriptor {

}

func (mrc *multidimensionalResourcesCostModel) GetTaskEquivClasses(util.TaskID) []util.EquivClass {

}

func (mrc *multidimensionalResourcesCostModel) GetOutgoingEquivClassPrefArcs(ec util.EquivClass) []util.ResourceID {

}

func (mrc *multidimensionalResourcesCostModel) GetTaskPreferenceArcs(util.TaskID) []util.ResourceID {

}

func (mrc *multidimensionalResourcesCostModel) GetEquivClassToEquivClassesArcs(util.EquivClass) []util.EquivClass {

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

}

func (mrc *multidimensionalResourcesCostModel) PrepareStats(accumulator *flowgraph.Node) {

}

func (mrc *multidimensionalResourcesCostModel) UpdateStats(accumulator, other *flowgraph.Node) *flowgraph.Node {

}

func (mrc *multidimensionalResourcesCostModel) DebugInfo() string {

}

func (mrc *multidimensionalResourcesCostModel) DebugInfoCSV() string {

}