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

func (mrc *multidimensionalResourcesCostModel) TaskToUnscheduledAggCost(util.TaskID) Cost {

}

func (mrc *multidimensionalResourcesCostModel) UnscheduledAggToSinkCost(util.JobID) Cost {

}

func (mrc *multidimensionalResourcesCostModel) TaskToResourceNodeCost(util.TaskID, util.ResourceID) Cost {

}

func (mrc *multidimensionalResourcesCostModel) ResourceNodeToResourceNodeCost(source, destination *pb.ResourceDescriptor) Cost {

}

func (mrc *multidimensionalResourcesCostModel) LeafResourceNodeToSinkCost(util.ResourceID) Cost {

}

func (mrc *multidimensionalResourcesCostModel) TaskContinuationCost(util.TaskID) Cost {

}

func (mrc *multidimensionalResourcesCostModel) TaskPreemptionCost(util.TaskID) Cost {

}

func (mrc *multidimensionalResourcesCostModel) TaskToEquivClassAggregator(util.TaskID, util.EquivClass) Cost {

}

func (mrc *multidimensionalResourcesCostModel) EquivClassToResourceNode(util.EquivClass, util.ResourceID) (Cost, uint64) {

}

func (mrc *multidimensionalResourcesCostModel) EquivClassToEquivClass(tec1, tec2 util.EquivClass) (Cost, uint64) {

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