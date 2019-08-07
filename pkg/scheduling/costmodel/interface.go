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

// Cost model interface implemented by the cost model(s) implementations like coco
// and used by the flow graph

package costmodel

import (
	"math"
	pb "nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	util "nickren/firmament-go/pkg/scheduling/utility"
)

type (
	Cost          int64
	CostModelType int64
)

type Gather func(accumulator, other *flowgraph.Node) *flowgraph.Node

type Prepare func(accumulator *flowgraph.Node)

type Update func(accumulator, other *flowgraph.Node) *flowgraph.Node

//Enum for list of cost models supported
const (
	CostModelTrivial CostModelType = iota
	CostModelRandom
	CostModelSjf
	CostModelQuincy
	CostModelWhare
	CostModelCoco
	CostModelOctopus
	CostModelVoid
	CostModelNet
)

var (
	ClusterAggregatorEC = util.HashBytesToEquivClass([]byte("CLUSTER_AGG"))
)

type ArcDescriptor struct {
	Cost     int64
	Capacity uint64
	MinFlow  uint64
	Gain     float64
}

func NewArcDescriptor(cost int64, capacity, minFlow uint64) ArcDescriptor {
	return ArcDescriptor{
		Cost:     cost,
		Capacity: capacity,
		MinFlow:  minFlow,
		Gain:     1.0,
	}
}

type RequestSlots int64

type MachineResourceSlots struct {
	CapacitySlots  RequestSlots
	AvailableSlots RequestSlots
}

// TODO: add test
// GetRequestSlots calculate requested slots according ResourceVector
func NewRequestSlots(request *pb.ResourceVector) RequestSlots {
	// TODO: machine need ceil, but task need floor
	requestCPUNum := math.Ceil(float64(request.GetCpuCores()))
	r := float64(request.GetRamCap()) / 4
	r = r / float64(1024)
	slots := math.Min(r, requestCPUNum)
	return RequestSlots(math.Ceil(slots))
}

func NewMachineResourceSlots(capacitySlots, availableSlots RequestSlots) MachineResourceSlots {
	return MachineResourceSlots{
		CapacitySlots:  capacitySlots,
		AvailableSlots: availableSlots,
	}
}

// CostModeler provides APIs:
// - Tell the cost of arcs so that graph manager can apply them in graph.
// - Add, remove tasks, machines (resources) for the cost modeler to update
//   knowledge. It should be refactored out.
// - Stats related for cost calculation.
type CostModeler interface {
	// Get the cost from a task node to its unscheduled aggregator node.
	// The method should return a monotonically increasing value upon subsequent
	// calls. It is used to adjust the cost of leaving a task unscheduled after
	// each iteration.
	TaskToUnscheduledAgg(util.TaskID) ArcDescriptor

	// TODO(ionel): The returned capacity is ignored because the cost models
	// do not set it correctly
	UnscheduledAggToSink(util.JobID) ArcDescriptor

	// Get the cost, the capacity and the minimum flow of a preference arc from a task node to a resource node.
	TaskToResourceNode(util.TaskID, util.ResourceID) ArcDescriptor

	// Get the cost, the capacity and the minimum flow of an arc between two resource nodes.

	ResourceNodeToResourceNode(source, destination *pb.ResourceDescriptor) ArcDescriptor

	// Get the cost, the capacity and the minimum flow of an arc from a resource to the sink.
	LeafResourceNodeToSink(util.ResourceID) ArcDescriptor

	// Costs pertaining to preemption (i.e. already running tasks)
	TaskContinuation(util.TaskID) ArcDescriptor
	TaskPreemption(util.TaskID) ArcDescriptor

	// Get the cost, the capacity and the minimum flow of an arc from a task node to an equivalence class node.
	TaskToEquivClassAggregator(util.TaskID, util.EquivClass) ArcDescriptor

	// Get the cost, the capacity and the minimum flow of an arc from an equivalence class node to a resource node
	EquivClassToResourceNode(util.EquivClass, util.ResourceID) ArcDescriptor

	// Get the cost, the capacity and the minimum flow of an arc from an equivalence class node to
	// another equivalence class node.
	// @param tec1 the source equivalence class
	// @param tec2 the destination equivalence class
	EquivClassToEquivClass(tec1, tec2 util.EquivClass) ArcDescriptor

	// Get the equivalence classes of a task.
	// @param task_id the task id for which to get the equivalence classes
	// @return a vector containing the task's equivalence classes
	GetTaskEquivClasses(util.TaskID) []util.EquivClass

	// Get the resource ids to which an equivalence class has arcs.
	// @param ec the equivalence class for which to get the resource ids
	GetOutgoingEquivClassPrefArcs(ec util.EquivClass) []util.ResourceID

	// Get the resource preference arcs of a task.
	// @param task_id the id of the task for which to get the preference arcs
	GetTaskPreferenceArcs(util.TaskID) []util.ResourceID

	// Get equivalence classes to which the outgoing arcs of an equivalence class
	// are pointing to.
	// @return a array of equivalence classes to which we have an outgoing arc.
	GetEquivClassToEquivClassesArcs(util.EquivClass) []util.EquivClass

	// Called by the flow_graph when a machine is added.
	AddMachine(*pb.ResourceTopologyNodeDescriptor)

	// Called by the flow graph when a task is submitted.
	AddTask(util.TaskID)

	// Called by the flow_graph when a machine is removed.
	RemoveMachine(util.ResourceID)

	RemoveTask(util.TaskID)

	// Gathers statistics during reverse traversal of resource topology (from
	// sink upwards). Called on pairs of connected nodes.
	GatherStats(accumulator, other *flowgraph.Node) *flowgraph.Node

	// The default Prepare action is a no-op. Cost models can override this if
	// they need to perform preparation actions before GatherStats is invoked.
	PrepareStats(accumulator *flowgraph.Node)

	// Generates updates for arc costs in the resource topology.
	UpdateStats(accumulator, other *flowgraph.Node) *flowgraph.Node

	// Handle to pull debug information from cost model; return string.
	DebugInfo() string

	DebugInfoCSV() string
}
