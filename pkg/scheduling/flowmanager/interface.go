package flowmanager

import (
	pb "nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/dimacs"
	"nickren/firmament-go/pkg/scheduling/flowgraph"
	"nickren/firmament-go/pkg/scheduling/utility"
)

// NOTE: GraphManager uses GraphChangeManager to change the graph.
type GraphManager interface {
	LeafNodeIDs() map[flowgraph.NodeID]struct{}
	SinkNode() *flowgraph.Node
	GraphChangeManager() GraphChangeManager

	AddOrUpdateJobNodes(jobs []*pb.JobDescriptor)

	// TODO: do we really need this method? this is just a wrapper around AddOrUpdateJobNodes
	UpdateTimeDependentCosts(jobs []*pb.JobDescriptor)

	// AddResourceTopology adds the entire resource topology tree. The method
	// also updates the statistics of the nodes up to the root resource.
	AddResourceTopology(topo *pb.ResourceTopologyNodeDescriptor)

	UpdateResourceTopology(rtnd *pb.ResourceTopologyNodeDescriptor)

	// NOTE: The original interface passed in pointers to member functions of the costModeler
	// Now we just call the costModeler methods directly
	ComputeTopologyStatistics(node *flowgraph.Node)

	JobCompleted(id utility.JobID)

	JobRemoved(id utility.JobID)

	// Notes from xiang90: I modified the interface a little bit. Originally, the
	// interface would modify the passed in delta array by appending the scheduling delta.
	// This is not easy to be done in go. Rr it is not the common way to do it. We return
	// the delta instead. Users can just append it to the delta array themselves.
	NodeBindingToSchedulingDelta(taskNodeID, resourceNodeID flowgraph.NodeID,
		taskBindings map[utility.TaskID]utility.ResourceID) *pb.SchedulingDelta

	// NOTE(haseeb): Returns a slice of deltas for the user to append
	SchedulingDeltasForPreemptedTasks(taskMapping TaskMapping, rmap *utility.ResourceMap) []pb.SchedulingDelta

	// As a result of task state change, preferences change or
	// resource removal we may end up with unconnected equivalence
	// class nodes. This method makes sure they are removed.
	// We cannot end up with unconnected unscheduled agg nodes,
	// task or resource nodes.
	PurgeUnconnectedEquivClassNodes()

	//  Removes the entire resource topology tree rooted at rd. The method also
	//  updates the statistics of the nodes up to the root resource.
	//  NOTE: Interface changed to return a slice of PUs to be removed by the caller
	RemoveResourceTopology(rd *pb.ResourceDescriptor) []flowgraph.NodeID

	TaskCompleted(id utility.TaskID) flowgraph.NodeID
	TaskEvicted(id utility.TaskID, rid utility.ResourceID)
	TaskFailed(id utility.TaskID)
	TaskKilled(id utility.TaskID)
	TaskMigrated(id utility.TaskID, from, to utility.ResourceID)
	TaskScheduled(id utility.TaskID, rid utility.ResourceID)

	// Update each task's arc to its unscheduled aggregator. Moreover, for
	// running tasks we update their continuation costs.
	UpdateAllCostsToUnscheduledAggs()
}

// The GraphChangeManager bridges GraphManager and Graph. Every
// graph change done by the GraphManager should be conducted via
// FlowGraphChangeManager's methods.
// The class stores all the changes conducted in-between two scheduling rounds.
// Moreover, FlowGraphChangeManager applies various algorithms to reduce
// the number of changes (e.g., merges idempotent changes, removes superfluous
// changes).
type GraphChangeManager interface {
	AddArc(src, dst *flowgraph.Node,
		capLowerBound, capUpperBound uint64,
		cost int64,
		arcType flowgraph.ArcType,
		changeType dimacs.ChangeType,
		comment string) *flowgraph.Arc

	AddNode(nodeType flowgraph.NodeType,
		excess int64,
		changeType dimacs.ChangeType,
		comment string) *flowgraph.Node

	ChangeArc(arc *flowgraph.Arc, capLowerBound uint64,
		capUpperBound uint64, cost int64,
		changeType dimacs.ChangeType, comment string)

	ChangeArcCapacity(arc *flowgraph.Arc, capacity uint64,
		changeType dimacs.ChangeType, comment string)

	ChangeArcCost(arc *flowgraph.Arc, cost int64,
		changeType dimacs.ChangeType, comment string)

	DeleteArc(arc *flowgraph.Arc, changeType dimacs.ChangeType, comment string)

	DeleteNode(arc *flowgraph.Node, changeType dimacs.ChangeType, comment string)

	GetGraphChanges() []dimacs.Change

	GetOptimizedGraphChanges() []dimacs.Change

	// ResetChanges resets the incremental changes that the manager keeps.
	// This method should be called after consuming all the recent changes.
	ResetChanges()

	// Graph returns flow graph instance for this manager.
	Graph() *flowgraph.Graph

	CheckNodeType(flowgraph.NodeID, flowgraph.NodeType) bool
}

