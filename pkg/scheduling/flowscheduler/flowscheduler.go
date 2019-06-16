package flowscheduler

import (
	"log"

	"nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/costmodel"
	"nickren/firmament-go/pkg/scheduling/dimacs"
	//"nickren/firmament-go/pkg/scheduling/flowgraph"
	"nickren/firmament-go/pkg/scheduling/flowmanager"
	ss "nickren/firmament-go/pkg/scheduling/solver"
	"nickren/firmament-go/pkg/scheduling/utility"
	"time"

)

// Set of tasks
type TaskSet map[utility.TaskID]struct{}

type scheduler struct {
	enableEviction bool
	enableMigration bool

	jobMap *utility.JobMap
	taskMap *utility.TaskMap
	resourceMap *utility.ResourceMap

	// Event driven scheduler specific fields
	// Note: taskBindings tracks the old state of which task maps to which resource (before each iteration).
	TaskBindings map[utility.TaskID]utility.ResourceID
	// Similar to taskBindings but tracks tasks binded to every resource. This is a multimap
	resourceBindings map[utility.ResourceID]TaskSet
	// A vector holding descriptors of the jobs to be scheduled in the next scheduling round.
	jobsToSchedule map[utility.JobID]*proto.JobDescriptor
	// Sets of runnable and blocked tasks in each job. Multimap
	// Originally maintained up by ComputeRunnableTasksForJob() and LazyGraphReduction()
	// by checking and resolving dependencies between tasks. We will avoid that for now
	// and simply declare all tasks as runnable
	runnableTasks map[utility.JobID]TaskSet


	// coordinatorResId utility.ResourceID


	graphManager flowmanager.GraphManager
	solver ss.Solver
	costModel costmodel.CostModeler

	lastUpdateTimeDepentCosts time.Time

	leafResourceIDs map[utility.ResourceID]struct{}

	pusRemovedDuringSolverRun map[uint64]struct{}
	tasksCompletedDuringSloverRun map[uint64]struct{}

	dimacsStats *dimacs.ChangeStats

	solverRunCnt uint64

	resourceRoots map[*proto.ResourceTopologyNodeDescriptor]struct{}
}

func NewScheduler(jobMap *utility.JobMap, resourceMap *utility.ResourceMap, root *proto.ResourceTopologyNodeDescriptor,
	taskMap *utility.TaskMap) Scheduler {
	s := &scheduler{
		jobMap: jobMap,
		resourceMap: resourceMap,
		taskMap: taskMap,

		lastUpdateTimeDepentCosts: time.Now(),
		solverRunCnt: 0,
		leafResourceIDs: make(map[utility.ResourceID]struct{}),

		dimacsStats: &dimacs.ChangeStats{},

		resourceRoots:    make(map[*proto.ResourceTopologyNodeDescriptor]struct{}),
		TaskBindings:     make(map[utility.TaskID]utility.ResourceID),
		resourceBindings: make(map[utility.ResourceID]TaskSet),
		jobsToSchedule:   make(map[utility.JobID]*proto.JobDescriptor),
		runnableTasks:    make(map[utility.JobID]TaskSet),

		tasksCompletedDuringSloverRun: make(map[uint64]struct{}),
		pusRemovedDuringSolverRun: make(map[uint64]struct{}),
	}

	s.graphManager = flowmanager.NewGraphManager()
	// Set up the initial flow graph
	s.graphManager.AddResourceTopology(root)

	s.costModel = costmodel.NewCostModel(s.graphManager)

	s.solver = ss.NewSolver(s.graphManager)

	return s
}

func (s *scheduler) GetTaskBindings() map[utility.TaskID]utility.ResourceID {
	return s.TaskBindings
}

func (sche *scheduler) AddJob(jd *proto.JobDescriptor) {
	sche.jobsToSchedule[utility.MustJobIDFromString(jd.Uuid)] = jd
}

func (sche *scheduler) CheckRunningTasksHealth() {}

func (sche *scheduler) dfsHandleTasksFromDeregisterResource(rtnd *proto.ResourceTopologyNodeDescriptor) {
	for _, childNode := range rtnd.Children {
		sche.dfsHandleTasksFromDeregisterResource(childNode)
	}

	sche.handleTasksFromDeregisterResource(rtnd)
}

func (sche *scheduler) handleTasksFromDeregisterResource(rtnd *proto.ResourceTopologyNodeDescriptor) {
	resourceDesc := rtnd.ResourceDesc
	rID := utility.MustResourceIDFromString(resourceDesc.Uuid)

	// Get the tasks bound to this resource
	tasks, ok := sche.resourceBindings[rID]
	if !ok {
		// TODO: add log here for debugging
		return
	}

	for taskID, _ :=range tasks {
		taskDesc := sche.taskMap.FindPtrOrNull(taskID)
		if taskDesc == nil {
			log.Panicf("Descriptor for task:%v must exist in taskMap\n", taskID)
		}

		// TODO: add this flag to Scheduler struct
		if (FLAG_reschedule_tasks_on_node_failure) {
			sche.HandleTaskEviction(taskDesc, resourceDesc)
		} else {
			sche.HandleTaskFailure(taskDesc)
		}
	}
}

func (sche *scheduler) dfsCleanStateForDeregisterResource(rtnd *proto.ResourceTopologyNodeDescriptor) {
	for _, childNode := range rtnd.Children {
		sche.dfsCleanStateForDeregisterResource(childNode)
	}

	sche.cleanStateForDeregisterResource(rtnd)
}

func (sche *scheduler) cleanStateForDeregisterResource(rtnd *proto.ResourceTopologyNodeDescriptor) {
	rID := utility.MustResourceIDFromString(rtnd.ResourceDesc.Uuid)
	// Originally had cleanups related to the executors and the trace generators but we don't need that
	delete(sche.resourceBindings, rID)
	delete(sche.resourceMap.UnsafeGet(), rID)
}

// RemoveResourceNodeFromParentChildrenList removes resource node from its parent's children list
func (sche *scheduler) RemoveResourceNodeFromParentChildrenList(rtnd *proto.ResourceTopologyNodeDescriptor) {
	parentID := utility.MustResourceIDFromString(rtnd.ParentId)
	parentResourceStatus := sche.resourceMap.FindPtrOrNull(parentID)
	if parentResourceStatus == nil {
		log.Panicf("Parent resource status for node:%v must exist", rtnd.ResourceDesc.Uuid)
	}

	parentNode := parentResourceStatus.TopologyNode
	children := parentNode.Children
	index := -1
	//Find the index of the child in the parent
	for i, childNode := range children {
		if childNode.ResourceDesc.Uuid == rtnd.ResourceDesc.Uuid {
			index = i
			break
		}
	}

	// Note: there is a bug here in ksched project of CoreOS
	// Remove the node from the parent's slice
	if index == -1 {
		log.Panicf("Resource node:%v not found as child of its parent:%v\n", rtnd.ResourceDesc.Uuid, parentID)
	} else {
		parentNode.Children = append(children[:index], children[index+1:]...)
	}
}

func (sche *scheduler) DeregisterResource(rtnd *proto.ResourceTopologyNodeDescriptor) {
	// Flow scheduler related work
	// Traverse the resource topology tree in order to evict tasks.
	// Do a dfs post order traversal to evict all tasks from the resource topology
	sche.dfsHandleTasksFromDeregisterResource(rtnd)

	// TODO: refactor RemoveResourceTopology to support pusRemovedDuringSolverRun arg
	// TODO: The scheduler is not an event based scheduler right now and so is not concurrent.
	// If it is implemented in an event based fashion then we need to implement locks
	// as well as make sure we don't place any tasks on the PUs that were removed
	// while the solver was running. In a non event based setting this won't be an issue.
	sche.graphManager.RemoveResourceTopology(rtnd, &sche.pusRemovedDuringSolverRun)

	// If it is an entire machine that was removed
	if rtnd.ParentId != "" {
		delete(sche.resourceRoots, rtnd)
	}

	sche.dfsCleanStateForDeregisterResource(rtnd)

	if rtnd.ParentId != "" {
		sche.RemoveResourceNodeFromParentChildrenList(rtnd)
	} else {
		log.Println("Deregister a node without a parent")
	}


}

func (sche *scheduler) HandleJobCompletion(jobID utility.JobID) {
	// Job completed, so remove its nodes
	sche.graphManager.JobCompleted(jobID)

	// Event scheduler related work
	jd := sche.jobMap.FindPtrOrNull(jobID)
	if jd == nil {
		log.Panicf("Job for id:%v must exist\n", jobID)
	}
	delete(sche.jobsToSchedule, jobID)
	delete(sche.runnableTasks, jobID)
	jd.State = proto.JobDescriptor_COMPLETED

}

func (sche *scheduler) HandleJobRemoval(jobID utility.JobID) {
	// job removed, so remove its nodes
	sche.graphManager.JobRemoved(jobID)

	// Event scheduler related work
	jd := sche.jobMap.FindPtrOrNull(jobID)
	if jd == nil {
		log.Panicf("Job for id:%v must exist\n", jobID)
	}
	delete(sche.jobsToSchedule, jobID)
	delete(sche.runnableTasks, jobID)
}

// unbindTaskFromResource is similar to BindTaskToResource, in that it just updates the metadata for a task being removed from a resource
// It is called in the event of a task failure, migration or eviction.
// Returns false in case the task was not already bound to the resource in the taskMappings or resourceMappings
// Event driven scheduler specific method
func (s *scheduler) unbindTaskFromResource(td *proto.TaskDescriptor, rID utility.ResourceID) bool {
	taskID := utility.TaskID(td.Uid)
	resourceStatus := s.resourceMap.FindPtrOrNull(rID)
	if resourceStatus == nil {
		return false
	}
	rd := resourceStatus.Descriptor
	// We don't have to remove the task from rd's running tasks because
	// we've already cleared the list in the scheduling iteration
	if len(rd.CurrentRunningTasks) == 0 {
		rd.State = proto.ResourceDescriptor_RESOURCE_IDLE
	}
	// Remove the task from the resource bindings, return false if not found in the mappings
	if _, ok := s.TaskBindings[taskID]; !ok {
		return false
	}

	taskSet := s.resourceBindings[rID]
	if _, ok := taskSet[taskID]; !ok {
		return false
	}
	delete(s.TaskBindings, taskID)
	delete(taskSet, taskID)
	return true
}

func (sche *scheduler) HandleTaskCompletion(td *proto.TaskDescriptor, report *proto.TaskFinalReport) {

	// event scheduler related work
	rID, ok := sche.TaskBindings[utility.TaskID(td.Uid)]
	if ok {
		resourceStatus := sche.resourceMap.FindPtrOrNull(rID)
		if resourceStatus == nil {
			log.Panicf("Resource:%v must have a resource status in the resourceMap\n", rID)
		}
		// Free the resource
		if !sche.unbindTaskFromResource(td, rID) {
			log.Panicf("Could not unbind task:%v from resource:%v for eviction\n", td.Uid, rID)
		}
	} else {
		// The task does not have a bound resource. It can happen when a machine
		// temporarly fails. As a result of the failure, we mark the task as failed
		// and unbind it from the machine's resource. However, upon machine recovery
		// we can receive a task completion notification.
		// do nothing here, add later if needed
	}
	// Set task state as completed
	td.State = proto.TaskDescriptor_COMPLETED

	taskInGraph := true
	if td.State == proto.TaskDescriptor_FAILED || td.State == proto.TaskDescriptor_ABORTED {
		// If the task is marked as failed/aborted then it has already been
		// removed from the flow network.
		taskInGraph = false
	}

	// We don't need to do any flow graph stuff for delegated tasks as
	// they are not currently represented in the flow graph.
	// Otherwise, we need to remove nodes, etc.
	if len(td.DelegatedFrom) == 0 && taskInGraph {
		nodeId := sche.graphManager.TaskCompleted(utility.TaskID(td.Uid))
		sche.tasksCompletedDuringSloverRun[uint64(nodeId)] = struct{}{}
	}
}

func (sche *scheduler) HandleTaskDelegationFailure(td *proto.TaskDescriptor) {

}

func (sche *scheduler) HandleTaskDelegationSuccess(td *proto.TaskDescriptor) {}

// InsertTaskIntoRunnables is a helper method used to update the runnable tasks set for the specified job by adding the new task
// Event driven scheduler specific method
func (s *scheduler) insertTaskIntoRunnables(jobID utility.JobID, taskID utility.TaskID) {
	// Create a task set for this job if it doesn't already exist
	if _, ok := s.runnableTasks[jobID]; !ok {
		s.runnableTasks[jobID] = make(TaskSet)
	}
	// Insert task into runnable set for this job
	s.runnableTasks[jobID][taskID] = struct{}{}
}

func (sche *scheduler) HandleTaskEviction(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor) {

	rID := utility.MustResourceIDFromString(rd.Uuid)
	taskID := utility.TaskID(td.Uid)
	jobID := utility.MustJobIDFromString(td.JobId)
	// Flow scheduler related work
	sche.graphManager.TaskEvicted(taskID, rID)

	// Event scheudler related work
	if !sche.unbindTaskFromResource(td, rID) {
		log.Panicf("Could not unbind task:%v from resource:%v for eviction\n", taskID, rID)
	}
	td.State = proto.TaskDescriptor_RUNNABLE
	sche.insertTaskIntoRunnables(jobID, taskID)
	// Some work is then done by the executor to handle the task eviction(update finish/running times)
	// but we don't need to account for that right now
}

func (sche *scheduler) HandleTaskFailure(td *proto.TaskDescriptor) {
	taskID := utility.TaskID(td.Uid)
	// Flow scheduler related work
	sche.graphManager.TaskFailed(taskID)

	// Event scheduler related work
	// Find resource for task
	rID, ok := sche.TaskBindings[taskID]
	if !ok {
		log.Panicf("No resource found for task:%v that failed/should have been running\n", taskID)
	}
	rs := sche.resourceMap.FindPtrOrNull(rID)
	if rs == nil {
		log.Panicf("resource:%v is not found in resource map\n", rID)
	}
	// Remove the task's resource binding (as it is no longer currently bound)
	if !sche.unbindTaskFromResource(td, rID) {
		log.Panicf("Could not unbind task:%v from resource:%v for eviction\n", taskID, rID)
	}
	// Set the task to "failed" state and deal with the consequences
	td.State = proto.TaskDescriptor_FAILED

	// We only need to run the scheduler if the failed task was not delegated from
	// elsewhere, i.e. if it is managed by the local scheduler. If so, we kick the
	// scheduler if we haven't exceeded the retry limit.
	if len(td.DelegatedFrom) != 0 {
		// XXX(malte): Need to forward message about task failure to delegator here!
	}
}

func (sche *scheduler) HandleTaskFinalReport(report *proto.TaskFinalReport, td *proto.TaskDescriptor) {

}

func (sche *scheduler) HandleTaskRemoval(td *proto.TaskDescriptor) {
	taskID := utility.TaskID(td.Uid)
	// TODO: add TaskRemoved func for flow graph manager
	sche.graphManager.TaskRemoved(taskID)

	// event scheduler related work
	// wasRunning := false
	if td.State == proto.TaskDescriptor_RUNNING {
		// wasRunning = true
		sche.KillRunningTask(taskID)
	} else {
		if td.State == proto.TaskDescriptor_RUNNABLE {
			jodID := utility.MustJobIDFromString(td.JobId)
			sche.insertTaskIntoRunnables(jodID, taskID)
		}
		td.State = proto.TaskDescriptor_ABORTED
	}
}

func (sche *scheduler) KillRunningTask(taskID utility.TaskID) {
	sche.graphManager.TaskKilled(taskID)

	// event scheduler related work
	td := sche.taskMap.FindPtrOrNull(taskID)
	if td == nil {
		// TODO: This could just be an error instead of a panic
		log.Panicf("Tried to kill unknown task:%v, not present in taskMap\n", taskID)
	}
	// Check if we have a bound resource for the task and if it is marked as running
	rID, ok := sche.TaskBindings[taskID]
	if td.State != proto.TaskDescriptor_RUNNING || !ok {
		// TODO: This could just be an error instead of a panic
		log.Panicf("Task:%v not bound or running on any resource", taskID)
	}
	td.State = proto.TaskDescriptor_ABORTED

	// TODO: Firmament project will check !rid, this is a bug there, otherwise, we need to delete the code below
	// Remove the task's resource binding (as it is no longer currently bound)
	if !sche.unbindTaskFromResource(td, rID) {
		log.Panicf("Could not unbind task:%v from resource:%v for eviction\n", taskID, rID)
	}
}

func (sche *scheduler) PlaceDelegatedTask(td *proto.TaskDescriptor, id utility.ResourceID) {

}

func (sche *scheduler) RegisterResource(rtnd *proto.ResourceTopologyNodeDescriptor) {

}

func (sche *scheduler) ScheduleAllJobs(stat *utility.SchedulerStats) (uint64, []proto.SchedulingDelta) {

}

func (sche *scheduler) ScheduleJob(jd *proto.JobDescriptor, stats *utility.SchedulerStats) uint64 {

}

func (sche *scheduler) ScheduleJobs(jds []*proto.JobDescriptor) (uint64, []proto.SchedulingDelta) {

}

func (sche *scheduler) HandleTaskMigration(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor) {

}

func (sche *scheduler) HandleTaskPlacement(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor) {

}

func (sche *scheduler) ComputeRunnableTasksForJob(jd *proto.JobDescriptor) []utility.TaskID {

}