package flowscheduler

import (
	"fmt"
	"log"
	"time"

	"nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/costmodel"
	"nickren/firmament-go/pkg/scheduling/dimacs"
	"nickren/firmament-go/pkg/scheduling/flowmanager"
	ss "nickren/firmament-go/pkg/scheduling/solver"
	"nickren/firmament-go/pkg/scheduling/utility"

	"nickren/firmament-go/pkg/scheduling/utility/queue"
)

// Set of tasks
type TaskSet map[utility.TaskID]struct{}

var timestart time.Time

type scheduler struct {
	// TODO: find a way to set these boolean values
	enableEviction  bool
	enableMigration bool

	rescheduleTasksOnNodeFailure     bool
	updateResourceTopologyCapacities bool
	timeDependentCostUpdateFrequency uint64
	purgeUnconnectedEcFrequency      uint64

	jobMap      *utility.JobMap
	taskMap     *utility.TaskMap
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
	solver       ss.Solver
	costModel    costmodel.CostModeler

	lastUpdateTimeDepentCosts time.Time

	leafResourceIDs map[utility.ResourceID]struct{}

	pusRemovedDuringSolverRun     map[uint64]struct{}
	tasksCompletedDuringSloverRun map[uint64]struct{}

	dimacsStats *dimacs.ChangeStats

	solverRunCnt uint64

	resourceRoots map[*proto.ResourceTopologyNodeDescriptor]struct{}
	taskMappings  flowmanager.TaskMapping
}

func NewScheduler(jobMap *utility.JobMap, resourceMap *utility.ResourceMap, root *proto.ResourceTopologyNodeDescriptor,
	taskMap *utility.TaskMap) Scheduler {
	// Initialize graph manager with cost model
	leafResourceIDs := make(map[utility.ResourceID]struct{})

	// TODO: refactor maxTasksPerMachine
	costModel := costmodel.NewCostModel(resourceMap, taskMap, leafResourceIDs, 100)

	s := &scheduler{
		updateResourceTopologyCapacities: true,

		jobMap:      jobMap,
		resourceMap: resourceMap,
		taskMap:     taskMap,

		lastUpdateTimeDepentCosts: time.Now(),
		solverRunCnt:              0,
		leafResourceIDs:           leafResourceIDs,

		costModel: costModel,

		dimacsStats: &dimacs.ChangeStats{},

		resourceRoots:    make(map[*proto.ResourceTopologyNodeDescriptor]struct{}),
		TaskBindings:     make(map[utility.TaskID]utility.ResourceID),
		resourceBindings: make(map[utility.ResourceID]TaskSet),
		jobsToSchedule:   make(map[utility.JobID]*proto.JobDescriptor),
		runnableTasks:    make(map[utility.JobID]TaskSet),

		tasksCompletedDuringSloverRun: make(map[uint64]struct{}),
		pusRemovedDuringSolverRun:     make(map[uint64]struct{}),
		taskMappings:                  flowmanager.TaskMapping{},
	}

	// TODO: refactor max tasks per PU
	s.graphManager = flowmanager.NewGraphManager(s.costModel, s.leafResourceIDs, s.dimacsStats, 100)
	// Set up the initial flow graph
	//s.graphManager.AddResourceTopology(root)

	// set cost model graph manager
	//s.costModel.SetFlowGraphManager(s.graphManager)

	// Set up the solver, which starts the flow solver
	s.solver = ss.NewSolver(s.graphManager)

	// TODO: init flag by cmd
	s.rescheduleTasksOnNodeFailure = false
	s.updateResourceTopologyCapacities = false
	s.timeDependentCostUpdateFrequency = 60
	s.purgeUnconnectedEcFrequency = 60

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

	for taskID := range tasks {
		taskDesc := sche.taskMap.FindPtrOrNull(taskID)
		if taskDesc == nil {
			log.Panicf("Descriptor for task:%v must exist in taskMap\n", taskID)
		}

		// TODO: add this flag to Scheduler struct
		if sche.rescheduleTasksOnNodeFailure {
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
	pusRemovedDuringSolverRun := sche.graphManager.RemoveResourceTopology(rtnd.ResourceDesc)

	for pu := range pusRemovedDuringSolverRun {
		sche.pusRemovedDuringSolverRun[uint64(pu)] = struct{}{}
	}

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
	if td == nil {
		return false
	}
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
		// TODO: change log lib
		log.Println("Task:", taskID, " not bound or running on any resource")
		return
	}
	td.State = proto.TaskDescriptor_ABORTED

	// TODO: Firmament project will check !rid, this is a bug there, otherwise, we need to delete the code below
	if !sche.unbindTaskFromResource(td, rID) {
		log.Panicf("Could not unbind task:%v from resource:%v for eviction\n", taskID, rID)
	}
}

func (sche *scheduler) PlaceDelegatedTask(td *proto.TaskDescriptor, resID utility.ResourceID) bool {
	taskID := utility.TaskID(td.Uid)
	resourceStatus := sche.resourceMap.FindPtrOrNull(resID)
	if resourceStatus == nil {
		// TODO: log error here
		log.Println("attempted to place delegated task: ", taskID, " on resource: ", resID, ", which is unknow")
		return false
	}
	rd := resourceStatus.Descriptor
	if td == nil {
		log.Panicf("resource descriptor in resource:%v is nil", resID)
	}

	if rd.State != proto.ResourceDescriptor_RESOURCE_IDLE {
		log.Println("attempted to place delegated task: ", taskID, " on resource: ", resID, ", which is not idle")
		return false
	}

	sche.taskMap.InsertIfNotPresent(taskID, td)
	sche.HandleTaskPlacement(td, rd)
	td.State = proto.TaskDescriptor_RUNNING
	return true
}

func (sche *scheduler) RegisterResource(rtnd *proto.ResourceTopologyNodeDescriptor) {
	// event scheduler related work
	// Do a BFS traversal starting from rtnd root and set each PU in this topology as schedulable
	toVisit := queue.NewFIFO()
	toVisit.Push(rtnd)
	for !toVisit.IsEmpty() {
		curNode := toVisit.Pop().(*proto.ResourceTopologyNodeDescriptor)
		// callback
		curRD := curNode.ResourceDesc
		if curRD.Type == proto.ResourceDescriptor_RESOURCE_MACHINE {
			curRD.Schedulable = true
			if curRD.State == proto.ResourceDescriptor_RESOURCE_UNKNOWN {
				curRD.State = proto.ResourceDescriptor_RESOURCE_IDLE
			}

			/*// create an executor for each resource
			resID, err := utility.ResourceIDFromString(curRD.Uuid)
			if err != nil {
				log.Panicf("get resource id from resource uuid error")
			}
			new executor(resID...)*/
		}

		// bfs: add children to queue
		for _, child := range curNode.Children {
			toVisit.Push(child)
		}
	}

	// flow scheduler related work
	sche.graphManager.AddResourceTopology(rtnd)
	if rtnd.ParentId == "" {
		sche.resourceRoots[rtnd] = struct{}{}
	}
}

func (sche *scheduler) ScheduleAllJobs(stat *utility.SchedulerStats) (uint64, []proto.SchedulingDelta) {
	timestart = time.Now()
	jds := make([]*proto.JobDescriptor, 0)
	for _, jobDesc := range sche.jobsToSchedule {
		// If at least one task is runnable in the job, add it for scheduling
		if len(sche.ComputeRunnableTasksForJob(jobDesc)) > 0 {
			jds = append(jds, jobDesc)
		}
	}
	// TODO: populate stat info in ScheduleJobs
	return sche.ScheduleJobs(jds)
}

func (sche *scheduler) ScheduleJob(jd *proto.JobDescriptor, stats *utility.SchedulerStats) uint64 {

	// implement this later
	return 0
}

func (s *scheduler) updateCostModelResourceStats() {
	// TODO: add log level here, do not always print the noisy log
	log.Println("updating resource statistics in flow graph")
	s.graphManager.ComputeTopologyStatistics(s.graphManager.SinkNode())
}

func (s *scheduler) applySchedulingDeltas(deltas []proto.SchedulingDelta) uint64 {
	numScheduled := uint64(0)
	for _, delta := range deltas {
		td := s.taskMap.FindPtrOrNull(utility.TaskID(delta.TaskId))
		if td == nil {
			panic("")
		}

		resID := utility.MustResourceIDFromString(delta.ResourceId)
		rs := s.resourceMap.FindPtrOrNull(resID)
		if rs == nil {
			panic("")
		}

		switch delta.Type {
		case proto.SchedulingDelta_NOOP:
			log.Println("NOOP Delta type:", delta.Type)
		case proto.SchedulingDelta_PLACE:
			// Tag the job to which this task belongs as running
			jd := s.jobMap.FindPtrOrNull(utility.MustJobIDFromString(td.JobId))
			if jd.State != proto.JobDescriptor_RUNNING {
				jd.State = proto.JobDescriptor_RUNNING
			}
			s.HandleTaskPlacement(td, rs.Descriptor)
			numScheduled++
		case proto.SchedulingDelta_PREEMPT:
			log.Printf("TASK PREEMPTION: task:%v from resource:%v\n", td.Uid, rs.Descriptor.FriendlyName)
			s.HandleTaskEviction(td, rs.Descriptor)
		case proto.SchedulingDelta_MIGRATE:
			log.Printf("TASK MIGRATION: task:%v to resource:%v\n", td.Uid, rs.Descriptor.FriendlyName)
			s.HandleTaskMigration(td, rs.Descriptor)
		default:
			log.Fatalf("Unknown delta type: %v", delta.Type)
		}
	}
	return numScheduled
}

func (s *scheduler) runSchedulingIteration() (uint64, []proto.SchedulingDelta) {
	// If it's time to revisit time-dependent costs, do so now, just before
	// we run the solver.
	// firmament time manager gets current time
	curTime := time.Now()
	shouldUpdate := s.lastUpdateTimeDepentCosts.Add(time.Duration(s.timeDependentCostUpdateFrequency) * time.Second)
	if shouldUpdate.Before(curTime) {
		// First collect all non-finished jobs
		// TODO(malte): this can be removed when we've factored archived tasks
		// and jobs out of the job_map_ into separate data structures.
		// (cf. issue #24).
		jobs := make([]*proto.JobDescriptor, 0)
		for _, job := range s.jobMap.UnsafeGet() {
			// we only need to reconsider this jon if it is still active
			if job.State != proto.JobDescriptor_COMPLETED &&
				job.State != proto.JobDescriptor_FAILED &&
				job.State != proto.JobDescriptor_ABORTED {
				jobs = append(jobs, job)
			}
		}

		// this will re-visit all jobs and update their time-dependent costs
		s.graphManager.UpdateTimeDependentCosts(jobs)
		s.lastUpdateTimeDepentCosts = curTime
	}

	if s.solverRunCnt%s.purgeUnconnectedEcFrequency == 0 {
		// periodically remove EC nodes without incoming arcs
		s.graphManager.PurgeUnconnectedEquivClassNodes()
	}
	// clear pus and completed tasks stats
	s.pusRemovedDuringSolverRun = make(map[uint64]struct{})
	s.tasksCompletedDuringSloverRun = make(map[uint64]struct{})

	timeElapsed := time.Since(timestart)
	fmt.Printf("construct graph took %v\n", timeElapsed)
	// Run the flow solver! This is where all the juicy goodness happens :)
	tm := s.solver.MCMFSolve(s.graphManager.GraphChangeManager().Graph())
	s.solverRunCnt++
	// firmament will populate max solve runtime and scheduler time stats
	// and play all the simulation events that happened while the solver was running
	// ignore here
	for taskID, resourceID := range tm {
		s.taskMappings[taskID] = resourceID
	}
	deltas := s.graphManager.SchedulingDeltasForPreemptedTasks(s.taskMappings, s.resourceMap)

	for taskID, resourceID := range s.taskMappings {
		_, ok := s.tasksCompletedDuringSloverRun[uint64(taskID)]
		if ok {
			// Ignore the task because it has already completed while the solver
			// was running.
			continue
		}

		_, ok = s.pusRemovedDuringSolverRun[uint64(resourceID)]
		if ok {
			// We can't place a task on this PU because the PU has been removed
			// while the solver was running. We will reconsider the task in the
			// next solver run.
			continue
		}

		//log.Println("bind task to resource")
		delta := s.graphManager.NodeBindingToSchedulingDelta(taskID, resourceID, s.TaskBindings)
		if delta != nil {
			deltas = append(deltas, *delta)
		}
	}

	// Move the time to solver_start_time + solver_run_time if this is not
	// the first run of a simulation.

	numScheduled := s.applySchedulingDeltas(deltas)

	// TODO: update_resource_topology_capacities??
	if s.updateResourceTopologyCapacities {
		for rtnd := range s.resourceRoots {
			s.graphManager.UpdateResourceTopology(rtnd)
		}
	}

	// write schedule graph
	start := time.Now()

	s.solver.WriteGraph("mcmf-after")
	elapsed := time.Since(start)
	fmt.Printf("write graph took %s\n", elapsed)
	return numScheduled, deltas
}

func (sche *scheduler) ScheduleJobs(jds []*proto.JobDescriptor) (uint64, []proto.SchedulingDelta) {
	numScheduledTasks := uint64(0)
	deltas := make([]proto.SchedulingDelta, 0)

	if len(jds) > 0 {
		// First, we update the cost model's resource topology statistics
		// (e.g. based on machine load and prior decisions); these need to be
		// known before AddOrUpdateJobNodes is invoked below, as it may add arcs
		// depending on these metrics.
		sche.updateCostModelResourceStats()
		sche.graphManager.AddOrUpdateJobNodes(jds)
		numScheduledTasks, deltas = sche.runSchedulingIteration()
		// TODO: add log level here
		log.Printf("Scheduling Iteration complete, placed %v tasks\n", numScheduledTasks)
		// firmament add debug info, ignore here

		// We reset the DIMACS stats here because all the graph changes we make
		// from now on are going to be included in the next scheduler run.
		sche.dimacsStats.ResetStats()
		// TODO: populate total runtime stat in scheduler stats struct
		// TODO: If the support for the trace generator is ever added then log the dimacs changes
		// for this iteration before resetting them
	}
	return numScheduledTasks, deltas

}

// BindTaskToResource is used to update metadata anytime a task is placed on a some resource by the scheduler
// either through a placement or migration
// Event driven scheduler specific method
func (s *scheduler) bindTaskToResource(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor) {
	taskID := utility.TaskID(td.Uid)
	rID := utility.MustResourceIDFromString(rd.Uuid)
	// Mark resource as busy and record task binding
	rd.State = proto.ResourceDescriptor_RESOURCE_BUSY
	rd.CurrentRunningTasks = append(rd.CurrentRunningTasks, uint64(taskID))
	// Insert mapping into task bindings, must not already exist
	if _, ok := s.TaskBindings[taskID]; ok {
		// TODO: revisit this later to check if we need to panic
		log.Panicf("scheduler/bindTaskToResource: mapping for taskID:%v in taskBindings must not already exist\n", taskID)
	}
	s.TaskBindings[taskID] = rID
	// Update resource bindings, create a binding set if it doesn't exist already
	if _, ok := s.resourceBindings[rID]; !ok {
		s.resourceBindings[rID] = make(TaskSet)
	}
	s.resourceBindings[rID][taskID] = struct{}{}
}

func (sche *scheduler) HandleTaskMigration(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor) {
	taskID := utility.TaskID(td.Uid)
	oldRID := sche.TaskBindings[taskID]
	newRID := utility.MustResourceIDFromString(rd.Uuid)

	// Flow scheduler related work
	// XXX(ionel): HACK! We update scheduledToResource field here
	// and in the EventDrivenScheduler. We update it here because
	// TaskMigrated first calls TaskEvict and then TaskSchedule.
	// TaskSchedule requires scheduledToResource to be up to date.
	// Hence, we have to set it before we call the method.
	td.ScheduledToResource = rd.Uuid
	sche.graphManager.TaskMigrated(taskID, oldRID, newRID)

	// event scheduler related work
	// Unbind task from old resource and bind to new one
	rd.State = proto.ResourceDescriptor_RESOURCE_BUSY
	td.State = proto.TaskDescriptor_RUNNING
	if !sche.unbindTaskFromResource(td, oldRID) {
		log.Panicf("Task/Resource binding for taskID:%v to rID:%v must exist\n", taskID, oldRID)
	}
	sche.bindTaskToResource(td, rd)
	// firmament will add event and trace infos here
}

// ExecuteTask is used to actually execute the task on a resource via an excution handler
// For our purposes we skip that and only update the meta data to mark the task as running
// Event driven scheduler specific method
func (s *scheduler) executeTask(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor) {
	// This function actually executes the task asynchronously on that resource via an executor
	// but we don't need that
	td.State = proto.TaskDescriptor_RUNNING
	td.ScheduledToResource = rd.Uuid
	// can add exec and debug actions here
}

func (sche *scheduler) HandleTaskPlacement(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor) {
	// flow scheduler related work
	td.ScheduledToResource = rd.Uuid
	taskID := utility.TaskID(td.Uid)
	sche.graphManager.TaskScheduled(taskID, utility.MustResourceIDFromString(rd.Uuid))

	// event scheduler related work
	sche.bindTaskToResource(td, rd)
	// remove the task from the runnable_tasks
	jobID := utility.MustJobIDFromString(td.JobId)
	runnablesForJob := sche.runnableTasks[jobID]
	if runnablesForJob != nil {
		delete(runnablesForJob, taskID)
	}
	sche.executeTask(td, rd)

	// add event and trace infos here
}

// NOTE: This method is not implemented by the flow_scheduler but by the event_driven_sched
// Our implementation should be to ignore dependencies and mark all runnable tasks as runnable
// ComputeRunnableTasksForJob finds runnable tasks for the job in the argument and adds them to the
// global runnable set.
// jd: the descriptor of the job for which to find tasks
// Returns the set of tasks that are runnable for this job
func (sche *scheduler) ComputeRunnableTasksForJob(jd *proto.JobDescriptor) TaskSet {
	jobID := utility.MustJobIDFromString(jd.Uuid)
	rootTask := jd.RootTask
	sche.LazyGraphReduction(rootTask, jobID)

	runnableTasksForJob := sche.runnableTasks[jobID]
	if runnableTasksForJob != nil {
		return runnableTasksForJob
	} else {
		sche.runnableTasks[jobID] = make(TaskSet)
		return sche.runnableTasks[jobID]
	}
}

// Implementation of lazy graph reduction algorithm, as per p58, fig. 3.5 in
// Derek Murray's thesis on CIEL.
func (sche *scheduler) LazyGraphReduction(rootTask *proto.TaskDescriptor, jobId utility.JobID) {
	log.Println("performing lazy graph reduction")
	newlyActiveTasks := queue.NewFIFO()

	// TODO: need to revisit this, if we need to roottask concept ?
	root := sche.taskMap.FindPtrOrNull(utility.TaskID(rootTask.Uid))
	if root == nil {
		log.Panicf("root task of job:%v is nil", jobId)
	}

	// TODO: change the logic below: even if root task is failed, we need to go on scheduling others
	// only add the root task if it is not already scheduled. running, done or failed
	if root.State == proto.TaskDescriptor_CREATED || root.State == proto.TaskDescriptor_RUNNING ||
		root.State == proto.TaskDescriptor_RUNNABLE || root.State == proto.TaskDescriptor_COMPLETED {
		newlyActiveTasks.Push(root)
	}

	for !newlyActiveTasks.IsEmpty() {
		curTask := newlyActiveTasks.Pop().(*proto.TaskDescriptor)
		// Find any unfulfilled dependencies which will be executed in firmament, ignore here

		// add all children here, firmament will check the dependencies
		for _, childTask := range curTask.Spawned {
			newlyActiveTasks.Push(childTask)
		}

		if curTask.State == proto.TaskDescriptor_CREATED || curTask.State == proto.TaskDescriptor_BLOCKING {
			curTask.State = proto.TaskDescriptor_RUNNABLE
			sche.insertTaskIntoRunnables(utility.MustJobIDFromString(curTask.JobId), utility.TaskID(curTask.Uid))
		}
	}
}
