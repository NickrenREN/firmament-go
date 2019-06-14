package flowscheduler

import (
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

func (sche *scheduler) DeregisterResource(rtnd *proto.ResourceTopologyNodeDescriptor) {

}

func (sche *scheduler) HandleJobCompletion(id utility.JobID) {

}

func (sche *scheduler) HandleJobRemoval(id utility.JobID) {}

func (sche *scheduler) HandleTaskCompletion(td *proto.TaskDescriptor, report *proto.TaskFinalReport) {

}

func (sche *scheduler) HandleTaskDelegationFailure(td *proto.TaskDescriptor) {

}

func (sche *scheduler) HandleTaskDelegationSuccess(td *proto.TaskDescriptor) {}

func (sche *scheduler) HandleTaskEviction(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor) {

}

func (sche *scheduler) HandleTaskFailure(td *proto.TaskDescriptor) {

}

func (sche *scheduler) HandleTaskFinalReport(report *proto.TaskFinalReport, td *proto.TaskDescriptor) {

}

func (sche *scheduler) HandleTaskRemoval(td *proto.TaskDescriptor) {

}

func (sche *scheduler) KillRunningTask(id utility.TaskID) {

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