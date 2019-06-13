package flowscheduler

import (
	"nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/utility"
)

// Set of tasks
type TaskSet map[utility.TaskID]struct{}

type scheduler struct {

}

func NewScheduler() Scheduler {
	return &scheduler{}
}

func (sche *scheduler) AddJob(jd *proto.JobDescriptor) {

}

func (sche *scheduler) BoundResourceForTask(id utility.TaskID) utility.ResourceID {

}

func (sche *scheduler) BoundTasksForResource(id utility.ResourceID) []utility.TaskID {

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