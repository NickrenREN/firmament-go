package flowscheduler

import (
	"nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/utility"
)

type Scheduler interface {
	GetTaskBindings() map[utility.TaskID]utility.ResourceID

	// AddJob adds a new job. The job will be scheduled on the next run of the scheduler
	// if it has any runnable tasks.
	// jd: JobDescriptor of the job to add
	// NOTE: This method was originally implemented only by event_scheduler and not flow_scheduler
	AddJob(jd *proto.JobDescriptor)

	// Checks if all running tasks managed by this scheduler are healthy. It
    // invokes failure handlers if any failures are detected.
	CheckRunningTasksHealth()

	// DeregisterResource unregisters a resource ID from the scheduler. No-op if the resource ID is
	// not actually registered with it.
	// rtnd: pointer to the resource topology node descriptor of the resource to deregister
	DeregisterResource(rtnd *proto.ResourceTopologyNodeDescriptor)

	// HandleJobCompletion handles the completion of a job (all tasks are completed, failed or
	// aborted). May clean up scheduler-specific state.
	// jobID: the id of the completed job
	HandleJobCompletion(id utility.JobID)

	// Handles the removal of a job. It should only be called after all job's tasks are removed.
    // id: the id of the job to be removed
	HandleJobRemoval(id utility.JobID)

	// HandleTaskCompletion handles the completion of a task. This usually involves freeing up its
	// resource by setting it idle, and recording any bookkeeping data required.
	// td: the task descriptor of the completed task
	// report: the task report to be populated with statistics
	// (e.g., finish time).
	// NOTE: Modified to not include processing the TaskFinalReport
	// originally: HandleTaskCompletion(td *TaskDescriptor, report *TaskFinalReport)
	HandleTaskCompletion(td *proto.TaskDescriptor, report *proto.TaskFinalReport)

	// Handles the failure of an attempt to delegate a task to a subordinate
	// coordinator. This can happen because the resource is no longer there (it
    // failed) or it is no longer idle (someone else put a task there).
    // td: the descriptor of the task that could not be delegated
	HandleTaskDelegationFailure(td *proto.TaskDescriptor)

	HandleTaskDelegationSuccess(td *proto.TaskDescriptor)

	// HandleTaskEviction handles the eviction of a task.
	// td: The task descriptor of the evicted task
	// rd: The resource descriptor of the resource from which the task was evicted
	HandleTaskEviction(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor)

	// HandleTaskFailure handles the failure of a task. This usually involves freeing up its
	// resource by setting it idle, and kicking off the necessary fault tolerance
	// handling procedures.
	// td: the task descriptor of the failed task
	HandleTaskFailure(td *proto.TaskDescriptor)

	// Updates the state using the task's report.
    // report: the report to use
    // td: the descriptor of the task for which to update the state.
	HandleTaskFinalReport(report *proto.TaskFinalReport, td *proto.TaskDescriptor)

	// Handles the removal of a task. If the task is running, then
    // it is killed, otherwise the task is just removed from internal data
    // structures.
    // td: the task descriptor of the task to remove
	HandleTaskRemoval(td *proto.TaskDescriptor)

	// KillRunningTask kills a running task.
	// task_id: the id of the task to kill
	// NOTE: modified to not include kill message
	KillRunningTask(id utility.TaskID)

	// Places a task delegated from a superior coordinator to a resource managed
    // by this scheduler.
    // td: the task descriptor of the delegated task
    // id: the id of the resource on which to place the task
	PlaceDelegatedTask(td *proto.TaskDescriptor, id utility.ResourceID) bool

	// RegisterResource registers a resource with the scheduler, who may subsequently assign
	// work to this resource.
	// rtnd: the resource topology node descriptor
	// local: boolean to indicate if the resource is local or not
	// NOTE: We don't distinguish between local(cpu), remote(storage) or simulated resources
	// Original interface modified to not take inputs for local or simulated flags
	RegisterResource(rtnd *proto.ResourceTopologyNodeDescriptor)

	// ScheduleAllJobs runs a scheduling iteration for all active jobs. Computes runnable jobs and then calls ScheduleJobs()
	// Returns the number of tasks scheduled, and the scheduling deltas
	// NOTE: Modified from original interface to return deltas rather than passing in and modifying the deltas
	ScheduleAllJobs(stat *utility.SchedulerStats) (uint64, []proto.SchedulingDelta)

	// Schedules all runnable tasks in a job.
    // WARNING: Using this method is inefficient because for every
    // invocation it traverses the entire resource graph.
    // jd: the job descriptor for which to schedule tasks
    // return the number of tasks scheduled
	ScheduleJob(jd *proto.JobDescriptor, stats *utility.SchedulerStats) uint64

	// ScheduleJobs schedules the given jobs. This is called by ScheduleAllJobs()
	// jds: a slice of job descriptors
	// Returns the number of tasks scheduled, and the scheduling deltas
	// NOTE: Modified from original interface to return deltas rather than passing in and modifying the deltas
	// Also removed the schedulerStats from the input arguments
	ScheduleJobs(jds []*proto.JobDescriptor) (uint64, []proto.SchedulingDelta)

	// HandleTaskMigration handles the migration of a task.
	// td: the descriptor of the migrated task
	// rd: the descriptor of the resource to which the task was migrated
	HandleTaskMigration(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor)

	// HandleTaskPlacement places a task to a resource, i.e. effects a scheduling assignment.
	// This will modify various bits of meta-data tracking assignments. It will
	// then delegate the actual execution of the task binary to the appropriate
	// local execution handler.
	// td: the descriptor of the task to bind
	// rd: the descriptor of the resource to bind to
	// This method is called by the flow scheduler for every PLACE scheduling delta
	// indicating a successful placement of a task on a resource
	HandleTaskPlacement(td *proto.TaskDescriptor, rd *proto.ResourceDescriptor)

	// Finds runnable tasks for the job in the argument and adds them to the
    // global runnable set.
    // jd: the descriptor of the job for which to find tasks
	ComputeRunnableTasksForJob(jd *proto.JobDescriptor) TaskSet

}