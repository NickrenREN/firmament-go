package firmamentservice

import (
	"context"

	"nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/flowscheduler"
	"nickren/firmament-go/pkg/scheduling/utility"
	"log"
	"fmt"
)


var _ proto.FirmamentSchedulerServer = &schedulerServer{}

type schedulerServer struct {
	scheduler flowscheduler.Scheduler

	jobMap *utility.JobMap
	taskMap *utility.TaskMap
	resourceMap *utility.ResourceMap

	topLevelResID utility.ResourceID

	// Mapping from JobID_t to number of incomplete job tasks
	jobIncompleteTasksNumMap map[utility.JobID]uint64
	// Mapping from JobID_t to number of job tasks left to be removed
	jobTasksNumToRemoveMap map[utility.JobID]uint64

}

func NewSchedulerServer() proto.FirmamentSchedulerServer {
	ss := &schedulerServer{
		jobMap: utility.NewJobMap(),
		taskMap: utility.NewTaskMap(),
		resourceMap: utility.NewResourceMap(),
	}

	// create top level resource node
	rs := utility.CreateTopLevelResourceStatus()
	// insert top level resource into resourceMap
	ss.resourceMap.InsertIfNotPresent(utility.MustResourceIDFromString(rs.Descriptor.Uuid), rs)

	ss.topLevelResID = utility.MustResourceIDFromString(rs.Descriptor.Uuid)

	ss.scheduler = flowscheduler.NewScheduler(ss.jobMap, ss.resourceMap, rs.TopologyNode, ss.taskMap)

	return ss
}

func (ss *schedulerServer) handlePlaceDelta(delta proto.SchedulingDelta) {
	task := ss.taskMap.FindPtrOrNull(utility.TaskID(delta.TaskId))
	if task == nil {
		log.Panicf("task already placed is not in the task map of the scheduler")
	}
	// set task start time
	// task.StartTime =
}

func (ss *schedulerServer) handlePreemptionDelta(delta proto.SchedulingDelta) {}

func (ss *schedulerServer) handleMigrationDelta(delta proto.SchedulingDelta) {}

func (ss *schedulerServer) Schedule(context.Context, *proto.ScheduleRequest) (*proto.SchedulingDeltas, error) {
	schedulerStats := &utility.SchedulerStats{}
	numScheduled, schedulingDeltas := ss.scheduler.ScheduleAllJobs(schedulerStats)
	log.Printf("%v tasks are scheduled, scheduling deltas are:%v", numScheduled, schedulingDeltas)
	schedulingDeltasReturned := &proto.SchedulingDeltas{
		Deltas: make([]*proto.SchedulingDelta, 0),
	}
	for _, delta := range schedulingDeltas {
		if delta.Type == proto.SchedulingDelta_PLACE {
			ss.handlePlaceDelta(delta)
		} else if delta.Type == proto.SchedulingDelta_PREEMPT {
			ss.handlePreemptionDelta(delta)
		} else if delta.Type == proto.SchedulingDelta_MIGRATE {
			ss.handleMigrationDelta(delta)
		} else if delta.Type == proto.SchedulingDelta_NOOP {
			// do nothing here
		} else {
			log.Fatalf("encounter unsupported scheduling delta type")
		}
		schedulingDeltasReturned.Deltas = append(schedulingDeltasReturned.Deltas, &delta)
	}

	return schedulingDeltasReturned, nil
}

func (ss *schedulerServer) TaskCompleted(context context.Context, taskID *proto.TaskUID) (*proto.TaskCompletedResponse, error) {
	response := &proto.TaskCompletedResponse{}
	task := ss.taskMap.FindPtrOrNull(utility.TaskID(taskID.TaskUid))
	if task == nil {
		response.Type = proto.TaskReplyType_TASK_NOT_FOUND
		return response, nil
	}

	jobID := utility.MustJobIDFromString(task.JobId)
	job := ss.jobMap.FindPtrOrNull(jobID)
	if job == nil {
		response.Type = proto.TaskReplyType_TASK_JOB_NOT_FOUND
		return response, nil
	}

	// TODO: set finish time
	//task.FinishTime =

	report := &proto.TaskFinalReport{}
	ss.scheduler.HandleTaskCompletion(task, report)
	// populate task final report
	ss.scheduler.HandleTaskFinalReport(report, task)

	// Check if it was the last task of the job.
	incompletedNum, ok := ss.jobIncompleteTasksNumMap[jobID]
	if !ok {
		return nil, fmt.Errorf("can not find the job in job incompleted tasks map of scheduer service")
	}
	if incompletedNum < 1 {
		return nil, fmt.Errorf("num of incompleted task of the job is less than 1")
	}
	ss.jobIncompleteTasksNumMap[jobID]--
	if ss.jobIncompleteTasksNumMap[jobID] == 0 {
		ss.scheduler.HandleJobCompletion(jobID)
	}

	response.Type = proto.TaskReplyType_TASK_COMPLETED_OK

	return response, nil
}

func (ss *schedulerServer) TaskFailed(context context.Context, taskID *proto.TaskUID) (*proto.TaskFailedResponse, error) {
	response := &proto.TaskFailedResponse{}
	task := ss.taskMap.FindPtrOrNull(utility.TaskID(taskID.TaskUid))
	if task == nil {
		response.Type = proto.TaskReplyType_TASK_NOT_FOUND
		return response, nil
	}
	ss.scheduler.HandleTaskFailure(task)

	response.Type = proto.TaskReplyType_TASK_FAILED_OK

	return response, nil
}

func (ss *schedulerServer) TaskRemoved(context context.Context, taskID *proto.TaskUID) (*proto.TaskRemovedResponse, error) {
	response := &proto.TaskRemovedResponse{}
	task := ss.taskMap.FindPtrOrNull(utility.TaskID(taskID.TaskUid))
	if task == nil {
		response.Type = proto.TaskReplyType_TASK_NOT_FOUND
		return response, nil
	}
	ss.scheduler.HandleTaskRemoval(task)

	jobID := utility.MustJobIDFromString(task.JobId)
	job := ss.jobMap.FindPtrOrNull(jobID)
	if job == nil {
		response.Type = proto.TaskReplyType_TASK_JOB_NOT_FOUND
		return response, nil
	}

	// Don't remove the root task so that tasks can still be appended to
	// the job. We only remove the root task when the job completes.
	if taskID.TaskUid != job.RootTask.Uid {
		delete(ss.taskMap.UnsafeGet(), utility.TaskID(taskID.TaskUid))
	}

	_, ok := ss.jobTasksNumToRemoveMap[jobID]
	if !ok {
		return nil, fmt.Errorf("could not find tasks num to remove in the map of scheduler service")
	}
	ss.jobTasksNumToRemoveMap[jobID]--
	if ss.jobTasksNumToRemoveMap[jobID] == 0 {
		incompletedTasksNum := ss.jobIncompleteTasksNumMap[jobID]
		if incompletedTasksNum > 0 {
			ss.scheduler.HandleJobRemoval(jobID)
		}
		// Delete the job because we removed its last task.
		delete(ss.taskMap.UnsafeGet(), utility.TaskID(job.RootTask.Uid))
		delete(ss.jobMap.UnsafeGet(), jobID)
		delete(ss.jobIncompleteTasksNumMap, jobID)
		delete(ss.jobTasksNumToRemoveMap, jobID)
	}

	response.Type = proto.TaskReplyType_TASK_REMOVED_OK
	return response, nil
}

func (ss *schedulerServer) TaskSubmitted(context context.Context, td *proto.TaskDescription) (*proto.TaskSubmittedResponse, error) {
	response := &proto.TaskSubmittedResponse{}
	taskID := utility.TaskID(td.TaskDescriptor.Uid)
	task := ss.taskMap.FindPtrOrNull(taskID)
	if task != nil {
		response.Type = proto.TaskReplyType_TASK_ALREADY_SUBMITTED
		return response, nil
	}

	if td.TaskDescriptor.State != proto.TaskDescriptor_CREATED {
		response.Type = proto.TaskReplyType_TASK_STATE_NOT_CREATED
		return response, nil
	}

	jobID := utility.MustJobIDFromString(td.TaskDescriptor.JobId)
	job := ss.jobMap.FindPtrOrNull(jobID)
	if job == nil {
		ss.jobMap.InsertIfNotPresent(jobID, td.JobDescriptor)
		job = ss.jobMap.FindPtrOrNull(jobID)
		// TODO: there may be a BUG: the root task of the job may not be the task submitted now
		// TODO: solution: make the task submitted now as the root task and make the previous root task as its spawn
		rootTask := job.RootTask
		ss.taskMap.InsertIfNotPresent(utility.TaskID(rootTask.Uid), rootTask)
		// TODO: set root task submit time
		// rootTask.SubmitTime =
		ss.jobTasksNumToRemoveMap[jobID] = 0
		ss.jobIncompleteTasksNumMap[jobID] = 0
	} else {
		taskToSubmit := td.TaskDescriptor
		job.RootTask.Spawned = append(job.RootTask.Spawned, taskToSubmit)
		ss.taskMap.InsertIfNotPresent(utility.TaskID(taskToSubmit.Uid), taskToSubmit)
		// TODO: set task submit time
		// taskToSubmit.SubmitTime =
	}

	incompletedNum, ok := ss.jobIncompleteTasksNumMap[jobID]
	if !ok {
		return nil, fmt.Errorf("can not find the job in job incompleted tasks map of scheduer service")
	}
	if incompletedNum == 0 {
		ss.scheduler.AddJob(job)
	}
	ss.jobIncompleteTasksNumMap[jobID]++

	_, ok = ss.jobTasksNumToRemoveMap[jobID]
	if !ok {
		return nil, fmt.Errorf("could not find tasks num to remove in the map of scheduler service")
	}
	ss.jobTasksNumToRemoveMap[jobID]++

	response.Type = proto.TaskReplyType_TASK_SUBMITTED_OK
	return response, nil
}

func (ss *schedulerServer) TaskUpdated(context context.Context, td *proto.TaskDescription) (*proto.TaskUpdatedResponse, error) {
	response := &proto.TaskUpdatedResponse{}
	taskID := utility.TaskID(td.TaskDescriptor.Uid)
	task := ss.taskMap.FindPtrOrNull(taskID)
	if task == nil {
		response.Type = proto.TaskReplyType_TASK_NOT_FOUND
		return response, nil
	}
	// The scheduler will notice that the task's properties (e.g.,
	// resource requirements, labels) are different and react accordingly.
	updatedTask := td.TaskDescriptor
	task.Priority = updatedTask.Priority
	// TODO: copy from updatedTask instead of setting directly
	task.ResourceRequest = updatedTask.ResourceRequest
	task.Labels = updatedTask.Labels
	task.LabelSelectors = updatedTask.LabelSelectors
	// We may want to add support for other field updates as well.

	response.Type = proto.TaskReplyType_TASK_UPDATED_OK
	return response, nil
}

func (ss *schedulerServer) CheckResourceDoesntExist(rd *proto.ResourceDescriptor) bool {
	resource := ss.resourceMap.FindPtrOrNull(utility.MustResourceIDFromString(rd.Uuid))
	return resource == nil
}

func (ss *schedulerServer) AddResource(rtnd *proto.ResourceTopologyNodeDescriptor) {
	rd := rtnd.ResourceDesc
	resID := utility.MustResourceIDFromString(rd.Uuid)
	rs := &utility.ResourceStatus{
		Descriptor: rd,
		TopologyNode: rtnd,
		EndpointUri: rd.FriendlyName,
		LastHeartbeat: 0,
	}
	ss.resourceMap.InsertIfNotPresent(resID, rs)
}

// CheckResourceDoesntExist:   true: not exist   false: exist
// dfs function:               true: not exist   false: exist
func (ss *schedulerServer) DFSTraverseResourceProtobufTreeWhileTrue(rtnd *proto.ResourceTopologyNodeDescriptor) bool {
	if !ss.CheckResourceDoesntExist(rtnd.ResourceDesc) {
		return false
	}
	for _, child := range rtnd.Children {
		if !ss.DFSTraverseResourceProtobufTreeWhileTrue(child) {
			return false
		}
	}

	return true
}

func (ss *schedulerServer) DFSTraverseResourceProtobufTreeReturnRTND(rtnd *proto.ResourceTopologyNodeDescriptor) {
	ss.AddResource(rtnd)

	for _, child := range rtnd.Children {
		ss.DFSTraverseResourceProtobufTreeReturnRTND(child)
	}
}

func (ss *schedulerServer) NodeAdded(context context.Context, rtnd *proto.ResourceTopologyNodeDescriptor) (*proto.NodeAddedResponse, error) {
	response := &proto.NodeAddedResponse{}
	doesntExist := ss.DFSTraverseResourceProtobufTreeWhileTrue(rtnd)
	if !doesntExist {
		response.Type = proto.NodeReplyType_NODE_ALREADY_EXISTS
		return response, nil
	}

	rootRs := ss.resourceMap.FindPtrOrNull(ss.topLevelResID)
	if rootRs == nil {
		return nil, fmt.Errorf("root resource status is nil")
	}

	rootRs.TopologyNode.Children = append(rootRs.TopologyNode.Children, rtnd)
	rtnd.ParentId = string(ss.topLevelResID)

	ss.DFSTraverseResourceProtobufTreeReturnRTND(rtnd)

	ss.scheduler.RegisterResource(rtnd)

	response.Type = proto.NodeReplyType_NODE_ADDED_OK
	return response, nil
}

func (ss *schedulerServer) NodeFailed(context context.Context, resUID *proto.ResourceUID) (*proto.NodeFailedResponse, error) {
	response := &proto.NodeFailedResponse{}

	resID := utility.MustResourceIDFromString(resUID.ResourceUid)
	rs := ss.resourceMap.FindPtrOrNull(resID)
	if rs == nil {
		response.Type = proto.NodeReplyType_NODE_NOT_FOUND
		return response, nil
	}

	ss.scheduler.DeregisterResource(rs.TopologyNode)

	response.Type = proto.NodeReplyType_NODE_FAILED_OK
	return response, nil
}

func (ss *schedulerServer) NodeRemoved(context context.Context, resUID *proto.ResourceUID) (*proto.NodeRemovedResponse, error) {
	response := &proto.NodeRemovedResponse{}
	resID := utility.MustResourceIDFromString(resUID.ResourceUid)
	rs := ss.resourceMap.FindPtrOrNull(resID)
	if rs == nil {
		response.Type = proto.NodeReplyType_NODE_NOT_FOUND
		return response, nil
	}

	ss.scheduler.DeregisterResource(rs.TopologyNode)

	response.Type = proto.NodeReplyType_NODE_REMOVED_OK
	return response, nil
}

func (ss *schedulerServer) UpdateNodeLabels(oldRtnd, newRtnd *proto.ResourceTopologyNodeDescriptor) {
	oldRD := oldRtnd.ResourceDesc
	newRD := newRtnd.ResourceDesc

	// TODO: copy node labels from new node to old node
	/*oldRD.clearLabels()
	for _, label := range newRD.Labels {
		labelCopy := label.Copy()
		oldRD.addLabel(labelCopy)
	}*/
}

func (ss *schedulerServer) DFSTraverseResourceProtobufTreesReturnRTNDs(oldRtnd, newRtnd *proto.ResourceTopologyNodeDescriptor) {
	ss.UpdateNodeLabels(oldRtnd, newRtnd)

	len1 := len(oldRtnd.Children)
	len2 := len(newRtnd.Children)
	if len1 != len2 {
		log.Panicf("children length of the two node is not equal")
	}

	for i := 0; i < len1; i++ {
		ss.DFSTraverseResourceProtobufTreesReturnRTNDs(oldRtnd.Children[i], newRtnd.Children[i])
	}
}

func (ss *schedulerServer) NodeUpdated(context context.Context, rtnd *proto.ResourceTopologyNodeDescriptor) (*proto.NodeUpdatedResponse, error) {
	response := &proto.NodeUpdatedResponse{}

	resID := utility.MustResourceIDFromString(rtnd.ResourceDesc.Uuid)
	rs := ss.resourceMap.FindPtrOrNull(resID)
	if rs == nil {
		response.Type = proto.NodeReplyType_NODE_NOT_FOUND
		return response, nil
	}

	ss.DFSTraverseResourceProtobufTreesReturnRTNDs(rs.TopologyNode, rtnd)
	// TODO: support other types of node updates

	response.Type = proto.NodeReplyType_NODE_UPDATED_OK
	return response, nil
}

func (ss *schedulerServer) AddTaskStats(context context.Context, taskStats *proto.TaskStats) (*proto.TaskStatsResponse, error) {
	response := &proto.TaskStatsResponse{}

	taskID := utility.TaskID(taskStats.TaskId)
	task := ss.taskMap.FindPtrOrNull(taskID)
	if task == nil {
		response.Type = proto.TaskReplyType_TASK_NOT_FOUND
		return response, nil
	}

	// TODO: knowledge base add task stats sample

	// TODO: add more states for the stats operations
	response.Type = proto.TaskReplyType_TASK_SUBMITTED_OK
	return response, nil
}

func (ss *schedulerServer) AddNodeStats(context context.Context, resStats *proto.ResourceStats) (*proto.ResourceStatsResponse, error) {
	response := &proto.ResourceStatsResponse{}

	resID := utility.MustResourceIDFromString(resStats.ResourceId)
	res := ss.resourceMap.FindPtrOrNull(resID)
	if res == nil || res.Descriptor == nil {
		response.Type = proto.NodeReplyType_NODE_NOT_FOUND
		return response, nil
	}

	// TODO: knowledge base add machine stats sample
	// TODO: add more states for the stats operations
	response.Type = proto.NodeReplyType_NODE_ADDED_OK
	return response, nil
}
