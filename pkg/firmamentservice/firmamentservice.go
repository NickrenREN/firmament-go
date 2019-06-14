package firmamentservice

import (
	"context"

	"nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/flowscheduler"
	"nickren/firmament-go/pkg/scheduling/utility"
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

	ss.scheduler = flowscheduler.NewScheduler()

	return ss
}

func (ss *schedulerServer) Schedule(context.Context, *proto.ScheduleRequest) (*proto.SchedulingDeltas, error) {
	return nil, nil
}

func (ss *schedulerServer) TaskCompleted(context.Context, *proto.TaskUID) (*proto.TaskCompletedResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) TaskFailed(context.Context, *proto.TaskUID) (*proto.TaskFailedResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) TaskRemoved(context.Context, *proto.TaskUID) (*proto.TaskRemovedResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) TaskSubmitted(context.Context, *proto.TaskDescription) (*proto.TaskSubmittedResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) TaskUpdated(context.Context, *proto.TaskDescription) (*proto.TaskUpdatedResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) NodeAdded(context.Context, *proto.ResourceTopologyNodeDescriptor) (*proto.NodeAddedResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) NodeFailed(context.Context, *proto.ResourceUID) (*proto.NodeFailedResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) NodeRemoved(context.Context, *proto.ResourceUID) (*proto.NodeRemovedResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) NodeUpdated(context.Context, *proto.ResourceTopologyNodeDescriptor) (*proto.NodeUpdatedResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) AddTaskStats(context.Context, *proto.TaskStats) (*proto.TaskStatsResponse, error) {
	return nil, nil
}

func (ss *schedulerServer) AddNodeStats(context.Context, *proto.ResourceStats) (*proto.ResourceStatsResponse, error) {
	return nil, nil
}
