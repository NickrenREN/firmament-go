package test

import (
	"context"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"log"
	"nickren/firmament-go/pkg/proto"
	"strconv"
)

type MockService struct {
	service proto.FirmamentSchedulerServer
}

func (ms *MockService) addOneMachine(id, core int, _ ...int) {
	uid := strconv.FormatInt(int64(id), 10)
	rtnd := createMockRTND(uid, core)
	response, err := ms.service.NodeAdded(context.Background(), rtnd)
	Expect(err).Should(BeNil())
	Expect(response.Type).To(Equal(proto.NodeReplyType_NODE_ADDED_OK))
}

func (ms *MockService) addOneJob(id, core int, arg ...int) {
	if len(arg) == 0 {
		log.Fatal("Miss jobID")
	}
	jobID := arg[0]
	jdUid := strconv.FormatInt(int64(jobID), 10)
	td := createMockTaskDescription(jdUid, uint64(jobID*100+id), core)
	response, err := ms.service.TaskSubmitted(context.Background(), td)
	Expect(err).Should(BeNil())
	Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_SUBMITTED_OK))
}

func (ms *MockService) AddResources(ids []int, cores []int, add func(int, int, ...int), arg ...int) {
	if len(ids) != len(cores) {
		log.Fatal("Numbers of parameters is not equal")
	}

	for idx, id := range ids {
		add(id, cores[idx], arg...)
	}
}

func (ms *MockService) taskCompleted(id uint64) {
	response, err := ms.service.TaskCompleted(context.Background(), &proto.TaskUID{TaskUid: id})
	Expect(err).Should(BeNil())
	Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_COMPLETED_OK))
}

func (ms *MockService) taskRemoved(id uint64) {
	response, err := ms.service.TaskRemoved(context.Background(), &proto.TaskUID{TaskUid: id})
	Expect(err).Should(BeNil())
	Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_REMOVED_OK))
}

func (ms *MockService) machineRemoved(id uint64) {
	uid := strconv.FormatInt(int64(id), 10)
	response, err := ms.service.NodeRemoved(context.Background(), &proto.ResourceUID{ResourceUid: uid})
	Expect(err).Should(BeNil())
	Expect(response.Type).To(Equal(proto.NodeReplyType_NODE_REMOVED_OK))
}

func (ms *MockService) ChangeResources(ids []uint64, change func(uint64)) {
	for _, id := range ids {
		change(id)
	}
}

// TODO: bugs
func (ms *MockService) taskUpdate(id, core, jobID int) {
	jdUid := strconv.FormatInt(int64(jobID), 10)
	td := createMockTaskDescription(jdUid, uint64(jobID*100+id), core)
	response, err := ms.service.TaskUpdated(context.Background(), td)
	Expect(err).Should(BeNil())
	Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_UPDATED_OK))
}
func (ms *MockService) Schedule(b Benchmarker) {
	runtime := b.Time("runtime", func() {
		sq := &proto.ScheduleRequest{}
		//deltas, err := ss.Schedule(context.Background(), sq)
		/*for _, delta := range deltas.Deltas {
			log.Printf("task:", delta.TaskId, " is scheduled to node:", delta.ResourceId)
		}*/
		_, err := ms.service.Schedule(context.Background(), sq)
		Expect(err).Should(BeNil())
	})
	Expect(runtime.Seconds()).Should(BeNumerically("<", 600), "runtime must be short")
}

func createMockRTND(uid string, core int) *proto.ResourceTopologyNodeDescriptor {
	rtnd := &proto.ResourceTopologyNodeDescriptor{
		ResourceDesc: &proto.ResourceDescriptor{
			Uuid:         uid,
			Type:         proto.ResourceDescriptor_RESOURCE_MACHINE,
			State:        proto.ResourceDescriptor_RESOURCE_IDLE,
			FriendlyName: uid,
			ResourceCapacity: &proto.ResourceVector{
				RamCap:   uint64(1024 * core * 4),
				CpuCores: float32(core),
			},
			AvailableResources: &proto.ResourceVector{
				RamCap:   uint64(1024 * core * 4),
				CpuCores: float32(core),
			},
			ReservedResources: &proto.ResourceVector{
				RamCap:   uint64(0),
				CpuCores: float32(0),
			},
		},
		ParentId: "",
	}
	return rtnd
}

func createMockTaskDescription(jdUid string, taskId uint64, core int) *proto.TaskDescription {
	jd := &proto.JobDescriptor{
		Uuid:  jdUid,
		Name:  "mock_job",
		State: proto.JobDescriptor_CREATED,
	}
	td := &proto.TaskDescriptor{
		Name: "mock_task",
		// TODO: fix proto
		// Namespace: "default",
		State: proto.TaskDescriptor_CREATED,
		JobId: jdUid,
		ResourceRequest: &proto.ResourceVector{
			// TODO(ionel): Update types so no cast is required.
			CpuCores: float32(core),
			RamCap:   uint64(1024 * core * 4),
		},
	}
	jd.RootTask = td
	td.Uid = taskId
	taskDescription := &proto.TaskDescription{
		TaskDescriptor: td,
		JobDescriptor:  jd,
	}
	return taskDescription
}

func generateSlice(length int, gen func(int) int) []int {
	slice := make([]int, length)
	for i := range slice {
		slice[i] = gen(i)
	}
	return slice
}
