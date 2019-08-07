package firmamentservice

import (
	"context"
	"github.com/golang/glog"
	"github.com/labstack/gommon/log"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"nickren/firmament-go/pkg/proto"
	"strconv"
)

var _ = Describe("Firmametservice", func() {
	glog.Info("Test")
	var ss = NewSchedulerServer()

	Describe("Add Machine using firmament service", func() {
		Context("start test", func() {
			It("example 1", func() {
				for idx := 1; idx <= 500; idx++ {
					id := int64(idx)
					uid := strconv.FormatInt(id, 10)
					puUid := strconv.FormatInt(id+1000, 10)
					//fmt.Printf("%s,%s \n", uid, puUid)
					rtnd := createMockRTND(uid, puUid, 64)
					response, err := ss.NodeAdded(context.Background(), rtnd)
					Expect(err).Should(BeNil())
					Expect(response.Type).To(Equal(proto.NodeReplyType_NODE_ADDED_OK))
				}
			})
			PIt("example 2", func() {
				rtnd := createMockRTND("222", "2222", 32)
				response, err := ss.NodeAdded(context.Background(), rtnd)
				Expect(err).Should(BeNil())
				Expect(response.Type).To(Equal(proto.NodeReplyType_NODE_ADDED_OK))
			})
			PIt("example 3", func() {
				rtnd := createMockRTND("333", "3333", 33)
				response, err := ss.NodeAdded(context.Background(), rtnd)
				Expect(err).Should(BeNil())
				Expect(response.Type).To(Equal(proto.NodeReplyType_NODE_ADDED_OK))
			})
		})
	})
	Describe("Add Taks using firmament service", func() {
		Context("start test", func() {
			It("example job 1", func() {
				By(" first task with core 4")
				td := createMockTaskDescription("77", 71, 4)
				response, err := ss.TaskSubmitted(context.Background(), td)
				Expect(err).Should(BeNil())
				Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_SUBMITTED_OK))
				By(" second task with core 8")
				td = createMockTaskDescription("77", 72, 8)
				response, err = ss.TaskSubmitted(context.Background(), td)
				Expect(err).Should(BeNil())
				Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_SUBMITTED_OK))
			})
			It("example job 2", func() {
				By(" first task with core 4")
				td := createMockTaskDescription("88", 81, 4)
				response, err := ss.TaskSubmitted(context.Background(), td)
				Expect(err).Should(BeNil())
				Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_SUBMITTED_OK))
				By(" first task with core 8")
				td = createMockTaskDescription("88", 82, 8)
				response, err = ss.TaskSubmitted(context.Background(), td)
				Expect(err).Should(BeNil())
				Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_SUBMITTED_OK))
			})
		})
	})
	Describe("Schedule after adding tasks and machines", func() {
		Context("start test", func() {
			Measure("measure schedule", func(b Benchmarker) {
				runtime := b.Time("runtime", func() {
					sq := &proto.ScheduleRequest{}
					deltas, err := ss.Schedule(context.Background(), sq)
					for _, delta := range deltas.Deltas {
						log.Printf("task:", delta.TaskId, " is scheduled to node:", delta.ResourceId)
					}
					Expect(err).Should(BeNil())
				})
				Expect(runtime.Seconds()).Should(BeNumerically("<", 2), "runtime must be short")
			}, 3)
		})
	})
})

func createMockRTND(uid string, puUid string, core int) *proto.ResourceTopologyNodeDescriptor {
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

	friendlyName := "PU #0"
	puRtnd := &proto.ResourceTopologyNodeDescriptor{
		ResourceDesc: &proto.ResourceDescriptor{
			Uuid:         puUid,
			Type:         proto.ResourceDescriptor_RESOURCE_PU,
			State:        proto.ResourceDescriptor_RESOURCE_IDLE,
			FriendlyName: friendlyName,
			ResourceCapacity: &proto.ResourceVector{
				RamCap:   uint64(1024 * core * 4),
				CpuCores: float32(core),
			},
		},
		ParentId: uid,
	}
	puRtnd.ParentId = uid
	//rtnd.Children = append(rtnd.Children, puRtnd)
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
