package firmamentservice

import (
	"context"
	"github.com/golang/glog"
	"github.com/labstack/gommon/log"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"nickren/firmament-go/pkg/proto"
)

var _ = Describe("Firmametservice", func() {
	glog.Info("Test")
	var ss = NewSchedulerServer()

	Describe("Add Machine using firmament service", func() {
		Context("start test", func() {
			It("example 1", func() {
				rtnd := createMockRTND("1")
				response, err := ss.NodeAdded(context.Background(), rtnd)
				Expect(err).Should(BeNil())
				Expect(response.Type).To(Equal(proto.NodeReplyType_NODE_ADDED_OK))
			})
		})
	})
	Describe("Add Taks using firmament service", func() {
		Context("start test", func() {
			It("example 1", func() {
				td := createMockTaskDescription()
				response, err := ss.TaskSubmitted(context.Background(), td)
				Expect(err).Should(BeNil())
				Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_SUBMITTED_OK))
			})
		})
	})
	Describe("Schedule after adding tasks and machines", func() {
		Context("start test", func() {
			It("example 1", func() {
				sq := &proto.ScheduleRequest{}
				deltas, err := ss.Schedule(context.Background(), sq)
				Expect(err).Should(BeNil())
				for _, delta := range deltas.Deltas {
					log.Printf("task:", delta.TaskId, " is scheduled to node:", delta.ResourceId)
				}
			})
		})
	})
})

func createMockRTND(idx string) *proto.ResourceTopologyNodeDescriptor {
	rtnd := &proto.ResourceTopologyNodeDescriptor{
		ResourceDesc: &proto.ResourceDescriptor{
			Uuid:         "6209743969",
			Type:         proto.ResourceDescriptor_RESOURCE_MACHINE,
			State:        proto.ResourceDescriptor_RESOURCE_IDLE,
			FriendlyName: idx,
			ResourceCapacity: &proto.ResourceVector{
				RamCap:   uint64(1024 * 256),
				CpuCores: float32(64),
			},
			AvailableResources: &proto.ResourceVector{
				RamCap:   uint64(1024 * 256),
				CpuCores: float32(64),
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
			Uuid:         "5224113196",
			Type:         proto.ResourceDescriptor_RESOURCE_PU,
			State:        proto.ResourceDescriptor_RESOURCE_IDLE,
			FriendlyName: friendlyName,
			ResourceCapacity: &proto.ResourceVector{
				RamCap:   uint64(1024 * 6),
				CpuCores: float32(64),
			},
		},
		ParentId: "6209743969",
	}
	puRtnd.ParentId = "6209743969"
	rtnd.Children = append(rtnd.Children, puRtnd)
	return rtnd
}

func createMockTaskDescription() *proto.TaskDescription {
	jdUid := "1"
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
			CpuCores: float32(4),
			RamCap:   uint64(1024 * 16),
		},
	}
	jd.RootTask = td
	td.Uid = 11
	taskDescription := &proto.TaskDescription{
		TaskDescriptor: td,
		JobDescriptor:  jd,
	}
	return taskDescription
}
