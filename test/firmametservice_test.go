package test

import (
	"context"
	"fmt"
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"math/rand"
	"nickren/firmament-go/pkg/firmamentservice"
	"nickren/firmament-go/pkg/proto"
	"strconv"
)

var _ = Describe("Firmametservice", func() {
	glog.Info("Test")
	var ss = firmamentservice.NewSchedulerServer()
	var addMachine = func(id int64, core int) {
		uid := strconv.FormatInt(id, 10)
		puUid := strconv.FormatInt(id+1000, 10)
		rtnd := createMockRTND(uid, puUid, core)
		response, err := ss.NodeAdded(context.Background(), rtnd)
		Expect(err).Should(BeNil())
		Expect(response.Type).To(Equal(proto.NodeReplyType_NODE_ADDED_OK))
	}
	var addJobs = func(id, core, jobID int) {
		jdUid := strconv.FormatInt(int64(jobID), 10)
		td := createMockTaskDescription(jdUid, uint64(jobID*100+id), core)
		response, err := ss.TaskSubmitted(context.Background(), td)
		Expect(err).Should(BeNil())
		Expect(response.Type).To(Equal(proto.TaskReplyType_TASK_SUBMITTED_OK))
	}
	Describe("Add Machine using firmament service", func() {
		Context("start test", func() {
			It("example 1", func() {
				addMachine(1, 16)
				addMachine(2, 32)
				addMachine(3, 48)
				addMachine(4, 64)
			})
			PIt("example 1", func() {
				for i := 1; i <= 1000; i++ {
					if i % 4 == 0 {
						addMachine(int64(i), 32)
					} else if i %4 == 1 {
						addMachine(int64(i), 64)
					} else if i %4 == 2 {
						addMachine(int64(i), 96)
					} else {
						addMachine(int64(i), 128)
					}
				}
			})
		})
	})
	Describe("Add Taks using firmament service", func() {
		Context("start test", func() {
			PIt("example job 1", func() {
				By(" first job with 10slots and 10 tasks")
				for id := 1; id <= 5000; id++ {
					addJobs(id, rand.Intn(24) + 1, 11)
				}
			})
			PIt("example job 1", func() {
				By(" first job with 10slots and 10 tasks")
				total := 0

				for id := 1; id <= 4000; id++ {
					if id % 4 == 0 {
						addJobs(id, 10, 11)
						total += 10
					} else if id % 4 == 1 {
						addJobs(id, 20, 11)
						total += 20
					} else if id % 4 == 2 {
						addJobs(id, 30, 11)
						total += 30
					} else {
						addJobs(id, 40, 11)
						total += 40
					}
				}

				fmt.Printf("fuck you %v", total)
			})
			PIt("example job 2", func() {
				By(" first job with 10slots and 5 tasks")
				for id := 1; id <= 10; id++ {
					addJobs(id, 10, 11)
					addJobs(id, 10, 22)
				}
			})
			It("example job 2", func() {
				By(" first job with 10slots and 5 tasks")
				for id := 1; id <= 5; id++ {
					addJobs(id, 10, 11)
					addJobs(id, 20, 22)
				}
			})
			PIt("example job 2", func() {
				By(" first job with 10slots and 5 tasks")
				for id := 1; id <= 6; id++ {
					addJobs(id, 10, 11)
					addJobs(id, 20, 22)
				}
			})
		})
	})
	Describe("Schedule after adding tasks and machines", func() {
		Context("start test", func() {
			Measure("measure schedule", func(b Benchmarker) {
				runtime := b.Time("runtime", func() {
					sq := &proto.ScheduleRequest{}
					//deltas, err := ss.Schedule(context.Background(), sq)
					/*for _, delta := range deltas.Deltas {
						log.Printf("task:", delta.TaskId, " is scheduled to node:", delta.ResourceId)
					}*/
					_, err := ss.Schedule(context.Background(), sq)
					Expect(err).Should(BeNil())
				})
				Expect(runtime.Seconds()).Should(BeNumerically("<", 600), "runtime must be short")
			}, 1)
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
