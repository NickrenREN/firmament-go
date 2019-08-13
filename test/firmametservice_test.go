package test

import (
	"github.com/golang/glog"
	. "github.com/onsi/ginkgo"
	"math/rand"
	"nickren/firmament-go/pkg/firmamentservice"
)

var _ = Describe("Firmametservice", func() {
	glog.Info("Test")
	var ms = &MockService{service: firmamentservice.NewSchedulerServer()}
	Describe("basic unit test including adding machines and tasks ", func() {
		Context("test adding machines", func() {
			It("example 1: 4 machines ", func() {
				ids := []int{1, 2, 3, 4}
				cores := []int{16, 32, 48, 64}
				ms.AddResources(ids, cores, ms.addOneMachine)
			})
			PIt("example 2: 1000 machines ", func() {
				ids := generateSlice(1000, func(i int) int { return i + 1 })
				cores := generateSlice(1000, func(i int) int {
					switch i % 4 {
					case 0:
						return 32
					case 1:
						return 64
					case 2:
						return 96
					case 4:
						return 128
					}
					return 32
				})
				ms.AddResources(ids, cores, ms.addOneMachine)
			})
		})
		Context(" test adding jobs", func() {
			PIt("example job 1", func() {
				By(" first job with random 25 slots and 1000 tasks")
				ids := generateSlice(5000, func(i int) int { return i + 1 })
				cores := generateSlice(5000, func(i int) int { return rand.Intn(24) + 1 })
				ms.AddResources(ids, cores, ms.addOneJob)
			})
			PIt("example job 2", func() {
				By(" first job with 10, 20, 30, 40 slots and 4000 tasks")
				ids := generateSlice(4000, func(i int) int { return i + 1 })
				cores := generateSlice(4000, func(i int) int {
					switch i % 4 {
					case 0:
						return 10
					case 1:
						return 20
					case 2:
						return 30
					case 4:
						return 40
					}
					return 10
				})
				ms.AddResources(ids, cores, ms.addOneJob, 11)
			})
			PIt("example job 3", func() {
				ids := generateSlice(10, func(i int) int { return i + 1 })
				cores := generateSlice(10, func(i int) int { return 10 })
				ms.AddResources(ids, cores, ms.addOneJob, 11)
				ms.AddResources(ids, cores, ms.addOneJob, 22)
			})
			It("example job 4", func() {
				ms.AddResources([]int{1, 2, 3, 4, 5}, []int{10, 10, 10, 10, 10}, ms.addOneJob, 11)
				ms.AddResources([]int{1, 2, 3, 4, 5}, []int{20, 20, 20, 20, 20}, ms.addOneJob, 22)
			})
			PIt("example job 5", func() {
				ms.AddResources([]int{1, 2, 3, 4, 5, 6}, []int{10, 10, 10, 10, 10, 10}, ms.addOneJob, 11)
				ms.AddResources([]int{1, 2, 3, 4, 5, 6}, []int{10, 10, 10, 10, 10, 10}, ms.addOneJob, 22)
			})
		})
		Context("start test", func() {
			Measure("measure Schedule", func(b Benchmarker) {
				ms.Schedule(b)
			}, 1)
		})
	})
	PDescribe(" add and change resource test", func() {
		Context("init machine and tasks", func() {
			It("add 4 machines", func() {
				ms.AddResources([]int{1, 2, 3, 4}, []int{64, 64, 64, 64}, ms.addOneMachine)
			})
			It("add 4 tasks", func() {
				ms.AddResources([]int{1, 2, 3, 4}, []int{32, 32, 32, 32}, ms.addOneJob, 11)
			})
			Measure("schedule firstly", func(b Benchmarker) {
				ms.Schedule(b)
			}, 1)
		})
		Context(" adding mew tasks", func() {
			It("add 4 tasks", func() {
				ms.AddResources([]int{1, 2, 3, 4}, []int{32, 32, 32, 32}, ms.addOneJob, 22)
			})
			Measure("schedule after adding new task", func(b Benchmarker) {
				ms.Schedule(b)
			}, 1)
		})
		Context(" change resource ", func() {
			It(" notify 4 tasks completed", func() {
				ms.ChangeResources([]uint64{1101, 1102, 1103, 1104}, ms.taskCompleted)
			})
			It(" notify 4 tasks removed", func() {
				ms.ChangeResources([]uint64{1101, 1102, 1103, 1104}, ms.taskRemoved)
			})
			It(" notify 4 new machines add", func() {
				ms.AddResources([]int{5, 6, 7, 8}, []int{48, 48, 48, 48}, ms.addOneMachine)
			})
			It(" notify 4 old machines removed", func() {
				ms.ChangeResources([]uint64{1, 2}, ms.machineRemoved)
			})
		})
		Context(" restart scheduling again", func() {
			Measure("schedule after changing resources", func(b Benchmarker) {
				ms.Schedule(b)
			}, 1)
		})
	})
})
