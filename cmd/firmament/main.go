package main

import (
	"context"

	"nickren/firmament-go/pkg/firmamentservice"
	"nickren/firmament-go/pkg/proto"
	"fmt"
	"time"
)

func main() {
	fmt.Println("firmament starting...")
	server := firmamentservice.NewSchedulerServer()

	for {
		fmt.Println("calling firmament schedule service...")
		deltas, err := server.Schedule(context.Background(), &proto.ScheduleRequest{})
		if err != nil {
			fmt.Println("call firmament schedule error: ", err)
			return
		} else {
			for _, delta := range deltas.Deltas {
				fmt.Println("task:", delta.TaskId, " is scheduled to node:", delta.ResourceId)
			}
		}

		time.Sleep(10 * time.Second)
	}
}
