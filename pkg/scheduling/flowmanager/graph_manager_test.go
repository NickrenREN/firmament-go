package flowmanager

import (
	"strconv"
	"testing"

	pb "nickren/firmament-go/pkg/proto"
	"nickren/firmament-go/pkg/scheduling/costmodel"
	"nickren/firmament-go/pkg/scheduling/dimacs"
	"nickren/firmament-go/pkg/scheduling/utility"
)

func TestAddResourceNode(t *testing.T) {
	//TODO
}

// Create a Graph Manager using the trivial cost model
func createTestGMTrivial() GraphManager {
	resourceMap := utility.NewResourceMap()
	taskMap := utility.NewTaskMap()
	leafResourceIDs := make(map[utility.ResourceID]struct{})
	dimacsStats := &dimacs.ChangeStats{}
	costModeler := costmodel.NewCostModel(resourceMap, taskMap, leafResourceIDs)
	gm := NewGraphManager(costModeler, leafResourceIDs, dimacsStats, 1)
	return gm
}

// TODO: Helper functions that may just be duplicated into each unit test later
func createTestMachine(rtnd *pb.ResourceTopologyNodeDescriptor, machineName string) *pb.ResourceDescriptor {
	utility.SeedRNGWithString(machineName)
	rID := utility.GenerateResourceID()
	rd := rtnd.ResourceDesc
	rd.Uuid = strconv.FormatUint(uint64(rID), 10)
	rd.Type = pb.ResourceDescriptor_RESOURCE_MACHINE
	return rd
}

func createTestTask(jd pb.JobDescriptor, jobIDSeed uint64) {
	utility.SeedRNGWithInt(int64(jobIDSeed))
	jobID := utility.GenerateJobID()
	jd.Uuid = strconv.FormatUint(uint64(jobID), 10)
}
