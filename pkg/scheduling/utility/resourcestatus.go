package utility

import (
	"strconv"

	pb "nickren/firmament-go/pkg/proto"
)

type ResourceStatus struct {
	Descriptor    *pb.ResourceDescriptor
	TopologyNode  *pb.ResourceTopologyNodeDescriptor
	EndpointUri   string
	LastHeartbeat uint64
}

func CreateTopLevelResourceStatus() *ResourceStatus {
	resID := GenerateResourceID()

	idString := strconv.FormatUint(uint64(resID), 10)
	rd := &pb.ResourceDescriptor{
		Uuid: idString,
		// TaskCapacity: uint64(0),
		Type: pb.ResourceDescriptor_RESOURCE_COORDINATOR,
		// Default state and type
		State:       pb.ResourceDescriptor_RESOURCE_IDLE,
		Schedulable: true,
	}

	rtnd := &pb.ResourceTopologyNodeDescriptor{
		ResourceDesc: rd,
	}

	return &ResourceStatus{
		Descriptor:    rd,
		TopologyNode:  rtnd,
		EndpointUri:   "root_resource",
		LastHeartbeat: 0,
	}
}
