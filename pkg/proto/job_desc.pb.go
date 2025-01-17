// Code generated by protoc-gen-go. DO NOT EDIT.
// source: job_desc.proto

package proto

import (
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type JobDescriptor_JobState int32

const (
	JobDescriptor_NEW       JobDescriptor_JobState = 0
	JobDescriptor_CREATED   JobDescriptor_JobState = 1
	JobDescriptor_RUNNING   JobDescriptor_JobState = 2
	JobDescriptor_COMPLETED JobDescriptor_JobState = 3
	JobDescriptor_FAILED    JobDescriptor_JobState = 4
	JobDescriptor_ABORTED   JobDescriptor_JobState = 5
	JobDescriptor_UNKNOWN   JobDescriptor_JobState = 6
)

var JobDescriptor_JobState_name = map[int32]string{
	0: "NEW",
	1: "CREATED",
	2: "RUNNING",
	3: "COMPLETED",
	4: "FAILED",
	5: "ABORTED",
	6: "UNKNOWN",
}

var JobDescriptor_JobState_value = map[string]int32{
	"NEW":       0,
	"CREATED":   1,
	"RUNNING":   2,
	"COMPLETED": 3,
	"FAILED":    4,
	"ABORTED":   5,
	"UNKNOWN":   6,
}

func (x JobDescriptor_JobState) String() string {
	return proto.EnumName(JobDescriptor_JobState_name, int32(x))
}

func (JobDescriptor_JobState) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_4f2b7ee7452ce50f, []int{0, 0}
}

type JobDescriptor struct {
	Uuid                 string                 `protobuf:"bytes,1,opt,name=uuid,proto3" json:"uuid,omitempty"`
	Name                 string                 `protobuf:"bytes,2,opt,name=name,proto3" json:"name,omitempty"`
	State                JobDescriptor_JobState `protobuf:"varint,3,opt,name=state,proto3,enum=proto.JobDescriptor_JobState" json:"state,omitempty"`
	RootTask             *TaskDescriptor        `protobuf:"bytes,4,opt,name=root_task,json=rootTask,proto3" json:"root_task,omitempty"`
	OutputIds            [][]byte               `protobuf:"bytes,5,rep,name=output_ids,json=outputIds,proto3" json:"output_ids,omitempty"`
	XXX_NoUnkeyedLiteral struct{}               `json:"-"`
	XXX_unrecognized     []byte                 `json:"-"`
	XXX_sizecache        int32                  `json:"-"`
}

func (m *JobDescriptor) Reset()         { *m = JobDescriptor{} }
func (m *JobDescriptor) String() string { return proto.CompactTextString(m) }
func (*JobDescriptor) ProtoMessage()    {}
func (*JobDescriptor) Descriptor() ([]byte, []int) {
	return fileDescriptor_4f2b7ee7452ce50f, []int{0}
}

func (m *JobDescriptor) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_JobDescriptor.Unmarshal(m, b)
}
func (m *JobDescriptor) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_JobDescriptor.Marshal(b, m, deterministic)
}
func (m *JobDescriptor) XXX_Merge(src proto.Message) {
	xxx_messageInfo_JobDescriptor.Merge(m, src)
}
func (m *JobDescriptor) XXX_Size() int {
	return xxx_messageInfo_JobDescriptor.Size(m)
}
func (m *JobDescriptor) XXX_DiscardUnknown() {
	xxx_messageInfo_JobDescriptor.DiscardUnknown(m)
}

var xxx_messageInfo_JobDescriptor proto.InternalMessageInfo

func (m *JobDescriptor) GetUuid() string {
	if m != nil {
		return m.Uuid
	}
	return ""
}

func (m *JobDescriptor) GetName() string {
	if m != nil {
		return m.Name
	}
	return ""
}

func (m *JobDescriptor) GetState() JobDescriptor_JobState {
	if m != nil {
		return m.State
	}
	return JobDescriptor_NEW
}

func (m *JobDescriptor) GetRootTask() *TaskDescriptor {
	if m != nil {
		return m.RootTask
	}
	return nil
}

func (m *JobDescriptor) GetOutputIds() [][]byte {
	if m != nil {
		return m.OutputIds
	}
	return nil
}

func init() {
	proto.RegisterEnum("proto.JobDescriptor_JobState", JobDescriptor_JobState_name, JobDescriptor_JobState_value)
	proto.RegisterType((*JobDescriptor)(nil), "proto.JobDescriptor")
}

func init() { proto.RegisterFile("job_desc.proto", fileDescriptor_4f2b7ee7452ce50f) }

var fileDescriptor_4f2b7ee7452ce50f = []byte{
	// 266 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x54, 0x8e, 0xcf, 0x4e, 0x83, 0x40,
	0x10, 0x87, 0xe5, 0x6f, 0xcb, 0xd4, 0x56, 0x32, 0x89, 0x09, 0x31, 0x69, 0x42, 0x7a, 0xe2, 0xd4,
	0x03, 0x7d, 0x02, 0x2c, 0x68, 0xd0, 0xba, 0x98, 0xb5, 0x4d, 0x8f, 0x04, 0x0a, 0x07, 0x6c, 0x74,
	0x09, 0xbb, 0x3c, 0x91, 0x2f, 0x6a, 0x06, 0x34, 0xda, 0xd3, 0xce, 0xef, 0x9b, 0x6f, 0x66, 0x07,
	0x16, 0xef, 0xa2, 0xcc, 0xab, 0x5a, 0x9e, 0xd6, 0x6d, 0x27, 0x94, 0x40, 0x6b, 0x78, 0xee, 0x6e,
	0x54, 0x21, 0xcf, 0xff, 0xf8, 0xea, 0x4b, 0x87, 0xf9, 0x93, 0x28, 0xe3, 0x5a, 0x9e, 0xba, 0xa6,
	0x55, 0xa2, 0x43, 0x04, 0xb3, 0xef, 0x9b, 0xca, 0xd3, 0x7c, 0x2d, 0x70, 0xf8, 0x50, 0x13, 0xfb,
	0x2c, 0x3e, 0x6a, 0x4f, 0x1f, 0x19, 0xd5, 0xb8, 0x01, 0x4b, 0xaa, 0x42, 0xd5, 0x9e, 0xe1, 0x6b,
	0xc1, 0x22, 0x5c, 0x8e, 0x0b, 0xd7, 0x17, 0xcb, 0x28, 0xbd, 0x91, 0xc4, 0x47, 0x17, 0x43, 0x70,
	0x3a, 0x21, 0x54, 0x4e, 0x67, 0x78, 0xa6, 0xaf, 0x05, 0xb3, 0xf0, 0xf6, 0x67, 0x70, 0x5f, 0xc8,
	0xf3, 0xdf, 0x24, 0x9f, 0x92, 0x47, 0x0c, 0x97, 0x00, 0xa2, 0x57, 0x6d, 0xaf, 0xf2, 0xa6, 0x92,
	0x9e, 0xe5, 0x1b, 0xc1, 0x35, 0x77, 0x46, 0x92, 0x56, 0x72, 0x55, 0xc2, 0xf4, 0xf7, 0x17, 0x9c,
	0x80, 0xc1, 0x92, 0xa3, 0x7b, 0x85, 0x33, 0x98, 0x6c, 0x79, 0x12, 0xed, 0x93, 0xd8, 0xd5, 0x28,
	0xf0, 0x03, 0x63, 0x29, 0x7b, 0x74, 0x75, 0x9c, 0x83, 0xb3, 0xcd, 0x5e, 0x5e, 0x77, 0x09, 0xf5,
	0x0c, 0x04, 0xb0, 0x1f, 0xa2, 0x74, 0x97, 0xc4, 0xae, 0x49, 0x5e, 0x74, 0x9f, 0x71, 0x6a, 0x58,
	0x14, 0x0e, 0xec, 0x99, 0x65, 0x47, 0xe6, 0xda, 0xa5, 0x3d, 0x9c, 0xb8, 0xf9, 0x0e, 0x00, 0x00,
	0xff, 0xff, 0xd8, 0x22, 0x26, 0xfa, 0x56, 0x01, 0x00, 0x00,
}
