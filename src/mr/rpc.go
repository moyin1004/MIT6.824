package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	NoTask TaskType = iota
	MapTask
	ReduceTask
	WaitTask
)

type ReqestTaskArgs struct {
	WorkerId int
}

type ReqestTaskReply struct {
	TaskId   int
	FileName string
	Type     TaskType
	NReduce  int
	NMap     int
}

type SubmitResultArgs struct {
	TaskId int
	Type   TaskType
}

type SubmitResultReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
