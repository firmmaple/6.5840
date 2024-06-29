package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
)

// example to show how to declare the arguments
// and reply for an RPC.
type TaskCompletionArgs struct {
	WorkerID int
	TaskID   int
	Type     TaskType
}

func (t *TaskCompletionArgs) String() string {
	switch t.Type {
	case MapTask:
		return fmt.Sprintf("(Map Task %%%d)", t.TaskID)
	case ReduceTask:
		return fmt.Sprintf("(Reduce Task %%%d)", t.TaskID)
	case NoTaskAvailable:
		return "(SIGNAL: waiting for task)"
	case AllTasksCompleted:
		return "(SIGNAL: all task finish)"
	default:
		return "(It should be error to see this)"
	}
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType int

const (
	MapTask TaskType = iota + 1 // 如果从0开始，rpc不会传递0
	ReduceTask
	NoTaskAvailable   // 当前没有可分配的任务
	AllTasksCompleted // 所有任务都以完成
)

type AssignmentReply struct {
	Type     TaskType
	TaskName string
	TaskID   int
	NReduce  int
	NMap     int
}

func (a *AssignmentReply) String() string {
	switch a.Type {
	case MapTask:
		return fmt.Sprintf("(Map Task %%%d)", a.TaskID)
	case ReduceTask:
		return fmt.Sprintf("(Reduce Task %%%d)", a.TaskID)
	case NoTaskAvailable:
		return "(SIGNAL: waiting for task)"
	case AllTasksCompleted:
		return "(SIGNAL: all task finish)"
	default:
		return "(It should be error to see this)"
	}
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
