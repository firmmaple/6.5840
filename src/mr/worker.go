package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"net/rpc"
	"os"
	"sort"
	"time"

	"6.5840/logger"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

var (
	topicGeneral = logger.LogTopic("WK-G")
	topicMap     = logger.LogTopic("WK-M")
	topicReduce  = logger.LogTopic("WK-R")
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	// Your worker implementation here.
	assignmentReply := &AssignmentReply{}
	ok := call("Coordinator.RequireTask", os.Getpid(), assignmentReply)
	if !ok {
		logger.Fatalln(topicGeneral, "Call failed(Require Task)!")
	}
	logger.Info(topicGeneral, "Received first assignemnt: %v\n", assignmentReply)
	taskCompletionDetails := &TaskCompletionArgs{}
	for {
		if assignmentReply.Type == MapTask { // 处理Map任务
			handleMapTask(assignmentReply, mapf)
			taskCompletionDetails.Type = MapTask
		} else if assignmentReply.Type == ReduceTask { // 处理Reduce任务
			handleReduceTask(assignmentReply, reducef)
			taskCompletionDetails.Type = ReduceTask
		} else if assignmentReply.Type == NoTaskAvailable { // 等待
			logger.Infoln(topicGeneral, "No task Available, waiting ...")
			for assignmentReply.Type == NoTaskAvailable {
				time.Sleep(1 * time.Second)
				logger.Debug(topicGeneral, "Try another request after waitting\n")
				ok := call("Coordinator.RequireTask", os.Getpid(), assignmentReply)
				if !ok {
					logger.Fatalln(topicGeneral, "Call failed(Require Task)!")
				}
				logger.Info(topicReduce, "Assignemnt after waiting: %v\n", assignmentReply)
			}
			continue
		} else if assignmentReply.Type == AllTasksCompleted {
			logger.Infoln(topicGeneral, "All task Completed")
			break
		}

		taskCompletionDetails.WorkerID = os.Getpid()
		taskCompletionDetails.TaskID = assignmentReply.TaskID
		logger.Info(topicGeneral, "Complete task %v\n", taskCompletionDetails)
		// https://github.com/golang/go/issues/8997
		// 无语，被这个bug浪费了一天时间，当变量为0时RPC不会传输
		// 所以当reduce任务开始时，尽管coordinator将taskid设置为0，worker接受到的
		// taskID的值还是map任务遗留下的值，会出现问题
		// 所以这边手动重置了一下
		assignmentReply.TaskID = 0
		ok := call("Coordinator.ReportTaskCompletion", taskCompletionDetails, assignmentReply)
		if !ok {
			logger.Fatalln(topicGeneral, "Call failed(Report Task Completion)!")
		}
		logger.Info(topicReduce, "Received assignemnt: %v\n", assignmentReply)

	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func handleMapTask(
	assignment *AssignmentReply,
	mapf func(string, string) []KeyValue,
) {
	intermediate := []KeyValue{}
	file, err := os.Open(assignment.TaskName)
	if err != nil {
		logger.Fatal(topicMap, "Cannot open %v: %v\n", assignment.TaskName, err)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		logger.Fatal(topicMap, "Cannot read %v: %v\n", assignment.TaskName, err)
	}
	file.Close()
	kva := mapf(assignment.TaskName, string(content))
	intermediate = append(intermediate, kva...)
	logger.Info(
		topicMap,
		"Generating intermediate files for map task %v(%%%v), \n",
		assignment.TaskName,
		assignment.TaskID,
	)

	intermediateFiles := make([]*os.File, assignment.NReduce)
	encoders := make([]*json.Encoder, assignment.NReduce)
	for i := 0; i < assignment.NReduce; i++ {
		// A reasonable naming convention for intermediate files is mr-X-Y,
		// where X is the Map task number, and Y is the reduce task number.
		intermediateFileName := fmt.Sprintf("mr-i-%d-%d", assignment.TaskID, i)
		file, err = os.Create(intermediateFileName)
		if err != nil {
			logger.Fatal(topicMap, "Cannot create intermediate file %v", intermediateFileName)
		}
		defer file.Close()
		intermediateFiles[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	for _, kv := range kva {
		bucket := ihash(kv.Key) % assignment.NReduce
		encoders[bucket].Encode(kv)
	}
}

func handleReduceTask(
	assignment *AssignmentReply,
	reducef func(string, []string) string,
) {
	intermediate := []KeyValue{}
	for i := 0; i < assignment.NMap; i++ {
		intermediateFileName := fmt.Sprintf("mr-i-%d-%d", i, assignment.TaskID)
		file, err := os.Open(intermediateFileName)
		if err != nil {
			logger.Fatal(topicReduce, "Cannot open file %v: %v\n", intermediateFileName, err)
		}
		defer file.Close()

		decoder := json.NewDecoder(file)
		var kvs []KeyValue
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				logger.Fatal(topicReduce, "Error decoding file %v: %v", intermediateFileName, err)
			}
			kvs = append(kvs, kv)
		}

		intermediate = append(intermediate, kvs...)
	}

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%d", assignment.TaskID)
	ofile, err := os.Create(oname)
	if err != nil {
		logger.Fatal(topicReduce, "Cannot create file %v: %v", oname, err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {
	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		logger.Fatal(topicGeneral, "dialing: %v", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
