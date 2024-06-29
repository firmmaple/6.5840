package mr

import (
	"fmt"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	"6.5840/logger"
)

type TaskState int

// var topicCoordinator = logger.LogTopic(fmt.Sprintf("MAP%d", os.Getpid()))
var (
	topicCoordinator = logger.LogTopic("CORD")
	topicMonitor     = logger.LogTopic("MNTR")
)

const (
	ts_unassigned TaskState = iota
	ts_inProcess
	ts_crashed
	ts_completed
)

type Tasks struct {
	filename      string
	workerID      int
	taskState     TaskState
	taskStartTime time.Time
}

type Coordinator struct {
	// Your definitions here.
	mapTasks []Tasks
	// nextMapTaskID     int
	nMapTaskCompleted int

	reduceTasks []Tasks
	// nextReduceTaskID     int
	nReduceTaskCompleted int
	nReduce              int

	mu sync.Mutex
}

func (c *Coordinator) getNextTaskID(taskType TaskType) int {
	var tasks []Tasks
	var end int

	if taskType == MapTask {
		tasks = c.mapTasks
		end = len(c.mapTasks)
	} else if taskType == ReduceTask {
		tasks = c.reduceTasks
		end = len(c.reduceTasks)
	} else {
		logger.Warn(topicCoordinator, "Wrong Task Type: %v", taskType)
		return -1
	}

	for i := 0; i < end; i++ {
		if tasks[i].taskState == ts_unassigned || tasks[i].taskState == ts_crashed {
			return i
		}
	}

	return end
}

func (c *Coordinator) assignMapTask(workerID int, reply *AssignmentReply) *AssignmentReply {
	reply.Type = MapTask
	reply.TaskID = c.getNextTaskID(MapTask)
	reply.TaskName = c.mapTasks[reply.TaskID].filename
	logger.Debug(topicCoordinator, "Generating map task %%%v\n", reply.TaskID)
	reply.NReduce = c.nReduce
	c.mapTasks[reply.TaskID].taskState = ts_inProcess
	c.mapTasks[reply.TaskID].taskStartTime = time.Now()
	c.mapTasks[reply.TaskID].workerID = workerID

	return reply
}

func (c *Coordinator) assignReduceTask(workerID int, reply *AssignmentReply) *AssignmentReply {
	reply.Type = ReduceTask
	reply.TaskID = c.getNextTaskID(ReduceTask)
	logger.Debug(topicCoordinator, "Generating reduce task %%%v\n", reply.TaskID)
	reply.NReduce = c.nReduce
	reply.NMap = c.nMapTaskCompleted
	c.reduceTasks[reply.TaskID].taskState = ts_inProcess
	c.reduceTasks[reply.TaskID].taskStartTime = time.Now()
	c.reduceTasks[reply.TaskID].workerID = workerID

	return reply
}

func (c *Coordinator) assignTask(workerID int, reply *AssignmentReply) {
	if c.getNextTaskID(MapTask) != len(c.mapTasks) { // 还有未开始的map任务
		logger.Debugln(topicCoordinator, "Has unassigned map task")
		c.assignMapTask(workerID, reply)
	} else if c.nMapTaskCompleted != len(c.mapTasks) {
		// 还有未完成的map任务，需要等待所有map完成后才能开始reduce任务
		logger.Debug(topicCoordinator, "Has in process map task, %d\n", c.getNextTaskID(MapTask))
		reply.Type = NoTaskAvailable
	} else if c.getNextTaskID(ReduceTask) != c.nReduce { // 还有未开始的reduce任务
		logger.Debugln(topicCoordinator, "Has unassigned reduce task")
		c.assignReduceTask(workerID, reply)
	} else if c.nReduceTaskCompleted < c.nReduce { // 还有未完成的reduce任务
		reply.Type = NoTaskAvailable
	} else if c.nReduceTaskCompleted == c.nReduce { // 所有reduce任务都已完成，程序应当结束
		reply.Type = AllTasksCompleted
	} else {
		msg := fmt.Sprintf("nextReduceTaskID: %v, nReduceTaskCompleted: %v, nReduce: %v",
			c.getNextTaskID(ReduceTask), c.nReduceTaskCompleted, c.nReduce)
		logger.Fatalln(topicCoordinator, "This message should never be saw. "+msg)
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) RequireTask(workerID int, reply *AssignmentReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.assignTask(workerID, reply)
	logger.Info(topicCoordinator, "Senting Assignemnt(RequireTask) %v\n", reply)

	return nil
}

func (c *Coordinator) ReportTaskCompletion(
	taskCompletionDetails *TaskCompletionArgs,
	reply *AssignmentReply,
) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// log.Printf("Receive Task %v completion report\n", taskCompletionDetails.TaskID)
	logger.Info(topicCoordinator, "Receive completion report of %v\n", taskCompletionDetails)
	if taskCompletionDetails.Type == MapTask {
		c.mapTasks[taskCompletionDetails.TaskID].taskState = ts_completed
		c.nMapTaskCompleted++
	} else if taskCompletionDetails.Type == ReduceTask {
		c.reduceTasks[taskCompletionDetails.TaskID].taskState = ts_completed
		c.nReduceTaskCompleted++
	} else {
		logger.Fatal(topicCoordinator, "There must be some error in program: %v\n", taskCompletionDetails)
	}

	c.assignTask(taskCompletionDetails.WorkerID, reply)
	logger.Info(topicCoordinator, "Senting Assignemnt(ReportTaskCompletion) %v\n", reply)

	return nil
}

func (c *Coordinator) taskMonitor() {
	logger.Infoln(topicMonitor, "Monitor Started ...")
	nMapTasks := len(c.mapTasks)
	nReduceTasks := len(c.reduceTasks)
	for {
		c.mu.Lock()
		if c.nMapTaskCompleted != nMapTasks {
			for i := 0; i < nMapTasks; i++ {
				if c.mapTasks[i].taskState == ts_inProcess {
					if time.Since(c.mapTasks[i].taskStartTime) > 10*time.Second {
						logger.Errorln(topicMonitor, "The task(reduce) state should be inProcess")
						crashedTask := &c.mapTasks[i]
						logger.Info(
							topicMonitor,
							"Found a crashed map task (%d, %d, %v)\n",
							i,
							crashedTask.workerID,
							crashedTask.taskStartTime,
						)
						crashedTask.taskState = ts_crashed
					}
				}
			}
		} else if c.nReduceTaskCompleted != nReduceTasks {
			for i := 0; i < nReduceTasks; i++ {
				if c.reduceTasks[i].taskState == ts_inProcess {
					if time.Since(c.reduceTasks[i].taskStartTime) > 10*time.Second {
						crashedTask := &c.reduceTasks[i]
						logger.Info(
							topicMonitor,
							"Found a crashed reduce task (%d, %d, %v)\n",
							i,
							crashedTask.workerID,
							crashedTask.taskStartTime,
						)
						crashedTask.taskState = ts_crashed
					}
				}
			}
		}
		c.mu.Unlock()
		time.Sleep(500 * time.Millisecond)
	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		logger.Fatal(topicCoordinator, "listen error: %v", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	// Your code here.
	ret = c.nReduceTaskCompleted == c.nReduce

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		// nextMapTaskID:        0,
		mapTasks:          make([]Tasks, len(files)),
		nMapTaskCompleted: 0,
		// nextReduceTaskID:     0,
		reduceTasks:          make([]Tasks, nReduce),
		nReduceTaskCompleted: 0,
		nReduce:              nReduce,
	}

	for i := range c.mapTasks {
		c.mapTasks[i].filename = files[i]
		c.mapTasks[i].taskState = ts_unassigned
	}

	for j := 0; j < c.nReduce; j++ {
		c.reduceTasks[j].taskState = ts_unassigned
	}

	c.server()
	go c.taskMonitor()
	return &c
}
