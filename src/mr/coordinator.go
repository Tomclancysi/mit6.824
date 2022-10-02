package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	workerHasFinished []bool
	taskAllocatedMark []bool
	taskDue           []int64
	intermediateDir   string
	inputFilePath     []string
	registeredWorkers []int
	NReduce           int
	NMap              int
	Stage             string
}

const ROOTDIR = "/home/lining/Desktop/6.824/src/main"

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// 在rpc里面设置 请求和返回时参数对象struct或者叫interface
func (c *Coordinator) GetWorkerParameter(args *ExecutorRequestArgs, reply *ExecutorReply) error {
	allocable := func(idx int) bool {
		return !c.taskAllocatedMark[idx] || (!c.workerHasFinished[idx] && time.Now().Unix() > c.taskDue[idx])
	}
	for i := 0; i < len(c.taskAllocatedMark); i++ {
		if allocable(i) {
			reply.Stage = c.Stage
			reply.FilePath = c.inputFilePath[i]
			reply.TaskId = i
			reply.NMAP = c.NMap
			reply.NReduce = c.NReduce

			c.taskAllocatedMark[i] = true
			c.taskDue[i] = time.Now().Unix() + 10
			c.registeredWorkers[i] = args.WokerId
			log.Printf("Stage %v, TaskId %v, NMap %v, NReduce %v \n", c.Stage, reply.TaskId, c.NMap, c.NReduce)
			return nil
		}
	}
	reply.TaskId = -1
	return nil
}

func (c *Coordinator) ReportTaskFinish(args *MapTaskFinishArgs, reply *MapTaskFinishReply) error {
	log.Println("worker has finished")
	tid := args.MapTaskId
	if c.registeredWorkers[tid] == args.WorkerId {
		c.workerHasFinished[tid] = true
		for i := 0; i < c.NReduce; i++ {
			os.Rename(IntermediateTempFilePath(tid, i, args.WorkerId), IntermediateFormalFilePath(tid, i))
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	fmt.Printf("Server listen at %v", sockname)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func IntermediateTempFilePath(mapTaskId int, reduceTaskId int, workerId int) string {
	return ROOTDIR + "/" + fmt.Sprintf("intermediateResult/mapTask%d-reduceTask%d-worker%d", mapTaskId, reduceTaskId, workerId)
}

func IntermediateFormalFilePath(mapTaskId int, reduceTaskId int) string {
	return ROOTDIR + "/" + fmt.Sprintf("intermediateResult/mapTask%d-reduceTask%d", mapTaskId, reduceTaskId)
}

func ReducedResultPath(reduceTaskId int) string {
	cur, _ := os.Getwd()
	return filepath.Join(cur, fmt.Sprintf("mr-out-%d", reduceTaskId))
	// return ROOTDIR + "/" + fmt.Sprintf("mr-out-%d", reduceTaskId)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	if c.Stage == "MAP" {
		for _, v := range c.workerHasFinished {
			if !v {
				return false
			}
		}
		c.Stage = "REDUCE"
		log.Printf("Map stage finished")
		c.workerHasFinished = make([]bool, c.NReduce)
		c.taskAllocatedMark = make([]bool, c.NReduce)
		c.taskDue = make([]int64, c.NReduce)
		c.inputFilePath = make([]string, c.NReduce)
		c.registeredWorkers = make([]int, c.NReduce)
	} else {
		for _, v := range c.workerHasFinished {
			if !v {
				return false
			}
		}
		return true
	}

	return false
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 应该有nReduce个worker，但是work在哪里开呢，是map打开若干worker，还是worker找map？
	fileCnt := len(files)
	c := Coordinator{
		workerHasFinished: make([]bool, fileCnt),
		taskAllocatedMark: make([]bool, fileCnt),
		registeredWorkers: make([]int, fileCnt),
		taskDue:           make([]int64, fileCnt),
		intermediateDir:   "./intermediateResult",
		inputFilePath:     files,
		NReduce:           nReduce,
		NMap:              fileCnt,
		Stage:             "MAP",
	}

	// Your code here.

	c.server()
	return &c
}
