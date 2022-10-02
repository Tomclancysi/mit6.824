package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := os.Getpid()
	reply := CallForWorkerParameter(workerId)
	retry := 2
	for reply.TaskId != -1 || retry > 0 {
		if reply.TaskId != -1 {
			if reply.Stage == "MAP" {
				WorkerInMapStage(workerId, reply, mapf)
			} else {
				WorkerInReduceStage(workerId, reply, reducef)
			}
		} else {
			retry -= 1
			time.Sleep(time.Second)
		}

		reply = CallForWorkerParameter(workerId)
	}
}

func WorkerInMapStage(workerId int, reply ExecutorReply, mapf func(string, string) []KeyValue) {
	fmt.Printf("worker %v are ready for MapWork %v\n", workerId, reply.TaskId)
	b, err := os.ReadFile(reply.FilePath)
	if err != nil {
		log.Fatalf("Cannot read file %v", reply.FilePath)
	}
	fileContent := string(b)
	result := mapf(reply.FilePath, fileContent)
	bucket := make(map[int][]KeyValue)
	for _, kv := range result {
		bucket[ihash(kv.Key)%reply.NReduce] = append(bucket[ihash(kv.Key)%reply.NReduce], kv)
	}
	for reduceIdx := 0; reduceIdx < reply.NReduce; reduceIdx++ {
		outPath := IntermediateTempFilePath(reply.TaskId, reduceIdx, workerId)
		// 使用JSONEncoder把结果保存到文件中
		f, err := os.Create(outPath)
		if err != nil {
			log.Fatal("Cannot create file")
		}
		defer f.Close()
		enc := json.NewEncoder(f)
		for _, kv := range bucket[reduceIdx] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("Cannot encode to file")
			}
		}
	}
	// 通知coordinator，map任务完成
	ReportTaskFinish(reply.TaskId, workerId)
}

func WorkerInReduceStage(workerId int, reply ExecutorReply, reducef func(string, []string) string) {
	fmt.Printf("worker %v are ready for ReduceWork %v\n", workerId, reply.TaskId)
	kvList := make([]KeyValue, 0)
	for mapId := 0; mapId < reply.NMAP; mapId++ {
		filePath := IntermediateFormalFilePath(mapId, reply.TaskId)
		f, err := os.Open(filePath)
		if err != nil {
			log.Printf("can not open file %v\n", filePath)
		}
		// 使用json decoder解码文件的每一行
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvList = append(kvList, kv)
		}
	}
	sort.Sort(ByKey(kvList))
	outPath := ReducedResultPath(reply.TaskId)
	ofile, err := os.Create(outPath)
	if err != nil {
		log.Println("can not create new file")
	}
	for i, j, n := 0, 0, len(kvList); i < n; {
		for ; j < n && kvList[j].Key == kvList[i].Key; j++ {

		}
		valueList := make([]string, 0)
		for k := i; k < j; k++ {
			valueList = append(valueList, kvList[k].Value)
		}
		reducedResult := reducef(kvList[i].Key, valueList)
		fmt.Fprintf(ofile, "%v %v\n", kvList[i].Key, reducedResult)
		i = j
	}
	log.Printf("reduce task finished, output saved in %v\n", outPath)
	ReportTaskFinish(reply.TaskId, workerId)
}

// request for works' parameter needed when running
func CallForWorkerParameter(workerId int) ExecutorReply {
	args := ExecutorRequestArgs{}
	args.WokerId = workerId
	reply := ExecutorReply{}
	call("Coordinator.GetWorkerParameter", &args, &reply)
	return reply
}

func ReportTaskFinish(taskId int, workerId int) {
	args := MapTaskFinishArgs{}
	args.MapTaskId = taskId
	args.WorkerId = workerId
	reply := MapTaskFinishReply{}
	call("Coordinator.ReportTaskFinish", &args, &reply)
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
	ok := call("Coordinator.Example", &args, &reply) // 请求谁？我觉得应该是硬编码了
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
