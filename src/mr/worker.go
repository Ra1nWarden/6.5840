package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		task := TaskRequest()
		if task == nil {
			break
		}
		if task.IsFinished {
			break
		}
		if task.IsMapTask {
			res := runMapTask(task.InputFiles[0], task.NumReduce, task.TaskNumber, mapf)
			TaskCompleteRequest(task.TaskNumber, res)
		} else {
			res := runReduceTask(task.InputFiles, task.ReduceKey, reducef)
			TaskCompleteRequest(task.TaskNumber, res)
		}
	}

}

func runMapTask(filename string, nReduce int, taskNumber int, mapf func(string, string) []KeyValue) bool {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	mapped := mapf(filename, string(content))
	partitioned := make([][]KeyValue, nReduce)
	for _, kv := range mapped {
		idx := ihash(kv.Key) % nReduce
		partitioned[idx] = append(partitioned[idx], kv)
	}
	for i, v := range partitioned {
		outputFile, err := os.Create(fmt.Sprintf("mr-%d-%d", taskNumber, i))
		if err != nil {
			return false
		}
		defer outputFile.Close()
		enc := json.NewEncoder(outputFile)
		for _, kv := range v {
			err = enc.Encode(&kv)
			if err != nil {
				return false
			}
		}
	}
	return true
}

func runReduceTask(filenames []string, reduceKey string, reducef func(string, []string) string) bool {
	kva := []KeyValue{}
	for _, filename := range filenames {
		file, err := os.Open(filename)
		if err != nil {
			return false
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%s", reduceKey)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
	return true
}

func TaskRequest() *TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}

	ok := call("Coordinator.ServeTask", &args, &reply)
	if !ok {
		return nil
	}
	return &reply
}

func TaskCompleteRequest(task int, success bool) {
	args := TaskCompleteArgs{}
	args.TaskNumber = task
	args.Success = success

	reply := TaskReply{}
	call("Coordinator.ReceiveTaskComplete", &args, &reply)
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
