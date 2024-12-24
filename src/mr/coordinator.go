package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mapTasks       map[string]int
	reduceTasks    map[string]int
	taskMap        map[int]taskStatus
	nReduce        int
	nextTaskNumber int
	lock           sync.Mutex
}

type taskStatus struct {
	startTime time.Time
	endTime   time.Time
	success   bool
}

func (c *Coordinator) checkTask(task int) bool {
	if task == 0 {
		return false
	}
	taskState := c.taskMap[task]
	if taskState.success {
		return true
	}
	elapsedTime := time.Since(taskState.startTime)
	return elapsedTime <= (10 * time.Second)
}

func (c *Coordinator) phaseFinished(mapPhase bool) bool {
	c.lock.Lock()
	iterateMap := c.reduceTasks
	if mapPhase {
		iterateMap = c.mapTasks
	}
	for _, v := range iterateMap {
		if v == 0 {
			c.lock.Unlock()
			return false
		}
		if !c.taskMap[v].success {
			c.lock.Unlock()
			return false
		}
	}
	c.lock.Unlock()
	return true
}

func (c *Coordinator) ServeTask(args *TaskArgs, reply *TaskReply) error {
	for {
		if c.phaseFinished(true) {
			break
		}
		c.lock.Lock()
		for k, v := range c.mapTasks {
			if !c.checkTask(v) {
				reply.InputFiles = []string{k}
				reply.IsMapTask = true
				reply.NumReduce = c.nReduce
				reply.TaskNumber = c.nextTaskNumber
				c.taskMap[c.nextTaskNumber] = taskStatus{startTime: time.Now()}
				c.mapTasks[k] = c.nextTaskNumber
				c.nextTaskNumber++
				c.lock.Unlock()
				return nil
			}
		}
		c.lock.Unlock()
		time.Sleep(time.Second)
	}
	for {
		if c.phaseFinished(false) {
			break
		}
		c.lock.Lock()
		for k, v := range c.reduceTasks {
			if !c.checkTask(v) {
				fileNames := []string{}
				for _, vv := range c.mapTasks {
					fileNames = append(fileNames, fmt.Sprintf("mr-%d-%s", vv, k))
				}
				reply.InputFiles = fileNames
				reply.IsMapTask = false
				reply.TaskNumber = c.nextTaskNumber
				reply.ReduceKey = k
				c.taskMap[c.nextTaskNumber] = taskStatus{startTime: time.Now()}
				c.reduceTasks[k] = c.nextTaskNumber
				c.nextTaskNumber++
				c.lock.Unlock()
				return nil
			}
		}
		c.lock.Unlock()
		time.Sleep(time.Second)
	}
	reply.IsFinished = true
	return nil
}

func (c *Coordinator) ReceiveTaskComplete(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.lock.Lock()
	taskState := c.taskMap[args.TaskNumber]
	taskState.endTime = time.Now()
	taskState.success = args.Success
	c.taskMap[args.TaskNumber] = taskState
	c.lock.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return c.phaseFinished(true) && c.phaseFinished(false)
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.mapTasks = make(map[string]int)
	c.reduceTasks = make(map[string]int)
	c.taskMap = make(map[int]taskStatus)
	c.nReduce = nReduce

	for _, v := range files {
		c.mapTasks[v] = 0
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[strconv.Itoa(i)] = 0
	}

	c.nextTaskNumber = 1
	c.lock = sync.Mutex{}

	c.server()
	return &c
}
