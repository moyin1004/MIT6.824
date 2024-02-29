package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus int

const (
	Init TaskStatus = iota
	Runing
	Done
)

type Task struct {
	TaskId     int
	Status     TaskStatus
	AssignTime int64
}

type Coordinator struct {
	// Your definitions here.
	files       []string
	mapTasks    []Task
	mapDone     bool
	reduceTasks []Task
	reduceDone  bool
	pos         int
	sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(tasks []Task, typ TaskType, reply *ReqestTaskReply) {
	if c.pos >= len(tasks) {
		c.pos = 0
	}
	for ; c.pos < len(tasks); c.pos++ {
		if tasks[c.pos].Status == Done {
			continue
		}
		if tasks[c.pos].Status == Init || tasks[c.pos].AssignTime+1000 <= time.Now().UnixMilli() {
			*reply = ReqestTaskReply{
				TaskId:  c.pos,
				Type:    typ,
				NReduce: len(c.reduceTasks),
				NMap:    len(c.files),
			}
			if typ == MapTask {
				reply.FileName = c.files[c.pos]
			}
			tasks[c.pos].Status = Runing
			tasks[c.pos].AssignTime = time.Now().UnixMilli()
			c.pos++
			return
		}
	}
}

func (c *Coordinator) ReqestTask(args *ReqestTaskArgs, reply *ReqestTaskReply) error {
	reply.Type = WaitTask
	c.Lock()
	defer c.Unlock()
	if !c.mapDone {
		c.AssignTask(c.mapTasks, MapTask, reply)
	} else if !c.reduceDone {
		c.AssignTask(c.reduceTasks, ReduceTask, reply)
	} else {
		// 任务全部完成 worker可以退出
		reply.Type = NoTask
	}
	return nil
}

func GetDone(tasks []Task) bool {
	for _, task := range tasks {
		if task.Status != Done {
			return false
		}
	}
	return true
}

func (c *Coordinator) SubmitResult(args *SubmitResultArgs, reply *SubmitResultReply) error {
	c.Lock()
	defer c.Unlock()
	switch args.Type {
	case MapTask:
		c.mapTasks[args.TaskId].Status = Done
		c.mapDone = GetDone(c.mapTasks)
	case ReduceTask:
		c.reduceTasks[args.TaskId].Status = Done
		c.reduceDone = GetDone(c.reduceTasks)
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
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
	ret := true

	// Your code here.
	c.Lock()
	ret = c.reduceDone
	c.Unlock()

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		pos:   0,
	}
	// Your code here.
	for i := 0; i < len(files); i++ {
		c.mapTasks = append(c.mapTasks, Task{
			TaskId:     i,
			Status:     Init,
			AssignTime: 0,
		})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTasks = append(c.reduceTasks, Task{
			TaskId:     i,
			Status:     Init,
			AssignTime: 0,
		})
	}

	c.server()
	return &c
}
