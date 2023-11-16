package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce        int
	files          []string
	isTaskAssign   []bool
	isReduceAssign []bool
	timeList       []time.Time
	ReducetimeList []time.Time
	finished       bool
	lock           sync.Mutex
	finishedlock   sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) Finished(args *DoneTaskArgs, reply *DoneTaskReply) error {
	switch args.Mytask {
	case MapTask:
		c.lock.Lock()
		defer c.lock.Unlock()
		c.isTaskAssign[args.Filenum] = true
	case ReduceTask:
		c.lock.Lock()
		defer c.lock.Unlock()
		c.isReduceAssign[args.Filenum] = true
	}
	return nil
}
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	for {
		alldone := true
		c.lock.Lock()
		for i, isAssgn := range c.isTaskAssign {
			if isAssgn {
			} else if c.timeList[i].IsZero() || time.Since(c.timeList[i]) > 10*time.Second {
				reply.Yourtask = MapTask
				reply.Filename = c.files[i]
				reply.NReduce = c.nReduce
				reply.Filenum = i
				c.timeList[i] = time.Now()
				c.lock.Unlock()
				return nil
			} else {
				alldone = false
			}
		}
		c.lock.Unlock()
		if alldone {
			break
		} else {
			time.Sleep(time.Second)
		}
	}
	for {
		alldone := true
		c.lock.Lock()
		for i, isAssgn := range c.isReduceAssign {
			if isAssgn {
			} else if c.ReducetimeList[i].IsZero() || time.Since(c.ReducetimeList[i]) > 20*time.Second {
				reply.Yourtask = ReduceTask
				reply.NReduce = c.nReduce
				reply.Filenum = i
				reply.Nfile = len(c.files)
				c.ReducetimeList[i] = time.Now()
				c.lock.Unlock()
				return nil
			} else {
				alldone = false
			}
		}
		c.lock.Unlock()
		if alldone {
			break
		} else {
			time.Sleep(time.Second)
		}
	}
	reply.Yourtask = DoneTask
	c.finishedlock.Lock()
	c.finished = true
	c.finishedlock.Unlock()
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
	c.finishedlock.Lock()
	defer c.finishedlock.Unlock()
	if c.finished {
		for i := 0; i < c.nReduce; i++ {
			for j := 0; j < len(c.timeList); j++ {
				filename := gettaskfilename(j, i)
				err := os.Remove(filename)
				if err != nil {
					fmt.Printf("无法删除 %s", filename)
				}
			}
		}
	}
	return c.finished
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce}
	c.isTaskAssign = make([]bool, len(files))
	c.timeList = make([]time.Time, len(files))
	c.isReduceAssign = make([]bool, nReduce)
	c.ReducetimeList = make([]time.Time, nReduce)
	// Your code here.
	c.server()
	return &c
}
