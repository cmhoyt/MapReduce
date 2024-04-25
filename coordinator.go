package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	NumReduce      int             // Number of reduce tasks
	Files          []string        // Files for map tasks, len(Files) is number of Map tasks
	MapTasks       chan MapTask    // Channel for uncompleted map tasks
	CompletedTasks map[string]bool // Map to check if task is completed
	Lock           sync.Mutex      // Lock for contolling shared variables
	ReduceTasks	   chan ReduceTask
	Finished 	   int				// 0 = no, 1 = yes
	Status		   int 				// 0 = map, 1 = reduce
}


// Starting coordinator logic
func (c *Coordinator) Start() {
	//fmt.Println("Starting Coordinator, adding Map Tasks to channel")

	// Prepare initial MapTasks with their map number and add them to the queue
	number := 0
	for _, file := range c.Files {
		mapTask := MapTask{
			Filename:  file,
			NumReduce: c.NumReduce,
			MapNum: number,
		}

		//fmt.Println("MapTask", mapTask, "added to channel")

		c.MapTasks <- mapTask
		c.CompletedTasks["map_"+mapTask.Filename] = false
		number = number + 1
	}

	c.server()
}

//rpc call so worker knows whether it should break, request map task, request reduce task 
func (c *Coordinator) TaskType(args *EmptyArs, reply *TaskType) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	if len(c.ReduceTasks) == 0 && c.Status == 1 && len(c.CompletedTasks) > len(c.Files){
		dtask := TaskType{
			TaskType: -1,
		}
		*reply = dtask
	} else if c.Status == 0{
		mtask := TaskType{
			TaskType: 0,
		}
		*reply = mtask
	} else if c.Status == 1{
		rtask := TaskType{
			TaskType: 1,
		}
		*reply = rtask
	}
	return nil
}

// RPC that worker calls when idle (worker requests a map task)
func (c *Coordinator) RequestMapTask(args *EmptyArs, reply *MapTask) error {
	//fmt.Println("Map task requested")

	task, _ := <-c.MapTasks // check if there are uncompleted map tasks. Keep in mind, if MapTasks is empty, this will halt

	//fmt.Println("Map task found,", task.Filename)
	*reply = task

	go c.WaitForWorkerM(task)

	return nil
}
// rpc that worker calls when requests a reduce task
func (c *Coordinator) RequestReduceTask(args *EmptyArs, reply *ReduceTask) error {
	//fmt.Println("Reduce task requested")

	task, _ := <-c.ReduceTasks
	
	//fmt.Println("Reduce task found,", task.ReduceNumber)
	*reply = task
	go c.WaitForWorkerR(task)
	
	return nil
}

// Goroutine will wait 10 seconds and check if map task is completed or not
func (c *Coordinator) WaitForWorkerM(task MapTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	if c.CompletedTasks["map_"+task.Filename] == false {
		//fmt.Println("Timer expired, task", task.Filename, "is not finished. Putting back in queue.")
		c.MapTasks <- task
	}
	c.Lock.Unlock()
}
// goroutine waits 10 seconds to check if reduce task is completed
func (c *Coordinator) WaitForWorkerR(task ReduceTask) {
	time.Sleep(time.Second * 10)
	c.Lock.Lock()
	name := fmt.Sprintf("%s%d", "reduce_", task.ReduceNumber)
	if c.CompletedTasks[name] == false {
		//fmt.Println("Timer expired, task", task, "is not finished. Putting back in queue.")
		c.ReduceTasks <- task
	}
	c.Lock.Unlock()
}

// RPC for reporting a completion of a task
func (c *Coordinator) TaskCompletedM(args *MapTask, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()
	c.CompletedTasks["map_"+args.Filename] = true

	//fmt.Println("Task", args, "completed")

	// If all of map tasks are completed, go to reduce phase
	// ...
	b := true
	for _, boo := range c.CompletedTasks{
		if boo == false{
			b = false
		}
	}
	//all map tasks complete so initialize reduce phase by adding tasks to the channel
	if b == true{
		c.Status = 1
		for i := 0; i < c.NumReduce; i++ {
			c.ReduceTasks <- ReduceTask{
				ReduceNumber: i,
				FilesNum: len(c.Files),
				NumReduce: c.NumReduce,
			}
			name := fmt.Sprintf("%s%d", "reduce_", i)
			c.CompletedTasks[name] = false
		} 
	}
	time.Sleep(time.Second * 2)
	return nil
}
// rpc for reporting completed reduce task
func (c *Coordinator) TaskCompletedR(args *ReduceTask, reply *EmptyReply) error {
	c.Lock.Lock()
	defer c.Lock.Unlock()

	name := fmt.Sprintf("%s%d", "reduce_", args.ReduceNumber)
	c.CompletedTasks[name] = true 

	//fmt.Println("Task", args.ReduceNumber, "completed")

	//check if all reduce tasks are done
	b := true
	for _, boo := range c.CompletedTasks{
		if boo == false{
			b = false
		}
	}
	if b == true{
		c.Finished = 1
	}
	time.Sleep(time.Second * 2)
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
	c.Lock.Lock()
	defer c.Lock.Unlock()
	ret := false
	// Your code here.
	
	if c.Finished == 1{
		ret = true
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		NumReduce:      nReduce,
		Files:          files,
		MapTasks:       make(chan MapTask, 100),
		CompletedTasks: make(map[string]bool),
		ReduceTasks: 	make(chan ReduceTask, 100),
		Finished: 		0,
		Status: 		0,
	}

	//fmt.Println("Starting coordinator")

	c.Start()

	return &c
}


