package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"encoding/json"
	"sort"
)
type ByKey []KeyValue
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
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

type WorkerSt struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	w := WorkerSt{
		mapf:    mapf,
		reducef: reducef,
	}

	//RPC call to know which phase
	reply := TaskType{}
	args := EmptyArs{}
	call("Coordinator.TaskType", &args, &reply)
	if reply.TaskType == 0{
		fmt.Println("map worker")
		w.RequestMapTask()
	} else if reply.TaskType == 1{
		fmt.Println("reduce worker")
		w.RequestReduceTask()
	}
}

// Requests map task, tries to do it, and repeats
func (w *WorkerSt) RequestMapTask() {
	for {
		//check to make sure still in map phase, if not switch the worker to reduce phase
		r := TaskType{}
		a := EmptyArs{}
		call("Coordinator.TaskType", &a, &r)
		if r.TaskType == 1{
			w.RequestReduceTask()
			break
		}

		args := EmptyArs{}
		reply := MapTask{}
		call("Coordinator.RequestMapTask", &args, &reply)

		file, err := os.Open(reply.Filename)
		if err != nil {
			//retry task if something goes wrong, don't exit worker
			w.RequestMapTask()
			//log.Fatalf("cannot open %v", reply.Filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()

		kva := w.mapf(reply.Filename, string(content))

		// store kva in multiple files according to rules described in the README
		// ...
		intermediate := make([][]KeyValue, reply.NumReduce)
		for _, value := range kva {
			key := ihash(value.Key) % reply.NumReduce
			intermediate[key] = append(intermediate[key], value)
		}

		x := reply.MapNum
		// naming as files on local 
		for y := 0; y < reply.NumReduce; y++ {
			oname := fmt.Sprintf("%s%d%s%d", "mr-", x, "-", y)
			ofile, _ := os.Create(oname)
			enc := json.NewEncoder(ofile)
  			for _, kv := range intermediate[y] {
    			_ = enc.Encode(&kv)
			}
		}

		//fmt.Println("Map task for", reply.Filename, "completed")
		//fmt.Println(kva)

		emptyReply := EmptyReply{}
		call("Coordinator.TaskCompletedM", &reply, &emptyReply)
		
		
	}
}


//requests reduce task
func (w *WorkerSt) RequestReduceTask() {
	for {
		// make sure still in reduce phase, if done then exit worker
		fmt.Println("doing reduce")
		r := TaskType{}
		a := EmptyArs{}
		call("Coordinator.TaskType", &a, &r)
		if r.TaskType == -1{
			break
		}
		
		args := EmptyArs{}
		reply := ReduceTask{}
		call("Coordinator.RequestReduceTask", &args, &reply)
		
		intermediate := []KeyValue{}
		IntermediateFiles := []string{}
		for i := 0; i < reply.FilesNum; i++ {
			name := fmt.Sprintf("%s%d%s%d", "mr-", i, "-", reply.ReduceNumber)
			IntermediateFiles = append(IntermediateFiles, name)
		}

		for _, filename := range IntermediateFiles {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			
			dec := json.NewDecoder(file)
  			for {
    			var kv KeyValue
    			if err := dec.Decode(&kv); err != nil {
      				break
    			}
    			intermediate = append(intermediate, kv)
  			} 
			file.Close()
		}

		sort.Sort(ByKey(intermediate))

		oname := fmt.Sprintf("%s%d", "mr-out-", reply.ReduceNumber)
		ofile, _ := os.Create(oname)

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output := w.reducef(intermediate[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

			i = j
		}

		ofile.Close()


		//fmt.Println("Reduce task", reply.ReduceNumber, "completed")
		//fmt.Println(intermediate)

		emptyReply := EmptyReply{}
		call("Coordinator.TaskCompletedR", &reply, &emptyReply)

		
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
