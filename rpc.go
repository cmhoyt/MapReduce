package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type EmptyArs struct {
}

type EmptyReply struct {
}

type TaskType struct {
	TaskType	int
}

// Universal Task structure
type MapTask struct {
	Filename  string // Filename = key
	NumReduce int    // Number of reduce tasks, used to figure out number of buckets
	MapNum    int	 // number for the map task
}
type ReduceTask struct { 
	ReduceNumber      int	// number of the reducer
	FilesNum		  int	// number of total files
	NumReduce		  int	// total number of reducers
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}