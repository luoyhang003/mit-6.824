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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

//
// Request struct (by Worker)
//	- Phase: ["Register", "Map", "Reduce",  "Waiting"]
//	- fileIndex: mapped file index or reduced file index

// Respond struct (by Master)
//	- Phase: ["Map", "Reduce", "Waiting", "Done"]
//	- fileIndex: index of prepared to map/reduce job
//	- filename
//	- nReduce: reduce number
//

type phaseType string

const (
	regPhase    phaseType = "Register"
	mapPhase    phaseType = "Map"
	reducePhase phaseType = "Reduce"
	waitPhase   phaseType = "Waiting"
	donePhase   phaseType = "Done"
)

type Request struct {
	Phase     phaseType
	FileIndex int
}

type Response struct {
	Phase     phaseType
	FileName  string
	FileIndex string
	NReduce   int
	NFile     int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
