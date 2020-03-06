package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type Master struct {
	// Your definitions here.
	sync.Mutex

	NFile         int
	NReduce       int
	Files         []string
	MappingFiles  []bool
	MappedFiles   []bool
	ReducingFiles []bool
	ReducedFiles  []bool
	Timeout       bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// Dispatch tasks for Workers.
//	* deal with requests from Worker(s)
//
// 1. Start server, waiting for requests from Worker(s).
// 2. Respond the requests:
//		|- Register: (Request.Phase == "Register")
//		|	|- Steps:
//		|		|- i: check phase of whole MR jobs
//		|		|- ii: respond accroding current phase,list cases:
//		|			|- case 0: still have non-mapping jobs
//		|			|			|-> mark a non-mapping file as mapping
//		|			|			|-> respond with { Phase: Map, fileIndex: non-maaping job's index, filename, nReduce: nReduce}
//		|			|- case 1: no non-mapping jobs & not all mapping jobs done => set a timeout timer
//		|			|			|-> if not time-out, respond with { Phase: Waiting, fileIndex: nil, nReduce: nil}
//		|			|			|-> if time-out, reset timer, respond with { Phase: Map, fileIndex: un-done mapping job's index, filename, nReduce: nReduce}
//		|			|- case 2: all mapping jobs done & still have non-reducing jobs
//		|			|			|-> respond with { Phase: Reduce, fileIndex: reducing index, nReduce: nReduce}
//		|			|- case 3: no non-reducing jobs & not all reducing jobs done => set a timeout timer
//		|			|			|-> if not time-out, respond with { Phase: Waiting, fileIndex: nil, nReduce: nil}
//		|			|			|-> if time-out, reset timer, respond with { Phase: Reduce, fileIndex: un-done reducing job's index, nReduce: nReduce}
//		|			|- case 4: all reduce jobs done
//		|						|-> respond with { Phase: Done, fileIndex: nil, nReduce: nil}
//		|- Worker has done map:  (Request.Phase == "Map")
//		|	|=> make job as mapped, then do cases check mentioned above(case 0, 1, 2)
//		|- Worker has done reduce: (Request.Phase == "Reduce")
//		|	|=> make reduce job as reduced, then do cases check mentioned above(case 2, 3, 4)
//		|- Worker waiting: (Request.Phase == "Waiting")
//			|=> do as Register phase do.
// 3. Complete Done() check
//		* check case 4
//
// Reminder: All working request threads share the same data of maintained by Master, use Mutex with proper granularity lock.

func (m *Master) Dispatch(request *Request, response *Response) error {
	switch request.Phase {
	case "Register", "Waiting":
		m.Lock()

		isAllMapping := true
		isAllMapped := true
		isAllReducing := true
		isAllReduced := true

		mappingIndex := -1
		mappedIndex := -1
		reducingIndex := -1
		reducedIndex := -1

		for i, v := range m.MappingFiles {
			if !v {
				isAllMapping = false
				mappingIndex = i
				break
			}
		}

		for i, v := range m.MappedFiles {
			if !v {
				isAllMapped = false
				mappedIndex = i
				break
			}
		}

		for i, v := range m.ReducingFiles {
			if !v {
				isAllReducing = false
				reducingIndex = i
				break
			}
		}

		for i, v := range m.ReducedFiles {
			if !v {
				isAllReduced = false
				reducedIndex = i
				break
			}
		}

		if !isAllMapping {
			// case 0
			m.MappingFiles[mappingIndex] = true

			response.Phase = mapPhase
			response.FileIndex = strconv.Itoa(mappingIndex)
			response.FileName = m.Files[mappingIndex]
			response.NReduce = m.NReduce
			response.NFile = m.NFile
		} else if isAllMapping && !isAllMapped {
			// case 1
			if !m.Timeout {
				m.Timeout = true

				response.Phase = waitPhase
				response.FileIndex = strconv.Itoa(-1)
				response.FileName = ""
				response.NReduce = -1
			} else {
				m.Timeout = false

				response.Phase = mapPhase
				response.FileIndex = strconv.Itoa(mappedIndex)
				response.FileName = m.Files[mappedIndex]
				response.NReduce = m.NReduce
				response.NFile = m.NFile
			}
		} else if isAllMapped && !isAllReducing {
			// case 2
			response.Phase = reducePhase
			response.FileIndex = strconv.Itoa(reducingIndex)
			response.FileName = ""
			response.NReduce = m.NReduce
			response.NFile = m.NFile
		} else if isAllReducing && !isAllReduced {
			// case 3
			if !m.Timeout {
				m.Timeout = true

				response.Phase = waitPhase
				response.FileIndex = strconv.Itoa(-1)
				response.FileName = ""
				response.NReduce = -1
				response.NFile = -1
			} else {
				m.Timeout = false

				response.Phase = reducePhase
				response.FileIndex = strconv.Itoa(reducedIndex)
				response.FileName = ""
				response.NReduce = m.NReduce
				response.NFile = m.NFile
			}
		} else if isAllReduced {
			// case 4
			response.Phase = "Done"
			response.FileIndex = strconv.Itoa(-1)
			response.FileName = ""
			response.NReduce = -1
			response.NFile = -1
		}

		m.Unlock()
	case mapPhase:
		m.Lock()

		m.MappedFiles[request.FileIndex] = true

		isAllMapping := true
		isAllMapped := true
		isAllReducing := true

		mappingIndex := -1
		mappedIndex := -1
		reducingIndex := -1

		for i, v := range m.MappingFiles {
			if !v {
				isAllMapping = false
				mappingIndex = i
				break
			}
		}

		for i, v := range m.MappedFiles {
			if !v {
				isAllMapped = false
				mappedIndex = i
				break
			}
		}

		for i, v := range m.ReducingFiles {
			if !v {
				isAllReducing = false
				reducingIndex = i
				break
			}
		}

		if !isAllMapping {
			// case 0
			m.MappingFiles[mappingIndex] = true

			response.Phase = mapPhase
			response.FileIndex = strconv.Itoa(mappingIndex)
			response.FileName = m.Files[mappingIndex]
			response.NReduce = m.NReduce
			response.NFile = m.NFile
		} else if isAllMapping && !isAllMapped {
			// case 1
			if !m.Timeout {
				m.Timeout = true

				response.Phase = waitPhase
				response.FileIndex = strconv.Itoa(-1)
				response.FileName = ""
				response.NReduce = -1
				response.NFile = -1
			} else {
				m.Timeout = false

				response.Phase = mapPhase
				response.FileIndex = strconv.Itoa(mappedIndex)
				response.FileName = m.Files[mappedIndex]
				response.NReduce = m.NReduce
				response.NFile = m.NFile
			}
		} else if isAllMapped && !isAllReducing {
			// case 2
			m.ReducingFiles[reducingIndex] = true

			response.Phase = reducePhase
			response.FileIndex = strconv.Itoa(reducingIndex)
			response.FileName = ""
			response.NReduce = m.NReduce
			response.NFile = m.NFile
		}

		m.Unlock()
	case reducePhase:
		m.Lock()

		m.ReducedFiles[request.FileIndex] = true

		isAllMapped := true
		isAllReducing := true
		isAllReduced := true

		reducingIndex := -1
		reducedIndex := -1

		for _, v := range m.MappedFiles {
			if !v {
				isAllMapped = false
				break
			}
		}

		for i, v := range m.ReducingFiles {
			if !v {
				isAllReducing = false
				reducingIndex = i
				break
			}
		}

		for i, v := range m.ReducedFiles {
			if !v {
				isAllReduced = false
				reducedIndex = i
				break
			}
		}

		if isAllMapped && !isAllReducing {
			// case 2
			m.ReducingFiles[reducingIndex] = true

			response.Phase = reducePhase
			response.FileIndex = strconv.Itoa(reducingIndex)
			response.FileName = ""
			response.NReduce = m.NReduce
			response.NFile = m.NFile
		} else if isAllReducing && !isAllReduced {
			// case 3
			if !m.Timeout {
				m.Timeout = true

				response.Phase = waitPhase
				response.FileName = ""
				response.FileIndex = strconv.Itoa(-1)
				response.NReduce = -1
				response.NFile = -1
			} else {
				m.Timeout = false

				response.Phase = reducePhase
				response.FileName = ""
				response.FileIndex = strconv.Itoa(reducedIndex)
				response.NReduce = m.NReduce
				response.NFile = m.NFile
			}
		} else if isAllReduced {
			// case 4
			response.Phase = "Done"
			response.FileName = ""
			response.FileIndex = strconv.Itoa(-1)
			response.NReduce = -1
		}

		m.Unlock()
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := true

	// Your code here.
	for _, v := range m.ReducedFiles {
		if !v {
			ret = false
			break
		}
	}

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	n := len(files)
	m.NFile = n
	m.NReduce = nReduce
	m.Files = files
	m.MappingFiles = make([]bool, n)
	m.MappedFiles = make([]bool, n)
	m.ReducingFiles = make([]bool, nReduce)
	m.ReducedFiles = make([]bool, nReduce)
	m.Timeout = false

	for i := 0; i < n; i++ {
		m.MappingFiles[i] = false
		m.MappedFiles[i] = false
	}

	for i := 0; i < nReduce; i++ {
		m.ReducingFiles[i] = false
		m.ReducedFiles[i] = false
	}

	m.server()
	return &m
}
