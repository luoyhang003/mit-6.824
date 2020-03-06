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
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()

	Call(mapf, reducef)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// RPC callings to Master for getting/submitting tasks.
// 	* call("Master.Dispatch", &request, &respond)
//
// 1. register to Master and waiting for respond
// 2. According to respond:
//		|- Map task:(Respond.Phase == "Map")
//		|	|- need: filename, fileIndex, nReduce
//		|	|- steps:
//		|		|- i:   read file
//		|		|- ii:  call mapf -> key-value -> sort -> encode -> ihash(key)%nReduce -> write to nReduce intermediate files
//		|		|- iii: respond with { Phase: Map,  fileIndex: fileIndex }
//		|		|- iv:  waiting for repond of Master, keep going with step 2
//		|- Reduce task:(Respond.Phase == "Reduce")
//		|	|- need: filename, fileIndex, nReduce
//		|	|- steps:
//		|		|- i:   read intermediate file with same keys[grouped by ihash(key)]
//		|		|- ii:  decode -> group same key -> reducef -> write to mt-out-* file
//		|		|- iii: respond with { Phase: Reduce,  fileIndex: fileIndex }
//		|		|- iv:  waiting for repond of Master, keep going with step 2
//		|- Wating: (Respond.Phase == "Waiting")
//		|	|- need: Nothing
//		|	|- steps:
//		|		|- i:   sleep...
//		|		|- ii:  next call with { Phase: Waiting,  fileIndex: nil }
//		|		|- iii: waiting for repond of Master, keep going with step 2
//		|- Done: (Respond.Phase == "Done")
//			|- Worker exits
// 3. Done
//
// Reminder: Worker needs to be stateless, cause it is possible to crash at any time.
//			 So, repond to Master only if the task has been done(write to file).
//			 Considering, there are possibly mutliple Workers which may crash at any time. (Concurrency & Fault Tolerance)
//
func Call(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	request := Request{}

	request.Phase = "Register"
	request.FileIndex = -1

	response := Response{}

	call("Master.Dispatch", &request, &response)

	for response.Phase != "Done" {
		switch response.Phase {
		case mapPhase:
			filename := response.FileName
			index, _ := strconv.Atoi(response.FileIndex)
			nReduce := response.NReduce

			intermediate := []KeyValue{}
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Open file failed: %v, %v", filename, err)
			}

			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Read file failed: %v, %v", filename, err)
			}
			file.Close()

			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))

			encoders := make([]*json.Encoder, nReduce)
			imfiles := make([]*os.File, nReduce)

			for i := 0; i < nReduce; i++ {
				outName := "mr-" + strconv.Itoa(index) + "-" + strconv.Itoa(i)
				out, err := os.Create(outName)
				if err != nil {
					fmt.Println(outName, err)
					return
				}
				encoders[i] = json.NewEncoder(out)
				imfiles[i] = out
			}

			for i := 0; i < len(intermediate); {
				j := i
				output := KeyValue{intermediate[i].Key, intermediate[i].Value}

				for ; j < len(intermediate) && intermediate[j].Key == intermediate[i].Key; j++ {

					err := encoders[ihash(intermediate[i].Key)%nReduce].Encode(&output)
					if err != nil {
						fmt.Printf("%s Encode Failed %v\n", intermediate[i].Key, err)
					}
				}
				i = j
			}

			request = Request{}
			request.Phase = mapPhase
			request.FileIndex = index

			call("Master.Dispatch", &request, &response)
		case reducePhase:
			index, _ := strconv.Atoi(response.FileIndex)
			nFile := response.NFile

			kva := []KeyValue{}

			for i := 0; i < nFile; i++ {
				filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(index)
				file, err := os.Open(filename)
				if err != nil {
					fmt.Printf("Open file Failed: %v %v\n", filename, err)
					break
				}
				decoder := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				file.Close()
			}

			sort.Sort(ByKey(kva))

			outname := "mr-out-" + strconv.Itoa(index)
			outfile, err := os.Create(outname)
			if err != nil {
				fmt.Printf("Create file Failed: %v %v\n", outname, err)
				break
			}
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

				fmt.Fprintf(outfile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			request = Request{}
			request.Phase = reducePhase
			request.FileIndex = index

			call("Master.Dispatch", &request, &response)
		case waitPhase:
			time.Sleep(time.Duration(3) * time.Second)

			request = Request{}
			request.Phase = waitPhase
			request.FileIndex = -1

			call("Master.Dispatch", &request, &response)

		}
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
