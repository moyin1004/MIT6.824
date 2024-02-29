package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
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

func WriteFile(filename string, content []byte) error {
	file, err := os.CreateTemp("", filename+".tmp")
	if err != nil {
		log.Printf("CreateTemp failed")
		return err
	}
	_, err = file.Write(content)
	if err != nil {
		log.Printf("Write failed")
		return err
	}
	err = os.Rename(file.Name(), filename)
	if err != nil {
		log.Printf("Rename failed")
		return err
	}
	file.Close()
	return nil
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		req := &ReqestTaskArgs{}
		rsp := &ReqestTaskReply{}
		ret := call("Coordinator.ReqestTask", req, rsp)
		if !ret {
			log.Printf("ReqestTask failed")
			break
		}
		switch rsp.Type {
		case NoTask:
			return
		case WaitTask:
			time.Sleep(100 * time.Millisecond)
		case MapTask:
			intermediate := []KeyValue{}
			content, err := os.ReadFile(rsp.FileName)
			if err != nil {
				log.Fatalf("cannot read %v", rsp.FileName)
				continue
			}
			kva := mapf(rsp.FileName, string(content))
			intermediate = append(intermediate, kva...)
			buckets := make([][]KeyValue, rsp.NReduce)
			// 分配reduce任务
			for _, data := range intermediate {
				pos := ihash(data.Key) % rsp.NReduce
				buckets[pos] = append(buckets[pos], data)
			}
			for i := range buckets {
				oname := "mrtmp" + strconv.Itoa(rsp.TaskId) + strconv.Itoa(i)
				jsonData, err := json.Marshal(buckets[i])
				if err != nil {
					log.Printf("Marshal failed, %v", err)
					break
				}
				err = WriteFile(oname, jsonData)
				if err != nil {
					log.Printf("Write failed, %v", err)
					break
				}
			}
		case ReduceTask:
			intermediate := []KeyValue{}
			for i := 0; i < rsp.NMap; i++ {
				filname := "mrtmp" + strconv.Itoa(i) + strconv.Itoa(rsp.TaskId)
				content, err := os.ReadFile(filname)
				if err != nil {
					log.Fatalf("cannot read %v", filname)
					continue
				}
				tmp := []KeyValue{}
				err = json.Unmarshal(content, &tmp)
				if err != nil {
					log.Fatalf("Unmarshal %v failed, %v", filname, err)
					continue
				}
				intermediate = append(intermediate, tmp...)
			}
			sort.Sort(ByKey(intermediate))
			oname := "mr-out-" + strconv.Itoa(rsp.TaskId)
			buf := bytes.NewBuffer(make([]byte, 0))
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
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(buf, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			err := WriteFile(oname, buf.Bytes())
			if err != nil {
				log.Printf("Write failed, %v", err)
			}
			for i := 0; i < rsp.NMap; i++ {
				filname := "mrtmp" + strconv.Itoa(i) + strconv.Itoa(rsp.TaskId)
				os.Remove(filname)
			}
		}
		submitreq := &SubmitResultArgs{
			TaskId: rsp.TaskId,
			Type:   rsp.Type,
		}
		submitrsp := &SubmitResultReply{}
		retry := 3
		for i := 0; i < retry; i++ {
			ret = call("Coordinator.SubmitResult", submitreq, submitrsp)
			if !ret {
				log.Printf("SubmitResult failed, retry %v", i)
				num := math.Pow(2, float64(i))
				time.Sleep(time.Duration(num) * 100 * time.Millisecond)
			}
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
