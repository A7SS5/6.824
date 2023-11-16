package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	//	CallExample()
	// uncomment to send the Example RPC to the coordinator.
	for {
		task := CallGetTask()
		switch task.Yourtask {
		case MapTask:
			MyMap(task.Filename, task.NReduce, task.Filenum, mapf)
			Done(task.Filenum, MapTask)
		case ReduceTask:
			MyReduce(task.Filenum, task.Nfile, task.NReduce, reducef)
			Done(task.Filenum, ReduceTask)
		case DoneTask:
			break
		}
	}
	time.Sleep(3)
}
func MyMap(filename string, nReduce int, filenum int, mapf func(string, string) []KeyValue) {

	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %s", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	filelist, errcreatefile := createTempFiles(nReduce)
	if errcreatefile != nil {
		log.Fatalf("cannot errcreatefile")
	}
	JSONEncoders, errCreateEncoder := createJSONEncoders(filelist)
	if errCreateEncoder != nil {
		log.Fatalf("cannot create  errCreateEncoder")
	}
	for _, k := range kva {
		JSONEncoders[ihash(k.Key)%nReduce].Encode(&k)
	}
	renameTempFilesWithPrefix(filelist, strconv.Itoa(filenum))
}
func createJSONEncoders(files []*os.File) ([]*json.Encoder, error) {
	var encoders []*json.Encoder
	for _, file := range files {
		encoder := json.NewEncoder(file)
		encoders = append(encoders, encoder)
	}

	return encoders, nil
}
func createTempFiles(numFiles int) ([]*os.File, error) {
	var tempFiles []*os.File
	currentDir, _ := os.Getwd()
	for i := 0; i < numFiles; i++ {
		tempFile, err := ioutil.TempFile(currentDir, "example") // 创建临时文件
		if err != nil {
			// 处理错误
			return nil, err
		}
		tempFiles = append(tempFiles, tempFile)
	}
	return tempFiles, nil
}
func closeFiles(files []*os.File) {
	for _, file := range files {
		file.Close()
	}
}
func renameTempFilesWithPrefix(tempFiles []*os.File, prefix string) error {
	for i, tempFile := range tempFiles {
		newName := fmt.Sprintf("%s-%d", prefix, i)
		tempFile.Close()
		err := os.Rename(tempFile.Name(), newName) // 重命名临时文件
		if err != nil {
			fmt.Println("命名失败", err)
			return err
		}
	}

	return nil
}
func MyReduce(filenum int, Nfile int, nReduce int, reducef func(string, []string) string) {

	intermediate := []KeyValue{}
	for i := 0; i < Nfile; i++ {
		name := gettaskfilename(i, filenum)
		file, err := os.OpenFile(name, os.O_CREATE|os.O_RDWR, 666)
		if err != nil {
			fmt.Printf("open %s err %v\n", name, err)
		}
		kv := KeyValue{}
		encoder := json.NewDecoder(file)
		for {
			if err2 := encoder.Decode(&kv); err2 != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + strconv.Itoa(filenum)
	currentDir, _ := os.Getwd()
	ofile, err := ioutil.TempFile(currentDir, "example") // 创建临时文件
	if err != nil {
		fmt.Printf("reduce tempfile create failed\n")
		return
	}
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	ofile.Close()
	err = os.Rename(ofile.Name(), oname)
	if err != nil {
		fmt.Println("命名失败", err)
		return
	}
}
func gettaskfilename(mapnum int, readucenum int) string {
	return fmt.Sprintf("%d-%d", mapnum, readucenum)
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
func CallGetTask() *GetTaskReply {

	// declare an argument structure.
	args := GetTaskArgs{}

	// fill in the argument(s).
	// declare a reply structure.
	reply := GetTaskReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.GetTask", &args, &reply)
	return &reply

}
func Done(filenum int, task TaskNum) {
	args := DoneTaskArgs{}
	args.Filenum = filenum
	args.Mytask = task
	reply := DoneTaskReply{}
	call("Coordinator.Finished", &args, &reply)

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
