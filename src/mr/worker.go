package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

const (
	masterGetTask              = "Master.GetTask"
	masterNotifyMapperJobDone  = "Master.NotifyMapperJobDone"
	masterNotifyReducerJobDone = "Master.NotifyReducerJobDone"
	outFileFormat              = "mr-out-%d"
	outTmpFileFormat           = "mr-tmp-%d"
	intermediateFileFormat     = "mr-%d-%d"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type fileDecoder struct {
	File     *os.File
	Decoder  *json.Decoder
	KeyValue *KeyValue
	err      error
}

type fileEncoder struct {
	File    *os.File
	Encoder *json.Encoder
}

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

	// call to get mapping task
	// 1. call to get task
	// 1.1 if the file starts with mr-X-Y, this is a reduce task
	// 1.2 if the file name is empty, we should not wait
	// 1.3 if the file name is something else, we should not wait

	for {
		task := getTask()
		log.Println("task = ", task)
		if task.MapperTask != (MapperTask{}) {
			doMap(mapf, task.MapperTask.FileName, task.MapperTask.TaskID, task.NReduce)
			continue
		}

		if task.ReducerTask != (ReducerTask{}) { // indicate it's reduce task
			log.Println(task.ReducerTask)
			doReduce(reducef, task.NReduce, task.ReducerTask)
			continue
		}
		break
	}
}

func doMap(mapf func(string, string) []KeyValue, filename string, mapperID, nReducer int) {
	log.Println("Running doMap")
	result := make(map[int][]KeyValue)
	writers := getMapperWriter(mapperID, nReducer)
	keyValuePairs := mapf(filename, getFileContent(filename))
	for _, wordToCount := range keyValuePairs {
		key := ihash(wordToCount.Key) % nReducer
		result[key] = append(result[key], wordToCount)
		enc := writers[key].Encoder
		if err := enc.Encode(wordToCount); err != nil {
			log.Fatal("Failed writing ", wordToCount)
		}
	}
	log.Println("Running close")
	closeMapperWriter(writers)
	call(masterNotifyMapperJobDone, &NotifyMapperJobDoneArgs{TaskID: mapperID}, &NotifyMapperJobDoneReply{})
}

func doReduce(reducef func(string, []string) string, nReduce int, reducerTask ReducerTask) {
	result := make(map[string][]string)
	for mapperID := 0; mapperID < reducerTask.NMap; mapperID++ {
		intermediateFileName := fmt.Sprintf(intermediateFileFormat, mapperID, reducerTask.ReducerID)
		intermediateFile, err := os.Open(intermediateFileName)
		if err != nil {
			log.Fatal("Cannot open file:", intermediateFile)
		}
		dec := json.NewDecoder(intermediateFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					// log.Println("Finish reading file", intermediateFileName)
					break
				}
				log.Fatal("Reading key value pair error", err)
			}
			if result[kv.Key] == nil {
				result[kv.Key] = make([]string, 0)
			}
			result[kv.Key] = append(result[kv.Key], kv.Value)
		}
		if err := intermediateFile.Close(); err != nil {
			log.Fatal("Error in closing intermediate file ", intermediateFileName)
		}
	}

	tmpFileNamePattern := fmt.Sprintf(outTmpFileFormat, reducerTask.ReducerID)
	tmpFile, err := ioutil.TempFile(".", tmpFileNamePattern)
	if err != nil {
		log.Fatal("Error when creating ", tmpFileNamePattern, err)
	}
	for key, value := range result {
		count := reducef(key, value)
		fmt.Fprintf(tmpFile, "%v %v\n", key, count)
	}
	if err := tmpFile.Close(); err != nil {
		log.Fatal("Error when closing tmp file.", tmpFile.Name())
	}
	outFileName := fmt.Sprintf(outFileFormat, reducerTask.ReducerID)
	if err := os.Rename(tmpFile.Name(), outFileName); err != nil {
		log.Fatal("err in renaming files", err)
	}
	if isSuccessful := call(masterNotifyReducerJobDone, &NotifyReducerJobDoneArgs{ReducerID: reducerTask.ReducerID, TaskID: reducerTask.TaskID}, &NotifyMapperJobDoneReply{}); !isSuccessful {
		log.Fatalln("Error in notify reduce job done. Id =", reducerTask.ReducerID)
	}
}

func getMapperWriter(mapperID, nReducer int) map[int]fileEncoder {
	writers := make(map[int]fileEncoder)
	for i := 0; i < nReducer; i++ {
		fileName := fmt.Sprintf(intermediateFileFormat, mapperID, i)
		if file, err := os.Create(fileName); os.IsExist(err) {
			log.Println("Ignoring err ", fileName)
		} else {
			writers[i] = fileEncoder{
				Encoder: json.NewEncoder(file),
				File:    file,
			}
		}
	}
	return writers
}

func closeMapperWriter(writers map[int]fileEncoder) {
	for _, w := range writers {
		if err := w.File.Close(); err != nil {
			log.Fatal("Error in closing ", err)
		}
	}
}

func getFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	if err := file.Close(); err != nil {
		log.Fatalln("Error in closing file", file)
	}
	return string(content)
}

func getTask() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	call(masterGetTask, &args, &reply)
	log.Println("Got tasks in getTask ", reply)
	return reply
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Println("error in call", rpcname, err, args, reply)
	return false
}
