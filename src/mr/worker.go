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
	log.Println("Running for loop")
	for i, wordToCount := range keyValuePairs {
		key := ihash(wordToCount.Key) % nReducer
		if i == 0 {
			log.Println(wordToCount)
		}
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
	intermediateFileReaders := getIntermediateFileReader(reducerTask.NMap, reducerTask.ReducerID)
	tmpFileName := fmt.Sprintf(outTmpFileFormat, reducerTask.ReducerID)
	tmpFile, err := ioutil.TempFile(".", tmpFileName)
	if err != nil {
		log.Fatal("Error when creating ", tmpFileName, err)
	}
	for len(intermediateFileReaders) > 0 {
		sort.Slice(intermediateFileReaders, func(i, j int) bool {
			return intermediateFileReaders[i].KeyValue.Key < intermediateFileReaders[j].KeyValue.Key
		})
		fmt.Println("intermediateFileReaders =", intermediateFileReaders[0].KeyValue.Key)
		toRemove := intermediateFileReaders[0]
		prev := toRemove.KeyValue.Key
		list := make([]string, 0)
		for len(intermediateFileReaders) > 0 {
			sort.Slice(intermediateFileReaders, func(i, j int) bool {
				return intermediateFileReaders[i].KeyValue.Key < intermediateFileReaders[j].KeyValue.Key
			})
			toRemove := intermediateFileReaders[0]
			if toRemove.KeyValue.Key != prev {
				break
			}
			log.Println("Key", toRemove.KeyValue.Key)
			intermediateFileReaders = intermediateFileReaders[1:]
			list = append(list, toRemove.KeyValue.Value)
			dec := toRemove.Decoder
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			toRemove.KeyValue = &kv
			intermediateFileReaders = append(intermediateFileReaders, toRemove)
		} // end inner loop
		output := reducef(prev, list)
		fmt.Fprintf(tmpFile, "%v %v\n", prev, output)
	}
	closeReducerReader(intermediateFileReaders)
	if err := tmpFile.Close(); err != nil {
		log.Fatal("Error when closing tmp file.", tmpFileName)
	}
	outFileName := fmt.Sprintf(outFileFormat, reducerTask.ReducerID)
	os.Rename(tmpFileName, outFileName)
	call(masterNotifyReducerJobDone, &NotifyReducerJobDoneArgs{TaskID: reducerTask.ReducerID}, &NotifyMapperJobDoneReply{})
}

func getIntermediateFileReader(nMap int, reducerID int) []fileDecoder {
	result := make([]fileDecoder, 0)
	for mapperID := 0; mapperID < nMap; mapperID++ {
		fileName := fmt.Sprintf(intermediateFileFormat, mapperID, reducerID)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatal("Cannot open file:", file)
		}
		dec := json.NewDecoder(file)
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			log.Fatal("Reading key value pair error", err)
			continue
		}
		result = append(result, fileDecoder{
			File:     file,
			Decoder:  dec,
			KeyValue: &kv,
		})
	}
	return result
}

func closeReducerReader(readers []fileDecoder) {
	for _, reader := range readers {
		if err := reader.File.Close(); err != nil {
			log.Fatal("Closing reader failure in ", reader, err)
		}
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
	file.Close()
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

	fmt.Println(err)
	return false
}

func logFatalIfError(e error) {
	if e != nil {
		log.Fatal(e)
	}
}
