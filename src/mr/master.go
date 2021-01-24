package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const taskTimeOut = 30 * time.Second

type Master struct {
	mu                    sync.Mutex
	nReduce               int
	mapperTaskPendingMap  map[int]*masterTask
	reducerTaskPendingMap map[int]*masterTask
	nextReducerTaskID     int
}

type masterTask struct {
	mapperTask  MapperTask
	reducerTask ReducerTask
	time        time.Time
	isCompleted bool
}

func (m *Master) NotifyMapperJobDone(args *NotifyMapperJobDoneArgs, reply *NotifyMapperJobDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mapperTaskPendingMap[args.TaskID].isCompleted = true
	log.Println("Received mapper job", args.TaskID, "done")
	return nil
}

func (m *Master) NotifyReducerJobDone(args *NotifyReducerJobDoneArgs, reply *NotifyReducerJobDoneReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	log.Println("Received reducer job done. ReducerID =", args.ReducerID, "TaskID = ", args.TaskID)
	if args.TaskID != m.reducerTaskPendingMap[args.ReducerID].reducerTask.TaskID {
		log.Println("Ignoring notify reducer job done due to reducerID =", args.ReducerID, "expecting taskID =", m.reducerTaskPendingMap[args.ReducerID].reducerTask.TaskID, "but args.TaskID =", args.TaskID)
		return nil
	}
	m.reducerTaskPendingMap[args.ReducerID].isCompleted = true
	return nil
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	// clean unassigned first
	mapperTask := m.getMapperTaskIfExist()
	log.Println("Mapper Task", mapperTask, time.Now())
	if mapperTask != (MapperTask{}) {
		reply.NReduce = m.nReduce
		reply.MapperTask = mapperTask
		return nil
	}

	reducerTask := m.getReducerTaskIfExist()
	log.Println("Reducer Task", reducerTask, time.Now())
	if reducerTask != (ReducerTask{}) {
		reply.NReduce = m.nReduce
		reply.ReducerTask = reducerTask
		log.Println("Reducer Task", reply.ReducerTask, reducerTask)
		return nil
	}
	// return says you don't need to work any more.
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	for _, task := range m.reducerTaskPendingMap {
		if !task.isCompleted {
			// log.Println("Found", i, "is not completed.")
			return false
		}
	}
	return true
}

func (m *Master) getMapperTaskIfExist() MapperTask {
	isAllCompleted := false
	for !isAllCompleted {
		isAllCompleted = true
		for _, task := range m.mapperTaskPendingMap {
			if !task.isCompleted {
				isAllCompleted = false
			}
			m.mu.Lock()
			if !task.isCompleted && task.time.Add(taskTimeOut).Before(time.Now()) {
				task.time = time.Now()
				m.mu.Unlock()
				return task.mapperTask
			}
			m.mu.Unlock()
		}
		time.Sleep(time.Second)
	}
	return MapperTask{}
}

func (m *Master) getReducerTaskIfExist() ReducerTask {
	isAllCompleted := false
	for !isAllCompleted {
		isAllCompleted = true
		for _, task := range m.reducerTaskPendingMap {
			if !task.isCompleted {
				isAllCompleted = false
			}
			m.mu.Lock()
			if !task.isCompleted && task.time.Add(taskTimeOut).Before(time.Now()) {
				task.time = time.Now()
				m.nextReducerTaskID++
				task.reducerTask.TaskID = m.nextReducerTaskID
				m.mu.Unlock()
				return task.reducerTask
			}
			m.mu.Unlock()
		}
		time.Sleep(time.Second)
	}
	return ReducerTask{}
}

//
// create a Master.
// main/mrmaster.go calls this function.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nReduce:               nReduce,
		mapperTaskPendingMap:  make(map[int]*masterTask),
		reducerTaskPendingMap: make(map[int]*masterTask),
	}
	for i, sourceFile := range files {
		m.mapperTaskPendingMap[i] = &masterTask{
			mapperTask: MapperTask{
				FileName: sourceFile,
				TaskID:   i,
			},
		}
	}
	for i := 0; i < nReduce; i++ {
		m.reducerTaskPendingMap[i] = &masterTask{
			reducerTask: ReducerTask{
				ReducerID: i,
				NMap:      len(files),
			},
		}
	}
	log.Println("Initial State of Master ", &m)
	m.server()
	return &m
}
