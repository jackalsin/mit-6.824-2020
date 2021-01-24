package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MapperTask struct {
	TaskID   int
	FileName string
}

type ReducerTask struct {
	ReducerID int // Reduce Job ID
	TaskID    int // Task ID
	NMap      int
}

type GetTaskReply struct {
	MapperTask  MapperTask
	ReducerTask ReducerTask
	NReduce     int
}

type GetTaskArgs struct {
}

type NotifyMapperJobDoneArgs struct {
	TaskID int
}

type NotifyMapperJobDoneReply struct {
}

type NotifyReducerJobDoneArgs struct {
	ReducerID int
	TaskID    int
}

type NotifyReducerJobDoneReply struct {
}
