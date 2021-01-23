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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type MapperTask struct {
	TaskID   int
	FileName string
}

type ReducerTask struct {
	ReducerID int
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
	TaskID int
}

type NotifyReducerJobDoneReply struct {
}
