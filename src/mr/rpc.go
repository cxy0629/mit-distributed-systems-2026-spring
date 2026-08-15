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

// worker可执行map或reduce任务
// worker向coordinator请求任务
// coordinator根据当前任务状态分配map或reduce任务给worker
// 可能job未完成但当前没有可分配的任务，worker等待，一段时间后再次请求
// 可能job已完成，worker退出

type TaskType int

const (
	TaskMap TaskType = iota
	TaskReduce
	TaskWait
	TaskExit
)

// worker         coordinator
// AskTaskArgs -> AskTaskReply
// ReportTaskArgs -> ReportTaskReply

type AskTaskArgs struct{}

type AskTaskReply struct {
	Type     TaskType // 任务类型
	TaskID   int      // map或reduce任务编号
	FileName string   // map任务输入文件名
	NMap     int      // map任务总数
	NReduce  int      // reduce任务总数
}

type ReportTaskArgs struct {
	Type   TaskType // 任务类型
	TaskID int      // map或reduce任务编号
}

type ReportTaskReply struct{}

// Add your RPC definitions here.
