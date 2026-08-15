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

// 任务状态，包含三种
// 未分配，在执行，完成

type TaskState int

const (
	StateIdle TaskState = iota
	StateInprogress
	StateDone
)

// Job当前阶段
// 完整工作流程：map -> reduce -> done
// reduce阶段必须在map任务全部完成后才能开始

type JobPhase int

const (
	PhaseMap JobPhase = iota
	PhaseReduce
	PhaseDone
)

// coordinator管理的全局信息

type Coordinator struct {
	mu              sync.Mutex  // 互斥访问coordinator全局信息
	files           []string    // map阶段的所有输入文件
	nMap            int         // map任务个数
	nReduce         int         // reduce任务个数
	phase           JobPhase    // Job当前阶段
	mapTasks        []TaskState // map任务状态，长度为nMap
	reduceTasks     []TaskState // reduce任务状态，长度为nReduce
	mapStartTime    []time.Time // map任务开始时间，长度为nMap
	reduceStartTime []time.Time // reduce任务开始时间，长度为nReduce
}

// AskTask rpc handler
func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	reply.NMap = c.nMap
	reply.NReduce = c.nReduce

	switch c.phase {
	// map阶段
	case PhaseMap:
		// 查找未分配的map任务或超时任务，超时认为worker崩溃
		for idx, state := range c.mapTasks {
			// 若存在，则分配
			if state == StateIdle || (state == StateInprogress && time.Since(c.mapStartTime[idx]) > 10*time.Second) {
				c.mapTasks[idx] = StateInprogress
				c.mapStartTime[idx] = time.Now()
				reply.Type = TaskMap
				reply.TaskID = idx
				reply.FileName = c.files[idx]
				return nil
			}
		}
		// 若不存在，说明当前阶段任务已分配但未全部完成
		reply.Type = TaskWait
		return nil

	// reduce阶段
	case PhaseReduce:
		for idx, state := range c.reduceTasks {
			if state == StateIdle || (state == StateInprogress && time.Since(c.reduceStartTime[idx]) > 10*time.Second) {
				c.reduceTasks[idx] = StateInprogress
				c.reduceStartTime[idx] = time.Now()
				reply.Type = TaskReduce
				reply.TaskID = idx
				return nil
			}
		}
		reply.Type = TaskWait
		return nil

	// Done阶段
	case PhaseDone:
		reply.Type = TaskExit
		return nil
	}

	return nil
}

// 检查所有任务是否已完成
func allTaskDone(tasks []TaskState) bool {
	for _, state := range tasks {
		if state != StateDone {
			return false
		}
	}
	return true
}

// ReportTask rpc handler
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	// map任务
	case TaskMap:
		// 报告过期则忽略
		if c.phase != PhaseMap {
			return nil
		}
		c.mapTasks[args.TaskID] = StateDone
		// coordinator阶段推进
		if allTaskDone(c.mapTasks) {
			c.phase = PhaseReduce
		}

	// reduce任务
	case TaskReduce:
		if c.phase != PhaseReduce {
			return nil
		}
		c.reduceTasks[args.TaskID] = StateDone
		if allTaskDone(c.reduceTasks) {
			c.phase = PhaseDone
		}

	}
	return nil
}

//

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.phase == PhaseDone
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	// 初始化coordinator信息
	c := Coordinator{
		files:           files,
		nMap:            len(files),
		nReduce:         nReduce,
		phase:           PhaseMap,
		mapTasks:        make([]TaskState, len(files)),
		reduceTasks:     make([]TaskState, nReduce),
		mapStartTime:    make([]time.Time, len(files)),
		reduceStartTime: make([]time.Time, nReduce),
	}

	// 监听
	c.server(sockname)
	return &c
}
