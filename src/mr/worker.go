package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// 适配sort.Interface，用于按Key排序
type ByKey []KeyValue

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

var coordSockName string // socket for coordinator

// 向coordinator汇报结果
func reportTask(taskType TaskType, taskID int) {
	args := ReportTaskArgs{
		Type:   taskType,
		TaskID: taskID,
	}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

// map worker
func doMap(task AskTaskReply, mapf func(string, string) []KeyValue) {
	file, err := os.Open(task.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", task.FileName)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.FileName)
	}

	// 读取文件内的所有KeyValue
	kva := mapf(task.FileName, string(content))

	// 对所有KeyValue分桶
	buckets := make([][]KeyValue, task.NReduce)
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % task.NReduce
		buckets[reduceID] = append(buckets[reduceID], kv)
	}

	for reduceId := 0; reduceId < task.NReduce; reduceId++ {
		finalName := fmt.Sprintf("mr-%d-%d", task.TaskID, reduceId)

		// 临时文件解决输出部分文件问题
		// *保证多个执行相同map任务的worker不会写入相同的临时文件
		// 同时也保证了多个worker对最终文件不会交替写入
		tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
		if err != nil {
			log.Fatalf("cannot create temp file for %v", finalName)
		}
		tmpName := tmpFile.Name()

		// 要么任务成功临时文件消失
		// 要么任务失败exit不会执行Remove
		// defer os.Remove(tmpName)

		enc := json.NewEncoder(tmpFile)
		for _, kv := range buckets[reduceId] {
			err := enc.Encode(&kv)
			if err != nil {
				tmpFile.Close()
				os.Remove(tmpName)
				log.Fatalf("cannot encode %v", tmpName)
			}
		}

		if err := tmpFile.Close(); err != nil {
			os.Remove(tmpName)
			log.Fatalf("cannot close %v", tmpName)
		}

		// 待临时文件完整写入后修改文件名
		// 保证最终写入操作是原子性的
		if err := os.Rename(tmpName, finalName); err != nil {
			os.Remove(tmpName)
			log.Fatalf("cannot rename %v to %v", tmpName, finalName)
		}
	}
}

// reduce worker
func doReduce(task AskTaskReply, reducef func(string, []string) string) {
	// 收集该分区所有key对应的所有KeyValue
	kva := make([]KeyValue, 0)
	for mapId := 0; mapId < task.NMap; mapId++ {
		filename := fmt.Sprintf("mr-%d-%d", mapId, task.TaskID)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err == io.EOF {
				break
			}
			if err != nil {
				file.Close()
				log.Fatalf("cannot decode %v", filename)
			}
			kva = append(kva, kv)
		}

		file.Close()
	}

	// 根据Key进行排序
	sort.Sort(ByKey(kva))

	finalName := fmt.Sprintf("mr-out-%d", task.TaskID)

	tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
	if err != nil {
		log.Fatalf("cannot create temp file for %v", finalName)
	}
	tmpName := tmpFile.Name()

	// 按照Key切分kva并输入至reducef
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

		_, err := fmt.Fprintf(tmpFile, "%v %v\n", kva[i].Key, output)
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpName)
			log.Fatalf("cannot write %v", tmpName)
		}

		i = j
	}

	if err := tmpFile.Close(); err != nil {
		os.Remove(tmpName)
		log.Fatalf("cannot close %v", tmpName)
	}

	if err := os.Rename(tmpName, finalName); err != nil {
		os.Remove(tmpName)
		log.Fatalf("cannot rename %v to %v", tmpName, finalName)
	}
}

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	coordSockName = sockname

	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}

		ok := call("Coordinator.AskTask", &args, &reply)

		if !ok {
			return
		}

		switch reply.Type {
		case TaskMap:
			doMap(reply, mapf)
			reportTask(TaskMap, reply.TaskID)
		case TaskReduce:
			doReduce(reply, reducef)
			reportTask(TaskReduce, reply.TaskID)
		case TaskWait:
			time.Sleep(time.Second)
		case TaskExit:
			return
		}
	}

}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
