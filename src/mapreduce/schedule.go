package mapreduce

import "fmt"

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	taskStatus := make(map[int]bool)

	for i := 0; i < ntasks; i++ {
		fmt.Printf("waiting to schedule task %d\n", i)
		doTaskArgs := &DoTaskArgs{JobName: mr.jobName, File: mr.files[i], Phase: phase, TaskNumber: i, NumOtherPhase: nios}
		freeWorker := <-mr.registerChannel
		// go func() {
		ok := call(freeWorker, "Worker.DoTask", doTaskArgs, new(struct{}))
		if ok == false {
			fmt.Printf("DoTask: RPC %s Worker.DoTask error %+v\n", freeWorker, doTaskArgs)
		}
		ok = call(mr.address, "Master.Register", RegisterArgs{Worker: freeWorker}, new(struct{}))
		if ok == false {
			fmt.Printf("Register: Worker %s failed to register with master %s\n", freeWorker, mr.address)
		}
		// }()
	}

	fmt.Printf("Schedule: %v phase done\n", phase)
}

func (mr *Master) scheduleTask(doTaskArgs *DoTaskArgs, inProgressChan chan *InProgressTask )  {
	worker := <-mr.registerChannel
	task := InProgressTask{TaskArgs: doTaskArgs, Worker: worker, CallChan: make(chan bool)}
	go func() {
		task.CallChan <- call(task.Worker, "Worker.DoTask", &task.DoTaskArgs, new(struct{}))
	}()
	inProgressChan <- task
}

func (mr *Master) completedTask(inProgressChan chan *InProgressTask, taskStatus map[int]bool) {
	for task := range inProgressChan {
		if <-task.CallChan {
			
		} else {
			
		}
	}
}

type InProgressTask struct {
	TaskArgs *DoTaskArgs
	Worker   string
	CallChan chan bool
}
