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

	completedTaskChan := make(chan taskNumber)
	allTasksDoneChan := make(chan bool)
	go mr.bookKeeping(ntasks, completedTaskChan, allTasksDoneChan)

	taskChan := make(chan *DoTaskArgs)
	go mr.scheduler(taskChan, completedTaskChan)

	for i := 0; i < ntasks; i++ {
		fmt.Printf("waiting to schedule task %d\n", i)
		task := &DoTaskArgs{JobName: mr.jobName, File: mr.files[i], Phase: phase, TaskNumber: i, NumOtherPhase: nios}
		taskChan <- task
	}

	<-allTasksDoneChan

	fmt.Printf("Schedule: %v phase done\n", phase)
}

type taskNumber int

func (mr *Master) bookKeeping(ntasks int, completedTaskChan <-chan taskNumber, allTasksDoneChan chan<- bool) {
	tasksCompleted := make([]bool, ntasks)
	for task := range completedTaskChan {
		tasksCompleted[task] = true

		allDone := true // dangerous to assume success?
		for _, val := range tasksCompleted {
			if val != true {
				allDone = false
			}
		}

		if allDone == true {
			allTasksDoneChan <- true
		}
	}
}

func (mr *Master) scheduler(taskChan chan *DoTaskArgs, completedChan chan<- taskNumber) {
	for task := range taskChan {
		worker := <-mr.registerChannel
		ok := call(worker, "Worker.DoTask", task, new(struct{}))
		switch ok {
		case true:
			completedChan <- taskNumber(task.TaskNumber)
			mr.myRegister(worker)
		case false:
			fmt.Printf("DoTask: RPC %s Worker.DoTask error %+v. Retrying\n", worker, task)
			taskChan <- task
		}
	}
}

func (mr *Master) myRegister(worker string) {
	ok := call(mr.address, "Master.Register", RegisterArgs{Worker: worker}, new(struct{}))
	if ok == false {
		fmt.Printf("Register: Worker %s failed to register with master %s\n", worker, mr.address)
	}
}
