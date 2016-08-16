package main

// replicates, then applies -- better name to encompass both steps?
func replicateLog(command interface{}) {
	replicatedCh := make(chan struct{})

	go replicate(command, replicatedCh) // push go down to replicate, or go here?

	select {
	case <-replicatedCh: // block until replicated. IRL probably timeout
		// go commit(command)

		// case time.After(foobar):
		// 	DPrintf("this timed out\n")
	}

}

func replicate(command interface{}, doneCh chan<- struct{}) {
	nextLog = LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, nextLog)

	peerConfirmationCh := make(chan struct{})
	go replicateWorker(peerConfirmationCh, doneCh)

	for i := range rf.peers {
		// appendEntries RPC
	}
}

func replicateWorker(peerConfirmationCh <-chan struct{}, doneCh chan<- struct{}) {

}
