package raft

// replicates, then applies -- better name to encompass both steps?
func replicateLog(command interface{}) {
	replicatedCh := make(chan struct{})

	// go replicate(command, replicatedCh) // push go down to replicate, or go here?

	select {
	case <-replicatedCh: // block until replicated. IRL probably timeout
		// go commit(command)

		// case time.After(foobar):
		// 	DPrintf("this timed out\n")
	}

}

func (rf *Raft) replicate(command interface{}, doneCh chan<- struct{}) {
	nextLog := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, nextLog)

	peerConfirmationCh := make(chan struct{})
	go replicateWorker(peerConfirmationCh, doneCh)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// if ok := rf.sendAppendEntries
		// boilerplate
		// appendEntries RPC
	}
}

func replicateWorker(majority int, peerConfirmationCh <-chan struct{}, doneCh chan<- struct{}) {
	var confirmedPeers int

	for range peerConfirmationCh {
		confirmedPeers++
		if confirmedPeers > majority {
			close(doneCh)
			return
		}
	}
}
