package raft

// replicates, then applies -- better name to encompass both steps?
func (rf *Raft) replicateLog(command interface{}) {
	doneCh := make(chan struct{})

	go rf.replicate(command, doneCh) // push go down to replicate, or go here?

	<-doneCh // block until replicated. IRL probably timeout
	DPrintf("leader %d successfully replicated command %v\n", rf.me, command)

	// commit now (update commitIndex, then apply)
	rf.commitIndex++

	// extract
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		applyMsg := ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied-1].Command}
		rf.applyCh <- applyMsg
		DPrintf("peer %d applied msg: %+v\n", rf.me, applyMsg)
	}
}

func (rf *Raft) replicate(command interface{}, doneCh chan struct{}) {
	nextLog := LogEntry{Command: command, Term: rf.currentTerm}
	rf.log = append(rf.log, nextLog)

	peerConfirmationCh := make(chan int)
	go replicateWorker(rf.majority(), peerConfirmationCh, doneCh)

	for i := range rf.peers {
		if i == rf.me {
			peerConfirmationCh <- rf.me
			continue
		}

		go func(peerIndex int) {
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: []LogEntry{nextLog}, LeaderCommit: rf.commitIndex}
			reply := AppendEntriesReply{}
			if ok := rf.sendAppendEntries(peerIndex, args, &reply); !ok {
				// need to retry here or something?
				DPrintf("append RPC from leader %d to peer %d failed!\n", rf.me, i)
			}

			if reply.Success { // TODO push this down to just `peerConfirmationCh <- reply` ???
				DPrintf("leader %d got successful append entries response from peer %d\n", rf.me, peerIndex)
				select {
				case <-doneCh:
					return // already replicated, don't care
				case peerConfirmationCh <- peerIndex: // probably should just send the reply here and let
					// TODO bookkeeping or something
				}
			}
		}(i)
	}
}

func replicateWorker(majority int, peerConfirmationCh <-chan int, doneCh chan<- struct{}) {
	var confirmedPeers int

	for range peerConfirmationCh {
		confirmedPeers++
		if confirmedPeers > majority {
			close(doneCh)
			return
		}
	}
}
