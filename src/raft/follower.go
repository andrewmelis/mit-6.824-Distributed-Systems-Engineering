package raft

func (rf *Raft) beFollower() {
	rf.currentState = follower
	DPrintf("peer %d raftState: %v\n", rf.me, rf.currentState)

	for {
		select {
		case <-rf.stateChangeCh:
			DPrintf("peer %d done being follower\n", rf.me)
			return // better way to do this?
		case <-rf.requestVoteCh:
			DPrintf("peer %d concluded a request vote RPC\n", rf.me)
			rf.resetCh <- struct{}{}
		case <-rf.appendEntriesCh:
			DPrintf("peer %d concluded an append entries RPC\n", rf.me)
			rf.resetCh <- struct{}{}
		}
	}
}
