package raft

import (
	"time"
)

func (rf *Raft) beFollower() {
	rf.currentState = follower
	DPrintf("peer %d raftState: %v\n", rf.me, rf.currentState)

	for {
		select {
		case <-rf.requestVoteCh:
			DPrintf("peer %d received a request vote RPC\n", rf.me)
		case <-rf.appendEntriesCh:
			DPrintf("peer %d received an append entries RPC\n", rf.me)
		case <-time.After(time.Duration(rf.electionTimeout) * time.Millisecond):
			DPrintf("peer %d election timeout... convert to candidate\n", rf.me)
			go rf.beCandidate()
			return
		}
	}
}
