package raft

import (
	"time"
)

func (rf *Raft) beLeader() {
	rf.currentState = leader
	DPrintf("peer %d raftState: %v\n", rf.me, rf.currentState)

	for {
		select {
		case <-rf.appendEntriesCh:
			go rf.beFollower()
			return
		// case <- call from client:
		case <-time.After(heartbeatTimeout):
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go func(peerIndex int) {
					if ok := rf.sendAppendEntries(peerIndex, AppendEntriesArgs{Term: rf.currentTerm}, &AppendEntriesReply{}); !ok {
						// TODO: need to return here?
					}

					// TODO do some bookkeeping on peers and stuff
				}(i)
			}
		}
	}
}
