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
			DPrintf("peer %d received appendEntries msg ... convert to follower\n", rf.me)
			go rf.beFollower()
			return
		// case <- call from client:
		case <-time.After(heartbeatTimeout):
			DPrintf("leader peer %d heartbeat timeout triggered, sending out empty AppendEntries rpcs...\n", rf.me)
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go func(peerIndex int) {
					DPrintf("leader peer %d sending heartbeat AppendEntries rpc to peer %d\n", rf.me, peerIndex)
					if ok := rf.sendAppendEntries(peerIndex, AppendEntriesArgs{Term: rf.currentTerm}, &AppendEntriesReply{}); !ok {
						DPrintf("sendAppendEntries rpc from leader peer %d to peer %d failed\n", rf.me, peerIndex)
						// TODO: need to return here?
					}

					// TODO do some bookkeeping on peers and stuff
				}(i)
			}
		}
	}
}
