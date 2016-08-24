package raft

import (
	"time"
)

const (
	heartbeatTimeout time.Duration = 75 * time.Millisecond // half of minimum election timeout as specified in section 9.1
)

func (rf *Raft) beLeader() {
	rf.currentState = leader
	DPrintf("peer %d raftState: %v\n", rf.me, rf.currentState)

	for {
		select {
		case <-rf.appendEntriesCh:
			rf.resetCh <- struct{}{}
			go rf.beFollower()
			return
		case <-rf.clientRequestCh:
			rf.resetCh <- struct{}{}
		case <-time.After(heartbeatTimeout):
			rf.resetCh <- struct{}{}
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go func(peerIndex int) {
					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex}
					if ok := rf.sendAppendEntries(peerIndex, args, &AppendEntriesReply{}); !ok {
						// TODO: need to return here?
					}

					// TODO do some bookkeeping on peers and stuff
				}(i)
			}
		}
	}
}
