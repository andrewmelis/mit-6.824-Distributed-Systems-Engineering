package raft

import (
	"time"
)

const (
	heartbeatTimeout time.Duration = 75 * time.Millisecond // half of minimum election timeout as specified in section 9.1
)

func (rf *Raft) beLeader() {
	rf.currentState = leader
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.commitIndex + 1
		rf.matchIndex[i] = 0
	}
	DPrintf("peer %d raftState: %v\n", rf.me, rf.currentState)

	for {
		select {
		case <-rf.appendEntriesCh:
			rf.resetCh <- struct{}{}
			go rf.beFollower()
			return
		case <-rf.requestVoteCh:
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

				DPrintf("leader peer %d sending heartbeat to peer %d\n", rf.me, i)

				go func(peerIndex int) {
					prevLogIndex := rf.nextIndex[peerIndex] - 1
					var prevLogTerm int
					if prevLogIndex == 0 {
						prevLogTerm = 1
					} else {
						prevLogTerm = rf.log[prevLogIndex-1].Term
					}

					args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: []LogEntry{}, LeaderCommit: rf.commitIndex, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}
					if ok := rf.sendAppendEntries(peerIndex, args, &AppendEntriesReply{}); !ok {
						// TODO: need to return here?
					}

					// TODO do some bookkeeping on peers and stuff
				}(i)
			}
		}
	}
}
