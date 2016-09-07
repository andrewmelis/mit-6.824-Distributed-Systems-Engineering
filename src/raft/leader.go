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
		case <-rf.stateChangeCh:
			rf.resetCh <- struct{}{}
			go rf.beFollower()
		case <-rf.appendEntriesCh:
			rf.resetCh <- struct{}{}
			go rf.beFollower()
			return
		case handler := <-rf.leaderRequestVoteCh:
			rf.leaderHandleRequestVote(handler)
		case <-rf.clientRequestCh:
			rf.resetCh <- struct{}{}
		case <-time.After(heartbeatTimeout):
			rf.resetCh <- struct{}{}
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go rf.sendHeartbeat(i)
			}
		}
	}
}

func (rf *Raft) leaderHandleRequestVote(handler RequestVoteHandler) {
	DPrintf("in new leader extracted bit\n")
	args := handler.args
	reply := handler.reply

	replyCh := handler.replyCh
	defer func(reply *RequestVoteReply) { replyCh <- reply }(reply)

	reply.Term = rf.currentTerm

	var shouldConvert bool

	switch {
	case args.Term < rf.currentTerm:
		reply.VoteGranted = false
		return
	case args.Term > rf.currentTerm:
		rf.setTerm(args.Term)
		reply.Term = rf.currentTerm
		shouldConvert = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.AtLeastAsUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}

	// TODO move me somewhere else
	if reply.VoteGranted || shouldConvert {
		rf.stateChangeCh <- struct{}{}
	}
}

func (rf *Raft) sendHeartbeat(peerIndex int) {
	DPrintf("leader peer %d sending heartbeat to peer %d\n", rf.me, peerIndex)

	prevLogIndex := rf.nextIndex[peerIndex] - 1
	var prevLogTerm int
	if prevLogIndex == 0 {
		prevLogTerm = 1
	} else {
		prevLogTerm = rf.log[prevLogIndex-1].Term
	}

	var logsToSend []LogEntry
	args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, Entries: logsToSend, LeaderCommit: rf.commitIndex, PrevLogIndex: prevLogIndex, PrevLogTerm: prevLogTerm}
	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(peerIndex, args, &reply); !ok {
		// TODO: need to return here?
		DPrintf("append RPC from leader %d to peer %d failed!\n", rf.me, peerIndex)
	}
}
