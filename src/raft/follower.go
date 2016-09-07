package raft

func (rf *Raft) beFollower() {
	rf.currentState = follower
	DPrintf("peer %d raftState: %v\n", rf.me, rf.currentState)

	for {
		select {
		case <-rf.stateChangeCh:
			return // better way to do this?
		case handler := <-rf.followerRequestVoteCh:
			DPrintf("peer %d handling request vote RPC\n", rf.me)
			rf.followerHandleRequestVote(handler)
		case <-rf.appendEntriesCh:
			DPrintf("peer %d concluded an append entries RPC\n", rf.me)
			rf.resetCh <- struct{}{}
		}
	}
}

func (rf *Raft) followerHandleRequestVote(handler RequestVoteHandler) {
	DPrintf("in new follower extracted bit\n")
	args := handler.args
	reply := handler.reply

	replyCh := handler.replyCh
	defer func(reply *RequestVoteReply) {
		DPrintf("replying with %+v\n", reply)
		replyCh <- reply
	}(reply)

	reply.Term = rf.currentTerm

	switch {
	case args.Term < rf.currentTerm:
		reply.VoteGranted = false
		return
	case args.Term > rf.currentTerm:
		rf.setTerm(args.Term) // only reset term (and votedFor) if rf is behind
		reply.Term = rf.currentTerm
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.AtLeastAsUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}

	if reply.VoteGranted {
		rf.resetCh <- struct{}{}
	}
}
