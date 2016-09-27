package raft

func (rf *Raft) beCandidate() {
	rf.requestVoteCh = make(chan RequestVoteHandler) // TODO HACK

	rf.currentState = candidate
	rf.setTerm(rf.currentTerm + 1)
	DPrintf("peer %d raftState: %v\n", rf.me, rf.currentState)

	wonElectionCh := make(chan struct{})

	go rf.startElection(wonElectionCh)

	select {
	case <-rf.stateChangeCh:
		DPrintf("peer %d election timed out while candidate\n", rf.me)
	case <-wonElectionCh:
		rf.resetCh <- struct{}{}
		go rf.beLeader()
	case <-rf.appendEntriesCh:
		rf.resetCh <- struct{}{}
		go rf.beFollower()
	case <-rf.requestVoteCh:
		rf.resetCh <- struct{}{}
		go rf.beFollower()
	case handler := <-rf.candidateRequestVoteCh:
		rf.followerHandleRequestVote(handler)
	}
}

func (rf *Raft) candidateHandleRequestVote(handler RequestVoteHandler) {
	DPrintf("in new candidate extracted bit\n")
	args := handler.args
	reply := handler.reply

	replyCh := handler.replyCh
	defer func(reply *RequestVoteReply) { replyCh <- reply }(reply)

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

func (rf *Raft) startElection(wonElectionCh chan struct{}) {
	DPrintf("peer %d starting election\n", rf.me)
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastApplied, LastLogTerm: rf.lastLogEntry().Term}
	reply := RequestVoteReply{}

	electionVotesCh := make(chan int)
	go electionWorker(electionVotesCh, rf.majority(), wonElectionCh)

	for i := range rf.peers {
		if i == rf.me {
			rf.voteForSelf(electionVotesCh)
			continue
		}

		go func(peerIndex int) {
			if ok := rf.sendRequestVote(peerIndex, args, &reply); !ok {
				return
			}

			DPrintf("candidate %d received reply %+v from peer %d\n", rf.me, reply, peerIndex)

			if reply.VoteGranted {
				select {
				case <-wonElectionCh: // way to do this without an extra channel?
					return
				case electionVotesCh <- peerIndex:
					// TODO bookkeeping with term of replying server
				}
			}
		}(i)
	}
}

func (rf *Raft) voteForSelf(electionVotesCh chan<- int) {
	rf.votedFor = rf.me
	electionVotesCh <- rf.me
}

func electionWorker(electionVotesCh <-chan int, majority int, wonElectionCh chan<- struct{}) {
	var votesReceived int

	for range electionVotesCh {
		votesReceived++
		// TODO eventually setup leader data structures here
		if votesReceived > majority {
			// TODO should i close all this stuff if this peer loses election? or just let GC handle it?
			close(wonElectionCh)
			return
		}
	}
}
