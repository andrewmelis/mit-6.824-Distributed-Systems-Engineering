package raft

func (rf *Raft) beCandidate() {
	rf.requestVoteCh = make(chan struct{}) // TODO HACK

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
