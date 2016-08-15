package raft

import (
	"time"
)

func (rf *Raft) beCandidate() {
	rf.requestVoteCh = make(chan struct{}) // TODO HACK

	rf.currentState = candidate
	DPrintf("peer %d raftState: %v\n", rf.me, rf.currentState)

	rf.currentTerm++
	DPrintf("peer %d increments its current term from %d to %d\n", rf.me, rf.currentTerm-1, rf.currentTerm)

	wonElectionCh := make(chan struct{})
	go rf.startElection(wonElectionCh)

	// for { // do i need this for? only happening once i think
	select {
	case <-wonElectionCh:
		DPrintf("peer %d received winElection msg ... convert to leader\n", rf.me)
		go rf.beLeader()
	case <-rf.appendEntriesCh:
		DPrintf("peer %d received appendEntries msg ... convert to follower\n", rf.me)
		go rf.beFollower()
	case <-rf.requestVoteCh:
		DPrintf("peer %d received requestVote msg ... convert to follower\n", rf.me)
		go rf.beFollower()
	case <-time.After(time.Duration(rf.electionTimeout) * time.Millisecond):
		DPrintf("peer %d election timeout during election... convert to candidate\n", rf.me)
		go rf.beCandidate()
	}
}

func (rf *Raft) startElection(wonElectionCh chan<- struct{}) {
	DPrintf("peer %d starts election\n", rf.me)

	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.LastLogEntry().Term, LastLogTerm: rf.lastApplied}
	reply := RequestVoteReply{}

	electionVotesCh := make(chan int)
	electionDoneCh := make(chan struct{})
	go electionWorker(electionVotesCh, rf.majority(), electionDoneCh, wonElectionCh)

	for i := range rf.peers {
		if i == rf.me {
			rf.voteForSelf(electionVotesCh)
			continue
		}

		go func(peerIndex int) {
			DPrintf("candidate peer %d sending request vote rpc to peer %d\n", rf.me, peerIndex)
			if ok := rf.sendRequestVote(peerIndex, args, &reply); !ok {
				DPrintf("sendRequestVote from candidate %d to peer %d failed\n", rf.me, peerIndex)
				return
			}

			if reply.VoteGranted {
				select {
				case <-electionDoneCh:
					DPrintf("election finished before peer %d sent in vote\n", peerIndex)
				case electionVotesCh <- peerIndex: // can't just always send this because sending on a closed ch will block forever
					DPrintf("peer %d votes for peer %d\n", peerIndex, rf.me)
					// TODO bookkeeping with term of replying server
				}
			}
		}(i)
	}
}

func (rf *Raft) voteForSelf(electionVotesCh chan<- int) {
	rf.votedFor = rf.me
	electionVotesCh <- rf.me // bookkeeping for self
	DPrintf("peer %d votes for self\n", rf.me)
}

// TODO find a way to use this instead of passing in poorly named int
// func (rf *Raft) majority(candidate int) bool {
// 	return candidate > len(rf.peers)/2
// }
// TODO poorly named. need greater than this number to have majority
func (rf *Raft) majority() int {
	return len(rf.peers) / 2
}

func electionWorker(electionVotesCh <-chan int, majority int, electionDoneCh chan<- struct{}, wonElectionCh chan<- struct{}) {
	var votesReceived int

	for range electionVotesCh {
		votesReceived++
		// TODO eventually setup leader data structures here
		DPrintf("candidate has now received %d votes\n", votesReceived)
		if votesReceived > majority {
			// TODO should i close all this stuff if this peer loses election? or just let GC handle it?
			close(electionDoneCh)
			wonElectionCh <- struct{}{}
			return
		}
	}
}
