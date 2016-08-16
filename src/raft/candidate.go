package raft

import (
	"time"
)

func (rf *Raft) beCandidate() {
	rf.requestVoteCh = make(chan struct{}) // TODO HACK

	rf.currentState = candidate
	rf.setTerm(rf.currentTerm + 1)
	DPrintf("peer %d raftState: %v\n", rf.me, rf.currentState)

	wonElectionCh := make(chan struct{})

	go rf.startElection(wonElectionCh)

	select {
	case <-wonElectionCh:
		go rf.beLeader()
	case <-rf.appendEntriesCh:
		go rf.beFollower()
	case <-rf.requestVoteCh:
		go rf.beFollower()
	case <-time.After(time.Duration(rf.electionTimeout) * time.Millisecond):
		go rf.beCandidate()
	}
}

func (rf *Raft) startElection(wonElectionCh chan<- struct{}) {
	args := RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogIndex: rf.lastLogEntry().Term, LastLogTerm: rf.lastApplied}
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
			if ok := rf.sendRequestVote(peerIndex, args, &reply); !ok {
				return
			}

			if reply.VoteGranted {
				select {
				case <-electionDoneCh:
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
		if votesReceived > majority {
			// TODO should i close all this stuff if this peer loses election? or just let GC handle it?
			close(electionDoneCh)
			wonElectionCh <- struct{}{}
			return
		}
	}
}
