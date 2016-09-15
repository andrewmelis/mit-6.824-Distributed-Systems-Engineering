package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type raftState string

const (
	follower  raftState = "follower"
	candidate           = "candidate"
	leader              = "leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	/* persistent state */
	currentTerm int // latest term server has seen

	// TODO should votedFor be updated / nulled if vote for losing candidate?
	votedFor int // candidateId (`me`) that received vote in current term

	electionTimeout int // needs to be static throughout life of peer(?)

	// TODO spec says first index is 1 not zero...
	log []LogEntry // array of pointers or structs

	/* volatile state */
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	/* volatile state on leaders */
	nextIndex  []int
	matchIndex []int

	currentState raftState

	resetCh       chan struct{}
	stateChangeCh chan struct{} // TODO this is ugly

	// rpc channels
	requestVoteCh          chan RequestVoteHandler
	followerRequestVoteCh  chan RequestVoteHandler
	candidateRequestVoteCh chan RequestVoteHandler
	leaderRequestVoteCh    chan RequestVoteHandler
	appendEntriesCh        chan struct{}
	clientRequestCh        chan struct{}

	applyCh chan ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	term = rf.currentTerm

	// isLeader = rf.votedFor == rf.me // TODO does this always work?
	// isLeader = false
	isLeader = rf.currentState == leader

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

func (rf *Raft) setTerm(newTerm int) {
	rf.currentTerm = newTerm
	rf.votedFor = -1 // always set back to null?
}

// TODO find a way to use this instead of passing in poorly named int
// func (rf *Raft) majority(candidate int) bool {
// 	return candidate > len(rf.peers)/2
// }
// TODO poorly named. need greater than this number to have majority
func (rf *Raft) majority() int {
	return len(rf.peers) / 2
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if rf.currentState != leader {
		return index, term, isLeader
	}

	index = rf.commitIndex + 1
	term = rf.currentTerm
	isLeader = true

	go rf.replicateLog(command)

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log) + 1 // per spec figure 2
		rf.matchIndex[i] = 0              // per spec figure 2
	}

	rf.applyCh = applyCh
	rf.resetCh = make(chan struct{})
	rf.stateChangeCh = make(chan struct{})

	rf.requestVoteCh = make(chan RequestVoteHandler)

	rf.leaderRequestVoteCh = make(chan RequestVoteHandler)
	rf.followerRequestVoteCh = make(chan RequestVoteHandler)
	rf.candidateRequestVoteCh = make(chan RequestVoteHandler)

	rf.appendEntriesCh = make(chan struct{})
	rf.clientRequestCh = make(chan struct{})

	rf.electionTimeout = rand.Intn(150) + 150 // paper suggests timeout between 150ms - 300ms

	// TODO should i initialize raftState here? or just assume the beFollower() call below takes care of it?

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.beFollower()
	go rf.manageTimeout()

	return rf
}

func (rf *Raft) manageTimeout() {
	for {
		select {
		case <-rf.resetCh:
			continue
		case <-time.After(time.Duration(rf.electionTimeout) * time.Millisecond):
			DPrintf("peer %d election timeout... convert to candidate\n", rf.me)
			rf.stateChangeCh <- struct{}{} // TODO how else to close state logic for?
			go rf.beCandidate()            // always convert to candidat here. possibly abstract to something like `advance`
		}
	}
}
