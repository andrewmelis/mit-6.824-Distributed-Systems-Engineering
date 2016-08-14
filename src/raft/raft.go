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

import "sync"
import "labrpc"
import "math/rand"
import "time"

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

type LogEntry struct {
	Command string
	Term    int
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
	votedFor      int  // candidateId (`me`) that received vote in current term
	votedThisTerm bool // can't null an int so must separate votedFor(id) && voted(bool)

	// TODO spec says first index is 1 not zero...
	log []LogEntry // array of pointers or structs

	/* volatile state */
	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine

	currentState raftState

	voteCh chan struct{}
}

func (rf *Raft) LastLogEntry() *LogEntry {
	return &rf.log[len(rf.log)-1] // TODO pointer or value?
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

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	rf.voteCh <- struct{}{}
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	}

	if rf.votedThisTerm == false || rf.votedFor == args.CandidateId && rf.AtLeastAsUpToDate(args) {
		reply.VoteGranted = true
	} else {
		reply.VoteGranted = false
	}
}

// TODO is there an official compare interface?
// returns true if candidate is "at least as up-to-date"
// as defined at end of section 5.4.1
func (rf *Raft) AtLeastAsUpToDate(candidate RequestVoteArgs) bool {
	lastLogEntry := rf.LastLogEntry() // NOTE: this could be "zero" struct
	switch {
	case candidate.LastLogTerm > lastLogEntry.Term:
		return true
	case candidate.LastLogTerm == lastLogEntry.Term:
		return candidate.LastLogIndex >= rf.lastApplied
	case candidate.LastLogTerm < lastLogEntry.Term:
		return false
	default: // TODO need this?
		return false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
	rf.voteCh = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.beFollower()

	return rf
}

func (rf *Raft) beFollower() {
	// TODO randomize each cycle or persist in struct?
	electionTimeout := rand.Intn(150) + 150 // paper suggests timeout between 150ms - 300ms
	fmt.Printf("electionTimeout for peer %d: %dms\n", rf.me, electionTimeout)

	for {
		select {
		case <-rf.voteCh:
			fmt.Printf("peer %d got a vote\n")
		case <-time.After(time.Duration(electionTimeout) * time.Millisecond):
			fmt.Printf("peer %d election timeout... convert to candidate\n", rf.me)
			// convert to candidate
		}
	}
}

// func follower() {}

// func candidate() {}

// func leader() {}

// func (rf *Raft) startElection() {
// 	// rf.
// 	args := RequestVoteArgs{Term: lakjsdf, CandidateId: , LastLogIndex: , LastLogTerm: }

// 	for i := range peers {
// 		// send out RequestVote rpcs
// 		sendRequestVote(i, args, &reply)
// 	}
// }
