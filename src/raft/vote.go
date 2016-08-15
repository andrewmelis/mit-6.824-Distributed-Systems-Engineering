package raft

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
	DPrintf("ELECTION: peer %d is in state %s, received RequestVote rpc from peer %d\n", rf.me, rf.currentState, args.CandidateId)

	switch {
	case args.Term < rf.currentTerm:
		reply.VoteGranted = false
		DPrintf("ELECTION: peer %d sends failing RequestVoteRepl bc args.term %d < currentTerm %d\n", rf.me, args.Term, rf.currentTerm)

		return
	case args.Term > rf.currentTerm:
		rf.setTerm(args.Term) // only reset term (and votedFor) if rf is behind
	}

	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId && rf.AtLeastAsUpToDate(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}

	// TODO move me somewhere else
	if reply.VoteGranted {
		rf.requestVoteCh <- struct{}{}
	}

	DPrintf("ELECTION: peer %d sends RequestVoteRepl %+v\n", rf.me, reply)
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
		return candidate.LastLogIndex >= rf.lastApplied // is lastApplied correct here?
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
	DPrintf("ELECTION: call to peer %d from candidate %d\n", server, args.CandidateId)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
