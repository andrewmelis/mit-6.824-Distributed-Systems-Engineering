package raft

type AppendEntriesArgs struct {
	Term int
}

type AppendEntriesReply struct {
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	var shouldNotify bool

	switch {
	case args.Term > rf.currentTerm:
		rf.setTerm(args.Term)
		shouldNotify = true
	case args.Term == rf.currentTerm && rf.currentState == leader:
		shouldNotify = false
	case args.Term == rf.currentTerm && rf.currentState != leader: // better style to put if/else inside single case, or have minor duplication here?
		shouldNotify = true // figured should be explicit about all cases rather than implict true default
	case args.Term < rf.currentTerm:
		shouldNotify = false
	}

	if shouldNotify {
		rf.appendEntriesCh <- struct{}{}
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
