package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	reply.Term = rf.currentTerm
	DPrintf("peer %d received append RPC %+v\n", rf.me, args)

	switch {
	case args.Term > rf.currentTerm:
		reply.Success = true
		rf.setTerm(args.Term)
	case args.Term == rf.currentTerm && rf.currentState == leader:
		reply.Success = false
	case args.Term == rf.currentTerm && rf.currentState != leader: // better style to put if/else inside single case, or have minor duplication here?
		reply.Success = true // figured should be explicit about all cases rather than implict true default
	case args.Term < rf.currentTerm:
		reply.Success = false
	}

	if reply.Success {
		// delete conflicting entries (pg 4, append step #4)

		// for i:= // TODO make sure only "new" entries are appended
		for _, entry := range args.Entries {
			rf.log = append(rf.log, entry)
		}
		DPrintf("peer %d log after appends: %+v\n", rf.me, rf.log)

		// reset commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log))
			DPrintf("peer %d commit index = %d (and leader commit = %d)\n", rf.me, rf.commitIndex, args.LeaderCommit)
		}
	}

	// extract
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		applyMsg := ApplyMsg{Index: rf.lastApplied, Command: rf.log[rf.lastApplied-1].Command}
		rf.applyCh <- applyMsg
		DPrintf("peer %d applied msg: %+v.\n\t commit index: %d, lastApplied: %d\n", rf.me, applyMsg, rf.commitIndex, rf.lastApplied)
	}

	if reply.Success {
		rf.appendEntriesCh <- struct{}{}
	}

}

func min(x, y int) int {
	if x > y {
		return y
	}
	return x
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
