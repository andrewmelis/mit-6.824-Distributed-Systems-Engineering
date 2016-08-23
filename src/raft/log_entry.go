package raft

type LogEntry struct {
	Command interface{}
	Term    int
	// Index int // include this here? or rely on log index?
}

func (rf *Raft) lastLogEntry() *LogEntry {
	if len(rf.log) == 0 {
		return &LogEntry{}
	}
	return &rf.log[len(rf.log)-1] // TODO pointer or value?
}
