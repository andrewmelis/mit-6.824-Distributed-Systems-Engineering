package raft

type LogEntry struct {
	Command string
	Term    int
}

func (rf *Raft) lastLogEntry() *LogEntry {
	if len(rf.log) == 0 {
		return &LogEntry{}
	}
	return &rf.log[len(rf.log)-1] // TODO pointer or value?
}
