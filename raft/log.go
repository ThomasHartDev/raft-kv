package raft

type LogEntry struct {
	Term  Term
	Index uint64
	Data  []byte
}

type raftLog struct {
	entries []LogEntry
}

func newRaftLog() *raftLog {
	return &raftLog{entries: []LogEntry{{}}}
}

func (l *raftLog) lastIndex() uint64 {
	return l.entries[len(l.entries)-1].Index
}

func (l *raftLog) termAt(index uint64) (Term, bool) {
	if index >= uint64(len(l.entries)) {
		return 0, false
	}
	return l.entries[index].Term, true
}

func (l *raftLog) entriesFrom(index uint64) []LogEntry {
	if index >= uint64(len(l.entries)) {
		return nil
	}
	out := make([]LogEntry, len(l.entries[index:]))
	copy(out, l.entries[index:])
	return out
}

func (l *raftLog) append(term Term, data []byte) LogEntry {
	e := LogEntry{Term: term, Index: l.lastIndex() + 1, Data: append([]byte(nil), data...)}
	l.entries = append(l.entries, e)
	return e
}

func (l *raftLog) truncateTo(index uint64) {
	if index+1 >= uint64(len(l.entries)) {
		return
	}
	l.entries = l.entries[:index+1]
}

// appendFrom truncates at the first term mismatch (the new leader wins that slot)
// and leaves matching entries untouched so a resent AppendEntries can't undo
// an already-committed suffix it happens to overlap.
func (l *raftLog) appendFrom(prevIndex uint64, entries []LogEntry) {
	for i, e := range entries {
		idx := prevIndex + uint64(i) + 1
		if idx < uint64(len(l.entries)) {
			if l.entries[idx].Term == e.Term {
				continue
			}
			l.entries = l.entries[:idx]
		}
		if idx == uint64(len(l.entries)) {
			e.Data = append([]byte(nil), e.Data...)
			l.entries = append(l.entries, e)
		}
	}
}
