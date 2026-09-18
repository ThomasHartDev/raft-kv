package raft

func (n *Node) Propose(data []byte) (uint64, Term, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, 0, ErrNotLeader
	}
	entry := n.log.append(n.term, data)
	n.matchIndex[n.id] = entry.Index
	n.advanceCommitLocked()
	if err := n.persistLocked(); err != nil {
		n.mu.Unlock()
		return 0, 0, err
	}
	msgs := n.replicateAllLocked()
	term := n.term
	trans := n.trans
	n.mu.Unlock()
	for _, m := range msgs {
		if trans != nil {
			_ = trans.Send(m)
		}
	}
	return entry.Index, term, nil
}

func (n *Node) replicateAllLocked() []Message {
	out := make([]Message, 0, len(n.peers))
	for _, p := range n.peers {
		out = append(out, n.replicateToLocked(p))
	}
	return out
}

func (n *Node) replicateToLocked(peer NodeID) Message {
	next := n.nextIndex[peer]
	if next == 0 {
		next = 1
	}
	prevIndex := next - 1
	prevTerm, _ := n.log.termAt(prevIndex)
	return Message{
		From:         n.id,
		To:           peer,
		Term:         n.term,
		Type:         MsgAppendEntries,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      n.log.entriesFrom(next),
		LeaderCommit: n.commitIndex,
	}
}

func (n *Node) stepAppendEntries(msg Message) (*Message, bool) {
	reject := &Message{From: n.id, To: msg.From, Term: n.term, Type: MsgAppendEntriesResp, Success: false}
	if msg.Term < n.term {
		return reject, false
	}
	if n.role != Follower {
		n.role = Follower
		n.votes = nil
	}
	n.resetElectionLocked()
	prevTerm, ok := n.log.termAt(msg.PrevLogIndex)
	if !ok || prevTerm != msg.PrevLogTerm {
		return reject, false
	}
	n.log.appendFrom(msg.PrevLogIndex, msg.Entries)
	if msg.LeaderCommit > n.commitIndex {
		if last := n.log.lastIndex(); msg.LeaderCommit < last {
			n.commitIndex = msg.LeaderCommit
		} else {
			n.commitIndex = last
		}
	}
	return &Message{
		From:       n.id,
		To:         msg.From,
		Term:       n.term,
		Type:       MsgAppendEntriesResp,
		Success:    true,
		MatchIndex: msg.PrevLogIndex + uint64(len(msg.Entries)),
	}, true
}

func (n *Node) stepAppendEntriesResp(msg Message) *Message {
	if n.role != Leader || msg.Term != n.term {
		return nil
	}
	if !msg.Success {
		if n.nextIndex[msg.From] > 1 {
			n.nextIndex[msg.From]--
		}
		// Synchronous resend per rejection is fine over MemoryNetwork; a real network
		// transport will want batching/backoff here before this is more than in-memory tests.
		retry := n.replicateToLocked(msg.From)
		return &retry
	}
	if msg.MatchIndex > n.matchIndex[msg.From] {
		n.matchIndex[msg.From] = msg.MatchIndex
	}
	n.nextIndex[msg.From] = msg.MatchIndex + 1
	n.advanceCommitLocked()
	return nil
}

// advanceCommitLocked only commits entries from the leader's own term directly:
// an older-term entry can be on a quorum of logs and still get overwritten by a
// future leader, so it only becomes safe once a current-term entry commits over it (Raft §5.4.2).
func (n *Node) advanceCommitLocked() {
	for idx := n.log.lastIndex(); idx > n.commitIndex; idx-- {
		term, ok := n.log.termAt(idx)
		if !ok {
			continue
		}
		if term != n.term {
			return
		}
		count := 1
		for _, p := range n.peers {
			if n.matchIndex[p] >= idx {
				count++
			}
		}
		if count >= majority(n.clusterSize()) {
			n.commitIndex = idx
			return
		}
	}
}

func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) LastLogIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.log.lastIndex()
}

func (n *Node) LogEntry(index uint64) (LogEntry, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if index == 0 || index >= uint64(len(n.log.entries)) {
		return LogEntry{}, false
	}
	return n.log.entries[index], true
}
