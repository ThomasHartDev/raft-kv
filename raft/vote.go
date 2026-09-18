package raft

func (n *Node) Step(msg Message) {
	if msg.To != n.id {
		return
	}
	n.mu.Lock()
	snap := n.snapshotDurableLocked()
	persist := false
	if msg.Term > n.term {
		n.term = msg.Term
		n.role = Follower
		n.votedFor = nil
		n.votes = nil
		// WHY: leftover election deadline would campaign against a new leader.
		n.resetElectionLocked()
		persist = true
	}
	var reply *Message
	switch msg.Type {
	case MsgRequestVote:
		var p bool
		reply, p = n.stepRequestVote(msg)
		persist = persist || p
	case MsgRequestVoteResp:
		n.stepRequestVoteResp(msg)
	case MsgHeartbeat:
		n.stepHeartbeat(msg)
	case MsgAppendEntries:
		var p bool
		reply, p = n.stepAppendEntries(msg)
		persist = persist || p
	case MsgAppendEntriesResp:
		reply = n.stepAppendEntriesResp(msg)
	}
	if persist {
		if err := n.persistLocked(); err != nil {
			n.restoreDurableLocked(snap)
			n.mu.Unlock()
			return
		}
	}
	trans := n.trans
	n.mu.Unlock()
	if reply != nil && trans != nil {
		_ = trans.Send(*reply)
	}
}

func (n *Node) stepRequestVote(msg Message) (*Message, bool) {
	granted := false
	persist := false
	if msg.Term == n.term && (n.votedFor == nil || *n.votedFor == msg.From) {
		if n.votedFor == nil {
			id := msg.From
			n.votedFor = &id
		}
		granted = true
		persist = true
		n.resetElectionLocked()
	}
	return &Message{
		From:        n.id,
		To:          msg.From,
		Term:        n.term,
		Type:        MsgRequestVoteResp,
		VoteGranted: granted,
	}, persist
}

func (n *Node) stepRequestVoteResp(msg Message) {
	if n.role != Candidate || msg.Term != n.term || !msg.VoteGranted {
		return
	}
	if n.votes == nil {
		n.votes = make(map[NodeID]struct{})
	}
	n.votes[msg.From] = struct{}{}
	if len(n.votes) >= majority(n.clusterSize()) {
		n.becomeLeaderLocked()
	}
}
