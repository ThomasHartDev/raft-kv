package raft

func (n *Node) Step(msg Message) {
	if msg.To != n.id {
		return
	}
	n.mu.Lock()
	if msg.Term > n.term {
		n.term = msg.Term
		n.role = Follower
		n.votedFor = nil
		n.votes = nil
		// WHY: leftover election deadline would campaign against a new leader.
		n.resetElectionLocked()
	}
	var reply *Message
	switch msg.Type {
	case MsgRequestVote:
		reply = n.stepRequestVote(msg)
	case MsgRequestVoteResp:
		n.stepRequestVoteResp(msg)
	case MsgHeartbeat:
		n.stepHeartbeat(msg)
	}
	trans := n.trans
	n.mu.Unlock()
	if reply != nil && trans != nil {
		_ = trans.Send(*reply)
	}
}

func (n *Node) stepRequestVote(msg Message) *Message {
	granted := false
	if msg.Term == n.term && (n.votedFor == nil || *n.votedFor == msg.From) {
		id := msg.From
		n.votedFor = &id
		granted = true
		n.resetElectionLocked()
	}
	return &Message{
		From:        n.id,
		To:          msg.From,
		Term:        n.term,
		Type:        MsgRequestVoteResp,
		VoteGranted: granted,
	}
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
