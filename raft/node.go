package raft

import (
	"errors"
	"sync"
)

var ErrNotCandidate = errors.New("raft: not a candidate")

type Node struct {
	mu       sync.Mutex
	id       NodeID
	role     Role
	term     Term
	votedFor *NodeID
	votes    map[NodeID]struct{}
	peers    []NodeID
	trans    Transport
}

func NewNode(id NodeID, peers []NodeID, trans Transport) *Node {
	clean := make([]NodeID, 0, len(peers))
	seen := map[NodeID]struct{}{id: {}}
	for _, p := range peers {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		clean = append(clean, p)
	}
	return &Node{
		id:    id,
		role:  Follower,
		peers: clean,
		trans: trans,
	}
}

func (n *Node) ID() NodeID { return n.id }

func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) Term() Term {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.term
}

func (n *Node) VotedFor() *NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.votedFor == nil {
		return nil
	}
	id := *n.votedFor
	return &id
}

func (n *Node) Peers() []NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	out := make([]NodeID, len(n.peers))
	copy(out, n.peers)
	return out
}

func (n *Node) clusterSize() int {
	return len(n.peers) + 1
}

func majority(size int) int {
	return size/2 + 1
}

func (n *Node) StartElection() Term {
	n.mu.Lock()
	n.term++
	n.role = Candidate
	self := n.id
	n.votedFor = &self
	n.votes = map[NodeID]struct{}{self: {}}
	term := n.term
	peers := append([]NodeID(nil), n.peers...)
	won := len(n.votes) >= majority(n.clusterSize())
	if won {
		n.role = Leader
	}
	n.mu.Unlock()
	if n.trans != nil {
		for _, p := range peers {
			_ = n.trans.Send(Message{
				From: self,
				To:   p,
				Term: term,
				Type: MsgRequestVote,
			})
		}
	}
	return term
}

func (n *Node) PromoteLeader() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.role != Candidate {
		return ErrNotCandidate
	}
	n.role = Leader
	return nil
}

func (n *Node) ObserveTerm(term Term) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if term < n.term {
		return false
	}
	if term > n.term {
		n.term = term
		n.role = Follower
		n.votedFor = nil
		n.votes = nil
		return true
	}
	return false
}

func (n *Node) VoteCount() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.votes)
}

func (n *Node) Ping(to NodeID) error {
	n.mu.Lock()
	term := n.term
	id := n.id
	n.mu.Unlock()
	if n.trans == nil {
		return ErrUnknownPeer
	}
	return n.trans.Send(Message{
		From: id,
		To:   to,
		Term: term,
		Type: MsgPing,
	})
}
