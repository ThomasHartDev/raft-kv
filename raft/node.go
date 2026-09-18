package raft

import (
	"errors"
	"math/rand/v2"
	"sync"
	"time"
)

var (
	ErrNotCandidate = errors.New("raft: not a candidate")
	ErrNotLeader    = errors.New("raft: not leader")
)

type Node struct {
	mu          sync.Mutex
	id          NodeID
	role        Role
	term        Term
	votedFor    *NodeID
	votes       map[NodeID]struct{}
	peers       []NodeID
	trans       Transport
	clock       Clock
	rng         *rand.Rand
	electMin    time.Duration
	electMax    time.Duration
	heartbeat   time.Duration
	electDue    time.Time
	hbDue       time.Time
	log         *raftLog
	commitIndex uint64
	nextIndex   map[NodeID]uint64
	matchIndex  map[NodeID]uint64
	storage     Storage
}

func NewNode(id NodeID, peers []NodeID, trans Transport) *Node {
	return NewNodeWithConfig(id, peers, trans, Config{})
}

func NewNodeWithConfig(id NodeID, peers []NodeID, trans Transport, cfg Config) *Node {
	n, err := OpenNode(id, peers, trans, cfg)
	if err != nil {
		panic(err)
	}
	return n
}

func OpenNode(id NodeID, peers []NodeID, trans Transport, cfg Config) (*Node, error) {
	cfg = cfg.normalized()
	clean := make([]NodeID, 0, len(peers))
	seen := map[NodeID]struct{}{id: {}}
	for _, p := range peers {
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		clean = append(clean, p)
	}
	n := &Node{
		id:        id,
		role:      Follower,
		peers:     clean,
		trans:     trans,
		clock:     cfg.Clock,
		rng:       cfg.RNG,
		electMin:  cfg.ElectMin,
		electMax:  cfg.ElectMax,
		heartbeat: cfg.Heartbeat,
		log:       newRaftLog(),
		storage:   cfg.Storage,
	}
	if n.storage != nil {
		st, err := n.storage.Load()
		if err != nil {
			return nil, err
		}
		n.restorePersistent(st)
	}
	n.resetElectionLocked()
	return n, nil
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
	if n.role != Leader {
		n.electDue = n.clock.Now()
	}
	msgs := n.startElectionLocked()
	term := n.term
	if msgs != nil {
		if err := n.persistLocked(); err != nil {
			n.mu.Unlock()
			return term
		}
	}
	trans := n.trans
	n.mu.Unlock()
	for _, m := range msgs {
		if trans != nil {
			_ = trans.Send(m)
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
	n.becomeLeaderLocked()
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
		n.resetElectionLocked()
		_ = n.persistLocked()
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
