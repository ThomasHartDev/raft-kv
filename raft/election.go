package raft

import (
	"math/rand/v2"
	"time"
)

const (
	defaultElectMin  = 150 * time.Millisecond
	defaultHeartbeat = 50 * time.Millisecond
)

type Config struct {
	Clock     Clock
	RNG       *rand.Rand
	ElectMin  time.Duration
	ElectMax  time.Duration
	Heartbeat time.Duration
}

func (c Config) normalized() Config {
	if c.Clock == nil {
		c.Clock = realClock{}
	}
	if c.ElectMin <= 0 {
		c.ElectMin = defaultElectMin
	}
	if c.ElectMax < c.ElectMin {
		c.ElectMax = c.ElectMin * 2
	}
	// WHY: heartbeats must land before the shortest election timeout or followers campaign.
	if c.Heartbeat <= 0 || c.Heartbeat >= c.ElectMin {
		c.Heartbeat = defaultHeartbeat
		if c.Heartbeat >= c.ElectMin {
			c.Heartbeat = c.ElectMin / 3
		}
		if c.Heartbeat <= 0 {
			c.Heartbeat = time.Nanosecond
		}
	}
	if c.RNG == nil {
		c.RNG = rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64()))
	}
	return c
}

func jitter(min, max time.Duration, rng *rand.Rand) time.Duration {
	if min < 0 {
		min = 0
	}
	if max < min {
		max = min
	}
	span := int64(max - min)
	if span == 0 || rng == nil {
		return min
	}
	return min + time.Duration(rng.Int64N(span+1))
}

func (n *Node) resetElectionLocked() {
	n.electDue = n.clock.Now().Add(jitter(n.electMin, n.electMax, n.rng))
}

func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.hbDue = n.clock.Now()
}

func (n *Node) startElectionLocked() []Message {
	if n.role == Leader || n.clock.Now().Before(n.electDue) {
		return nil
	}
	n.term++
	n.role = Candidate
	self := n.id
	n.votedFor = &self
	n.votes = map[NodeID]struct{}{self: {}}
	n.resetElectionLocked()
	if len(n.votes) >= majority(n.clusterSize()) {
		n.becomeLeaderLocked()
	}
	out := make([]Message, 0, len(n.peers))
	for _, p := range n.peers {
		out = append(out, Message{
			From: self,
			To:   p,
			Term: n.term,
			Type: MsgRequestVote,
		})
	}
	return out
}

func (n *Node) Tick() {
	n.mu.Lock()
	now := n.clock.Now()
	var out []Message
	if n.role == Leader {
		if !now.Before(n.hbDue) {
			out = n.heartbeatMsgsLocked()
			n.hbDue = now.Add(n.heartbeat)
		}
	} else if (n.role == Follower || n.role == Candidate) && !now.Before(n.electDue) {
		out = n.startElectionLocked()
	}
	trans := n.trans
	n.mu.Unlock()
	for _, m := range out {
		if trans != nil {
			_ = trans.Send(m)
		}
	}
}

func (n *Node) heartbeatMsgsLocked() []Message {
	out := make([]Message, 0, len(n.peers))
	for _, p := range n.peers {
		out = append(out, Message{
			From: n.id,
			To:   p,
			Term: n.term,
			Type: MsgHeartbeat,
		})
	}
	return out
}

func (n *Node) stepHeartbeat(msg Message) {
	if msg.Term < n.term {
		return
	}
	if n.role != Follower {
		n.role = Follower
		n.votes = nil
	}
	n.resetElectionLocked()
}
