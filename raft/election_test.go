package raft

import (
	"math/rand/v2"
	"testing"
	"time"
)

func timedCluster(t *testing.T, clk *ManualClock, min, max, hb time.Duration, ids ...NodeID) (*MemoryNetwork, map[NodeID]*Node, map[NodeID]Transport) {
	t.Helper()
	net := NewMemoryNetwork(32)
	trans := make(map[NodeID]Transport, len(ids))
	nodes := make(map[NodeID]*Node, len(ids))
	for _, id := range ids {
		tr, err := net.Attach(id)
		if err != nil {
			t.Fatal(err)
		}
		trans[id] = tr
	}
	for _, id := range ids {
		nodes[id] = NewNodeWithConfig(id, ids, trans[id], Config{
			Clock:     clk,
			RNG:       rand.New(rand.NewPCG(uint64(id), 1)),
			ElectMin:  min,
			ElectMax:  max,
			Heartbeat: hb,
		})
	}
	return net, nodes, trans
}

func TestJitterStaysInsideRangeAndVaries(t *testing.T) {
	min := 150 * time.Millisecond
	max := 300 * time.Millisecond
	rng := rand.New(rand.NewPCG(7, 11))
	seen := map[time.Duration]struct{}{}
	for i := 0; i < 80; i++ {
		d := jitter(min, max, rng)
		if d < min || d > max {
			t.Fatalf("jitter %s outside [%s, %s]", d, min, max)
		}
		seen[d] = struct{}{}
	}
	if len(seen) < 8 {
		t.Fatalf("expected spread, got %d distinct values", len(seen))
	}
	if jitter(20*time.Millisecond, 20*time.Millisecond, rng) != 20*time.Millisecond {
		t.Fatal("min==max must be exact")
	}
	if jitter(5*time.Millisecond, time.Millisecond, nil) != 5*time.Millisecond {
		t.Fatal("nil rng falls back to min")
	}
}

func TestFollowerTimesOutIntoCandidate(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	_, nodes, trans := timedCluster(t, clk, 150*time.Millisecond, 150*time.Millisecond, 50*time.Millisecond, 1, 2, 3)
	clk.Advance(149 * time.Millisecond)
	nodes[1].Tick()
	if nodes[1].Role() != Follower || nodes[1].Term() != 0 {
		t.Fatalf("early tick role=%s term=%d", nodes[1].Role(), nodes[1].Term())
	}
	clk.Advance(time.Millisecond)
	nodes[1].Tick()
	if nodes[1].Role() != Candidate || nodes[1].Term() != 1 {
		t.Fatalf("role=%s term=%d", nodes[1].Role(), nodes[1].Term())
	}
	vf := nodes[1].VotedFor()
	if vf == nil || *vf != 1 {
		t.Fatalf("votedFor=%v", vf)
	}
	pump(t, nodes, trans, 32)
	if nodes[1].Role() != Leader {
		t.Fatalf("role=%s votes=%d", nodes[1].Role(), nodes[1].VoteCount())
	}
}

func TestLeaderHeartbeatPreventsFollowerElection(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	_, nodes, trans := timedCluster(t, clk, 150*time.Millisecond, 150*time.Millisecond, 50*time.Millisecond, 1, 2, 3)
	nodes[1].StartElection()
	pump(t, nodes, trans, 32)
	if nodes[1].Role() != Leader {
		t.Fatal("expected leader")
	}
	for i := 0; i < 4; i++ {
		nodes[1].Tick()
		pump(t, nodes, trans, 16)
		clk.Advance(50 * time.Millisecond)
	}
	nodes[2].Tick()
	nodes[3].Tick()
	if nodes[2].Role() != Follower || nodes[3].Role() != Follower {
		t.Fatalf("followers timed out: %s %s", nodes[2].Role(), nodes[3].Role())
	}
	if nodes[2].Term() != 1 || nodes[3].Term() != 1 {
		t.Fatalf("terms %d %d", nodes[2].Term(), nodes[3].Term())
	}
}

func TestCandidateTimeoutRetriesNewTerm(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	net, nodes, trans := timedCluster(t, clk, 100*time.Millisecond, 100*time.Millisecond, 30*time.Millisecond, 1, 2, 3)
	net.Isolate(1)
	clk.Advance(100 * time.Millisecond)
	nodes[1].Tick()
	pump(t, nodes, trans, 8)
	if nodes[1].Role() != Candidate || nodes[1].Term() != 1 {
		t.Fatalf("role=%s term=%d", nodes[1].Role(), nodes[1].Term())
	}
	clk.Advance(100 * time.Millisecond)
	nodes[1].Tick()
	if nodes[1].Term() != 2 || nodes[1].Role() != Candidate {
		t.Fatalf("retry role=%s term=%d", nodes[1].Role(), nodes[1].Term())
	}
}

func TestSplitVoteRecoversWhenOneTimeoutFires(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	_, nodes, trans := timedCluster(t, clk, 80*time.Millisecond, 80*time.Millisecond, 20*time.Millisecond, 1, 2)
	nodes[1].StartElection()
	nodes[2].StartElection()
	pump(t, nodes, trans, 32)
	if nodes[1].Role() != Candidate || nodes[2].Role() != Candidate {
		t.Fatalf("roles %s %s", nodes[1].Role(), nodes[2].Role())
	}
	clk.Advance(80 * time.Millisecond)
	nodes[1].Tick()
	pump(t, nodes, trans, 32)
	if nodes[1].Role() != Leader {
		t.Fatalf("role=%s term=%d votes=%d", nodes[1].Role(), nodes[1].Term(), nodes[1].VoteCount())
	}
	if nodes[2].Role() != Follower {
		t.Fatalf("loser role=%s", nodes[2].Role())
	}
	if nodes[1].Term() != 2 {
		t.Fatalf("term=%d", nodes[1].Term())
	}
}

func TestGrantingVoteResetsElectionTimer(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	_, nodes, trans := timedCluster(t, clk, 150*time.Millisecond, 150*time.Millisecond, 50*time.Millisecond, 1, 2, 3)
	clk.Advance(100 * time.Millisecond)
	nodes[1].StartElection()
	pump(t, nodes, trans, 32)
	if nodes[2].VotedFor() == nil || *nodes[2].VotedFor() != 1 {
		t.Fatal("node 2 should have granted")
	}
	clk.Advance(60 * time.Millisecond)
	nodes[2].Tick()
	if nodes[2].Role() != Follower {
		t.Fatalf("granted vote should have postponed campaign, role=%s", nodes[2].Role())
	}
}

func TestStaleHeartbeatDoesNotResetTimeout(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	n := NewNodeWithConfig(1, []NodeID{2}, nil, Config{
		Clock:     clk,
		ElectMin:  100 * time.Millisecond,
		ElectMax:  100 * time.Millisecond,
		Heartbeat: 20 * time.Millisecond,
		RNG:       rand.New(rand.NewPCG(1, 1)),
	})
	if !n.ObserveTerm(5) {
		t.Fatal("term bump")
	}
	clk.Advance(90 * time.Millisecond)
	n.Step(Message{From: 2, To: 1, Term: 1, Type: MsgHeartbeat})
	if n.Role() != Follower || n.Term() != 5 {
		t.Fatalf("role=%s term=%d", n.Role(), n.Term())
	}
	clk.Advance(10 * time.Millisecond)
	n.Tick()
	if n.Role() != Candidate {
		t.Fatalf("stale heartbeat suppressed timeout, role=%s", n.Role())
	}
}

func TestSameTermHeartbeatStepsDownCandidate(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	n := NewNodeWithConfig(1, []NodeID{2}, nil, Config{
		Clock:     clk,
		ElectMin:  100 * time.Millisecond,
		ElectMax:  100 * time.Millisecond,
		Heartbeat: 20 * time.Millisecond,
		RNG:       rand.New(rand.NewPCG(1, 1)),
	})
	n.StartElection()
	if n.Role() != Candidate {
		t.Fatal("expected candidate")
	}
	n.Step(Message{From: 2, To: 1, Term: 1, Type: MsgHeartbeat})
	if n.Role() != Follower {
		t.Fatalf("role=%s", n.Role())
	}
	clk.Advance(100 * time.Millisecond)
	n.Tick()
	if n.Role() != Candidate {
		t.Fatalf("timer should restart after heartbeat, role=%s", n.Role())
	}
}

func TestLeaderTickDoesNotStartElection(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	n := NewNodeWithConfig(1, nil, nil, Config{
		Clock:     clk,
		ElectMin:  50 * time.Millisecond,
		ElectMax:  50 * time.Millisecond,
		Heartbeat: 10 * time.Millisecond,
		RNG:       rand.New(rand.NewPCG(1, 1)),
	})
	n.StartElection()
	if n.Role() != Leader {
		t.Fatal("single node should win")
	}
	clk.Advance(time.Second)
	n.Tick()
	if n.Role() != Leader || n.Term() != 1 {
		t.Fatalf("role=%s term=%d", n.Role(), n.Term())
	}
}

func TestDefaultTimeoutsUsePaperRange(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	n := NewNodeWithConfig(1, []NodeID{2}, nil, Config{Clock: clk})
	clk.Advance(149 * time.Millisecond)
	n.Tick()
	if n.Role() != Follower {
		t.Fatal("below 150ms must not campaign")
	}
	clk.Advance(151 * time.Millisecond)
	n.Tick()
	if n.Role() != Candidate {
		t.Fatalf("by 300ms must campaign, role=%s", n.Role())
	}
}

func TestInvertedMaxBecomesTwiceMin(t *testing.T) {
	clk := NewManualClock(time.Unix(0, 0))
	n := NewNodeWithConfig(1, []NodeID{2}, nil, Config{
		Clock:    clk,
		ElectMin: 80 * time.Millisecond,
		ElectMax: 10 * time.Millisecond,
		RNG:      rand.New(rand.NewPCG(3, 4)),
	})
	clk.Advance(79 * time.Millisecond)
	n.Tick()
	if n.Role() != Follower {
		t.Fatal("below min")
	}
	clk.Advance(81 * time.Millisecond)
	n.Tick()
	if n.Role() != Candidate {
		t.Fatalf("role=%s", n.Role())
	}
}

func TestManualClockAdvanceIsMonotonic(t *testing.T) {
	clk := NewManualClock(time.Unix(10, 0))
	if !clk.Now().Equal(time.Unix(10, 0)) {
		t.Fatal("seed")
	}
	clk.Advance(5 * time.Millisecond)
	if clk.Now() != time.Unix(10, 0).Add(5*time.Millisecond) {
		t.Fatalf("now=%s", clk.Now())
	}
}
