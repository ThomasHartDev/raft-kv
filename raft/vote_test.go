package raft

import (
	"testing"
	"time"
)

func attachCluster(t *testing.T, ids ...NodeID) (*MemoryNetwork, map[NodeID]*Node, map[NodeID]Transport) {
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
		nodes[id] = NewNode(id, ids, trans[id])
	}
	return net, nodes, trans
}

func pump(t *testing.T, nodes map[NodeID]*Node, trans map[NodeID]Transport, rounds int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for r := 0; r < rounds; r++ {
		progressed := false
		for id, tr := range trans {
			select {
			case msg, ok := <-tr.Recv():
				if !ok {
					continue
				}
				nodes[id].Step(msg)
				progressed = true
			default:
			}
		}
		if !progressed {
			if time.Now().After(deadline) {
				return
			}
			if r == 0 {
				continue
			}
			return
		}
	}
}

func TestThreeNodeElectionGrantsMajority(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2, 3)
	term := nodes[1].StartElection()
	if term != 1 {
		t.Fatalf("term=%d", term)
	}
	pump(t, nodes, trans, 32)
	if nodes[1].Role() != Leader {
		t.Fatalf("role=%s votes=%d", nodes[1].Role(), nodes[1].VoteCount())
	}
	if nodes[2].Role() != Follower || nodes[3].Role() != Follower {
		t.Fatalf("followers %s %s", nodes[2].Role(), nodes[3].Role())
	}
	vf := nodes[2].VotedFor()
	if vf == nil || *vf != 1 {
		t.Fatalf("node 2 voted %v", vf)
	}
}

func TestSecondCandidateSameTermIsRefused(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2, 3)
	nodes[1].StartElection()
	pump(t, nodes, trans, 32)
	if nodes[1].Role() != Leader {
		t.Fatal("expected leader")
	}
	late := Message{From: 3, To: 2, Term: 1, Type: MsgRequestVote}
	if err := trans[3].Send(late); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-trans[2].Recv():
		nodes[2].Step(msg)
	case <-time.After(time.Second):
		t.Fatal("late vote never arrived")
	}
	vf := nodes[2].VotedFor()
	if vf == nil || *vf != 1 {
		t.Fatalf("vote flipped to %v", vf)
	}
	select {
	case resp := <-trans[3].Recv():
		if resp.Type != MsgRequestVoteResp || resp.VoteGranted {
			t.Fatalf("resp=%+v", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("missing rejection")
	}
}

func TestSplitVoteNeitherWins(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2)
	nodes[1].StartElection()
	nodes[2].StartElection()
	pump(t, nodes, trans, 32)
	if nodes[1].Role() != Candidate || nodes[2].Role() != Candidate {
		t.Fatalf("roles %s %s", nodes[1].Role(), nodes[2].Role())
	}
	if nodes[1].VoteCount() != 1 || nodes[2].VoteCount() != 1 {
		t.Fatalf("votes %d %d", nodes[1].VoteCount(), nodes[2].VoteCount())
	}
}

func TestIsolatedCandidateWinsAfterHeal(t *testing.T) {
	net, nodes, trans := attachCluster(t, 1, 2, 3)
	net.Isolate(1)
	nodes[1].StartElection()
	pump(t, nodes, trans, 16)
	if nodes[1].Role() != Candidate {
		t.Fatalf("isolated won: %s", nodes[1].Role())
	}
	if nodes[2].VotedFor() != nil || nodes[3].VotedFor() != nil {
		t.Fatal("partition leaked votes")
	}
	net.HealAll()
	nodes[1].StartElection()
	pump(t, nodes, trans, 32)
	if nodes[1].Role() != Leader {
		t.Fatalf("role=%s votes=%d", nodes[1].Role(), nodes[1].VoteCount())
	}
}

func TestStaleVoteRejectedOnHigherTerm(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2)
	nodes[2].StartElection()
	pump(t, nodes, trans, 16)
	if nodes[2].Role() != Leader {
		t.Fatal("node 2 should lead a two-node cluster after one grant")
	}
	stale := Message{From: 1, To: 2, Term: 1, Type: MsgRequestVote}
	if err := trans[1].Send(stale); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-trans[2].Recv():
		nodes[2].Step(msg)
	case <-time.After(time.Second):
		t.Fatal("stale vote never arrived")
	}
	select {
	case resp := <-trans[1].Recv():
		if resp.VoteGranted || resp.Term != nodes[2].Term() {
			t.Fatalf("resp=%+v", resp)
		}
	case <-time.After(time.Second):
		t.Fatal("expected stale rejection")
	}
}

func TestHigherTermRejectionStepsDownCandidate(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2)
	if !nodes[2].ObserveTerm(5) {
		t.Fatal("expected bump")
	}
	nodes[1].StartElection()
	pump(t, nodes, trans, 16)
	if nodes[1].Role() != Follower {
		t.Fatalf("role=%s term=%d", nodes[1].Role(), nodes[1].Term())
	}
	if nodes[1].Term() != 5 {
		t.Fatalf("term=%d", nodes[1].Term())
	}
}

func TestStepIgnoresWrongDestination(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2)
	nodes[2].Step(Message{From: 1, To: 1, Term: 1, Type: MsgRequestVote})
	if nodes[2].VotedFor() != nil {
		t.Fatal("accepted vote for someone else's mailbox")
	}
	_ = trans
}
