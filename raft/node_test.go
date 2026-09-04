package raft

import "testing"

func TestNewNodeStartsFollowerAtTermZero(t *testing.T) {
	n := NewNode(1, []NodeID{1, 2, 3}, nil)
	if n.ID() != 1 {
		t.Fatalf("id=%d", n.ID())
	}
	if n.Role() != Follower {
		t.Fatalf("role=%s", n.Role())
	}
	if n.Term() != 0 {
		t.Fatalf("term=%d", n.Term())
	}
	if n.VotedFor() != nil {
		t.Fatalf("votedFor=%v", n.VotedFor())
	}
	peers := n.Peers()
	if len(peers) != 2 {
		t.Fatalf("peers=%v", peers)
	}
	for _, p := range peers {
		if p == 1 {
			t.Fatalf("self in peers: %v", peers)
		}
	}
}

func TestNewNodeDropsDuplicatePeers(t *testing.T) {
	n := NewNode(1, []NodeID{2, 2, 3, 1}, nil)
	peers := n.Peers()
	if len(peers) != 2 {
		t.Fatalf("peers=%v", peers)
	}
}

func TestStartElectionIncrementsTermAndVotesSelf(t *testing.T) {
	n := NewNode(7, []NodeID{8, 9}, nil)
	term := n.StartElection()
	if term != 1 {
		t.Fatalf("term=%d", term)
	}
	if n.Role() != Candidate {
		t.Fatalf("role=%s", n.Role())
	}
	vf := n.VotedFor()
	if vf == nil || *vf != 7 {
		t.Fatalf("votedFor=%v", vf)
	}
	if n.StartElection() != 2 {
		t.Fatalf("second election term=%d", n.Term())
	}
}

func TestPromoteLeaderOnlyFromCandidate(t *testing.T) {
	n := NewNode(1, []NodeID{2}, nil)
	if err := n.PromoteLeader(); err != ErrNotCandidate {
		t.Fatalf("got %v", err)
	}
	n.StartElection()
	if err := n.PromoteLeader(); err != nil {
		t.Fatalf("promote: %v", err)
	}
	if n.Role() != Leader {
		t.Fatalf("role=%s", n.Role())
	}
}

func TestObserveTermStepsDownAndClearsVote(t *testing.T) {
	n := NewNode(1, []NodeID{2}, nil)
	n.StartElection()
	if err := n.PromoteLeader(); err != nil {
		t.Fatal(err)
	}
	if !n.ObserveTerm(3) {
		t.Fatal("expected step-down")
	}
	if n.Role() != Follower || n.Term() != 3 {
		t.Fatalf("role=%s term=%d", n.Role(), n.Term())
	}
	if n.VotedFor() != nil {
		t.Fatalf("vote should reset, got %v", n.VotedFor())
	}
}

func TestObserveTermIgnoresStale(t *testing.T) {
	n := NewNode(1, []NodeID{2}, nil)
	n.StartElection()
	if n.ObserveTerm(0) {
		t.Fatal("stale term must not change state")
	}
	if n.Role() != Candidate || n.Term() != 1 {
		t.Fatalf("role=%s term=%d", n.Role(), n.Term())
	}
}

func TestObserveTermSameTermLeavesCandidate(t *testing.T) {
	n := NewNode(1, []NodeID{2}, nil)
	n.StartElection()
	if n.ObserveTerm(1) {
		t.Fatal("another candidate in this term must not force a step-down")
	}
	if n.Role() != Candidate {
		t.Fatalf("role=%s", n.Role())
	}
}

func TestSingleNodeElectionWinsImmediately(t *testing.T) {
	n := NewNode(1, nil, nil)
	n.StartElection()
	if n.Role() != Leader {
		t.Fatalf("role=%s", n.Role())
	}
	if n.VoteCount() != 1 {
		t.Fatalf("votes=%d", n.VoteCount())
	}
}

func TestRoleString(t *testing.T) {
	if Follower.String() != "follower" || Candidate.String() != "candidate" || Leader.String() != "leader" {
		t.Fatalf("strings: %s %s %s", Follower, Candidate, Leader)
	}
	if Role(99).String() != "unknown" {
		t.Fatal("unknown role")
	}
}
