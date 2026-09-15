package raft

import "testing"

func electLeader(t *testing.T, nodes map[NodeID]*Node, trans map[NodeID]Transport, id NodeID) {
	t.Helper()
	nodes[id].StartElection()
	pump(t, nodes, trans, 32)
	if nodes[id].Role() != Leader {
		t.Fatalf("node %d did not win election, role=%s", id, nodes[id].Role())
	}
}

func TestProposeRejectedWhenNotLeader(t *testing.T) {
	_, nodes, _ := attachCluster(t, 1, 2, 3)
	if _, _, err := nodes[1].Propose([]byte("x")); err != ErrNotLeader {
		t.Fatalf("err=%v", err)
	}
}

func TestSingleNodeProposeCommitsImmediately(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1)
	electLeader(t, nodes, trans, 1)

	idx, _, err := nodes[1].Propose([]byte("set x=1"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if got := nodes[1].CommitIndex(); got != idx {
		t.Fatalf("commitIndex=%d, want %d (leader alone is a majority of 1)", got, idx)
	}
}

func TestProposeReplicatesAndCommitsOnMajority(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2, 3)
	electLeader(t, nodes, trans, 1)

	idx, term, err := nodes[1].Propose([]byte("set x=1"))
	if err != nil {
		t.Fatalf("propose: %v", err)
	}
	if idx != 1 || term != 1 {
		t.Fatalf("idx=%d term=%d", idx, term)
	}
	pump(t, nodes, trans, 32)

	if nodes[1].CommitIndex() != 1 {
		t.Fatalf("leader commitIndex=%d", nodes[1].CommitIndex())
	}
	for _, id := range []NodeID{2, 3} {
		e, ok := nodes[id].LogEntry(1)
		if !ok || string(e.Data) != "set x=1" || e.Term != 1 {
			t.Fatalf("node %d entry=%+v ok=%v", id, e, ok)
		}
	}
}

// Followers only learn a new commit index from the LeaderCommit field carried
// on a later AppendEntries, so their view lags the leader's by one round.
func TestFollowerCommitIndexLagsBehindLeaderByOneRound(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2, 3)
	electLeader(t, nodes, trans, 1)

	if _, _, err := nodes[1].Propose([]byte("a")); err != nil {
		t.Fatal(err)
	}
	pump(t, nodes, trans, 32)
	if nodes[1].CommitIndex() != 1 {
		t.Fatalf("leader commitIndex=%d", nodes[1].CommitIndex())
	}
	for _, id := range []NodeID{2, 3} {
		if nodes[id].CommitIndex() != 0 {
			t.Fatalf("node %d commitIndex=%d, want 0 before it hears about round 1", id, nodes[id].CommitIndex())
		}
	}

	if _, _, err := nodes[1].Propose([]byte("b")); err != nil {
		t.Fatal(err)
	}
	pump(t, nodes, trans, 32)
	if nodes[1].CommitIndex() != 2 {
		t.Fatalf("leader commitIndex=%d", nodes[1].CommitIndex())
	}
	for _, id := range []NodeID{2, 3} {
		if nodes[id].CommitIndex() != 1 {
			t.Fatalf("node %d commitIndex=%d, want 1 (round 1's commit, piggybacked on round 2)", id, nodes[id].CommitIndex())
		}
	}
}

func TestAppendEntriesRejectsOnLogInconsistency(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2)
	electLeader(t, nodes, trans, 1)

	msg := Message{
		From:         1,
		To:           2,
		Term:         nodes[1].Term(),
		Type:         MsgAppendEntries,
		PrevLogIndex: 5,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 1, Index: 6, Data: []byte("z")}},
	}
	if err := trans[1].Send(msg); err != nil {
		t.Fatal(err)
	}
	nodes[2].Step(<-trans[2].Recv())

	resp := <-trans[1].Recv()
	if resp.Type != MsgAppendEntriesResp || resp.Success {
		t.Fatalf("resp=%+v", resp)
	}
	if nodes[2].LastLogIndex() != 0 {
		t.Fatalf("follower accepted a gapped entry, lastIndex=%d", nodes[2].LastLogIndex())
	}
}

func TestAppendEntriesTruncatesConflictingSuffix(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2)
	nodes[2].Step(Message{
		From: 1, To: 2, Term: 1, Type: MsgAppendEntries,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 1, Index: 1, Data: []byte("stale")}},
	})
	if e, ok := nodes[2].LogEntry(1); !ok || string(e.Data) != "stale" {
		t.Fatalf("setup failed: %+v %v", e, ok)
	}

	nodes[2].Step(Message{
		From: 1, To: 2, Term: 2, Type: MsgAppendEntries,
		PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []LogEntry{{Term: 2, Index: 1, Data: []byte("fresh")}},
	})
	e, ok := nodes[2].LogEntry(1)
	if !ok || string(e.Data) != "fresh" || e.Term != 2 {
		t.Fatalf("conflicting entry not overwritten: %+v %v", e, ok)
	}
	_ = trans
}

func TestStaleLeaderAppendEntriesRejected(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2)
	nodes[2].ObserveTerm(9)

	if err := trans[1].Send(Message{From: 1, To: 2, Term: 3, Type: MsgAppendEntries}); err != nil {
		t.Fatal(err)
	}
	nodes[2].Step(<-trans[2].Recv())
	resp := <-trans[1].Recv()
	if resp.Success || resp.Term != 9 {
		t.Fatalf("resp=%+v", resp)
	}
}

// Simulates a follower that lost its log (e.g. restarted from disk before a
// snapshot existed). The leader must walk nextIndex back to 0 and resend the
// whole log before the follower accepts anything again.
func TestLeaderBacksOffNextIndexAndCatchesFollowerUp(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2)
	electLeader(t, nodes, trans, 1)

	for i := 0; i < 3; i++ {
		if _, _, err := nodes[1].Propose([]byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	pump(t, nodes, trans, 32)
	if nodes[1].CommitIndex() != 3 {
		t.Fatalf("commitIndex=%d", nodes[1].CommitIndex())
	}

	nodes[2].mu.Lock()
	nodes[2].log = newRaftLog()
	nodes[2].commitIndex = 0
	nodes[2].mu.Unlock()

	if _, _, err := nodes[1].Propose([]byte("y")); err != nil {
		t.Fatal(err)
	}
	pump(t, nodes, trans, 32)

	e, ok := nodes[2].LogEntry(4)
	if !ok || string(e.Data) != "y" {
		t.Fatalf("follower never caught back up: %+v %v", e, ok)
	}
	if nodes[1].CommitIndex() != 4 {
		t.Fatalf("leader commitIndex=%d", nodes[1].CommitIndex())
	}
	if nodes[2].CommitIndex() != 3 {
		t.Fatalf("follower commitIndex=%d, want 3 (the pre-catchup commit, piggybacked on the winning retry)", nodes[2].CommitIndex())
	}
}

// A leader can only commit an entry from its own term by counting replicas
// directly; an older-term entry that already reached a quorum still can't be
// committed on its own, since a future leader is free to overwrite it (Raft §5.4.2).
func TestOldTermEntryNotCommittedWithoutCurrentTermEntry(t *testing.T) {
	_, nodes, trans := attachCluster(t, 1, 2, 3)
	electLeader(t, nodes, trans, 1)
	if _, _, err := nodes[1].Propose([]byte("a")); err != nil {
		t.Fatal(err)
	}
	pump(t, nodes, trans, 32)
	if nodes[1].CommitIndex() != 1 {
		t.Fatalf("commitIndex=%d", nodes[1].CommitIndex())
	}

	nodes[1].mu.Lock()
	nodes[1].log.entries = append(nodes[1].log.entries, LogEntry{Term: 1, Index: 2, Data: []byte("stale")})
	nodes[1].term = 5
	nodes[1].matchIndex[2] = 2
	nodes[1].matchIndex[3] = 2
	nodes[1].advanceCommitLocked()
	got := nodes[1].commitIndex
	nodes[1].mu.Unlock()
	if got != 1 {
		t.Fatalf("must not commit an old-term entry directly even with quorum, commitIndex=%d", got)
	}
}
