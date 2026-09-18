package raft

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

type hookStorage struct{ err error }

func (*hookStorage) Load() (PersistentState, error) { return PersistentState{}, nil }

func (s *hookStorage) Save(PersistentState) error { return s.err }

func openFileNode(t *testing.T, id NodeID, peers []NodeID, trans Transport, path string) *Node {
	t.Helper()
	n, err := OpenNode(id, peers, trans, Config{Storage: NewFileStorage(path)})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func attachTwo(t *testing.T, a, b NodeID) (Transport, Transport) {
	t.Helper()
	net := NewMemoryNetwork(8)
	ta, e1 := net.Attach(a)
	tb, e2 := net.Attach(b)
	if e1 != nil || e2 != nil {
		t.Fatal(e1, e2)
	}
	return ta, tb
}

func TestFileStorageRoundTripAndCorrupt(t *testing.T) {
	dir := t.TempDir()
	s := NewFileStorage(filepath.Join(dir, "raft.dat"))
	st, err := s.Load()
	if err != nil || st.Term != 0 || st.VotedFor != nil || len(st.Entries) != 0 {
		t.Fatalf("missing file: %+v err=%v", st, err)
	}
	vote := NodeID(7)
	want := PersistentState{
		Term:     4,
		VotedFor: &vote,
		Entries: []LogEntry{
			{},
			{Term: 1, Index: 1, Data: []byte("set x=1")},
			{Term: 2, Index: 2, Data: []byte{0x00, 0xff}},
		},
	}
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if got.Term != 4 || got.VotedFor == nil || *got.VotedFor != 7 {
		t.Fatalf("%+v", got)
	}
	if len(got.Entries) != 3 || string(got.Entries[1].Data) != "set x=1" {
		t.Fatalf("entries %+v", got.Entries)
	}
	if got.Entries[2].Data[0] != 0 || got.Entries[2].Data[1] != 0xff {
		t.Fatalf("data %+v", got.Entries)
	}
	want.Term, want.VotedFor = 5, nil
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}
	got, err = s.Load()
	if err != nil || got.Term != 5 || got.VotedFor != nil {
		t.Fatalf("second save %+v err=%v", got, err)
	}

	raw, err := os.ReadFile(s.path)
	if err != nil {
		t.Fatal(err)
	}
	bad := [][]byte{raw[:10], append([]byte{}, raw...)}
	binary.LittleEndian.PutUint32(bad[1][0:4], 0)
	crc := append([]byte{}, raw...)
	crc[20] ^= 0xff
	ver := append([]byte{}, raw...)
	binary.LittleEndian.PutUint32(ver[4:8], 99)
	binary.LittleEndian.PutUint32(ver[8:12], crc32.ChecksumIEEE(ver[12:]))
	for i, b := range [][]byte{bad[0], bad[1], crc, ver} {
		if err := os.WriteFile(s.path, b, 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Load(); !errors.Is(err, ErrCorruptStorage) {
			t.Fatalf("case %d err=%v", i, err)
		}
	}
	junk := filepath.Join(dir, "junk.dat")
	if err := os.WriteFile(junk, []byte("nope"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenNode(1, []NodeID{2}, nil, Config{Storage: NewFileStorage(junk)}); !errors.Is(err, ErrCorruptStorage) {
		t.Fatalf("open err=%v", err)
	}
}

func TestProposeDoesNotAliasCallerBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.dat")
	n := openFileNode(t, 1, nil, nil, path)
	n.StartElection()
	if n.Role() != Leader {
		t.Fatalf("role=%s", n.Role())
	}
	buf := []byte("set x=1")
	if _, _, err := n.Propose(buf); err != nil {
		t.Fatal(err)
	}
	buf[4] = 'y'
	if _, _, err := n.Propose([]byte("set z=2")); err != nil {
		t.Fatal(err)
	}
	live, ok := n.LogEntry(1)
	if !ok {
		t.Fatal("missing live entry 1")
	}
	if string(live.Data) == "set y=1" {
		t.Fatal("live log aliased caller buffer")
	}
	if string(live.Data) != "set x=1" {
		t.Fatalf("live %+v", live)
	}
	recovered := openFileNode(t, 1, nil, nil, path)
	e, ok := recovered.LogEntry(1)
	if !ok {
		t.Fatal("missing recovered entry 1")
	}
	if string(e.Data) == "set y=1" {
		t.Fatal("durable log aliased caller buffer")
	}
	if string(e.Data) != "set x=1" {
		t.Fatalf("recovered %+v", e)
	}
}

func TestCrashRecoversTermVoteAndLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.dat")
	n := openFileNode(t, 1, nil, nil, path)
	n.StartElection()
	if n.Role() != Leader {
		t.Fatalf("role=%s", n.Role())
	}
	if _, _, err := n.Propose([]byte("set x=1")); err != nil {
		t.Fatal(err)
	}
	if n.CommitIndex() != 1 {
		t.Fatalf("commit=%d", n.CommitIndex())
	}
	crashed := openFileNode(t, 1, nil, nil, path)
	if crashed.Role() != Follower || crashed.Term() != 1 || crashed.CommitIndex() != 0 {
		t.Fatalf("role=%s term=%d commit=%d", crashed.Role(), crashed.Term(), crashed.CommitIndex())
	}
	vf := crashed.VotedFor()
	if vf == nil || *vf != 1 {
		t.Fatalf("votedFor=%v", vf)
	}
	e, ok := crashed.LogEntry(1)
	if !ok || e.Term != 1 || string(e.Data) != "set x=1" {
		t.Fatalf("entry %+v ok=%v", e, ok)
	}

	path2 := filepath.Join(t.TempDir(), "raft.dat")
	f := openFileNode(t, 1, []NodeID{1, 2}, nil, path2)
	f.Step(Message{
		From: 2, To: 1, Term: 1, Type: MsgAppendEntries,
		Entries: []LogEntry{{Term: 1, Index: 1, Data: []byte("a")}, {Term: 1, Index: 2, Data: []byte("old")}},
	})
	f.Step(Message{
		From: 2, To: 1, Term: 2, Type: MsgAppendEntries,
		PrevLogIndex: 1, PrevLogTerm: 1,
		Entries: []LogEntry{{Term: 2, Index: 2, Data: []byte("new")}},
	})
	revived := openFileNode(t, 1, []NodeID{1, 2}, nil, path2)
	e, ok = revived.LogEntry(2)
	if !ok || e.Term != 2 || string(e.Data) != "new" {
		t.Fatalf("truncated %+v ok=%v", e, ok)
	}
}

func TestRecoveredNodeKeepsVoteInSameTerm(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.dat")
	n := openFileNode(t, 1, []NodeID{1, 2, 3}, nil, path)
	n.Step(Message{From: 2, To: 1, Term: 4, Type: MsgRequestVote})
	t1, t3 := attachTwo(t, 1, 3)
	wired := openFileNode(t, 1, []NodeID{1, 2, 3}, t1, path)
	if wired.Term() != 4 {
		t.Fatalf("term=%d", wired.Term())
	}
	wired.Step(Message{From: 3, To: 1, Term: 4, Type: MsgRequestVote})
	select {
	case msg := <-t3.Recv():
		if msg.VoteGranted {
			t.Fatal("granted a second vote in the recovered term")
		}
	default:
		t.Fatal("expected RequestVote reply")
	}
}

func TestPersistFailureSuppressesRPCs(t *testing.T) {
	fail := errors.New("disk full")
	t1, t2 := attachTwo(t, 1, 2)
	n := NewNodeWithConfig(1, []NodeID{1, 2}, t1, Config{Storage: &hookStorage{err: fail}})
	if got := n.StartElection(); got != 0 {
		t.Fatalf("term=%d", got)
	}
	if n.Role() != Follower || n.Term() != 0 || n.VotedFor() != nil {
		t.Fatalf("role=%s term=%d vote=%v", n.Role(), n.Term(), n.VotedFor())
	}
	select {
	case <-t2.Recv():
		t.Fatal("must not send RequestVote before term is durable")
	default:
	}
	voter, cand := attachTwo(t, 1, 2)
	v := NewNodeWithConfig(1, []NodeID{1, 2}, voter, Config{Storage: &hookStorage{err: fail}})
	v.Step(Message{From: 2, To: 1, Term: 1, Type: MsgRequestVote})
	if v.Term() != 0 || v.VotedFor() != nil {
		t.Fatalf("term=%d vote=%v", v.Term(), v.VotedFor())
	}
	select {
	case <-cand.Recv():
		t.Fatal("must not send VoteGranted before votedFor is durable")
	default:
	}

	t3, t4 := attachTwo(t, 1, 2)
	gate := &hookStorage{}
	leader := NewNodeWithConfig(1, []NodeID{1, 2}, t3, Config{Storage: gate})
	leader.StartElection()
	if err := leader.PromoteLeader(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-t4.Recv():
	default:
	}
	gate.err = fail
	if _, _, err := leader.Propose([]byte("x")); err == nil {
		t.Fatal("expected persist error")
	}
	if leader.LastLogIndex() != 0 || leader.CommitIndex() != 0 {
		t.Fatalf("last=%d commit=%d", leader.LastLogIndex(), leader.CommitIndex())
	}
	select {
	case <-t4.Recv():
		t.Fatal("must not send AppendEntries before the log is durable")
	default:
	}
}

func TestProposePersistFailureRollsBackLog(t *testing.T) {
	fail := errors.New("disk full")
	gate := &hookStorage{}
	n := NewNodeWithConfig(1, nil, nil, Config{Storage: gate})
	n.StartElection()
	if n.Role() != Leader {
		t.Fatalf("role=%s", n.Role())
	}
	gate.err = fail
	if _, _, err := n.Propose([]byte("set x=1")); err == nil {
		t.Fatal("expected persist error")
	}
	if n.LastLogIndex() != 0 || n.CommitIndex() != 0 {
		t.Fatalf("last=%d commit=%d", n.LastLogIndex(), n.CommitIndex())
	}
	if _, ok := n.LogEntry(1); ok {
		t.Fatal("failed propose left an entry")
	}
	gate.err = nil
	idx, _, err := n.Propose([]byte("set x=1"))
	if err != nil {
		t.Fatal(err)
	}
	if idx != 1 {
		t.Fatalf("retry index=%d", idx)
	}
}

func TestStartElectionPersistFailureDoesNotBecomeLeader(t *testing.T) {
	fail := errors.New("disk full")
	n := NewNodeWithConfig(1, nil, nil, Config{Storage: &hookStorage{err: fail}})
	if got := n.StartElection(); got != 0 {
		t.Fatalf("term=%d", got)
	}
	if n.Role() != Follower || n.Term() != 0 || n.VotedFor() != nil {
		t.Fatalf("role=%s term=%d vote=%v", n.Role(), n.Term(), n.VotedFor())
	}
	n.Tick()
	if n.Role() != Follower || n.Term() != 0 {
		t.Fatalf("after tick role=%s term=%d", n.Role(), n.Term())
	}
}

func TestFollowerAppendEntriesPersistFailureRollsBackLog(t *testing.T) {
	fail := errors.New("disk full")
	t1, t2 := attachTwo(t, 1, 2)
	gate := &hookStorage{}
	f := NewNodeWithConfig(1, []NodeID{1, 2}, t1, Config{Storage: gate})
	f.Step(Message{
		From: 2, To: 1, Term: 1, Type: MsgAppendEntries,
		Entries: []LogEntry{
			{Term: 1, Index: 1, Data: []byte("keep")},
			{Term: 1, Index: 2, Data: []byte("old")},
		},
	})
	e, ok := f.LogEntry(2)
	if !ok || string(e.Data) != "old" {
		t.Fatalf("setup %+v ok=%v", e, ok)
	}
	select {
	case <-t2.Recv():
	default:
		t.Fatal("expected first AppendEntriesResp")
	}
	gate.err = fail
	f.Step(Message{
		From:         2,
		To:           1,
		Term:         2,
		Type:         MsgAppendEntries,
		PrevLogIndex: 1,
		PrevLogTerm:  1,
		Entries:      []LogEntry{{Term: 2, Index: 2, Data: []byte("new")}},
	})
	select {
	case <-t2.Recv():
		t.Fatal("must not send AppendEntriesResp")
	default:
	}
	e, ok = f.LogEntry(2)
	if !ok || e.Term != 1 || string(e.Data) != "old" {
		t.Fatalf("log changed %+v ok=%v", e, ok)
	}
	if f.Term() != 1 || f.LastLogIndex() != 2 {
		t.Fatalf("term=%d last=%d", f.Term(), f.LastLogIndex())
	}
}
