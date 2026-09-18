# raft-kv

A from-scratch Raft consensus implementation in Go, with a linearizable key-value store on top.

## What this demonstrates

Raft is the consensus algorithm most production systems actually run (etcd, Consul, CockroachDB). This repo implements it by hand: persistent term and vote, leader election, log replication, membership changes, snapshots, and a client API with linearizable reads. The point is to show the protocol in code, including what has to hit disk before a node can answer an RPC.

## Concepts demonstrated

- Consensus and replicated state machines (Ongaro / Ousterhout)
- Raft roles: follower, candidate, leader
- Monotonic terms and election safety (at most one vote per term)
- Majority quorum for leadership
- Split votes: two candidates in the same term, neither wins
- Randomized election timeouts that break split-vote symmetry
- Heartbeats as a lease on the follower election timer
- Discrete-time `Tick` and a `ManualClock` for deterministic election tests
- Candidate retry: a new term after an election timeout without a majority
- Asynchronous message-passing network model
- In-memory transport with bounded mailboxes (non-blocking send, drop on full)
- Network partition and heal on an undirected cut set
- Go modules, `go test`, `go vet`, GitHub Actions CI
- Replicated log with 1-indexed entries and a term-tagged sentinel at index 0
- AppendEntries consistency check: reject on a missing or term-mismatched `PrevLogIndex`/`PrevLogTerm`
- Conflicting suffix truncation: a follower drops and overwrites entries that disagree with the leader
- Per-follower `nextIndex`/`matchIndex` and backoff-then-retry catch-up on rejection
- Commit index advancement by counting `matchIndex` against quorum, restricted to entries from the leader's current term (Raft §5.4.2)
- Commit index propagation to followers via the `LeaderCommit` field on the next AppendEntries, not the one that triggered the commit
- Raft persistent state (Figure 2): `currentTerm`, `votedFor`, and the log
- Persist-before-reply: durable write completes before RequestVote or AppendEntries replies leave the node
- Crash-safe file update: temp file, `fsync`, atomic `rename`, directory `fsync`
- CRC-32 checksummed encoding and fail-closed `OpenNode` on a corrupt file
- Volatile vs persistent state: role, `commitIndex`, and in-memory vote tallies reset on restart

## What's implemented

- Scaffold: Go modules, a node abstraction, an in-memory transport, CI (`go test`)
- RequestVote RPCs, one vote per term, majority win, and MemoryNetwork partition/heal
- Raft leader election: terms, votes, randomized timeouts, and leader heartbeats
- Log replication with AppendEntries and commit index advancement
- Persist term, vote, and log to disk so a node recovers after crash

## Usage

```go
clk := raft.NewManualClock(time.Unix(0, 0))
base := raft.Config{
    Clock:     clk,
    ElectMin:  150 * time.Millisecond,
    ElectMax:  150 * time.Millisecond,
    Heartbeat: 50 * time.Millisecond,
}
cfg := func(path string) raft.Config {
    c := base
    c.Storage = raft.NewFileStorage(path)
    return c
}

net := raft.NewMemoryNetwork(16)
t1, _ := net.Attach(1)
t2, _ := net.Attach(2)
t3, _ := net.Attach(3)

a, _ := raft.OpenNode(1, []raft.NodeID{1, 2, 3}, t1, cfg("/var/lib/raft-kv/node-1.dat"))
b, _ := raft.OpenNode(2, []raft.NodeID{1, 2, 3}, t2, cfg("/var/lib/raft-kv/node-2.dat"))
c, _ := raft.OpenNode(3, []raft.NodeID{1, 2, 3}, t3, cfg("/var/lib/raft-kv/node-3.dat"))

clk.Advance(150 * time.Millisecond)
a.Tick() // times out, becomes candidate, broadcasts RequestVote
b.Step(<-t2.Recv())
c.Step(<-t3.Recv())
a.Step(<-t1.Recv())
a.Step(<-t1.Recv())
// a.Role() == raft.Leader
a.Tick() // empty heartbeats keep b and c from starting an election

idx, term, _ := a.Propose([]byte("set x=1")) // appends to a's log and replicates
b.Step(<-t2.Recv())
a.Step(<-t1.Recv())
c.Step(<-t3.Recv())
a.Step(<-t1.Recv())
// a.CommitIndex() == idx once a majority (including a) has the entry at `term`
```

## Tests

```bash
go test ./...
go vet ./...
```
