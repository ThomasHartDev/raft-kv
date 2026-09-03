# raft-kv

A from-scratch Raft consensus implementation in Go, with a linearizable key-value store on top.

## What this demonstrates

Raft is the consensus algorithm most production systems actually run (etcd, Consul, CockroachDB). This repo implements it by hand: persistent term and vote, leader election, log replication, membership changes, snapshots, and a client API with linearizable reads. The point is to show the protocol, not wrap a library.

## Concepts demonstrated

- Consensus and replicated state machines (Ongaro / Ousterhout)
- Raft roles: follower, candidate, leader
- Monotonic terms and election safety (at most one vote per term)
- Majority quorum for leadership
- Split votes: two candidates in the same term, neither wins
- Asynchronous message-passing network model
- In-memory transport with bounded mailboxes (non-blocking send, drop on full)
- Network partition and heal on an undirected cut set
- Go modules, `go test`, `go vet`, GitHub Actions CI

## What's implemented

- Scaffold: Go modules, a node abstraction, an in-memory transport, CI (`go test`)
- RequestVote RPCs, one vote per term, majority win, and MemoryNetwork partition/heal

## Usage

```go
net := raft.NewMemoryNetwork(16)
t1, _ := net.Attach(1)
t2, _ := net.Attach(2)
t3, _ := net.Attach(3)

a := raft.NewNode(1, []raft.NodeID{1, 2, 3}, t1)
b := raft.NewNode(2, []raft.NodeID{1, 2, 3}, t2)
c := raft.NewNode(3, []raft.NodeID{1, 2, 3}, t3)

net.Isolate(1)
_ = a.StartElection() // stays candidate: no majority
net.HealAll()
_ = a.StartElection()
b.Step(<-t2.Recv())
c.Step(<-t3.Recv())
a.Step(<-t1.Recv())
a.Step(<-t1.Recv())
// a.Role() == raft.Leader
```

## Tests

```bash
go test ./...
go vet ./...
```
