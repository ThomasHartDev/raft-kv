# raft-kv

A from-scratch Raft consensus implementation in Go, with a linearizable key-value store on top.

## What this demonstrates

Raft is the consensus algorithm most production systems actually run (etcd, Consul, CockroachDB). This repo implements it by hand: persistent term and vote, leader election, log replication, membership changes, snapshots, and a client API with linearizable reads. The point is to show the protocol, not wrap a library.

## Concepts demonstrated

- Consensus and replicated state machines (Ongaro / Ousterhout)
- Raft roles: follower, candidate, leader
- Monotonic terms and single-vote-per-term
- Asynchronous message-passing network model
- In-memory transport with bounded mailboxes (non-blocking send, drop on full)
- Go modules, `go test`, `go vet`, GitHub Actions CI

## What's implemented

- Scaffold: Go modules, a node abstraction, an in-memory transport, CI (`go test`)

## Usage

```go
net := raft.NewMemoryNetwork(16)
t1, _ := net.Attach(1)
t2, _ := net.Attach(2)

a := raft.NewNode(1, []raft.NodeID{1, 2, 3}, t1)
b := raft.NewNode(2, []raft.NodeID{1, 2, 3}, t2)

_ = a.StartElection()
_ = a.Ping(2)
msg := <-t2.Recv()
```

## Tests

```bash
go test ./...
go vet ./...
```
