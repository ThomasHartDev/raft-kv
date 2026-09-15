package raft

import "errors"

var (
	ErrUnknownPeer = errors.New("raft: unknown peer")
	ErrDropped     = errors.New("raft: message dropped")
	ErrSelfSend    = errors.New("raft: send to self")
)

type MsgType uint8

const (
	MsgPing MsgType = iota
	MsgRequestVote
	MsgRequestVoteResp
	MsgHeartbeat
	MsgAppendEntries
	MsgAppendEntriesResp
)

type Message struct {
	From         NodeID
	To           NodeID
	Term         Term
	Type         MsgType
	VoteGranted  bool
	Body         []byte
	PrevLogIndex uint64
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit uint64
	Success      bool
	MatchIndex   uint64
}

type Transport interface {
	ID() NodeID
	Send(msg Message) error
	Recv() <-chan Message
}
