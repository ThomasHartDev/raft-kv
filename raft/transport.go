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
)

type Message struct {
	From NodeID
	To   NodeID
	Term Term
	Type MsgType
	Body []byte
}

type Transport interface {
	ID() NodeID
	Send(msg Message) error
	Recv() <-chan Message
}
