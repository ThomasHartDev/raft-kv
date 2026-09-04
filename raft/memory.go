package raft

import (
	"errors"
	"sync"
)

var ErrAlreadyAttached = errors.New("raft: node already attached")

type pair struct {
	a NodeID
	b NodeID
}

func canon(a, b NodeID) pair {
	if a > b {
		return pair{a: b, b: a}
	}
	return pair{a: a, b: b}
}

type MemoryNetwork struct {
	mu       sync.Mutex
	capacity int
	boxes    map[NodeID]chan Message
	cuts     map[pair]struct{}
}

func NewMemoryNetwork(mailbox int) *MemoryNetwork {
	if mailbox < 1 {
		mailbox = 16
	}
	return &MemoryNetwork{
		capacity: mailbox,
		boxes:    make(map[NodeID]chan Message),
		cuts:     make(map[pair]struct{}),
	}
}

func (n *MemoryNetwork) Attach(id NodeID) (Transport, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, ok := n.boxes[id]; ok {
		return nil, ErrAlreadyAttached
	}
	ch := make(chan Message, n.capacity)
	n.boxes[id] = ch
	return &memoryTransport{id: id, net: n, recv: ch}, nil
}

func (n *MemoryNetwork) Detach(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if ch, ok := n.boxes[id]; ok {
		delete(n.boxes, id)
		close(ch)
	}
}

func (n *MemoryNetwork) Partition(a, b NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cuts[canon(a, b)] = struct{}{}
}

func (n *MemoryNetwork) Heal(a, b NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	delete(n.cuts, canon(a, b))
}

func (n *MemoryNetwork) Isolate(id NodeID) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for other := range n.boxes {
		if other != id {
			n.cuts[canon(id, other)] = struct{}{}
		}
	}
}

func (n *MemoryNetwork) HealAll() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.cuts = make(map[pair]struct{})
}

func (n *MemoryNetwork) deliver(msg Message) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if _, blocked := n.cuts[canon(msg.From, msg.To)]; blocked {
		return ErrDropped
	}
	ch, ok := n.boxes[msg.To]
	if !ok {
		return ErrUnknownPeer
	}
	// WHY: non-blocking send under lock so Detach cannot close mid-send.
	select {
	case ch <- msg:
		return nil
	default:
		return ErrDropped
	}
}

type memoryTransport struct {
	id   NodeID
	net  *MemoryNetwork
	recv <-chan Message
}

func (t *memoryTransport) ID() NodeID { return t.id }

func (t *memoryTransport) Recv() <-chan Message { return t.recv }

func (t *memoryTransport) Send(msg Message) error {
	if msg.From != t.id {
		msg.From = t.id
	}
	if msg.To == t.id {
		return ErrSelfSend
	}
	return t.net.deliver(msg)
}
