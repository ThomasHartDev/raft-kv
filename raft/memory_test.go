package raft

import (
	"sync"
	"testing"
	"time"
)

func TestMemoryPingBetweenNodes(t *testing.T) {
	net := NewMemoryNetwork(8)
	ta, err := net.Attach(1)
	if err != nil {
		t.Fatal(err)
	}
	tb, err := net.Attach(2)
	if err != nil {
		t.Fatal(err)
	}
	a := NewNode(1, []NodeID{2}, ta)
	b := NewNode(2, []NodeID{1}, tb)
	if err := a.Ping(2); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-b.trans.Recv():
		if msg.From != 1 || msg.To != 2 || msg.Type != MsgPing {
			t.Fatalf("msg=%+v", msg)
		}
		if msg.Term != a.Term() {
			t.Fatalf("term=%d", msg.Term)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestAttachRejectsDuplicateAndUnknownPeer(t *testing.T) {
	net := NewMemoryNetwork(1)
	if _, err := net.Attach(1); err != nil {
		t.Fatal(err)
	}
	if _, err := net.Attach(1); err != ErrAlreadyAttached {
		t.Fatalf("got %v", err)
	}
	t1, err := net.Attach(3)
	if err != nil {
		t.Fatal(err)
	}
	if err := t1.Send(Message{From: 3, To: 99, Type: MsgPing}); err != ErrUnknownPeer {
		t.Fatalf("got %v", err)
	}
	if err := t1.Send(Message{From: 3, To: 3, Type: MsgPing}); err != ErrSelfSend {
		t.Fatalf("got %v", err)
	}
}

func TestMailboxDropWhenFull(t *testing.T) {
	net := NewMemoryNetwork(1)
	ta, _ := net.Attach(1)
	_, _ = net.Attach(2)
	if err := ta.Send(Message{To: 2, Type: MsgPing}); err != nil {
		t.Fatal(err)
	}
	if err := ta.Send(Message{To: 2, Type: MsgPing}); err != ErrDropped {
		t.Fatalf("got %v", err)
	}
}

func TestDetachClosesMailbox(t *testing.T) {
	net := NewMemoryNetwork(2)
	ta, _ := net.Attach(1)
	tb, _ := net.Attach(2)
	net.Detach(2)
	if err := ta.Send(Message{To: 2, Type: MsgPing}); err != ErrUnknownPeer {
		t.Fatalf("got %v", err)
	}
	select {
	case _, ok := <-tb.Recv():
		if ok {
			t.Fatal("expected closed recv")
		}
	case <-time.After(time.Second):
		t.Fatal("timeout")
	}
}

func TestConcurrentSendsDoNotBlock(t *testing.T) {
	net := NewMemoryNetwork(64)
	senders := make([]Transport, 8)
	for i := NodeID(1); i <= 8; i++ {
		tr, err := net.Attach(i)
		if err != nil {
			t.Fatal(err)
		}
		senders[i-1] = tr
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		from := senders[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			for to := NodeID(1); to <= 8; to++ {
				_ = from.Send(Message{To: to, Type: MsgPing})
			}
		}()
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("senders blocked")
	}
}

func TestPingWithoutTransport(t *testing.T) {
	n := NewNode(1, []NodeID{2}, nil)
	if err := n.Ping(2); err != ErrUnknownPeer {
		t.Fatalf("got %v", err)
	}
}

func TestPartitionDropsAndHealRestores(t *testing.T) {
	net := NewMemoryNetwork(4)
	ta, _ := net.Attach(1)
	tb, _ := net.Attach(2)
	net.Partition(1, 2)
	if err := ta.Send(Message{To: 2, Type: MsgPing}); err != ErrDropped {
		t.Fatalf("got %v", err)
	}
	select {
	case <-tb.Recv():
		t.Fatal("message crossed the cut")
	default:
	}
	net.Heal(1, 2)
	if err := ta.Send(Message{To: 2, Type: MsgPing}); err != nil {
		t.Fatal(err)
	}
	select {
	case msg := <-tb.Recv():
		if msg.Type != MsgPing {
			t.Fatalf("msg=%+v", msg)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout after heal")
	}
}

func TestIsolateIsUndirected(t *testing.T) {
	net := NewMemoryNetwork(4)
	ta, _ := net.Attach(1)
	tb, _ := net.Attach(2)
	tc, _ := net.Attach(3)
	net.Isolate(2)
	if err := ta.Send(Message{To: 2, Type: MsgPing}); err != ErrDropped {
		t.Fatalf("a->b %v", err)
	}
	if err := tb.Send(Message{To: 3, Type: MsgPing}); err != ErrDropped {
		t.Fatalf("b->c %v", err)
	}
	if err := ta.Send(Message{To: 3, Type: MsgPing}); err != nil {
		t.Fatalf("a->c should stay up: %v", err)
	}
	select {
	case <-tc.Recv():
	case <-time.After(time.Second):
		t.Fatal("a-c should deliver")
	}
}
