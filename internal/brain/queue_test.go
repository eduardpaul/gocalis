package brain

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestNodeQueueRejectsCanceledContext(t *testing.T) {
	q := newNodeQueue()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	release, err := q.acquire(ctx, 10)
	if release != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("acquire canceled context = (%v, %v)", release != nil, err)
	}
	if q.busy || q.pq.Len() != 0 {
		t.Fatal("canceled request retained ownership or a queue entry")
	}
}

func TestNodeQueueCancellationWhileBusy(t *testing.T) {
	q := newNodeQueue()
	release, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		r, err := q.acquire(ctx, 10)
		if r != nil {
			r()
		}
		done <- err
	}()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("acquire = %v, want cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter did not return while node remained busy")
	}
	release()
	release() // Release remains idempotent.
	r, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	r()
}

func TestUnregisterCancelsQueuedTurns(t *testing.T) {
	q := newNodeQueue()
	release, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() { _, err := q.acquire(context.Background(), 0); done <- err }()
	q.close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("closed node admitted a turn")
		}
	case <-time.After(time.Second):
		t.Fatal("queued turn was not released on node shutdown")
	}
}
