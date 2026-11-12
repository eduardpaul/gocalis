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
	_, release, err := q.acquire(ctx, 10)
	if release != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("acquire canceled context = (%v, %v)", release != nil, err)
	}
	if q.busy || q.pq.Len() != 0 {
		t.Fatal("canceled request retained ownership or a queue entry")
	}
}

func TestNodeQueueCancellationWhileBusy(t *testing.T) {
	q := newNodeQueue()
	_, release, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, r, err := q.acquire(ctx, 10)
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
	_, r, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	r()
}

func TestUnregisterCancelsQueuedTurns(t *testing.T) {
	q := newNodeQueue()
	_, release, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() { _, _, err := q.acquire(context.Background(), 0); done <- err }()
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

func TestNodeQueueExclusivePreemptsHolder(t *testing.T) {
	q := newNodeQueue()
	turnCtx, release, err := q.acquire(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	// The holder releases once its turn context is cancelled, like a TTS
	// utterance whose playback is cut off.
	go func() {
		<-turnCtx.Done()
		release()
	}()

	done := make(chan error, 1)
	var releaseEx func()
	go func() {
		var err error
		_, releaseEx, err = q.acquireExclusive(context.Background())
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("acquireExclusive: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("exclusive acquire did not preempt the holder")
	}
	if !errors.Is(context.Cause(turnCtx), ErrPreempted) {
		t.Fatalf("holder cause = %v, want ErrPreempted", context.Cause(turnCtx))
	}

	// While held exclusively, ordinary turns are rejected right away.
	if _, _, err := q.acquire(context.Background(), 1000); !errors.Is(err, ErrIntercomActive) {
		t.Fatalf("acquire during exclusive hold = %v, want ErrIntercomActive", err)
	}
	if _, _, err := q.acquireExclusive(context.Background()); !errors.Is(err, ErrIntercomActive) {
		t.Fatalf("second exclusive acquire = %v, want ErrIntercomActive", err)
	}

	releaseEx()
	_, r, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatalf("acquire after exclusive release: %v", err)
	}
	r()
}

func TestNodeQueueExclusiveRejectsQueuedWaiters(t *testing.T) {
	q := newNodeQueue()
	turnCtx, release, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	queued := make(chan error, 1)
	go func() {
		_, r, err := q.acquire(context.Background(), 5)
		if r != nil {
			r()
		}
		queued <- err
	}()
	// Let the waiter park behind the holder.
	deadline := time.Now().Add(time.Second)
	for {
		q.mu.Lock()
		n := q.pq.Len()
		q.mu.Unlock()
		if n == 1 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	go func() {
		<-turnCtx.Done()
		release()
	}()
	_, releaseEx, err := q.acquireExclusive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer releaseEx()
	select {
	case err := <-queued:
		if !errors.Is(err, ErrIntercomActive) {
			t.Fatalf("queued waiter = %v, want ErrIntercomActive", err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued waiter was not rejected by the exclusive takeover")
	}
}

func TestNodeQueueAbandonedExclusiveClearsReservation(t *testing.T) {
	q := newNodeQueue()
	// A holder that ignores preemption keeps the node busy.
	_, release, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, _, err := q.acquireExclusive(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("acquireExclusive = %v, want deadline", err)
	}
	release()
	_, r, err := q.acquire(context.Background(), 0)
	if err != nil {
		t.Fatalf("acquire after abandoned takeover: %v", err)
	}
	r()
}
