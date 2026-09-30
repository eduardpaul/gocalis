package workqueue

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestStablePriorityAndBounds(t *testing.T) {
	q := New[string](3)
	for _, e := range []struct {
		v string
		p int
	}{{"low", 0}, {"first", 10}, {"second", 10}} {
		if err := q.Push(context.Background(), e.v, e.p); err != nil {
			t.Fatal(err)
		}
	}
	if err := q.Push(context.Background(), "overflow", 0); !errors.Is(err, ErrFull) {
		t.Fatalf("overflow = %v", err)
	}
	for _, want := range []string{"first", "second", "low"} {
		if got, ok := q.Pop(); !ok || got != want {
			t.Fatalf("Pop = %q, %v; want %q", got, ok, want)
		}
	}
	q.Close()
	if err := q.Push(context.Background(), "closed", 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed Push = %v", err)
	}
}

func TestCloseWakesWorkerAndDiscardsPending(t *testing.T) {
	q := New[int](1)
	done := make(chan struct{})
	go func() { q.Pop(); close(done) }()
	q.Close()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker remained blocked after Close")
	}
	q2 := New[int](1)
	_ = q2.Push(context.Background(), 1, 0)
	q2.Close()
	if _, ok := q2.Pop(); ok {
		t.Fatal("closed queue ran pending work")
	}
}

func TestCanceledSubmission(t *testing.T) {
	q := New[int](1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := q.Push(ctx, 1, 0); !errors.Is(err, context.Canceled) {
		t.Fatalf("Push = %v", err)
	}
}

func TestCanceledPendingJobReleasesAdmissionCapacity(t *testing.T) {
	q := New[string](1)
	ctx, cancel := context.WithCancel(context.Background())
	if err := q.Push(ctx, "expired", 100); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := q.Push(context.Background(), "fresh", 0); err != nil {
		t.Fatal("canceled job retained capacity:", err)
	}
	if got, ok := q.Pop(); !ok || got != "fresh" {
		t.Fatalf("Pop = %q, %v", got, ok)
	}
	q.Close()
}
