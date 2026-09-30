package taskgroup

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestBoundedAdmissionAndJoinedClose(t *testing.T) {
	group := New(context.Background(), 1)
	ctx, finish, err := group.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := group.Start(context.Background()); !errors.Is(err, ErrBusy) {
		t.Fatalf("second task = %v", err)
	}
	closed := make(chan struct{})
	go func() { group.Close(); close(closed) }()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("task was not cancelled")
	}
	select {
	case <-closed:
		t.Fatal("Close did not wait for task")
	default:
	}
	finish()
	finish()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not join task")
	}
	if _, _, err := group.Start(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("post-close task = %v", err)
	}
	group.Close()
}

func TestParentCancellationReachesTask(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	group := New(parent, 1)
	defer group.Close()
	ctx, finish, err := group.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	cancel()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("parent cancellation did not propagate")
	}
}
