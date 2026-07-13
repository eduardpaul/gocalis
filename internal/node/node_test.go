package node

import (
	"sync"
	"testing"
	"time"
)

func TestSetStateDeliversTransitionsInOrder(t *testing.T) {
	n := NewPhysicalNode("test", "rtc_stream")

	var mu sync.Mutex
	var got []NodeState
	done := make(chan struct{})

	want := []NodeState{StateProcessing, StateSpeaking, StateIdle, StateListening}

	n.OnStateChanged(func(_, newState NodeState) {
		mu.Lock()
		got = append(got, newState)
		if len(got) == len(want) {
			close(done)
		}
		mu.Unlock()
	})

	for _, s := range want {
		n.SetState(s)
	}

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for state callbacks; got %v", got)
	}

	mu.Lock()
	defer mu.Unlock()
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("transition %d = %q, want %q (full: %v)", i, got[i], want[i], got)
		}
	}
}

func TestSetStateIgnoresNoOpTransition(t *testing.T) {
	n := NewPhysicalNode("test", "rtc_stream")

	var mu sync.Mutex
	count := 0
	n.OnStateChanged(func(_, _ NodeState) {
		mu.Lock()
		count++
		mu.Unlock()
	})

	// Node starts in IDLE; setting IDLE again must not fire a callback.
	n.SetState(StateIdle)
	n.SetState(StateSpeaking)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("callback fired %d times, want 1", count)
	}
}
