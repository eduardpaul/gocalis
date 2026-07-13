package session

import "testing"

func TestSessionCaptureLifecycle(t *testing.T) {
	s := New("s1", "node1")
	if s.State() != StatePrompting {
		t.Fatalf("initial state = %s, want PROMPTING", s.State())
	}

	// Feeding before capture starts must not accumulate.
	s.Feed([]float32{0.1, 0.2})
	if s.CapturedCount() != 0 {
		t.Fatalf("captured before StartCapture = %d, want 0", s.CapturedCount())
	}

	s.ToListening()
	s.StartCapture()
	s.Feed([]float32{0.1, 0.2, 0.3})
	s.Feed([]float32{0.4})
	if got := s.CapturedCount(); got != 4 {
		t.Fatalf("CapturedCount = %d, want 4", got)
	}

	captured := s.StopCapture()
	if len(captured) != 4 {
		t.Fatalf("StopCapture len = %d, want 4", len(captured))
	}

	// After StopCapture, further feeds are ignored.
	s.Feed([]float32{0.9})
	if s.CapturedCount() != 4 {
		t.Fatalf("captured after StopCapture = %d, want 4", s.CapturedCount())
	}
}

func TestSessionBargeIn(t *testing.T) {
	s := New("s1", "node1")

	// Not armed: feed should not block or panic and nothing to receive.
	s.Feed([]float32{0.1})

	ch := s.ArmBargeIn()
	s.Feed([]float32{0.2})

	select {
	case <-ch:
	default:
		t.Fatal("expected barge-in signal after Feed while armed")
	}

	// Signal is one-shot (buffered size 1); a second feed must not block.
	s.Feed([]float32{0.3})

	s.DisarmBargeIn()
	// Disarmed: a fresh channel would not be signaled; ensure no panic.
	s.Feed([]float32{0.4})
}

func TestRegistryFanOut(t *testing.T) {
	r := NewRegistry()

	a := New("a", "node1")
	b := New("b", "node1")
	c := New("c", "node2")
	r.Add(a)
	r.Add(b)
	r.Add(c)

	if r.ActiveCount("node1") != 2 {
		t.Fatalf("ActiveCount(node1) = %d, want 2", r.ActiveCount("node1"))
	}
	if r.ActiveCount("node2") != 1 {
		t.Fatalf("ActiveCount(node2) = %d, want 1", r.ActiveCount("node2"))
	}

	for _, s := range []*Session{a, b, c} {
		s.StartCapture()
	}

	r.Feed("node1", []float32{0.1, 0.2})
	if a.CapturedCount() != 2 || b.CapturedCount() != 2 {
		t.Fatalf("node1 sessions captured = (%d,%d), want (2,2)", a.CapturedCount(), b.CapturedCount())
	}
	if c.CapturedCount() != 0 {
		t.Fatalf("node2 session captured = %d, want 0 (different node)", c.CapturedCount())
	}

	r.Remove(a)
	if r.ActiveCount("node1") != 1 {
		t.Fatalf("ActiveCount(node1) after remove = %d, want 1", r.ActiveCount("node1"))
	}

	r.Feed("node1", []float32{0.3})
	if a.CapturedCount() != 2 {
		t.Fatalf("removed session still fed: count = %d, want 2", a.CapturedCount())
	}
	if b.CapturedCount() != 3 {
		t.Fatalf("remaining session count = %d, want 3", b.CapturedCount())
	}
}
