package session

import "testing"

func TestContinuousCapturePreservesOnsetAndPause(t *testing.T) {
	s := New("turn", "doorbell")
	s.StartCapture()
	s.FeedPCM([]float32{1, 2}, false)
	if s.CapturedCount() != 0 {
		t.Fatal("silence counted as speech")
	}
	s.FeedPCM([]float32{3}, true)
	s.FeedPCM([]float32{0, 0}, false)
	s.FeedPCM([]float32{4}, true)
	got := s.StopCapture()
	if len(got) != 6 || got[0] != 1 || got[5] != 4 {
		t.Fatalf("clipped waveform: %v", got)
	}
	s.StartCapture()
	s.FeedPCM([]float32{0}, false)
	started, _ := s.SpeechActivity()
	if !started.IsZero() || len(s.StopCapture()) != 0 {
		t.Fatal("previous turn leaked")
	}
}

func TestPreRollIsBounded(t *testing.T) {
	s := New("turn", "doorbell")
	s.StartCapture()
	s.FeedPCM(make([]float32, 16000), false)
	s.FeedPCM([]float32{1}, true)
	if len(s.StopCapture()) != 4001 {
		t.Fatal("unbounded pre-roll")
	}
}
