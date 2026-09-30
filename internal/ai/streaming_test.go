package ai

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

func TestStreamingAudioStreamReadsPushedChunks(t *testing.T) {
	s := newStreamingAudioStream(22050)
	if s.SampleRate() != 22050 {
		t.Fatalf("sample rate: got %d want 22050", s.SampleRate())
	}

	go func() {
		s.push(context.Background(), []float32{0.5, -0.5})
		s.push(context.Background(), []float32{1.0})
		s.finish(nil)
	}()

	var got []int16
	for {
		chunk, err := s.ReadPCM16(context.Background(), 2)
		got = append(got, chunk...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}

	if len(got) != 3 {
		t.Fatalf("expected 3 samples, got %d (%v)", len(got), got)
	}
}

func TestStreamingAudioStreamPropagatesError(t *testing.T) {
	s := newStreamingAudioStream(16000)
	wantErr := errors.New("boom")

	go func() {
		s.finish(wantErr)
	}()

	_, err := s.ReadPCM16(context.Background(), 1024)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected propagated error, got %v", err)
	}
}

func TestStreamingAudioStreamDrainsBeforeError(t *testing.T) {
	s := newStreamingAudioStream(16000)
	s.push(context.Background(), []float32{0.25})
	s.finish(errors.New("late error"))

	// First read returns the buffered sample, not the error.
	chunk, err := s.ReadPCM16(context.Background(), 1024)
	if err != nil {
		t.Fatalf("expected buffered sample before error, got err %v", err)
	}
	if len(chunk) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(chunk))
	}

	// Subsequent read surfaces the terminal error.
	if _, err := s.ReadPCM16(context.Background(), 1024); err == nil {
		t.Fatalf("expected terminal error on drained stream")
	}
}

func TestStreamingAudioStreamCancellationAndBackpressure(t *testing.T) {
	s := newStreamingAudioStream(4) // eight samples of buffer capacity
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.push(ctx, make([]float32, 16)) }()
	chunk, err := s.ReadPCM16(context.Background(), 4)
	if err != nil || len(chunk) != 4 {
		t.Fatalf("read = %v, %v", chunk, err)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("blocked producer = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not unblock on cancellation")
	}
	s.mu.Lock()
	buffered := len(s.buf)
	s.mu.Unlock()
	if buffered > 8 {
		t.Fatalf("buffer grew to %d samples", buffered)
	}
	empty := newStreamingAudioStream(4)
	canceled, stop := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer stop()
	if _, err := empty.ReadPCM16(canceled, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("empty read = %v", err)
	}
}

func TestStreamingAudioStreamDurationLimit(t *testing.T) {
	s := newStreamingAudioStream(4)
	if err := s.push(context.Background(), make([]float32, 4*MaxAudioSeconds+1)); err == nil {
		t.Fatal("oversized audio accepted")
	}
	if _, err := s.ReadPCM16(context.Background(), 1); err == nil {
		t.Fatal("duration error not propagated")
	}
}
