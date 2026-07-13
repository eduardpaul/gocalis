package ai

import (
	"errors"
	"io"
	"testing"
)

func TestStreamingAudioStreamReadsPushedChunks(t *testing.T) {
	s := newStreamingAudioStream(22050)
	if s.SampleRate() != 22050 {
		t.Fatalf("sample rate: got %d want 22050", s.SampleRate())
	}

	go func() {
		s.push([]float32{0.5, -0.5})
		s.push([]float32{1.0})
		s.finish(nil)
	}()

	var got []int16
	for {
		chunk, err := s.ReadPCM16(2)
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

	_, err := s.ReadPCM16(1024)
	if !errors.Is(err, wantErr) {
		t.Fatalf("expected propagated error, got %v", err)
	}
}

func TestStreamingAudioStreamDrainsBeforeError(t *testing.T) {
	s := newStreamingAudioStream(16000)
	s.push([]float32{0.25})
	s.finish(errors.New("late error"))

	// First read returns the buffered sample, not the error.
	chunk, err := s.ReadPCM16(1024)
	if err != nil {
		t.Fatalf("expected buffered sample before error, got err %v", err)
	}
	if len(chunk) != 1 {
		t.Fatalf("expected 1 sample, got %d", len(chunk))
	}

	// Subsequent read surfaces the terminal error.
	if _, err := s.ReadPCM16(1024); err == nil {
		t.Fatalf("expected terminal error on drained stream")
	}
}
