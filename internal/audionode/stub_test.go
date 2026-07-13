package audionode

import (
	"context"
	"io"
	"testing"
)

// sliceSource is a simple PCM16Source backed by an in-memory slice.
type sliceSource struct {
	samples    []int16
	sampleRate int
	offset     int
}

func (s *sliceSource) SampleRate() int { return s.sampleRate }

func (s *sliceSource) ReadPCM16(chunkSize int) ([]int16, error) {
	if s.offset >= len(s.samples) {
		return nil, io.EOF
	}
	end := s.offset + chunkSize
	if end > len(s.samples) {
		end = len(s.samples)
	}
	chunk := s.samples[s.offset:end]
	s.offset = end
	return chunk, nil
}

func TestStubPlayStreamDrainsSource(t *testing.T) {
	stub := &Stub{}
	src := &sliceSource{samples: []int16{1, 2, 3, 4, 5}, sampleRate: 16000}

	if err := stub.PlayStream(context.Background(), src); err != nil {
		t.Fatalf("PlayStream: %v", err)
	}

	if len(stub.Played) != 1 {
		t.Fatalf("expected 1 played buffer, got %d", len(stub.Played))
	}
	if got := len(stub.Played[0]); got != 5 {
		t.Fatalf("expected 5 samples played, got %d", got)
	}
}

func TestStubPlayStreamRespectsContext(t *testing.T) {
	stub := &Stub{}
	src := &sliceSource{samples: make([]int16, 100000), sampleRate: 16000}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := stub.PlayStream(ctx, src); err == nil {
		t.Fatalf("expected context cancellation error")
	}
}
