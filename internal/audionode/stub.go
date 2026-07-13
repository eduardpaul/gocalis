package audionode

import (
	"context"
	"io"
	"sync"
)

// Stub is an in-memory AudioNode implementation for tests and for node types
// that do not yet have a real transport. It records played audio and lets
// callers inject captured microphone audio via Emit.
type Stub struct {
	mu      sync.Mutex
	onAudio func(samples []float32)

	Played    [][]int16
	Connected bool
	Closed    bool
}

var _ AudioNode = (*Stub)(nil)

// Connect marks the stub as connected.
func (s *Stub) Connect(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Connected = true
	return nil
}

// Play records a copy of the played samples.
func (s *Stub) Play(_ context.Context, pcm16 []int16, _ int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := make([]int16, len(pcm16))
	copy(buf, pcm16)
	s.Played = append(s.Played, buf)
	return nil
}

// PlayStream drains src and records the concatenated samples as one entry.
func (s *Stub) PlayStream(ctx context.Context, src PCM16Source) error {
	var buf []int16
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := src.ReadPCM16(1024)
		buf = append(buf, chunk...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.Played = append(s.Played, buf)
	s.mu.Unlock()
	return nil
}

// OnAudio registers the microphone callback.
func (s *Stub) OnAudio(callback func(samples []float32)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onAudio = callback
}

// Close marks the stub as closed.
func (s *Stub) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Closed = true
	return nil
}

// Emit delivers samples to the registered OnAudio callback, simulating mic input.
func (s *Stub) Emit(samples []float32) {
	s.mu.Lock()
	cb := s.onAudio
	s.mu.Unlock()
	if cb != nil {
		cb(samples)
	}
}
