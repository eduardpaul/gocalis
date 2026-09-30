package ask

import (
	"context"
	"gocalis/internal/audionode"
	"io"
)

// promptSource appends the listening chime only after synthesis reaches EOF.
// Both share one playback route and cancellation lifetime.
type promptSource struct {
	prompt audionode.PCM16Source
	tail   *audionode.SliceSource
	ended  bool
}

func (s *promptSource) SampleRate() int { return s.prompt.SampleRate() }
func (s *promptSource) ReadPCM16(ctx context.Context, size int) ([]int16, error) {
	if !s.ended {
		pcm, err := s.prompt.ReadPCM16(ctx, size)
		if err != io.EOF {
			return pcm, err
		}
		s.ended = true
		if len(pcm) > 0 {
			return pcm, nil
		}
	}
	return s.tail.ReadPCM16(ctx, size)
}
