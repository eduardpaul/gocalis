package brain

import (
	"context"
	"errors"
	"fmt"

	"gocalis/internal/audionode"
	"gocalis/internal/node"
)

// SetRawAudioTap installs a continuous-microphone sink for a node. While set,
// every mic chunk delivered to ForwardRawAudio for that node is handed to fn
// (raw float32, model rate, NOT VAD-gated). The intercom engine uses this to
// mix the node's live mic into its call. Installing a tap does not affect the
// wake/VAD/session paths.
func (b *Brain) SetRawAudioTap(nodeID string, fn func(samples []float32)) {
	b.rawTapsMu.Lock()
	defer b.rawTapsMu.Unlock()
	b.rawTaps[nodeID] = fn
}

// ClearRawAudioTap removes the continuous-mic sink for a node (if any).
func (b *Brain) ClearRawAudioTap(nodeID string) {
	b.rawTapsMu.Lock()
	defer b.rawTapsMu.Unlock()
	delete(b.rawTaps, nodeID)
}

// ForwardRawAudio delivers a raw mic chunk to the node's installed tap, if any.
// It is called from the capture path for every chunk; with no tap installed it
// is a cheap map lookup and returns immediately.
func (b *Brain) ForwardRawAudio(nodeID string, samples []float32) {
	b.rawTapsMu.RLock()
	fn := b.rawTaps[nodeID]
	b.rawTapsMu.RUnlock()
	if fn != nil {
		fn(samples)
	}
}

// StreamOut plays a live PCM16 source on the turn's node for the duration of an
// intercom call. Holding the Turn (from AcquireNodeIntercom) is what makes this
// exclusive: other speak/ask/play requests for the node fail with
// ErrIntercomActive until the call releases it. StreamOut
// drives the node's state (INTERCOM while streaming, back to IDLE on return).
//
// onPlayed, when non-nil, is invoked with each chunk of PCM16 actually pushed to
// the device output (post output-gain). The intercom engine uses this as the
// far-end reference for that node's acoustic echo canceller.
func (t *Turn) StreamOut(ctx context.Context, src audionode.PCM16Source, onPlayed func(pcm []int16)) error {
	handle := t.Handle
	var effective audionode.PCM16Source = src
	if handle.Config.RTCStream.OutputGainDb != 0 {
		effective = &gainSource{src: effective, gainDb: handle.Config.RTCStream.OutputGainDb}
	}
	if onPlayed != nil {
		effective = &teeSource{src: effective, onRead: onPlayed}
	}

	handle.Node.SetState(node.StateIntercom)
	err := handle.Audio.PlayStream(ctx, effective)
	handle.Node.SetState(node.StateIdle)
	if err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("intercom stream on node '%s' failed: %w", handle.Node.NodeID, err)
	}
	return nil
}

// teeSource wraps a PCM16Source and invokes onRead with a copy of each chunk it
// yields, letting the caller observe exactly what is played (e.g. to feed an
// echo canceller's far-end reference) without altering the audio.
type teeSource struct {
	src    audionode.PCM16Source
	onRead func(pcm []int16)
}

var _ audionode.PCM16Source = (*teeSource)(nil)

func (t *teeSource) SampleRate() int { return t.src.SampleRate() }

func (t *teeSource) ReadPCM16(ctx context.Context, chunkSize int) ([]int16, error) {
	chunk, err := t.src.ReadPCM16(ctx, chunkSize)
	if len(chunk) > 0 && t.onRead != nil {
		cp := make([]int16, len(chunk))
		copy(cp, chunk)
		t.onRead(cp)
	}
	return chunk, err
}
