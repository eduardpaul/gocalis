package brain

import (
	"context"
	"errors"
	"fmt"

	"gocalis/internal/audionode"
	"gocalis/internal/node"
)

// ErrCallLeft is returned to an in-call speak, play or ask whose node went out
// of its intercom call before it was done.
var ErrCallLeft = errors.New("node left the intercom call")

// CallMode says what a speak or play request does when its node is in an
// intercom call, where the call owns the node's speaker.
type CallMode int

const (
	// CallReject fails the request with ErrIntercomActive.
	CallReject CallMode = iota
	// CallNode plays it in the call for that node alone, on top of what the
	// node hears from the call.
	CallNode
	// CallAll plays it in the call for every participant.
	CallAll
)

// CallLine is a node's line into the intercom call it is in: the way to reach
// the node, or its whole call, while the call owns the node's speaker and
// microphone. One thing at a time uses a node's line; the rest wait.
type CallLine interface {
	// Play mixes a clip (PCM16 at sampleRate) into the call, for the node alone
	// or for everyone in the call, and returns once it has played.
	Play(ctx context.Context, pcm []int16, sampleRate int, everyone bool) error

	// Hold sets the node aside from its call for a private exchange (an ask):
	// until the hold is released the others do not hear the node's microphone
	// and the node does not hear them.
	Hold(ctx context.Context) (CallHold, error)
}

// CallHold is a node set aside from its intercom call.
type CallHold interface {
	// Context ends when the hold is released, when the context it was taken on
	// ends, or (cause ErrCallLeft) when the node goes out of the call.
	Context() context.Context
	// Play plays a clip for the node alone and returns once it has played.
	Play(pcm []int16, sampleRate int) error
	// Release puts the node back in its call. It may be called more than once.
	Release()
}

// callSeat is what the intercom engine registers for a node in a call.
type callSeat struct {
	tap  func(samples []float32)
	line CallLine
}

// EnterCall registers a node as being in an intercom call. Until LeaveCall,
// every mic chunk delivered to ForwardRawAudio for that node is handed to tap
// (raw float32, model rate, NOT VAD-gated), which mixes it into the call, and
// line reaches the node inside the call.
func (b *Brain) EnterCall(nodeID string, tap func(samples []float32), line CallLine) {
	b.callsMu.Lock()
	defer b.callsMu.Unlock()
	b.calls[nodeID] = callSeat{tap: tap, line: line}
}

// LeaveCall removes the node's intercom call registration (if any).
func (b *Brain) LeaveCall(nodeID string) {
	b.callsMu.Lock()
	defer b.callsMu.Unlock()
	delete(b.calls, nodeID)
}

// CallLine returns the node's line into the intercom call it is in, or nil when
// it is in none.
func (b *Brain) CallLine(nodeID string) CallLine {
	b.callsMu.RLock()
	defer b.callsMu.RUnlock()
	// A missing seat yields the zero value, whose line is a nil interface.
	return b.calls[nodeID].line
}

// ForwardRawAudio delivers a raw mic chunk to the call the node is in, if any.
// It is called from the capture path for every chunk of a node in a call; with
// no call registered it is a cheap map lookup and returns immediately.
func (b *Brain) ForwardRawAudio(nodeID string, samples []float32) {
	b.callsMu.RLock()
	tap := b.calls[nodeID].tap
	b.callsMu.RUnlock()
	if tap != nil {
		tap(samples)
	}
}

// playInCall plays a clip in the intercom call the node is in, as mode says. It
// reports false when the request is not for the call: the mode rejects calls,
// or the node is in none.
func (b *Brain) playInCall(ctx context.Context, nodeID string, mode CallMode, clip func() ([]int16, int, error)) (bool, error) {
	if mode == CallReject {
		return false, nil
	}
	line := b.CallLine(nodeID)
	if line == nil {
		return false, nil
	}
	samples, rate, err := clip()
	if err != nil || len(samples) == 0 {
		return true, err
	}
	if err := line.Play(ctx, samples, rate, mode == CallAll); err != nil {
		return true, fmt.Errorf("node '%s': %w", nodeID, err)
	}
	return true, nil
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
