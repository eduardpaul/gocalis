package telegram

import (
	"context"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"gocalis/internal/audio"
	"gocalis/internal/audionode"
	"gocalis/internal/config"
)

// nodeRate is gocalis's internal model/bridge sample rate. Inbound call audio is
// downsampled to this before OnAudio; outbound audio is upsampled from it (or
// whatever the source reports) to TelegramRate before Write.
const nodeRate = 16000

// frameSamples is one 20ms outbound frame at TelegramRate (960 samples). The node
// paces Write calls at this cadence so playback runs in real time and Play/
// PlayStream block for the audio's duration, matching the AudioNode contract.
const frameSamples = TelegramRate / 50

// callState tracks the telegram node's call lifecycle.
type callState int

const (
	stateIdle    callState = iota // no call placed
	stateJoining                  // Join/WaitPeer in progress
	stateReady                    // a remote peer is present; media flows
)

// TelegramNode adapts a Telegram Call to the audionode.AudioNode +
// audionode.CallEndpoint contracts. It owns the call lifecycle: an autowake node
// joins on Connect and holds the call for the process lifetime; an on-demand node
// places the call on the first EnsureReady (driven by an ask/play/say/intercom)
// and leaves it after idleTimeout with no activity.
type TelegramNode struct {
	nodeID       string
	target       Target
	call         Call
	limiter      *Limiter
	autowake     bool
	readyTimeout time.Duration
	idleTimeout  time.Duration

	baseCtx    context.Context
	baseCancel context.CancelFunc

	onAudioMu sync.RWMutex
	onAudio   func([]float32)

	lastActivity int64 // unixnano, atomic; updated on any inbound/outbound frame

	mu         sync.Mutex
	state      callState
	readyWait  chan struct{} // non-nil while an establish attempt is in flight
	readyErr   error         // result of the last establish attempt
	sessCancel context.CancelFunc
	p2pHeld    bool
	closed     bool
}

var (
	_ audionode.AudioNode    = (*TelegramNode)(nil)
	_ audionode.CallEndpoint = (*TelegramNode)(nil)
)

// NewNode builds a telegram node for nodeCfg using the shared Manager (one login)
// and Limiter (serializes 1:1 calls). The call is created but not placed.
func NewNode(nodeID string, nodeCfg config.TelegramNodeConfig, mgr Manager, limiter *Limiter) (*TelegramNode, error) {
	if mgr == nil {
		return nil, fmt.Errorf("telegram node %q: nil manager", nodeID)
	}
	if limiter == nil {
		limiter = NewLimiter()
	}
	target := Target{Type: nodeCfg.TargetType, Peer: nodeCfg.Target}
	call, err := mgr.NewCall(target)
	if err != nil {
		return nil, fmt.Errorf("telegram node %q: %w", nodeID, err)
	}
	return &TelegramNode{
		nodeID:       nodeID,
		target:       target,
		call:         call,
		limiter:      limiter,
		autowake:     nodeCfg.AutoWake,
		readyTimeout: time.Duration(nodeCfg.GetReadyTimeoutSeconds() * float64(time.Second)),
		idleTimeout:  time.Duration(nodeCfg.GetIdleTimeoutSeconds() * float64(time.Second)),
		state:        stateIdle,
	}, nil
}

// Connect wires the inbound audio path and, for an autowake node, begins placing
// the call in the background so it is live (once a peer joins) without blocking
// startup. An on-demand node stays idle until the first EnsureReady.
func (n *TelegramNode) Connect(ctx context.Context) error {
	n.baseCtx, n.baseCancel = context.WithCancel(ctx)
	n.call.OnFrame(n.handleInbound)
	if n.autowake {
		go func() {
			if err := n.EnsureReady(n.baseCtx); err != nil && n.baseCtx.Err() == nil {
				log.Printf("[Telegram:%s] autowake call not ready: %v\n", n.nodeID, err)
			}
		}()
	}
	return nil
}

// EnsureReady blocks until the call has a remote peer, placing/joining it first
// when necessary. Concurrent callers coalesce onto a single establish attempt.
func (n *TelegramNode) EnsureReady(ctx context.Context) error {
	n.touch()
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return fmt.Errorf("telegram node %q is closed", n.nodeID)
	}
	if n.state == stateReady {
		n.mu.Unlock()
		return nil
	}
	if n.readyWait == nil {
		n.readyWait = make(chan struct{})
		n.state = stateJoining
		go n.establish()
	}
	wait := n.readyWait
	n.mu.Unlock()

	select {
	case <-wait:
		n.mu.Lock()
		err := n.readyErr
		n.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// establish drives one Join -> WaitPeer attempt under its own readiness deadline,
// rooted at baseCtx so it ends on Close. On success it marks the node ready and
// (for an on-demand node) starts the idle-teardown monitor. The result is
// published to any EnsureReady waiters via readyWait/readyErr.
func (n *TelegramNode) establish() {
	ctx, cancel := context.WithTimeout(n.baseCtx, n.readyTimeout)
	defer cancel()

	err := n.doEstablish(ctx)

	n.mu.Lock()
	n.readyErr = err
	if err != nil {
		n.state = stateIdle
	} else {
		n.state = stateReady
	}
	wait := n.readyWait
	n.readyWait = nil
	n.mu.Unlock()

	close(wait)
}

// doEstablish performs the actual join, serializing 1:1 calls through the limiter
// and cleaning up partial state on failure.
func (n *TelegramNode) doEstablish(ctx context.Context) error {
	if n.target.Type == "contact" {
		if err := n.limiter.AcquireP2P(ctx); err != nil {
			return fmt.Errorf("waiting for the single 1:1 call slot: %w", err)
		}
		n.mu.Lock()
		n.p2pHeld = true
		n.mu.Unlock()
	}

	if err := n.call.Join(ctx); err != nil {
		n.releaseP2P()
		return fmt.Errorf("joining call: %w", err)
	}
	if err := n.call.WaitPeer(ctx); err != nil {
		_ = n.call.Leave()
		n.releaseP2P()
		return fmt.Errorf("waiting for peer: %w", err)
	}

	// The call is live. Start a session context so the idle monitor (on-demand
	// only) and any teardown are scoped to this joined session.
	sessCtx, sessCancel := context.WithCancel(n.baseCtx)
	n.mu.Lock()
	n.sessCancel = sessCancel
	n.mu.Unlock()
	n.touch()
	if !n.autowake {
		go n.idleMonitor(sessCtx)
	}
	log.Printf("[Telegram:%s] call ready (%s %s)\n", n.nodeID, n.target.Type, n.target.Peer)
	return nil
}

// idleMonitor leaves an on-demand call after idleTimeout with no inbound or
// outbound audio, so a transient say/ask does not hold a phone line open.
func (n *TelegramNode) idleMonitor(ctx context.Context) {
	interval := n.idleTimeout / 2
	if interval < time.Second {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			last := time.Unix(0, atomic.LoadInt64(&n.lastActivity))
			if time.Since(last) >= n.idleTimeout {
				log.Printf("[Telegram:%s] leaving idle call after %s\n", n.nodeID, n.idleTimeout)
				n.leave()
				return
			}
		}
	}
}

// leave tears down the current call and resets to idle so a later EnsureReady can
// re-establish. It is safe to call repeatedly.
func (n *TelegramNode) leave() {
	n.mu.Lock()
	if n.state == stateIdle && n.sessCancel == nil {
		n.mu.Unlock()
		return
	}
	n.state = stateIdle
	cancel := n.sessCancel
	n.sessCancel = nil
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	_ = n.call.Leave()
	n.releaseP2P()
}

func (n *TelegramNode) releaseP2P() {
	n.mu.Lock()
	held := n.p2pHeld
	n.p2pHeld = false
	n.mu.Unlock()
	if held {
		n.limiter.ReleaseP2P()
	}
}

// Play sends a fixed PCM16 clip to the call, ensuring the call is live first and
// pacing the audio at real time so it blocks for the clip's duration.
func (n *TelegramNode) Play(ctx context.Context, pcm16 []int16, sampleRate int) error {
	if err := n.EnsureReady(ctx); err != nil {
		return err
	}
	pcm48 := audio.ResampleInt16(pcm16, sampleRate, TelegramRate)
	return n.writePaced(ctx, pcm48)
}

// PlayStream streams audio pulled incrementally from src to the call, ensuring
// the call is live first, resampling to TelegramRate and pacing at real time.
func (n *TelegramNode) PlayStream(ctx context.Context, src audionode.PCM16Source) error {
	if err := n.EnsureReady(ctx); err != nil {
		return err
	}
	srcRate := src.SampleRate()
	if srcRate <= 0 {
		srcRate = nodeRate
	}

	pacer := newPacer()
	var buf []int16
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunk, err := src.ReadPCM16(4096)
		if len(chunk) > 0 {
			buf = append(buf, audio.ResampleInt16(chunk, srcRate, TelegramRate)...)
			for len(buf) >= frameSamples {
				if werr := n.writeFramePaced(ctx, buf[:frameSamples], pacer); werr != nil {
					return werr
				}
				buf = buf[frameSamples:]
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	if len(buf) > 0 {
		frame := make([]int16, frameSamples)
		copy(frame, buf)
		if err := n.writeFramePaced(ctx, frame, pacer); err != nil {
			return err
		}
	}
	return nil
}

// writePaced splits pcm48 into real-time-paced frames and writes them.
func (n *TelegramNode) writePaced(ctx context.Context, pcm48 []int16) error {
	pacer := newPacer()
	for i := 0; i < len(pcm48); i += frameSamples {
		end := i + frameSamples
		frame := make([]int16, frameSamples)
		if end > len(pcm48) {
			copy(frame, pcm48[i:])
		} else {
			copy(frame, pcm48[i:end])
		}
		if err := n.writeFramePaced(ctx, frame, pacer); err != nil {
			return err
		}
	}
	return nil
}

// writeFramePaced writes exactly one TelegramRate frame and waits until its
// real-time slot so the aggregate write rate tracks wall-clock playback.
func (n *TelegramNode) writeFramePaced(ctx context.Context, frame []int16, p *pacer) error {
	if err := p.wait(ctx); err != nil {
		return err
	}
	n.touch()
	return n.call.Write(frame)
}

// handleInbound converts one inbound TelegramRate frame to gocalis's 16kHz float
// mic format and delivers it to the OnAudio sink (VAD/wake/capture path).
func (n *TelegramNode) handleInbound(pcm48 []int16) {
	if len(pcm48) == 0 {
		return
	}
	n.touch()
	pcm16 := audio.ResampleInt16(pcm48, TelegramRate, nodeRate)
	samples := audio.PCM16ToFloat(pcm16)
	n.onAudioMu.RLock()
	cb := n.onAudio
	n.onAudioMu.RUnlock()
	if cb != nil {
		cb(samples)
	}
}

// OnAudio registers the microphone sink for received call audio.
func (n *TelegramNode) OnAudio(callback func(samples []float32)) {
	n.onAudioMu.Lock()
	n.onAudio = callback
	n.onAudioMu.Unlock()
}

// Close ends any active call and prevents further use.
func (n *TelegramNode) Close() error {
	n.mu.Lock()
	if n.closed {
		n.mu.Unlock()
		return nil
	}
	n.closed = true
	n.mu.Unlock()

	if n.baseCancel != nil {
		n.baseCancel()
	}
	n.leave()
	return nil
}

// touch records activity so the idle monitor keeps an on-demand call alive while
// audio is flowing in either direction.
func (n *TelegramNode) touch() {
	atomic.StoreInt64(&n.lastActivity, time.Now().UnixNano())
}

// pacer releases one frame slot every 10ms of wall-clock time relative to its
// creation, so a producer that can deliver audio faster than real time is held
// back to the call's playback rate.
type pacer struct {
	start time.Time
	n     int
}

func newPacer() *pacer { return &pacer{start: time.Now()} }

func (p *pacer) wait(ctx context.Context) error {
	target := time.Duration(p.n) * 20 * time.Millisecond
	p.n++
	d := target - time.Since(p.start)
	if d <= 0 {
		return nil
	}
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
