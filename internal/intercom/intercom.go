// Package intercom implements a live N-node audio bridge: it connects two or
// more registered audio nodes so people at each device can talk to one another.
// Each participant hears a mix of every OTHER participant (mix-minus), so no one
// hears themselves. For the duration of a call every participant's turn slot is
// held on the brain, so any tts/ask/play targeting a participant queues until
// the call ends. A call always ends by a deadline (config default or per-request
// override) and can also be ended early by an external stop. The end is
// announced as an event.
package intercom

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"gocalis/internal/audio"
	"gocalis/internal/brain"
	"gocalis/internal/config"
)

// intercomPriority is the turn-queue priority used to hold each participating
// node for the whole call. It is above the wake/ask priority (10) so a call
// preempts queued speak/ask work; while held, the slot blocks all of them.
const intercomPriority = 100

// bridgeRate is the PCM sample rate carried between nodes. It matches the
// model/capture rate produced by the transports' OnAudio path.
const bridgeRate = 16000

// EchoCanceller removes the acoustic echo of a node's own speaker output from
// its microphone. Playback feeds the far-end reference (what is being played on
// the node); Capture returns the near-end mic with that echo removed. It is fed
// from two goroutines (playback vs. capture) and must tolerate that.
type EchoCanceller interface {
	Playback(farPCM []int16)
	Capture(nearPCM []int16) []int16
	Close()
}

// Event is an intercom lifecycle notification emitted to the transport layer.
// It is transport-neutral so this package does not depend on protocol.
type Event struct {
	Event           string   // "intercom_started" | "intercom_ended"
	NodeIDs         []string // all participants
	NodeID          string   // first participant (compatibility with 2-node clients)
	PeerNodeID      string   // second participant (compatibility with 2-node clients)
	Status          string   // "success" | "error"
	Reason          string   // end reason: "timeout" | "stopped" | "shutdown" | "error"
	Message         string   // error detail, when Status == "error"
	DurationSeconds float64  // elapsed call time, on "intercom_ended"
}

// Engine coordinates intercom calls against the central brain.
type Engine struct {
	brain *brain.Brain
	cfg   config.IntercomConfig
	emit  func(Event)

	// newAEC builds a per-node echo canceller. When nil (or AEC disabled in
	// config), mics are mixed verbatim. Injected so the cgo speex backend is
	// pluggable and tests can substitute a fake or none.
	newAEC func(rate, frameSamples, tailSamples int) (EchoCanceller, error)

	// baseCtx roots every call so calls survive the short-lived per-request
	// context of the transport that started them, and all end together on
	// Shutdown. baseCancel cancels it.
	baseCtx    context.Context
	baseCancel context.CancelFunc

	mu    sync.Mutex
	calls map[string]*call // nodeID -> active call (every participant maps to it)
}

// call is a single in-progress intercom session.
type call struct {
	nodes   []string
	started time.Time
	cancel  context.CancelFunc

	mu     sync.Mutex
	reason string // set by whoever ends the call before cancelling
}

// NewEngine creates an intercom engine backed by the brain, emitting lifecycle
// events through emit. newAEC may be nil to disable software echo cancellation.
func NewEngine(b *brain.Brain, cfg config.IntercomConfig, emit func(Event), newAEC func(rate, frameSamples, tailSamples int) (EchoCanceller, error)) *Engine {
	baseCtx, baseCancel := context.WithCancel(context.Background())
	return &Engine{
		brain:      b,
		cfg:        cfg,
		emit:       emit,
		newAEC:     newAEC,
		baseCtx:    baseCtx,
		baseCancel: baseCancel,
		calls:      make(map[string]*call),
	}
}

// Shutdown ends every active call (reason "shutdown") and prevents new ones.
func (e *Engine) Shutdown() { e.baseCancel() }

// Start begins an intercom call among the given nodes (two or more, distinct).
// duration bounds the call; when <= 0 the configured default is used. It returns
// immediately: the call runs in the background and its end is reported via an
// "intercom_ended" event. An error is returned only for invalid/rejected
// requests (fewer than two distinct nodes, an unknown node, or a node already in
// a call).
func (e *Engine) Start(nodes []string, duration time.Duration) error {
	if err := e.baseCtx.Err(); err != nil {
		return fmt.Errorf("intercom engine is shutting down")
	}
	uniq, err := validateNodes(nodes)
	if err != nil {
		return err
	}
	for _, n := range uniq {
		if e.brain.GetNodeHandle(n) == nil {
			return fmt.Errorf("node '%s' not registered", n)
		}
	}
	if duration <= 0 {
		duration = time.Duration(e.cfg.GetDefaultTimeoutSeconds() * float64(time.Second))
	}

	e.mu.Lock()
	for _, n := range uniq {
		if c, ok := e.calls[n]; ok {
			e.mu.Unlock()
			return fmt.Errorf("node '%s' is already in an intercom call (%s)", n, strings.Join(c.nodes, ", "))
		}
	}
	c := &call{nodes: uniq, started: time.Now()}
	for _, n := range uniq {
		e.calls[n] = c
	}
	e.mu.Unlock()

	// The "intercom_started" event is emitted inside run(), once every node is
	// ready — for a call-based node (Telegram) that means a remote peer has
	// actually joined. This is what makes an intercom to a group only begin once
	// one other user is on the call, rather than the moment it is requested.
	go e.run(c, duration)
	return nil
}

// Stop ends the active call that nodeID participates in, if any. It ends the
// whole call (all participants). It returns true when a call was found.
func (e *Engine) Stop(nodeID string) bool {
	e.mu.Lock()
	c := e.calls[nodeID]
	e.mu.Unlock()
	if c == nil {
		return false
	}
	c.setReason("stopped")
	c.cancel()
	return true
}

// run drives a call end to end: acquire every node slot (the gate), wire the
// mix-minus mixer across all participants, wait for the deadline or an early
// stop, then tear everything down and announce the end.
func (e *Engine) run(c *call, duration time.Duration) {
	ctx, cancel := context.WithCancel(e.baseCtx)
	c.cancel = cancel
	defer cancel()

	// Acquire all node slots in a deterministic (sorted) order so overlapping
	// calls can never deadlock acquiring each other's nodes.
	order := append([]string(nil), c.nodes...)
	sort.Strings(order)
	var releases []func()
	releaseAll := func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
	for _, n := range order {
		rel, err := e.brain.AcquireNode(ctx, n, intercomPriority)
		if err != nil {
			releaseAll()
			e.finish(c, "error", "could not acquire node '"+n+"': "+err.Error())
			return
		}
		releases = append(releases, rel)
	}

	// Establish readiness for every participant before the call is considered
	// live. For always-available transports this is instant; for a call-based
	// node (Telegram) EnsureNodeReady places/joins the call and blocks until a
	// remote peer is present (or its readiness deadline elapses). Only then is the
	// bridge wired and the "intercom_started" event emitted, so a group intercom
	// begins exactly when one other user has joined. Ordered by the same sorted
	// slot-acquisition order for determinism.
	started := time.Now()
	for _, n := range order {
		if err := e.brain.EnsureNodeReady(ctx, n); err != nil {
			releaseAll()
			e.finish(c, "error", "node '"+n+"' not ready: "+err.Error())
			return
		}
	}
	// Reset the call clock so the bounded duration is measured from when the call
	// actually went live (peer joined), not from when it was requested. Only the
	// run goroutine touches c.started after Start, and finish() runs in this same
	// goroutine, so no lock is needed.
	c.started = started

	peer := ""
	if len(c.nodes) > 1 {
		peer = c.nodes[1]
	}
	e.emit(Event{
		Event:      "intercom_started",
		NodeIDs:    c.nodes,
		NodeID:     c.nodes[0],
		PeerNodeID: peer,
		Status:     "success",
	})

	// Per-node echo cancellers (optional). Each node's canceller removes the echo
	// of what that node plays (its mix-minus output) from its mic. When echo
	// cancellation is disabled in config, buildCancellers returns nil and nothing
	// below touches the audio: mics are mixed verbatim.
	aec := e.buildCancellers(c.nodes)
	defer func() {
		for _, cc := range aec {
			cc.Close()
		}
	}()
	if aec != nil {
		log.Printf("[Intercom] %s started (echo cancellation ON)\n", strings.Join(c.nodes, ", "))
	} else {
		log.Printf("[Intercom] %s started (echo cancellation OFF)\n", strings.Join(c.nodes, ", "))
	}

	mix := newMixer(bridgeRate, c.nodes, bridgeRate/2)

	// Install a mic tap per node: mic -> (echo-cancel) -> that node's mixer input.
	for _, n := range c.nodes {
		node := n
		var ac EchoCanceller
		if aec != nil {
			ac = aec[node]
		}
		in := mix.input(node)
		e.brain.SetRawAudioTap(node, func(samples []float32) {
			pcm := audio.FloatToPCM16(samples)
			if ac != nil {
				pcm = ac.Capture(pcm)
			}
			in.push(pcm)
		})
	}

	// Start mixing, then stream each node's mix-minus output to its speaker. The
	// onPlayed hook feeds the played mix as that node's echo-canceller far-end.
	go mix.run()

	var wg sync.WaitGroup
	for _, n := range c.nodes {
		node := n
		var far func(pcm []int16)
		if aec != nil {
			far = aec[node].Playback
		}
		out := mix.output(node)
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := e.brain.StreamOut(ctx, node, out, far); err != nil {
				c.setReasonIfEmpty("error")
				cancel()
			}
		}()
	}

	// Wait for the deadline, an external stop, or process shutdown.
	timer := time.NewTimer(duration)
	select {
	case <-timer.C:
		c.setReasonIfEmpty("timeout")
	case <-ctx.Done():
		// Stop() set "stopped"; otherwise the parent context was cancelled.
		c.setReasonIfEmpty("shutdown")
	}
	timer.Stop()

	// Tear down: stop feeding mics, then stop the mixer, which closes each output
	// so its StreamOut reads io.EOF and drains cleanly (on a timeout this lets the
	// device play out its tail instead of being cut off mid-frame). Wait for the
	// stream goroutines to unwind, then release every node slot so queued
	// speak/ask work can proceed. The deferred cancel() then tears down the
	// context; on stop/shutdown it is already cancelled, which also unblocks
	// StreamOut.
	for _, n := range c.nodes {
		e.brain.ClearRawAudioTap(n)
	}
	mix.stop()
	wg.Wait()
	releaseAll()

	e.finish(c, "success", "")
}

// buildCancellers builds one echo canceller per node, or nil when AEC is
// disabled/unavailable. A construction failure degrades the whole call to no AEC
// rather than failing it.
func (e *Engine) buildCancellers(nodes []string) map[string]EchoCanceller {
	if !e.cfg.EchoCancellation.Enabled || e.newAEC == nil {
		return nil
	}
	frame := bridgeRate * e.cfg.EchoCancellation.GetFrameMs() / 1000
	tail := bridgeRate * e.cfg.EchoCancellation.GetTailMs() / 1000
	m := make(map[string]EchoCanceller, len(nodes))
	for _, n := range nodes {
		c, err := e.newAEC(bridgeRate, frame, tail)
		if err != nil {
			for _, cc := range m {
				cc.Close()
			}
			return nil
		}
		m[n] = c
	}
	return m
}

// finish removes the call from the registry and emits the ended event.
func (e *Engine) finish(c *call, status, message string) {
	e.mu.Lock()
	for _, n := range c.nodes {
		if e.calls[n] == c {
			delete(e.calls, n)
		}
	}
	e.mu.Unlock()

	reason := c.getReason()
	if status == "error" && reason == "" {
		reason = "error"
	}
	peer := ""
	if len(c.nodes) > 1 {
		peer = c.nodes[1]
	}
	e.emit(Event{
		Event:           "intercom_ended",
		NodeIDs:         c.nodes,
		NodeID:          c.nodes[0],
		PeerNodeID:      peer,
		Status:          status,
		Reason:          reason,
		Message:         message,
		DurationSeconds: time.Since(c.started).Seconds(),
	})
}

// validateNodes de-duplicates and validates a participant list: it requires at
// least two distinct, non-empty node ids and rejects repeats.
func validateNodes(nodes []string) ([]string, error) {
	seen := make(map[string]bool, len(nodes))
	uniq := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if n == "" {
			continue
		}
		if seen[n] {
			return nil, fmt.Errorf("intercom node '%s' listed more than once", n)
		}
		seen[n] = true
		uniq = append(uniq, n)
	}
	if len(uniq) < 2 {
		return nil, fmt.Errorf("intercom requires at least two distinct nodes")
	}
	return uniq, nil
}

func (c *call) setReason(r string) {
	c.mu.Lock()
	c.reason = r
	c.mu.Unlock()
}

func (c *call) setReasonIfEmpty(r string) {
	c.mu.Lock()
	if c.reason == "" {
		c.reason = r
	}
	c.mu.Unlock()
}

func (c *call) getReason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}
