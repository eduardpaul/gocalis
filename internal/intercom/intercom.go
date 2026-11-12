// Package intercom implements live audio calls through gocalis's nodes.
//
// A call has node participants (registered audio nodes: the doorbell, a local
// speakerphone) and optionally remote talkers (the intercom web page's
// microphone, streamed to gocalis). Every node participant is taken over for the
// whole call with brain.AcquireNodeIntercom, which preempts the TTS or ask turn
// in progress and rejects any other turn until the call ends: the call's mix is
// the only audio sent to that node's speaker, through the node's normal output
// path (for the doorbell, the one WHIP talkback track and AAC-ELD bridge). Each
// listener hears a mix of every OTHER talker (mix-minus), so no one hears
// themselves. A call always ends by a deadline (config default or per-request
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

// bridgeRate is the PCM sample rate carried between nodes. It matches the
// model/capture rate produced by the transports' OnAudio path.
const bridgeRate = 16000

// drainGrace bounds how long teardown waits for each node's StreamOut to play
// out its buffered tail after the mixer closes the outputs, before cancelling.
const drainGrace = 3 * time.Second

// EchoCanceller removes the acoustic echo of a node's own speaker output from
// its microphone. Playback feeds the far-end reference (what is being played on
// the node); Capture returns the near-end mic with that echo removed. It is fed
// from two goroutines (playback vs. capture) and must tolerate that.
type EchoCanceller interface {
	Playback(farPCM []int16)
	Capture(nearPCM []int16) []int16
	Close()
}

// remotePrimeMs is the jitter cushion held for a remote talker's microphone
// before its audio is mixed (and rebuilt after an underrun).
const remotePrimeMs = 60

// remoteMaxMs bounds a remote talker's buffered audio (drop-oldest), so a
// network stall followed by a burst cannot build up lasting latency.
const remoteMaxMs = 250

// Remote is a talker outside gocalis: the microphone of the intercom web page.
// It only talks; the page plays the doorbell straight from go2rtc, so gocalis
// sends it nothing. The page's transport pushes 16 kHz mono PCM16 with Push.
type Remote struct {
	ID  string
	mic *bridge
}

// RemoteSampleRate is the PCM16 rate a Remote expects in Push.
const RemoteSampleRate = bridgeRate

// Push queues microphone audio (16 kHz mono PCM16) from the remote talker.
func (r *Remote) Push(pcm []int16) { r.mic.push(pcm) }

// Event is an intercom lifecycle notification emitted to the transport layer.
// It is transport-neutral so this package does not depend on protocol.
type Event struct {
	Event           string   // "intercom_started" | "intercom_ended"
	NodeIDs         []string // all node participants
	RemoteIDs       []string // remote talkers (intercom page microphones)
	NodeID          string   // first node participant (compatibility with 2-node clients)
	PeerNodeID      string   // second node participant, if any (compatibility with 2-node clients)
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

	mu        sync.Mutex
	calls     map[string]*call // participant id -> active call (every participant maps to it)
	remoteSeq int
}

// call is a single in-progress intercom session.
type call struct {
	nodes   []string
	remotes []*Remote
	started time.Time
	done    chan struct{} // closed once the call has ended and its event is out
	ended   Event
	// ctx/cancel are created in Start, before the call is registered, so Stop can
	// always cancel it (even before run has been scheduled).
	ctx    context.Context
	cancel context.CancelFunc

	mu      sync.Mutex
	reason  string // set by whoever ends the call before cancelling
	failure string // first stream error, reported as the ended event's message
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
	uniq, err := validateNodes(nodes, 2)
	if err != nil {
		return err
	}
	_, err = e.start(uniq, nil, duration)
	return err
}

// NewRemote creates a remote talker (an intercom page microphone) with an id
// unique within this engine, ready to be passed to StartRemote.
func (e *Engine) NewRemote() *Remote {
	e.mu.Lock()
	e.remoteSeq++
	id := fmt.Sprintf("web-%d", e.remoteSeq)
	e.mu.Unlock()
	mic := newBridgeBuffer(bridgeRate, bridgeRate*remoteMaxMs/1000)
	mic.prime = bridgeRate * remotePrimeMs / 1000
	return &Remote{ID: id, mic: mic}
}

// StartRemote begins a call in which remote talks to the given nodes (usually a
// single doorbell). The nodes are taken over exactly like in Start; their own
// microphones are only bridged when another node participant can hear them.
// The returned Call reports the end and lets the remote's transport stop it.
func (e *Engine) StartRemote(remote *Remote, nodes []string, duration time.Duration) (*Call, error) {
	if remote == nil {
		return nil, fmt.Errorf("intercom: no remote talker")
	}
	uniq, err := validateNodes(nodes, 1)
	if err != nil {
		return nil, err
	}
	c, err := e.start(uniq, []*Remote{remote}, duration)
	if err != nil {
		return nil, err
	}
	return &Call{c: c}, nil
}

// Call is a handle on a running call, for the transport that started it.
type Call struct{ c *call }

// Done is closed once the call has ended and its "intercom_ended" event has been
// emitted.
func (h *Call) Done() <-chan struct{} { return h.c.done }

// Ended returns the call's "intercom_ended" event; valid after Done is closed.
func (h *Call) Ended() Event {
	<-h.c.done
	return h.c.ended
}

// Stop ends the call (reason "stopped") if it is still running.
func (h *Call) Stop() {
	h.c.setReasonIfEmpty("stopped")
	h.c.cancel()
}

func (e *Engine) start(uniq []string, remotes []*Remote, duration time.Duration) (*call, error) {
	if err := e.baseCtx.Err(); err != nil {
		return nil, fmt.Errorf("intercom engine is shutting down")
	}
	for _, n := range uniq {
		if e.brain.GetNodeHandle(n) == nil {
			return nil, fmt.Errorf("node '%s' not registered", n)
		}
	}
	for _, r := range remotes {
		if e.brain.GetNodeHandle(r.ID) != nil {
			return nil, fmt.Errorf("remote talker id '%s' clashes with a node", r.ID)
		}
	}
	if duration <= 0 {
		duration = time.Duration(e.cfg.GetDefaultTimeoutSeconds() * float64(time.Second))
	}

	e.mu.Lock()
	ids := append([]string(nil), uniq...)
	for _, r := range remotes {
		ids = append(ids, r.ID)
	}
	for _, id := range ids {
		if c, ok := e.calls[id]; ok {
			e.mu.Unlock()
			return nil, fmt.Errorf("node '%s' is already in an intercom call (%s)", id, strings.Join(c.participantIDs(), ", "))
		}
	}
	ctx, cancel := context.WithCancel(e.baseCtx)
	c := &call{nodes: uniq, remotes: remotes, started: time.Now(), ctx: ctx, cancel: cancel, done: make(chan struct{})}
	for _, id := range ids {
		e.calls[id] = c
	}
	e.mu.Unlock()

	e.emit(c.event("intercom_started", "success", "", "", 0))

	go e.run(c, duration)
	return c, nil
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
	ctx, cancel := c.ctx, c.cancel
	defer cancel()

	// Acquire all node slots in a deterministic (sorted) order so overlapping
	// calls can never deadlock acquiring each other's nodes.
	order := append([]string(nil), c.nodes...)
	sort.Strings(order)
	turns := make(map[string]*brain.Turn, len(order))
	releaseAll := func() {
		for i := len(order) - 1; i >= 0; i-- {
			if t := turns[order[i]]; t != nil {
				t.Release()
			}
		}
	}
	for _, n := range order {
		t, err := e.brain.AcquireNodeIntercom(ctx, n)
		if err != nil {
			releaseAll()
			if ctx.Err() != nil {
				// Stopped or shut down while still waiting for a node slot: that is
				// a normal end, not an acquisition failure.
				c.setReasonIfEmpty("shutdown")
				e.finish(c, "success", "")
				return
			}
			e.finish(c, "error", "could not acquire node '"+n+"': "+err.Error())
			return
		}
		turns[n] = t
	}

	// A node's microphone is bridged only when another node participant can
	// hear it. In a call between the intercom page and the doorbell nobody in
	// gocalis listens to the doorbell (the page plays it from go2rtc), so its mic
	// is not tapped at all; the runtime drops it while the node is INTERCOM.
	tapped := c.nodes
	if len(c.nodes) < 2 {
		tapped = nil
	}

	// Per-node echo cancellers (optional). Each node's canceller removes the echo
	// of what that node plays (its mix-minus output) from its mic. When echo
	// cancellation is disabled in config, buildCancellers returns nil and nothing
	// below touches the audio: mics are mixed verbatim.
	aec := e.buildCancellers(tapped)
	defer func() {
		for _, cc := range aec {
			cc.Close()
		}
	}()
	if aec != nil {
		log.Printf("[Intercom] %s started (echo cancellation ON)\n", strings.Join(c.participantIDs(), ", "))
	} else {
		log.Printf("[Intercom] %s started (echo cancellation OFF)\n", strings.Join(c.participantIDs(), ", "))
	}

	inputs := make(map[string]*bridge, len(tapped)+len(c.remotes))
	for _, n := range tapped {
		inputs[n] = newBridgeBuffer(bridgeRate, bridgeRate/2)
	}
	for _, r := range c.remotes {
		inputs[r.ID] = r.mic
	}
	mix := newMixer(bridgeRate, inputs, c.nodes, bridgeRate/2)

	// Install a mic tap per bridged node: mic -> (echo-cancel) -> its mixer input.
	for _, n := range tapped {
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
		turn := turns[node]
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := turn.StreamOut(turn.Context(), out, far); err != nil && ctx.Err() == nil {
				// A genuine stream failure (not a side effect of stop/shutdown/
				// drain cancellation) ends the call as an error.
				c.setFailure(err.Error())
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
	for _, n := range tapped {
		e.brain.ClearRawAudioTap(n)
	}
	mix.stop()
	// Bound the drain: a StreamOut blocked on something other than its source
	// (e.g. an rtc_stream node still waiting to connect) only unwinds on ctx, so
	// cancel after a grace period instead of holding every node slot forever.
	streamsDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(streamsDone)
	}()
	select {
	case <-streamsDone:
	case <-time.After(drainGrace):
		cancel()
		<-streamsDone
	}
	releaseAll()

	if msg := c.getFailure(); msg != "" {
		e.finish(c, "error", msg)
		return
	}
	e.finish(c, "success", "")
}

// buildCancellers builds one echo canceller per node, or nil when AEC is
// disabled/unavailable or no node is bridged. A construction failure degrades the whole call to no AEC
// rather than failing it.
func (e *Engine) buildCancellers(nodes []string) map[string]EchoCanceller {
	if !e.cfg.EchoCancellation.Enabled || e.newAEC == nil || len(nodes) == 0 {
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
	for _, id := range c.participantIDs() {
		if e.calls[id] == c {
			delete(e.calls, id)
		}
	}
	e.mu.Unlock()

	reason := c.getReason()
	if status == "error" && reason == "" {
		reason = "error"
	}
	c.ended = c.event("intercom_ended", status, reason, message, time.Since(c.started).Seconds())
	e.emit(c.ended)
	close(c.done)
}

// event builds a lifecycle event for the call.
func (c *call) event(name, status, reason, message string, duration float64) Event {
	ev := Event{
		Event:           name,
		NodeIDs:         c.nodes,
		NodeID:          c.nodes[0],
		Status:          status,
		Reason:          reason,
		Message:         message,
		DurationSeconds: duration,
	}
	if len(c.nodes) > 1 {
		ev.PeerNodeID = c.nodes[1]
	}
	for _, r := range c.remotes {
		ev.RemoteIDs = append(ev.RemoteIDs, r.ID)
	}
	return ev
}

// participantIDs lists the ids the call is registered under: its nodes, then
// its remote talkers.
func (c *call) participantIDs() []string {
	ids := append([]string(nil), c.nodes...)
	for _, r := range c.remotes {
		ids = append(ids, r.ID)
	}
	return ids
}

// validateNodes de-duplicates and validates a node list: it requires at least
// min distinct, non-empty node ids and rejects repeats.
func validateNodes(nodes []string, min int) ([]string, error) {
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
	if len(uniq) < min {
		if min == 1 {
			return nil, fmt.Errorf("intercom requires a node to talk to")
		}
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

// setFailure records a stream error: the first one wins and marks the call as
// ended by "error" unless an explicit reason (e.g. "stopped") was already set.
func (c *call) setFailure(msg string) {
	c.mu.Lock()
	if c.reason == "" {
		c.reason = "error"
	}
	if c.failure == "" {
		c.failure = msg
	}
	c.mu.Unlock()
}

func (c *call) getFailure() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.failure
}

func (c *call) getReason() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reason
}
