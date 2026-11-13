// Package intercom implements live audio calls through gocalis's nodes.
//
// A call is a conference its participants join and leave while it runs. A
// participant is either a node (a registered audio node: the doorbell, a local
// speakerphone) or a remote (an intercom web page, which streams its microphone
// to gocalis and plays the mix gocalis sends back). Every node is taken over for
// as long as it is in the call with brain.AcquireNodeIntercom, which preempts
// the TTS or ask turn in progress and rejects any other turn until the node
// leaves: the call's mix is the only audio sent to that node's speaker, through
// the node's normal output path (for the doorbell, the one WHIP talkback track
// and AAC-ELD bridge). Each participant hears a mix of every OTHER participant
// (mix-minus), so no one hears themselves. A bell chime tells the call that
// someone joined or left.
//
// A call ends when fewer than two participants remain, by its deadline (pushed
// back whenever someone joins), or when it is stopped. Every step of its life is
// announced as an event.
package intercom

import (
	"context"
	"errors"
	"fmt"
	"log"
	"slices"
	"strings"
	"sync"
	"time"

	"gocalis/internal/audio"
	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
)

// MaxParticipants bounds one call (nodes plus remotes); each participant costs a
// mixer input/output pair and a playback goroutine.
const MaxParticipants = 16

// bridgeRate is the PCM sample rate the call is mixed at. It matches the
// model/capture rate produced by the transports' OnAudio path.
const bridgeRate = 16000

// drainGrace bounds how long a node that leaves gets to play out its buffered
// tail before its stream is cancelled.
const drainGrace = 3 * time.Second

// remotePrimeMs is the jitter cushion held for a remote's microphone before its
// audio is mixed (and rebuilt after an underrun).
const remotePrimeMs = 60

// remoteMaxMs bounds a remote's buffered audio in each direction (drop-oldest),
// so a network stall followed by a burst cannot build up lasting latency.
const remoteMaxMs = 250

// EchoCanceller removes the acoustic echo of a node's own speaker output from
// its microphone. Playback feeds the far-end reference (what is being played on
// the node); Capture returns the near-end mic with that echo removed. It is fed
// from two goroutines (playback vs. capture) and must tolerate that.
type EchoCanceller interface {
	Playback(farPCM []int16)
	Capture(nearPCM []int16) []int16
	Close()
}

// RemoteSampleRate is the PCM16 rate of a Remote's audio, in both directions.
const RemoteSampleRate = bridgeRate

// Remote is a participant outside gocalis: an intercom web page. Its transport
// pushes the page's microphone with Push and sends the page what Read returns.
type Remote struct {
	ID string

	notify func(Event)
	mic    *bridge
	out    *bridge
	left   chan struct{}

	// plays is the node this page plays by itself (from the go2rtc viewer it
	// embeds), which gocalis therefore leaves out of what it sends the page.
	plays string
}

// Push queues microphone audio (16 kHz mono PCM16) from the page.
func (r *Remote) Push(pcm []int16) { r.mic.push(pcm) }

// Read returns the next piece of the call's mix for the page (16 kHz mono
// PCM16), blocking until there is some. It returns io.EOF once the page is out
// of the call and everything meant for it has been read.
func (r *Remote) Read(ctx context.Context) ([]int16, error) {
	return r.out.ReadPCM16(ctx, bridgeRate*mixFrameMs/1000)
}

// Left is closed once the page is out of the call (it left, or the call ended)
// and the last event about that has been delivered to its notify callback.
func (r *Remote) Left() <-chan struct{} { return r.left }

// Event is an intercom lifecycle notification emitted to the transport layer.
// It is transport-neutral so this package does not depend on protocol.
type Event struct {
	Event            string   // "intercom_ringing" | "intercom_started" | "intercom_joined" | "intercom_left" | "intercom_ended"
	ParticipantID    string   // who is rung to join a running call, joined or left (a node id or a remote id)
	NodeIDs          []string // nodes in the call after the event, in join order
	RemoteIDs        []string // remotes in the call after the event, in join order
	Status           string   // "success" | "error"
	Reason           string   // on "intercom_ended": "timeout" | "stopped" | "left" | "unanswered" | "shutdown" | "error"; on "intercom_left": "unanswered" | "hung_up", or empty
	Message          string   // error detail, when Status == "error"
	DurationSeconds  float64  // on "intercom_ended": how long the call lasted
	RemainingSeconds float64  // otherwise: time left until the call's deadline
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

	// joinChime and leaveChime are what a call plays when someone joins or
	// leaves (PCM16 at bridgeRate).
	joinChime, leaveChime []int16

	// baseCtx roots every node takeover so calls survive the short-lived
	// per-request context of the transport that started them, and all end
	// together on Shutdown. baseCancel cancels it.
	baseCtx    context.Context
	baseCancel context.CancelFunc

	// mu guards the registry and the state of every call.
	mu        sync.Mutex
	calls     map[string]*call // participant id -> the call it is in
	remoteSeq int

	// outbox holds the notifications still to deliver. They are queued under mu
	// (so their order is the order things happened in) and delivered outside it
	// by a single drain goroutine.
	outbox   []func()
	draining bool
}

// call is one conference. Its fields are guarded by Engine.mu, and its methods
// expect it held unless they say otherwise.
type call struct {
	e       *Engine
	started time.Time
	mix     *mixer
	over    chan struct{} // closed when the call has to end

	nodes    []*nodeMember // in join order
	remotes  []*Remote     // in join order
	duration time.Duration // how long the call was asked to last
	deadline time.Time
	waiting  bool   // it has not started: a node somebody has to answer is being rung
	live     bool   // the initial participants are in the mix; later ones join on their own
	ending   bool   // the call is over: nobody can join any more
	reason   string // why it is ending
	failure  string // the error that ended it, if one did
}

// nodeMember is a node's place in a call.
type nodeMember struct {
	id string

	// ctx bounds the node's takeover and playback; cancel cuts both short.
	ctx    context.Context
	cancel context.CancelFunc

	// seat ends (cause brain.ErrCallLeft) when the node goes out of the mix,
	// which is when whatever the assistant was doing with it has to stop.
	seat   context.Context
	unseat context.CancelCauseFunc

	turn     *brain.Turn   // the takeover, once the node has been acquired
	ringing  bool          // it is rung to join a call the others are already in
	out      *bridge       // the node's mix, once it is in the mix
	aec      EchoCanceller // its echo canceller, if enabled
	gone     bool          // it left the call
	streamed chan struct{} // closed once its playback is over and the node is free
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
		joinChime:  joinChime,
		leaveChime: leaveChime,
		baseCtx:    baseCtx,
		baseCancel: baseCancel,
		calls:      make(map[string]*call),
	}
}

// Shutdown ends every active call (reason "shutdown") and prevents new ones.
func (e *Engine) Shutdown() { e.baseCancel() }

// Connect puts the given nodes (two or more, distinct) in a call together. When
// none of them is in a call, a new one starts ("intercom_started"). When some
// already are in one, the others join it ("intercom_joined" for each). duration
// is how long the call lasts from now at least; when <= 0 the configured default
// is used. It returns at once: the call runs in the background.
//
// A node somebody has to answer is rung first. A new call with one only starts
// once they answered: "intercom_ringing" now, "intercom_started" then, and the
// other nodes are left alone until then. Nobody answering takes that node out
// ("intercom_left", reason "unanswered"), which ends the call ("intercom_ended",
// reason "unanswered") unless two others remain. Joining a running call, such a
// node is announced with "intercom_ringing" and "intercom_joined" once
// answered.
func (e *Engine) Connect(nodes []string, duration time.Duration) error {
	uniq, err := distinctNodes(nodes)
	if err != nil {
		return err
	}
	if len(uniq) < 2 {
		return fmt.Errorf("intercom requires at least two distinct nodes")
	}
	if len(uniq) > MaxParticipants {
		return fmt.Errorf("intercom supports at most %d participants", MaxParticipants)
	}
	for _, n := range uniq {
		if e.brain.GetNodeHandle(n) == nil {
			return fmt.Errorf("node '%s' not registered", n)
		}
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.baseCtx.Err() != nil {
		return fmt.Errorf("intercom engine is shutting down")
	}
	var c *call
	var joiners []string
	for _, n := range uniq {
		switch in := e.calls[n]; {
		case in == nil:
			joiners = append(joiners, n)
		case c != nil && in != c:
			return fmt.Errorf("nodes are in different intercom calls")
		default:
			c = in
		}
	}
	if c == nil {
		c = e.newCall(duration)
		for _, n := range uniq {
			c.addNode(n)
		}
		c.open()
		go c.run()
		return nil
	}
	if len(joiners) == 0 {
		return fmt.Errorf("nodes are already in the same intercom call")
	}
	if err := c.admit(len(joiners)); err != nil {
		return err
	}
	c.extend(duration)
	for _, n := range joiners {
		m := c.addNode(n)
		if e.brain.NeedsAnswer(n) {
			m.ringing = true
			c.announce(c.event("intercom_ringing", n))
		} else {
			c.announce(c.event("intercom_joined", n))
		}
		if c.live {
			go c.acquire(m)
		}
	}
	return nil
}

// NewRemote creates a remote participant, ready to be passed to ConnectRemote.
// Its id is unique within this engine. notify, when not nil, receives every
// event of the call the remote is in, in order; it must not block.
func (e *Engine) NewRemote(notify func(Event)) *Remote {
	e.mu.Lock()
	e.remoteSeq++
	id := fmt.Sprintf("web-%d", e.remoteSeq)
	e.mu.Unlock()
	mic := newBridgeBuffer(bridgeRate, bridgeRate*remoteMaxMs/1000)
	mic.prime = bridgeRate * remotePrimeMs / 1000
	return &Remote{
		ID:     id,
		notify: notify,
		mic:    mic,
		out:    newBridgeBuffer(bridgeRate, bridgeRate*remoteMaxMs/1000),
		left:   make(chan struct{}),
	}
}

// ConnectRemote puts the remote in a call with the node: it joins the call the
// node is in ("intercom_joined"), or starts one with it ("intercom_started").
// playsNode tells that the page plays that node by itself (from its go2rtc
// viewer), so gocalis sends it the rest of the call only. duration is as in
// Connect.
func (e *Engine) ConnectRemote(r *Remote, nodeID string, playsNode bool, duration time.Duration) error {
	if e.brain.GetNodeHandle(nodeID) == nil {
		return fmt.Errorf("node '%s' not registered", nodeID)
	}
	if e.brain.GetNodeHandle(r.ID) != nil {
		return fmt.Errorf("remote id '%s' clashes with a node", r.ID)
	}
	if playsNode {
		r.plays = nodeID
	}

	e.mu.Lock()
	defer e.mu.Unlock()
	if e.baseCtx.Err() != nil {
		return fmt.Errorf("intercom engine is shutting down")
	}
	if e.calls[r.ID] != nil {
		return fmt.Errorf("remote '%s' is already in an intercom call", r.ID)
	}
	c := e.calls[nodeID]
	if c == nil {
		c = e.newCall(duration)
		c.addNode(nodeID)
		c.addRemote(r)
		c.open()
		go c.run()
		return nil
	}
	if err := c.admit(1); err != nil {
		return err
	}
	c.extend(duration)
	c.addRemote(r)
	c.announce(c.event("intercom_joined", r.ID))
	if c.live {
		c.attachRemote(r)
		c.mix.play(c.e.joinChime, "")
	}
	return nil
}

// Leave takes one participant (a node id or a remote id) out of its call. The
// call goes on for the others, unless fewer than two remain. It returns false
// when the participant is not in a call.
func (e *Engine) Leave(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.calls[id]
	if c == nil {
		return false
	}
	if !c.ending {
		c.remove(id, "", "")
	}
	return true
}

// Stop ends the whole call that the participant is in, for everyone. It returns
// false when the participant is not in a call.
func (e *Engine) Stop(id string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	c := e.calls[id]
	if c == nil {
		return false
	}
	c.end("stopped")
	return true
}

// timeout resolves a requested call length against the configured default.
func (e *Engine) timeout(duration time.Duration) time.Duration {
	if duration > 0 {
		return duration
	}
	return time.Duration(e.cfg.GetDefaultTimeoutSeconds() * float64(time.Second))
}

// newCanceller builds a node's echo canceller, or nil when echo cancellation is
// off or unavailable (the microphone is then mixed verbatim).
func (e *Engine) newCanceller(nodeID string) EchoCanceller {
	if !e.cfg.EchoCancellation.Enabled || e.newAEC == nil {
		return nil
	}
	frame := bridgeRate * e.cfg.EchoCancellation.GetFrameMs() / 1000
	tail := bridgeRate * e.cfg.EchoCancellation.GetTailMs() / 1000
	c, err := e.newAEC(bridgeRate, frame, tail)
	if err != nil {
		log.Printf("[Intercom] no echo cancellation for '%s': %v\n", nodeID, err)
		return nil
	}
	return c
}

// post queues a notification for delivery outside mu, after the ones queued
// before it. The caller holds mu.
func (e *Engine) post(fn func()) {
	e.outbox = append(e.outbox, fn)
	if !e.draining {
		e.draining = true
		go e.drain()
	}
}

// drain delivers the queued notifications one by one. It does not hold mu while
// delivering.
func (e *Engine) drain() {
	for {
		e.mu.Lock()
		if len(e.outbox) == 0 {
			e.draining = false
			e.mu.Unlock()
			return
		}
		fn := e.outbox[0]
		e.outbox = e.outbox[1:]
		e.mu.Unlock()
		fn()
	}
}

func (e *Engine) newCall(duration time.Duration) *call {
	now := time.Now()
	return &call{
		e:        e,
		started:  now,
		mix:      newMixer(bridgeRate),
		over:     make(chan struct{}),
		duration: duration,
		deadline: now.Add(e.timeout(duration)),
	}
}

// open announces a new call: started, or, when one of its nodes has to be
// answered first, ringing.
func (c *call) open() {
	for _, m := range c.nodes {
		if c.e.brain.NeedsAnswer(m.id) {
			c.waiting = true
		}
	}
	if c.waiting {
		c.announce(c.event("intercom_ringing", ""))
	} else {
		c.announce(c.event("intercom_started", ""))
	}
}

// admit tells whether n more participants may join the call.
func (c *call) admit(n int) error {
	if c.ending {
		return fmt.Errorf("the intercom call is ending")
	}
	if len(c.nodes)+len(c.remotes)+n > MaxParticipants {
		return fmt.Errorf("intercom supports at most %d participants", MaxParticipants)
	}
	return nil
}

// extend pushes the deadline back so the call lasts at least duration from now.
func (c *call) extend(duration time.Duration) {
	if d := time.Now().Add(c.e.timeout(duration)); d.After(c.deadline) {
		c.deadline = d
	}
}

func (c *call) addNode(id string) *nodeMember {
	ctx, cancel := context.WithCancel(c.e.baseCtx)
	seat, unseat := context.WithCancelCause(ctx)
	m := &nodeMember{id: id, ctx: ctx, cancel: cancel, seat: seat, unseat: unseat, streamed: make(chan struct{})}
	c.nodes = append(c.nodes, m)
	c.e.calls[id] = c
	return m
}

func (c *call) addRemote(r *Remote) {
	c.remotes = append(c.remotes, r)
	c.e.calls[r.ID] = c
}

// event builds a lifecycle event from the call as it is now.
func (c *call) event(name, participant string) Event {
	ev := Event{Event: name, ParticipantID: participant, Status: "success"}
	for _, m := range c.nodes {
		ev.NodeIDs = append(ev.NodeIDs, m.id)
	}
	for _, r := range c.remotes {
		ev.RemoteIDs = append(ev.RemoteIDs, r.ID)
	}
	if name == "intercom_ended" {
		ev.Reason = c.reason
		ev.DurationSeconds = time.Since(c.started).Seconds()
		if c.failure != "" {
			ev.Status = "error"
			ev.Message = c.failure
		}
	} else {
		ev.RemainingSeconds = max(0, time.Until(c.deadline).Seconds())
	}
	return ev
}

// announce sends the event to the event bus and to every page in the call, plus
// the given ones (a page that has just left).
func (c *call) announce(ev Event, also ...*Remote) {
	pages := append(slices.Clone(c.remotes), also...)
	c.e.post(func() {
		c.e.emit(ev)
		for _, r := range pages {
			if r.notify != nil {
				r.notify(ev)
			}
		}
	})
}

// end makes the call finish for the given reason, unless it already is.
func (c *call) end(reason string) {
	if c.ending {
		return
	}
	c.ending = true
	c.reason = reason
	close(c.over)
	// Nodes still being taken over are not needed any more.
	for _, m := range c.nodes {
		if m.out == nil {
			m.cancel()
		}
	}
}

// remove takes a participant out of the call: it left, for the given reason if
// there is one to tell ("unanswered", "hung_up"), or was lost to an error
// (failure). The call ends when that leaves fewer than two.
func (c *call) remove(id, reason, failure string) {
	delete(c.e.calls, id)
	var page *Remote
	if i := slices.IndexFunc(c.nodes, func(m *nodeMember) bool { return m.id == id }); i >= 0 {
		m := c.nodes[i]
		c.nodes = slices.Delete(c.nodes, i, i+1)
		m.gone = true
		c.detachNode(m, c.e.leaveChime)
	} else if i := slices.IndexFunc(c.remotes, func(r *Remote) bool { return r.ID == id }); i >= 0 {
		page = c.remotes[i]
		c.remotes = slices.Delete(c.remotes, i, i+1)
		c.detachRemote(page, nil)
	}

	ev := c.event("intercom_left", id)
	ev.Reason = reason
	if failure != "" {
		ev.Status = "error"
		ev.Message = failure
	}
	if page != nil {
		c.announce(ev, page)
		c.e.post(func() { close(page.left) })
	} else {
		c.announce(ev)
	}

	switch {
	case len(c.nodes)+len(c.remotes) >= 2:
		if c.live {
			c.mix.play(c.e.leaveChime, "")
		}
	case failure != "":
		c.failure = failure
		c.end("error")
	case reason == "unanswered" && !c.live:
		// The call never started.
		c.end("unanswered")
	default:
		c.end("left")
	}
}

// goLive puts every participant in the mix and starts mixing. A call that was
// waiting for somebody to answer starts here: its time runs from now.
func (c *call) goLive() {
	c.live = true
	if c.waiting {
		c.waiting = false
		c.started = time.Now()
		c.deadline = c.started.Add(c.e.timeout(c.duration))
		c.announce(c.event("intercom_started", ""))
	}
	for _, m := range c.nodes {
		c.attachNode(m)
	}
	for _, r := range c.remotes {
		c.attachRemote(r)
	}
	c.mix.play(c.e.joinChime, "")
	go c.mix.run()

	aec := "OFF"
	if c.e.cfg.EchoCancellation.Enabled && c.e.newAEC != nil {
		aec = "ON"
	}
	log.Printf("[Intercom] %s started (echo cancellation %s)\n", c.participants(), aec)
}

// attachNode puts an acquired node in the mix: its microphone (echo-cancelled
// when enabled) becomes a talker and its speaker plays its mix-minus. From here
// on the brain reaches the node through its line into the call.
func (c *call) attachNode(m *nodeMember) {
	// Whoever answers a call does so on a device that cancels its own echo.
	if !c.e.brain.IsCallee(m.id) {
		m.aec = c.e.newCanceller(m.id)
	}
	in := newBridgeBuffer(bridgeRate, bridgeRate/2)
	m.out = newBridgeBuffer(bridgeRate, bridgeRate/2)
	c.mix.addTalker(m.id, in)
	c.mix.addListener(m.id, m.out, "")

	aec := m.aec
	c.e.brain.EnterCall(m.id, func(samples []float32) {
		pcm := audio.FloatToPCM16(samples)
		if aec != nil {
			pcm = aec.Capture(pcm)
		}
		in.push(pcm)
	}, newLine(c.mix, m.id, m.seat))
	go c.stream(m)
}

func (c *call) attachRemote(r *Remote) {
	c.mix.addTalker(r.ID, r.mic)
	c.mix.addListener(r.ID, r.out, r.plays)
}

// detachNode takes a node out of the mix and lets it go: its speaker plays what
// is still buffered plus tail, then the node is free for other turns.
func (c *call) detachNode(m *nodeMember, tail []int16) {
	if m.out == nil {
		// Never made it into the mix: it is still being taken over, or it was
		// and is waiting for the call to go live.
		m.cancel()
		if m.turn != nil {
			m.turn.Release()
		}
		return
	}
	c.e.brain.LeaveCall(m.id)
	m.unseat(brain.ErrCallLeft)
	c.mix.remove(m.id)
	m.out.push(tail)
	m.out.close()
	go func() {
		// A playback blocked on something other than its source (an rtc_stream
		// node still connecting) only unwinds on its context.
		select {
		case <-m.streamed:
		case <-time.After(drainGrace):
		}
		m.cancel()
	}()
}

// detachRemote takes a page out of the mix; it still gets tail.
func (c *call) detachRemote(r *Remote, tail []int16) {
	c.mix.remove(r.ID)
	r.out.push(tail)
	r.out.close()
}

// participants lists who is in the call, for logs.
func (c *call) participants() string {
	ev := c.event("", "")
	return strings.Join(append(ev.NodeIDs, ev.RemoteIDs...), ", ")
}

// acquire takes the member's node over for the call, cutting off whatever the
// node was doing. Once the call is live the node goes straight into the mix;
// before that, run puts every initial participant in together. It takes mu.
func (c *call) acquire(m *nodeMember) {
	e := c.e
	turn, err := e.brain.AcquireNodeIntercom(m.ctx, m.id)

	e.mu.Lock()
	defer e.mu.Unlock()
	shutdown := e.baseCtx.Err() != nil
	if err == nil && (m.gone || c.ending || shutdown) {
		turn.Release()
	}
	switch {
	case m.gone || c.ending:
		// It left, or the call ended, while the node was being taken over.
	case shutdown:
		c.end("shutdown")
	case errors.Is(err, audionode.ErrUnanswered):
		c.remove(m.id, "unanswered", "")
	case err != nil:
		c.remove(m.id, "", "could not acquire node '"+m.id+"': "+err.Error())
	default:
		m.turn = turn
		if m.ringing {
			m.ringing = false
			// In a call that has not started it is one of those the start
			// announces.
			if !c.waiting {
				c.announce(c.event("intercom_joined", m.id))
			}
		}
		if c.live {
			c.attachNode(m)
			c.mix.play(c.e.joinChime, "")
		}
	}
}

// stream plays the node's mix on its speaker until the node leaves the call,
// then frees the node. A playback failure drops the node from the call. It
// takes mu.
func (c *call) stream(m *nodeMember) {
	defer close(m.streamed)
	var played func(pcm []int16)
	if m.aec != nil {
		// What the node plays is its echo canceller's far-end reference.
		played = m.aec.Playback
	}
	err := m.turn.StreamOut(m.turn.Context(), m.out, played)
	hungUp := m.turn.HungUp() || errors.Is(err, audionode.ErrHungUp)
	if (err != nil || hungUp) && m.ctx.Err() == nil {
		c.e.mu.Lock()
		switch {
		case m.gone || c.ending:
		case hungUp:
			c.remove(m.id, "hung_up", "")
		default:
			c.remove(m.id, "", err.Error())
		}
		c.e.mu.Unlock()
	}
	m.turn.Release()
	if m.aec != nil {
		m.aec.Close()
	}
}

// run drives a call end to end: take the initial nodes over, put everyone in
// the mix, wait for the call to be over, then let every participant go and
// announce the end. It takes mu.
func (c *call) run() {
	e := c.e

	// Take the initial nodes over one by one; the call goes live once none is
	// left. Whoever joins meanwhile is one more initial participant.
	for {
		e.mu.Lock()
		// Nodes somebody has to answer go first, so the others are left
		// alone while those ring.
		var next *nodeMember
		for _, m := range c.nodes {
			if m.turn != nil {
				continue
			}
			if next == nil {
				next = m
			}
			if e.brain.IsCallee(m.id) {
				next = m
				break
			}
		}
		if next == nil && !c.ending {
			c.goLive()
		}
		ready := next == nil || c.ending
		e.mu.Unlock()
		if ready {
			break
		}
		c.acquire(next)
	}

	// Wait for the deadline (which joins push back), the end of the call, or
	// process shutdown.
	for {
		e.mu.Lock()
		left := time.Until(c.deadline)
		if left <= 0 {
			c.end("timeout")
		}
		ending := c.ending
		e.mu.Unlock()
		if ending {
			break
		}
		timer := time.NewTimer(left)
		select {
		case <-timer.C:
		case <-c.over:
		case <-e.baseCtx.Done():
			e.mu.Lock()
			c.end("shutdown")
			e.mu.Unlock()
		}
		timer.Stop()
	}

	// Let everyone go. Each one still plays the leave chime, so both sides hear
	// that the call is over; the nodes are free once their playback has drained.
	e.mu.Lock()
	tail := e.leaveChime
	if c.reason == "shutdown" {
		tail = nil
	}
	var playing []chan struct{}
	for _, m := range c.nodes {
		if m.out != nil {
			playing = append(playing, m.streamed)
		}
		c.detachNode(m, tail)
	}
	for _, r := range c.remotes {
		c.detachRemote(r, tail)
	}
	live := c.live
	e.mu.Unlock()

	if live {
		c.mix.stop()
	}
	for _, done := range playing {
		<-done
	}

	// Only now is every node free again: take the call off the registry and
	// announce its end.
	e.mu.Lock()
	for _, m := range c.nodes {
		delete(e.calls, m.id)
	}
	for _, r := range c.remotes {
		delete(e.calls, r.ID)
	}
	ev := c.event("intercom_ended", "")
	c.announce(ev)
	pages := slices.Clone(c.remotes)
	e.post(func() {
		for _, r := range pages {
			close(r.left)
		}
	})
	who := c.participants()
	e.mu.Unlock()
	log.Printf("[Intercom] %s ended (%s, %.0fs)\n", who, ev.Reason, ev.DurationSeconds)
}

// distinctNodes drops empty ids from a node list and rejects repeats.
func distinctNodes(nodes []string) ([]string, error) {
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
	return uniq, nil
}
