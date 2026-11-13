package telegramcall

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"gocalis/internal/intercom"
	"gocalis/internal/plugin"
)

// telegramAPI is what a ring needs from the logged-in Telegram account.
type telegramAPI interface {
	// open finds the voice chat of the chat, or starts one with that title.
	// started tells it started one.
	open(ctx context.Context, chat, title string) (vc voiceChat, started bool, err error)
	// join enters the voice chat with our transport's join payload and
	// returns the media server's connection params.
	join(ctx context.Context, vc voiceChat, payload string) (string, error)
	// present lists who else is in the voice chat right now and tells
	// whether we, who joined with that SSRC, still are. errVoiceChatClosed
	// means it is over.
	present(ctx context.Context, vc voiceChat, source uint32) (others []person, in bool, err error)
	// leave takes us out of the voice chat; source is the SSRC we joined with.
	leave(ctx context.Context, vc voiceChat, source uint32) error
	// end closes the voice chat for everybody.
	end(ctx context.Context, vc voiceChat) error
	// changes gets a value whenever Telegram reports something about a
	// voice chat: a cue to look at who is present without waiting.
	changes() <-chan struct{}
}

// voiceChat identifies a group's voice chat.
type voiceChat struct {
	ID         int64
	AccessHash int64
	Chat       string // the group's title
}

// person is somebody in a voice chat.
type person struct {
	ID   int64
	Name string
}

// media is the audio connection of a voice chat; link is the real one.
type media interface {
	joinPayload() (string, error)
	connect(params string) error
	send(pcm []int16) error
	connected() <-chan struct{}
	lost() <-chan struct{}
	source() uint32
	close()
}

func (l *link) connected() <-chan struct{} { return l.up }
func (l *link) lost() <-chan struct{}      { return l.down }
func (l *link) source() uint32             { return l.ssrc }

var (
	errVoiceChatClosed = errors.New("the voice chat was closed")
	errHangup          = errors.New("hangup requested")
	errShutdown        = errors.New("gocalis is shutting down")
	errOffline         = errors.New("the connection to Telegram was lost")
)

const (
	// presenceFailures is how many of those looks may fail in a row.
	presenceFailures = 4
	// connectTimeout bounds the media connection coming up.
	connectTimeout = 20 * time.Second
	// tailTimeout bounds playing out the end of the call (the leave chime) to
	// the voice chat before leaving it.
	tailTimeout = 2 * time.Second
	// cleanupTimeout bounds leaving and closing the voice chat.
	cleanupTimeout = 10 * time.Second
)

// ring is one use of the plugin from the "call" command to the end: it opens
// the voice chat and waits in it; when somebody from the group comes in it
// puts the voice chat in an intercom call with the node; when they are gone,
// or the intercom call is over, it leaves.
type ring struct {
	p        *Plugin
	api      telegramAPI
	node     string
	chat     string
	title    string
	ringFor  time.Duration
	callFor  time.Duration
	ctx      context.Context
	stop     context.CancelCauseFunc
	again    chan struct{} // the same node asked again: the wait starts over
	finished chan struct{}

	mu      sync.Mutex
	outcome string // why the intercom call let our remote go, from its events
	failure string
}

// outcome tells how a ring ended: for a reason, or with a failure.
type outcome struct {
	reason  string
	failure error
}

func (r *ring) run() {
	defer close(r.finished)
	began := time.Now()
	answered, result := r.work()
	r.p.release(r)

	ev := plugin.Event{
		Name:   "telegram_call_ended",
		NodeID: r.node,
		Status: "success",
		Reason: result.reason,
		Data: map[string]any{
			"chat":             r.chat,
			"answered":         answered,
			"duration_seconds": time.Since(began).Seconds(),
		},
	}
	if result.failure != nil {
		ev.Status, ev.Reason, ev.Message = "error", "error", result.failure.Error()
	}
	log.Printf("[Plugin %s] Call for '%s' ended: %s %s\n", r.p.host.ID, r.node, ev.Reason, ev.Message)
	r.p.host.Emit(ev)
}

// work is the whole ring. It returns whether somebody answered and how it
// ended.
func (r *ring) work() (answered bool, result outcome) {
	failed := func(format string, args ...any) outcome {
		return outcome{failure: fmt.Errorf(format, args...)}
	}
	// stopped is the outcome when the ring's context ends.
	stopped := func() outcome {
		switch cause := context.Cause(r.ctx); {
		case errors.Is(cause, errHangup):
			return outcome{reason: "stopped"}
		case errors.Is(cause, errShutdown):
			return outcome{reason: "shutdown"}
		default:
			return outcome{failure: cause}
		}
	}

	vc, started, err := r.api.open(r.ctx, r.chat, r.title)
	if err != nil {
		if r.ctx.Err() != nil {
			return false, stopped()
		}
		return false, failed("open the voice chat: %w", err)
	}
	r.chat = vc.Chat

	mix := newMixdown()
	conn, err := r.p.newMedia(mix)
	if err != nil {
		r.cleanup(vc, started, 0, false)
		return false, failed("prepare the media connection: %w", err)
	}
	joined := false
	defer func() {
		conn.close()
		r.cleanup(vc, started, conn.source(), joined)
	}()

	payload, err := conn.joinPayload()
	if err != nil {
		return false, failed("prepare the media connection: %w", err)
	}
	params, err := r.api.join(r.ctx, vc, payload)
	if err != nil {
		if r.ctx.Err() != nil {
			return false, stopped()
		}
		return false, failed("join the voice chat: %w", err)
	}
	joined = true
	if err := conn.connect(params); err != nil {
		return false, failed("connect to the voice chat: %w", err)
	}
	select {
	case <-conn.connected():
	case <-conn.lost():
		return false, failed("the media connection to the voice chat failed")
	case <-time.After(connectTimeout):
		return false, failed("the media connection to the voice chat did not come up in %v", connectTimeout)
	case <-r.ctx.Done():
		return false, stopped()
	}

	log.Printf("[Plugin %s] Waiting in the voice chat of '%s' for somebody to answer for '%s' (%v)\n", r.p.host.ID, vc.Chat, r.node, r.ringFor)
	r.p.host.Emit(plugin.Event{
		Name:   "telegram_call_waiting",
		NodeID: r.node,
		Data: map[string]any{
			"chat":               vc.Chat,
			"ring_seconds":       r.ringFor.Seconds(),
			"started_voice_chat": started,
		},
	})

	// Wait for somebody from the group.
	watch := r.newWatch(vc, conn)
	defer watch.stop()
	deadline := time.NewTimer(r.ringFor)
	defer deadline.Stop()
	var others []person
	for len(others) == 0 {
		select {
		case seen := <-watch.seen:
			if seen.err != nil {
				return false, seen.outcome()
			}
			others = seen.others
		case <-deadline.C:
			return false, outcome{reason: "unanswered"}
		case <-r.again:
			deadline.Reset(r.ringFor)
		case <-conn.lost():
			return false, failed("the media connection to the voice chat was lost")
		case <-r.ctx.Done():
			return false, stopped()
		}
	}

	// Somebody is there: this is where the intercom call starts.
	remote := r.p.host.Intercom.NewRemote("telegram", r.note)
	if err := r.p.host.Intercom.ConnectRemote(remote, r.node, false, r.callFor); err != nil {
		return false, failed("start the intercom call: %w", err)
	}
	mix.to(remote.Push)
	mixing := make(chan struct{})
	go mix.run(mixing)
	defer close(mixing)
	sent := make(chan struct{})
	go func() {
		defer close(sent)
		pump(remote, conn)
	}()

	names := make([]string, len(others))
	for i, o := range others {
		names[i] = o.Name
	}
	log.Printf("[Plugin %s] %v answered for '%s'\n", r.p.host.ID, names, r.node)
	r.p.host.Emit(plugin.Event{
		Name:   "telegram_call_answered",
		NodeID: r.node,
		Data: map[string]any{
			"chat":      vc.Chat,
			"answered":  names,
			"remote_id": remote.ID,
		},
	})

	// Stay until the people on Telegram are gone or the intercom call lets
	// us go.
	result = outcome{}
	for done := false; !done; {
		select {
		case <-remote.Left():
			result = r.noted()
			done = true
		case seen := <-watch.seen:
			if seen.err != nil {
				result = seen.outcome()
				done = true
			} else if len(seen.others) == 0 {
				result = outcome{reason: "hung_up"}
				done = true
			}
		case <-conn.lost():
			result = failed("the media connection to the voice chat was lost")
			done = true
		case <-r.ctx.Done():
			result = stopped()
			done = true
		}
	}

	// Out of the intercom call (a no-op when it already let us go), then
	// give the voice chat the end of it to hear.
	mix.to(nil)
	r.p.host.Intercom.Leave(remote.ID)
	select {
	case <-sent:
	case <-time.After(tailTimeout):
	}
	return true, result
}

// cleanup takes us out of the voice chat and closes it if this ring started
// it. It runs when the ring's own context may already be over.
func (r *ring) cleanup(vc voiceChat, started bool, source uint32, joined bool) {
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if joined {
		if err := r.api.leave(ctx, vc, source); err != nil && !errors.Is(err, errVoiceChatClosed) {
			log.Printf("[Plugin %s] Leaving the voice chat of '%s': %v\n", r.p.host.ID, vc.Chat, err)
		}
	}
	if started {
		if err := r.api.end(ctx, vc); err != nil && !errors.Is(err, errVoiceChatClosed) {
			log.Printf("[Plugin %s] Closing the voice chat of '%s': %v\n", r.p.host.ID, vc.Chat, err)
		}
	}
}

// note keeps why the intercom call let our remote go. It is the remote's
// notify callback, so it must not block.
func (r *ring) note(ev intercom.Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if ev.Event == "intercom_ended" {
		r.outcome, r.failure = ev.Reason, ""
		if ev.Status == "error" {
			r.failure = ev.Message
		}
	}
}

// noted is how the ring ended when the intercom call let our remote go.
func (r *ring) noted() outcome {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch {
	case r.failure != "":
		return outcome{failure: fmt.Errorf("the intercom call failed: %s", r.failure)}
	case r.outcome != "":
		return outcome{reason: r.outcome}
	}
	// Taken out of a call that goes on without us.
	return outcome{reason: "left"}
}

// sighting is one look at the voice chat.
type sighting struct {
	others []person
	err    error
}

func (s sighting) outcome() outcome {
	if errors.Is(s.err, errVoiceChatClosed) {
		return outcome{reason: "closed"}
	}
	return outcome{failure: s.err}
}

// watch looks at who is in the voice chat, on a timer and whenever Telegram
// reports a change, and delivers each look that differs from the one before.
type watch struct {
	seen chan sighting
	quit chan struct{}
	done chan struct{}
}

func (w *watch) stop() {
	close(w.quit)
	<-w.done
}

func (r *ring) newWatch(vc voiceChat, conn media) *watch {
	w := &watch{seen: make(chan sighting), quit: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		deliver := func(s sighting) bool {
			select {
			case w.seen <- s:
				return true
			case <-w.quit:
				return false
			}
		}
		ticker := time.NewTicker(r.p.lookEvery)
		defer ticker.Stop()
		failures, out, last := 0, 0, -1
		for {
			ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			others, in, err := r.api.present(ctx, vc, conn.source())
			cancel()
			switch {
			case errors.Is(err, errVoiceChatClosed):
				deliver(sighting{err: err})
				return
			case err != nil:
				if failures++; failures >= presenceFailures {
					deliver(sighting{err: fmt.Errorf("look at the voice chat: %w", err)})
					return
				}
			default:
				failures = 0
				// One look that misses us can be Telegram catching up; several
				// in a row mean we were dropped. Who else is there is only
				// believed from a look that has us in it.
				if !in {
					if out++; out >= presenceFailures {
						deliver(sighting{err: errors.New("gocalis is no longer in the voice chat")})
						return
					}
					break
				}
				out = 0
				if len(others) != last {
					if !deliver(sighting{others: others}) {
						return
					}
					last = len(others)
				}
			}
			select {
			case <-ticker.C:
			case <-r.api.changes():
			case <-w.quit:
				return
			}
		}
	}()
	return w
}

// pump sends the voice chat what the intercom call has for our remote, in
// whole frames no closer together than they play, until the remote is out of
// the call and everything meant for it is sent.
func pump(remote *intercom.Remote, conn media) {
	var pending []int16
	next := time.Now()
	for {
		pcm, err := remote.Read(context.Background())
		if err != nil {
			return
		}
		pending = append(pending, pcm...)
		for len(pending) >= frameSamples {
			if wait := time.Until(next); wait > 0 {
				time.Sleep(wait)
			}
			if now := time.Now(); now.After(next.Add(100 * time.Millisecond)) {
				next = now
			}
			next = next.Add(20 * time.Millisecond)
			if err := conn.send(pending[:frameSamples]); err != nil {
				return
			}
			pending = pending[frameSamples:]
		}
	}
}
