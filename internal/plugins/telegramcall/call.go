package telegramcall

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"gocalis/internal/audionode"
	"gocalis/internal/plugin"
)

// telegramAPI is what a call needs from the logged-in Telegram account.
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

// Why a call is told to end.
var (
	errVoiceChatClosed = errors.New("the voice chat was closed")
	errHangup          = errors.New("hangup requested")
	errDone            = errors.New("nothing more to say")
	errAbandoned       = errors.New("whoever was calling gave up")
	errShutdown        = errors.New("gocalis is shutting down")
	errOffline         = errors.New("the connection to Telegram was lost")
)

const (
	// presenceFailures is how many looks at the voice chat may fail in a row.
	presenceFailures = 4
	// connectTimeout bounds the media connection coming up.
	connectTimeout = 20 * time.Second
	// tailTime is left between the last audio sent and leaving, so it is
	// heard to the end.
	tailTime = 400 * time.Millisecond
	// cleanupTimeout bounds leaving and closing the voice chat.
	cleanupTimeout = 10 * time.Second
)

// call is one stay in the group's voice chat. gocalis opens the voice chat (or
// joins the one that runs) and rings: it waits there for somebody of the group.
// Once somebody came in, the call is the node's line to them, for every turn
// that follows, until they are gone or gocalis hangs up; then it leaves, and
// closes the voice chat if it opened it.
type call struct {
	p    *Plugin
	api  telegramAPI
	prev <-chan struct{} // the call before, which has to be out of the voice chat first
	ctx  context.Context
	stop context.CancelCauseFunc

	answered chan struct{} // closed once somebody is in the voice chat
	gone     chan struct{} // closed once nobody is on the line any more
	finished chan struct{} // closed once gocalis is out of the voice chat
	result   outcome       // how it ended; set before gone is closed
	conn     media         // the audio connection; set before answered is closed

	// Guarded by Plugin.mu.
	users int         // turns waiting for the answer or holding the line
	idle  *time.Timer // hangs up a line no turn holds
}

// outcome tells how a call ended: for a reason, or with a failure.
type outcome struct {
	reason  string
	failure error
}

// over tells whether the call is ending or has ended.
func (c *call) over() bool { return c.ctx.Err() != nil }

// isAnswered tells whether somebody came in.
func (c *call) isAnswered() bool {
	select {
	case <-c.answered:
		return true
	default:
		return false
	}
}

// err is what a turn is told when the call ended before anybody answered.
func (c *call) err() error {
	switch {
	case c.result.failure != nil:
		return c.result.failure
	case c.result.reason == "unanswered":
		return audionode.ErrUnanswered
	}
	return fmt.Errorf("the Telegram call ended before anybody answered (%s)", c.result.reason)
}

func (c *call) run() {
	defer close(c.finished)
	if c.prev != nil {
		<-c.prev
	}
	began := time.Now()
	st := &stay{}
	answered, result := c.work(st)

	// Whoever holds the line, or waits for it, learns now; leaving the voice
	// chat takes a moment more.
	c.result = result
	c.stop(nil)
	close(c.gone)
	if answered && result.failure == nil && result.reason != "hung_up" && result.reason != "closed" {
		time.Sleep(tailTime)
	}
	c.leave(st)
	c.p.release(c)

	ev := plugin.Event{
		Name:   "call_ended",
		NodeID: c.p.host.ID,
		Status: "success",
		Reason: result.reason,
		Data: map[string]any{
			"chat":             st.vc.Chat,
			"answered":         answered,
			"duration_seconds": time.Since(began).Seconds(),
		},
	}
	if result.failure != nil {
		ev.Status, ev.Reason, ev.Message = "error", "error", result.failure.Error()
	}
	log.Printf("[Plugin %s] Call ended: %s %s\n", c.p.host.ID, ev.Reason, ev.Message)
	c.p.host.Emit(ev)
}

// stay is what a call holds in Telegram, to be given back when it is over.
type stay struct {
	vc      voiceChat
	started bool // the call started the voice chat
	joined  bool
	conn    media
	mixing  chan struct{} // closed to stop the mixdown
}

// work is the call up to the moment it is over. It returns whether somebody
// answered and how it ended.
func (c *call) work(st *stay) (answered bool, result outcome) {
	failed := func(format string, args ...any) outcome {
		return outcome{failure: fmt.Errorf(format, args...)}
	}
	// stopped is the outcome when the call's context ends.
	stopped := func() outcome {
		switch cause := context.Cause(c.ctx); {
		case errors.Is(cause, errHangup):
			return outcome{reason: "stopped"}
		case errors.Is(cause, errDone):
			return outcome{reason: "completed"}
		case errors.Is(cause, errAbandoned):
			return outcome{reason: "cancelled"}
		case errors.Is(cause, errShutdown):
			return outcome{reason: "shutdown"}
		default:
			return outcome{failure: cause}
		}
	}
	if c.ctx.Err() != nil {
		return false, stopped()
	}

	settings := c.p.settings
	if settings.Chat == "" {
		return false, failed("no group to call: set 'chat' in the settings of plugin '%s'", c.p.host.ID)
	}
	var err error
	st.vc, st.started, err = c.api.open(c.ctx, settings.Chat, settings.Title)
	if err != nil {
		if c.ctx.Err() != nil {
			return false, stopped()
		}
		return false, failed("open the voice chat: %w", err)
	}

	mix := newMixdown()
	conn, err := c.p.newMedia(mix)
	if err != nil {
		return false, failed("prepare the media connection: %w", err)
	}
	st.conn = conn
	payload, err := conn.joinPayload()
	if err != nil {
		return false, failed("prepare the media connection: %w", err)
	}
	params, err := c.api.join(c.ctx, st.vc, payload)
	if err != nil {
		if c.ctx.Err() != nil {
			return false, stopped()
		}
		return false, failed("join the voice chat: %w", err)
	}
	st.joined = true
	if err := conn.connect(params); err != nil {
		return false, failed("connect to the voice chat: %w", err)
	}
	select {
	case <-conn.connected():
	case <-conn.lost():
		return false, failed("the media connection to the voice chat failed")
	case <-time.After(connectTimeout):
		return false, failed("the media connection to the voice chat did not come up in %v", connectTimeout)
	case <-c.ctx.Done():
		return false, stopped()
	}

	log.Printf("[Plugin %s] Ringing: waiting in the voice chat of '%s' for somebody to come in (%v)\n", c.p.host.ID, st.vc.Chat, c.p.ringFor)
	c.p.host.Emit(plugin.Event{
		Name:   "call_ringing",
		NodeID: c.p.host.ID,
		Data: map[string]any{
			"chat":               st.vc.Chat,
			"ring_seconds":       c.p.ringFor.Seconds(),
			"started_voice_chat": st.started,
		},
	})

	// Wait for somebody from the group.
	watch := c.newWatch(st.vc, conn)
	defer watch.stop()
	deadline := time.NewTimer(c.p.ringFor)
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
		case <-conn.lost():
			return false, failed("the media connection to the voice chat was lost")
		case <-c.ctx.Done():
			return false, stopped()
		}
	}

	// Somebody is there: from here the node has a line to them.
	mix.to(c.p.heard)
	st.mixing = make(chan struct{})
	go mix.run(st.mixing)
	c.conn = conn
	close(c.answered)

	names := make([]string, len(others))
	for i, o := range others {
		names[i] = o.Name
	}
	log.Printf("[Plugin %s] %v answered\n", c.p.host.ID, names)
	c.p.host.Emit(plugin.Event{
		Name:   "call_answered",
		NodeID: c.p.host.ID,
		Data:   map[string]any{"chat": st.vc.Chat, "answered": names},
	})

	// Stay until the people on Telegram are gone or gocalis hangs up.
	for {
		select {
		case seen := <-watch.seen:
			if seen.err != nil {
				return true, seen.outcome()
			}
			if len(seen.others) == 0 {
				return true, outcome{reason: "hung_up"}
			}
		case <-conn.lost():
			return true, failed("the media connection to the voice chat was lost")
		case <-c.ctx.Done():
			return true, stopped()
		}
	}
}

// leave takes gocalis out of the voice chat and closes it if this call started
// it. It runs when the call's own context is over.
func (c *call) leave(st *stay) {
	if st.mixing != nil {
		close(st.mixing)
	}
	if st.conn != nil {
		st.conn.close()
	}
	ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
	defer cancel()
	if st.joined {
		if err := c.api.leave(ctx, st.vc, st.conn.source()); err != nil && !errors.Is(err, errVoiceChatClosed) {
			log.Printf("[Plugin %s] Leaving the voice chat of '%s': %v\n", c.p.host.ID, st.vc.Chat, err)
		}
	}
	if st.started {
		if err := c.api.end(ctx, st.vc); err != nil && !errors.Is(err, errVoiceChatClosed) {
			log.Printf("[Plugin %s] Closing the voice chat of '%s': %v\n", c.p.host.ID, st.vc.Chat, err)
		}
	}
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

func (c *call) newWatch(vc voiceChat, conn media) *watch {
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
		ticker := time.NewTicker(c.p.lookEvery)
		defer ticker.Stop()
		failures, out, last := 0, 0, -1
		for {
			ctx, cancel := context.WithTimeout(context.Background(), cleanupTimeout)
			others, in, err := c.api.present(ctx, vc, conn.source())
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
			case <-c.api.changes():
			case <-w.quit:
				return
			}
		}
	}()
	return w
}
