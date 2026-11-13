package telegramcall

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/intercom"
	"gocalis/internal/node"
	"gocalis/internal/plugin"
)

// fakeTelegram is a Telegram account with one group, "Casa".
type fakeTelegram struct {
	mu       sync.Mutex
	running  bool // the group has a voice chat
	others   []person
	in       bool
	failOpen error
	log      []string
	nudge    chan struct{}
}

func newFakeTelegram() *fakeTelegram { return &fakeTelegram{nudge: make(chan struct{}, 1)} }

func (f *fakeTelegram) open(_ context.Context, chat, title string) (voiceChat, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failOpen != nil {
		return voiceChat{}, false, f.failOpen
	}
	started := !f.running
	f.running = true
	if started {
		f.log = append(f.log, "started:"+title)
	}
	return voiceChat{ID: 7, AccessHash: 9, Chat: "Casa"}, started, nil
}

func (f *fakeTelegram) join(_ context.Context, _ voiceChat, payload string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.in = true
	f.log = append(f.log, "joined:"+payload)
	return "params", nil
}

func (f *fakeTelegram) present(context.Context, voiceChat, uint32) ([]person, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.running {
		return nil, false, errVoiceChatClosed
	}
	return slices.Clone(f.others), f.in, nil
}

func (f *fakeTelegram) leave(context.Context, voiceChat, uint32) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.in = false
	f.log = append(f.log, "left")
	return nil
}

func (f *fakeTelegram) end(context.Context, voiceChat) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.running = false
	f.log = append(f.log, "ended")
	return nil
}

func (f *fakeTelegram) changes() <-chan struct{} { return f.nudge }

// set changes who is in the voice chat and, like Telegram, reports that
// something changed.
func (f *fakeTelegram) set(change func()) {
	f.mu.Lock()
	change()
	f.mu.Unlock()
	select {
	case f.nudge <- struct{}{}:
	default:
	}
}

func (f *fakeTelegram) did(what string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.ContainsFunc(f.log, func(entry string) bool { return strings.HasPrefix(entry, what) })
}

// fakeMedia is a voice chat's audio connection without a network.
type fakeMedia struct {
	heard sink

	mu     sync.Mutex
	sent   []int16
	up     chan struct{}
	down   chan struct{}
	closed bool
}

func (m *fakeMedia) joinPayload() (string, error) { return "payload", nil }
func (m *fakeMedia) connect(string) error         { close(m.up); return nil }
func (m *fakeMedia) connected() <-chan struct{}   { return m.up }
func (m *fakeMedia) lost() <-chan struct{}        { return m.down }
func (m *fakeMedia) source() uint32               { return 42 }

func (m *fakeMedia) send(pcm []int16) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sent = append(m.sent, pcm...)
	return nil
}

func (m *fakeMedia) close() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
}

func (m *fakeMedia) got() []int16 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.sent)
}

// rig is a gocalis with a door node, the plugin, and a fake Telegram.
type rig struct {
	t        *testing.T
	brain    *brain.Brain
	door     *audionode.Stub
	doorNode *node.PhysicalNode
	engine   *intercom.Engine
	telegram *fakeTelegram
	plugin   *Plugin
	offline  context.CancelFunc // loses the connection to Telegram
	stop     context.CancelFunc // shuts gocalis down
	stopped  chan struct{}

	mu     sync.Mutex
	events []plugin.Event
	media  []*fakeMedia
}

func pluginConfig(t *testing.T, settings string) config.PluginConfig {
	t.Helper()
	cfg := config.PluginConfig{ID: "telegram", Type: "telegram_call"}
	if err := yaml.Unmarshal([]byte(settings), &cfg.Settings); err != nil {
		t.Fatal(err)
	}
	return cfg
}

func newRig(t *testing.T) *rig {
	t.Helper()
	r := &rig{t: t, brain: brain.New(nil), door: &audionode.Stub{}, telegram: newFakeTelegram(), stopped: make(chan struct{})}
	r.doorNode = node.NewPhysicalNode("door", "local")
	t.Cleanup(r.doorNode.Close)
	r.brain.RegisterNode("door", &brain.NodeHandle{Node: r.doorNode, Audio: r.door, Config: config.NodeConfig{NodeID: "door", Type: "local"}})
	r.engine = intercom.NewEngine(r.brain, config.IntercomConfig{DefaultTimeoutSeconds: 30}, func(intercom.Event) {}, nil)
	t.Cleanup(r.engine.Shutdown)

	session := filepath.Join(t.TempDir(), "telegram.session")
	if err := os.WriteFile(session, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := New(pluginConfig(t, "api_id: 1\napi_hash: h\nchat: Casa\ntitle: Timbre\nring_seconds: 5\nsession_file: "+session), plugin.Host{
		ID:       "telegram",
		Intercom: r.engine,
		HasNode:  func(id string) bool { return r.brain.GetNodeHandle(id) != nil },
		Emit: func(ev plugin.Event) {
			r.mu.Lock()
			r.events = append(r.events, ev)
			r.mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	online, offline := context.WithCancel(context.Background())
	r.offline = offline
	p.connect = func(ready func(context.Context, telegramAPI, string) error) error {
		if online.Err() != nil {
			return errors.New("no route to Telegram")
		}
		return ready(online, r.telegram, "Portero")
	}
	p.newMedia = func(heard sink) (media, error) {
		m := &fakeMedia{heard: heard, up: make(chan struct{}), down: make(chan struct{})}
		r.mu.Lock()
		r.media = append(r.media, m)
		r.mu.Unlock()
		return m, nil
	}
	p.lookEvery = 50 * time.Millisecond
	r.plugin = p

	ctx, stop := context.WithCancel(context.Background())
	r.stop = stop
	go func() {
		defer close(r.stopped)
		_ = p.Run(ctx)
	}()
	t.Cleanup(func() {
		stop()
		<-r.stopped
	})
	r.until("the plugin to be connected", func() bool { return p.Status().Ready })
	return r
}

func (r *rig) until(what string, cond func() bool) {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (r *rig) command(name, nodeID, args string) error {
	return r.plugin.Command(context.Background(), plugin.Command{Name: name, NodeID: nodeID, Args: []byte(args)})
}

func (r *rig) find(name string) (plugin.Event, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ev := range r.events {
		if ev.Name == name {
			return ev, true
		}
	}
	return plugin.Event{}, false
}

func (r *rig) event(name string) plugin.Event {
	r.t.Helper()
	var ev plugin.Event
	r.until("the event "+name, func() (ok bool) {
		ev, ok = r.find(name)
		return ok
	})
	return ev
}

func (r *rig) conn() *fakeMedia {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.media[len(r.media)-1]
}

func (r *rig) played() []int16 {
	var out []int16
	for _, buf := range r.door.Played {
		out = append(out, buf...)
	}
	return out
}

func floats(pcm []int16) []float32 {
	out := make([]float32, len(pcm))
	for i, v := range pcm {
		out[i] = float32(v) / 32768
	}
	return out
}

var (
	ana  = person{ID: 100, Name: "Ana"}
	luis = person{ID: 200, Name: "Luis"}
)

// The whole use: the bell rings, the voice chat opens and waits, somebody
// comes in and that starts the intercom call, both sides hear each other,
// and when the people on Telegram are gone everything is put back.
func TestCallStartsTheIntercomWhenSomebodyComesIn(t *testing.T) {
	r := newRig(t)
	if err := r.command("call", "door", ""); err != nil {
		t.Fatal(err)
	}

	waiting := r.event("telegram_call_waiting")
	if waiting.NodeID != "door" || waiting.Data["chat"] != "Casa" || waiting.Data["started_voice_chat"] != true || waiting.Data["ring_seconds"] != 5.0 {
		t.Fatalf("unexpected waiting event: %+v", waiting)
	}
	if !r.telegram.did("started:Timbre") || !r.telegram.did("joined:payload") {
		t.Fatalf("the voice chat was not started and joined: %v", r.telegram.log)
	}
	// Nobody is there yet: the door is left alone.
	time.Sleep(150 * time.Millisecond)
	if state := r.doorNode.GetState(); state != node.StateIdle {
		t.Fatalf("the door is %s while nobody has answered", state)
	}
	if _, ok := r.find("telegram_call_answered"); ok {
		t.Fatal("answered before anybody came in")
	}

	r.telegram.set(func() { r.telegram.others = []person{ana, luis} })
	answered := r.event("telegram_call_answered")
	remoteID, _ := answered.Data["remote_id"].(string)
	if !slices.Equal(answered.Data["answered"].([]string), []string{"Ana", "Luis"}) || !strings.HasPrefix(remoteID, "telegram-") {
		t.Fatalf("unexpected answered event: %+v", answered)
	}
	r.until("the door to be in the call", func() bool { return r.doorNode.GetState() == node.StateIntercom })

	// The door talks, and the two on Telegram talk at once.
	conn := r.conn()
	door, anaVoice, luisVoice := tone(440, 8000, 40), tone(1000, 6000, 40), tone(600, 6000, 40)
	next := time.Now()
	for i := range door {
		r.brain.ForwardRawAudio("door", floats(door[i]))
		conn.heard.push(1, anaVoice[i])
		conn.heard.push(2, luisVoice[i])
		next = next.Add(20 * time.Millisecond)
		time.Sleep(time.Until(next))
	}
	time.Sleep(200 * time.Millisecond)
	if power := tonePower(conn.got(), 440); power < 1500 {
		t.Fatalf("the voice chat got the door's 440 Hz tone with amplitude %.0f", power)
	}
	if got := conn.got(); tonePower(got, 1000) > 300 || tonePower(got, 600) > 300 {
		t.Fatal("the voice chat was sent its own voices back")
	}

	// One leaves: the call goes on. Both leave: it is over.
	r.telegram.set(func() { r.telegram.others = []person{luis} })
	time.Sleep(200 * time.Millisecond)
	if _, ok := r.find("telegram_call_ended"); ok {
		t.Fatal("the call ended while somebody was still in the voice chat")
	}
	r.telegram.set(func() { r.telegram.others = nil })
	ended := r.event("telegram_call_ended")
	if ended.Status != "success" || ended.Reason != "hung_up" || ended.Data["answered"] != true || ended.NodeID != "door" {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	r.until("the door to be free", func() bool { return r.doorNode.GetState() == node.StateIdle })
	if !r.telegram.did("left") || !r.telegram.did("ended") {
		t.Fatalf("the voice chat was not left and closed: %v", r.telegram.log)
	}
	conn.mu.Lock()
	closed := conn.closed
	conn.mu.Unlock()
	if !closed {
		t.Fatal("the media connection was left open")
	}
	played := r.played()
	if a, l := tonePower(played, 1000), tonePower(played, 600); a < 500 || l < 500 {
		t.Fatalf("the door played Ana at amplitude %.0f and Luis at %.0f", a, l)
	}
	if status := r.plugin.Status(); !status.Ready || strings.Contains(status.Detail, "call for") {
		t.Fatalf("unexpected status after the call: %+v", status)
	}
}

func TestCallNobodyAnswers(t *testing.T) {
	r := newRig(t)
	if err := r.command("call", "door", `{"ring_seconds":0.3}`); err != nil {
		t.Fatal(err)
	}
	r.event("telegram_call_waiting")
	ended := r.event("telegram_call_ended")
	if ended.Reason != "unanswered" || ended.Status != "success" || ended.Data["answered"] != false {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	if !r.telegram.did("left") || !r.telegram.did("ended") {
		t.Fatalf("the voice chat was not left and closed: %v", r.telegram.log)
	}
	if state := r.doorNode.GetState(); state != node.StateIdle {
		t.Fatalf("the door is %s after an unanswered call", state)
	}
}

// The bell rung again while waiting makes the wait start over.
func TestCallAgainRestartsTheWait(t *testing.T) {
	r := newRig(t)
	if err := r.command("call", "door", `{"ring_seconds":0.5}`); err != nil {
		t.Fatal(err)
	}
	r.event("telegram_call_waiting")
	time.Sleep(350 * time.Millisecond)
	if err := r.command("call", "door", ""); err != nil {
		t.Fatalf("ringing again was refused: %v", err)
	}
	time.Sleep(350 * time.Millisecond)
	if _, ok := r.find("telegram_call_ended"); ok {
		t.Fatal("the wait did not start over")
	}
	if ended := r.event("telegram_call_ended"); ended.Reason != "unanswered" {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
}

// A voice chat that was already going, with people in it, is answered at
// once, and is not closed afterwards: it is theirs.
func TestCallJoinsARunningVoiceChat(t *testing.T) {
	r := newRig(t)
	r.telegram.running, r.telegram.others = true, []person{ana}
	if err := r.command("call", "door", ""); err != nil {
		t.Fatal(err)
	}
	if waiting := r.event("telegram_call_waiting"); waiting.Data["started_voice_chat"] != false {
		t.Fatalf("unexpected waiting event: %+v", waiting)
	}
	r.event("telegram_call_answered")
	r.until("the door to be in the call", func() bool { return r.doorNode.GetState() == node.StateIntercom })

	if err := r.command("hangup", "", ""); err != nil {
		t.Fatal(err)
	}
	if ended := r.event("telegram_call_ended"); ended.Reason != "stopped" || ended.Data["answered"] != true {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	r.until("the door to be free", func() bool { return r.doorNode.GetState() == node.StateIdle })
	if !r.telegram.did("left") || r.telegram.did("ended") || r.telegram.did("started") {
		t.Fatalf("a voice chat that was already going must be left, not closed: %v", r.telegram.log)
	}
}

func TestHangupWhileWaiting(t *testing.T) {
	r := newRig(t)
	if err := r.command("hangup", "", ""); err == nil {
		t.Fatal("expected hangup without a call to be refused")
	}
	if err := r.command("call", "door", ""); err != nil {
		t.Fatal(err)
	}
	r.event("telegram_call_waiting")
	if err := r.command("hangup", "garden", ""); err == nil {
		t.Fatal("expected hangup for another node to be refused")
	}
	if err := r.command("hangup", "door", ""); err != nil {
		t.Fatal(err)
	}
	if ended := r.event("telegram_call_ended"); ended.Reason != "stopped" || ended.Data["answered"] != false {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	if !r.telegram.did("ended") {
		t.Fatalf("the voice chat was not closed: %v", r.telegram.log)
	}
}

// When the intercom call is over first (stopped, timed out), the plugin
// leaves the voice chat and says why.
func TestCallEndsWithTheIntercomCall(t *testing.T) {
	for name, finish := range map[string]func(r *rig){
		"stopped": func(r *rig) { r.engine.Stop("door") },
		"timeout": func(*rig) {},
	} {
		t.Run(name, func(t *testing.T) {
			r := newRig(t)
			r.telegram.running, r.telegram.others = true, []person{ana}
			if err := r.command("call", "door", `{"duration_seconds":0.6}`); err != nil {
				t.Fatal(err)
			}
			r.event("telegram_call_answered")
			r.until("the door to be in the call", func() bool { return r.doorNode.GetState() == node.StateIntercom })
			finish(r)
			if ended := r.event("telegram_call_ended"); ended.Reason != name || ended.Status != "success" {
				t.Fatalf("unexpected ended event: %+v", ended)
			}
			if !r.telegram.did("left") {
				t.Fatalf("the voice chat was not left: %v", r.telegram.log)
			}
		})
	}
}

func TestCallEndsWhenTheVoiceChatIsClosed(t *testing.T) {
	r := newRig(t)
	if err := r.command("call", "door", ""); err != nil {
		t.Fatal(err)
	}
	r.event("telegram_call_waiting")
	r.telegram.set(func() { r.telegram.others = []person{ana} })
	r.event("telegram_call_answered")
	r.telegram.set(func() { r.telegram.running = false })
	if ended := r.event("telegram_call_ended"); ended.Reason != "closed" || ended.Status != "success" {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	r.until("the door to be free", func() bool { return r.doorNode.GetState() == node.StateIdle })
}

func TestCallReportsFailures(t *testing.T) {
	r := newRig(t)
	r.telegram.failOpen = errors.New("no group \"Casa\" among the account's chats")
	if err := r.command("call", "door", ""); err != nil {
		t.Fatal(err)
	}
	ended := r.event("telegram_call_ended")
	if ended.Status != "error" || ended.Reason != "error" || !strings.Contains(ended.Message, "no group") {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	// The plugin is free for the next call.
	r.telegram.failOpen = nil
	r.until("the plugin to be free", func() bool { return r.command("call", "door", `{"ring_seconds":0.2}`) == nil })
}

func TestCommandsAreChecked(t *testing.T) {
	r := newRig(t)
	r.brain.RegisterNode("garden", &brain.NodeHandle{Node: r.doorNode, Audio: &audionode.Stub{}, Config: config.NodeConfig{NodeID: "garden"}})
	for name, c := range map[string][3]string{
		"unknown command": {"dial", "door", ""},
		"no node":         {"call", "", ""},
		"unknown node":    {"call", "attic", ""},
		"misspelled arg":  {"call", "door", `{"ring":3}`},
		"ring too long":   {"call", "door", `{"ring_seconds":100000}`},
		"negative length": {"call", "door", `{"duration_seconds":-1}`},
		"hangup with arg": {"hangup", "", `{"now":true}`},
	} {
		if err := r.command(c[0], c[1], c[2]); err == nil {
			t.Errorf("%s: expected the command to be refused", name)
		}
	}
	if _, ok := r.find("telegram_call_waiting"); ok {
		t.Fatal("a refused command started a call")
	}

	if err := r.command("call", "door", ""); err != nil {
		t.Fatal(err)
	}
	if err := r.command("call", "garden", ""); err == nil || !strings.Contains(err.Error(), "busy") {
		t.Fatalf("expected a call for another node to be refused as busy, got %v", err)
	}
}

// Without a login the plugin waits for one; it picks it up when the setup
// writes the session file.
func TestRunWaitsForTheLogin(t *testing.T) {
	session := filepath.Join(t.TempDir(), "telegram.session")
	p, err := New(pluginConfig(t, "api_id: 1\napi_hash: h\nsession_file: "+session), plugin.Host{ID: "tg", HasNode: func(string) bool { return true }})
	if err != nil {
		t.Fatal(err)
	}
	connected := make(chan struct{})
	p.connect = func(ready func(context.Context, telegramAPI, string) error) error {
		close(connected)
		return ready(context.Background(), newFakeTelegram(), "Portero")
	}
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = p.Run(ctx)
	}()
	defer func() {
		stop()
		<-done
	}()

	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(p.Status().Detail, "gocalis -plugin-setup tg") {
		if time.Now().After(deadline) {
			t.Fatalf("expected the status to say how to log in, got %+v", p.Status())
		}
		time.Sleep(5 * time.Millisecond)
	}
	if p.Status().Ready {
		t.Fatal("ready without a login")
	}
	if err := p.Command(context.Background(), plugin.Command{Name: "call", NodeID: "door", Args: []byte(`{"chat":"Casa"}`)}); err == nil || !strings.Contains(err.Error(), "not connected") {
		t.Fatalf("expected a call without a login to be refused, got %v", err)
	}

	if err := os.WriteFile(session, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-connected:
	case <-time.After(10 * time.Second):
		t.Fatal("the plugin did not pick the login up")
	}
}

// Shutting gocalis down in the middle of a call puts everything back first.
func TestShutdownEndsTheCall(t *testing.T) {
	r := newRig(t)
	if err := r.command("call", "door", ""); err != nil {
		t.Fatal(err)
	}
	r.event("telegram_call_waiting")
	r.telegram.set(func() { r.telegram.others = []person{ana} })
	r.event("telegram_call_answered")

	r.stop()
	<-r.stopped
	ended, ok := r.find("telegram_call_ended")
	if !ok || ended.Reason != "shutdown" {
		t.Fatalf("expected the call to be over by the time the plugin stopped, got %+v (found=%v)", ended, ok)
	}
	if !r.telegram.did("left") || !r.telegram.did("ended") {
		t.Fatalf("the voice chat was not left and closed: %v", r.telegram.log)
	}
}

func TestSettingsAreChecked(t *testing.T) {
	for name, settings := range map[string]string{
		"no api id":     "api_hash: h",
		"no api hash":   "api_id: 1",
		"ring too long": "api_id: 1\napi_hash: h\nring_seconds: 5000",
		"unknown key":   "api_id: 1\napi_hash: h\ncolour: red",
		"empty session": "api_id: 1\napi_hash: h\nsession_file: ''",
		"negative ring": "api_id: 1\napi_hash: h\nring_seconds: -1",
	} {
		if _, err := New(pluginConfig(t, settings), plugin.Host{}); err == nil {
			t.Errorf("%s: expected the settings to be refused", name)
		}
	}
}

// The mixdown plays speakers together, each after a short cushion, and hands
// nothing on when nobody speaks.
func TestMixdownSumsSpeakers(t *testing.T) {
	m := newMixdown()
	var out [][]int16
	m.to(func(pcm []int16) { out = append(out, pcm) })

	flat := func(v int16) []int16 {
		pcm := make([]int16, frameSamples)
		for i := range pcm {
			pcm[i] = v
		}
		return pcm
	}
	m.tick()
	if len(out) != 0 {
		t.Fatal("a frame came out of silence")
	}
	m.push(1, flat(1000))
	m.tick()
	if len(out) != 0 {
		t.Fatal("a speaker was played before its cushion was there")
	}
	m.push(1, flat(1000))
	m.push(2, flat(200))
	m.push(2, flat(200))
	m.tick()
	if len(out) != 1 || out[0][0] != 1200 || out[0][frameSamples-1] != 1200 {
		t.Fatalf("expected the sum of both speakers, got %v", out)
	}

	// Loud speakers together are clamped, not wrapped.
	m.push(1, flat(30000))
	m.push(2, flat(30000))
	m.tick()
	m.tick()
	if got := out[len(out)-1][0]; got != math.MaxInt16 {
		t.Fatalf("expected the sum to be clamped, got %d", got)
	}

	// A stream that ended is forgotten; one that stalls builds no lasting delay.
	m.drop(2)
	for i := 0; i < 100; i++ {
		m.push(1, flat(5))
	}
	m.mu.Lock()
	waiting, known := len(m.sources[1].buf), len(m.sources)
	m.mu.Unlock()
	if known != 1 || waiting > pcmRate*sourceMaxMs/1000 {
		t.Fatalf("expected one speaker with at most %d ms waiting, got %d speakers and %d samples", sourceMaxMs, known, waiting)
	}

	// With nobody listening, nothing queues up.
	m.to(nil)
	m.push(3, flat(1))
	m.mu.Lock()
	_, queued := m.sources[3]
	m.mu.Unlock()
	if queued {
		t.Fatal("audio was queued with nobody listening")
	}
}
