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

	"gocalis/internal/ai"
	"gocalis/internal/ask"
	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/intercom"
	"gocalis/internal/node"
	"gocalis/internal/plugin"
	"gocalis/internal/protocol"
	"gocalis/internal/runtime"
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

// The gocalis around the plugin in these tests speaks a 660 Hz tone for every
// text, takes anything loud for speech, and transcribes whatever it captured
// as "abre la puerta".
type toneTTS struct{}

func (toneTTS) SynthesizeToStream(context.Context, string, ai.JobOptions) (ai.AudioStream, error) {
	var pcm []int16
	for _, frame := range tone(660, 8000, 25) {
		pcm = append(pcm, frame...)
	}
	return audionode.NewSliceSource(pcm, pcmRate), nil
}
func (toneTTS) SynthesizeToFile(context.Context, string, string, ai.JobOptions) error { return nil }
func (toneTTS) Close()                                                                {}

type loudGate struct{}

func (loudGate) Speech(samples []float32) bool {
	for _, v := range samples {
		if v > 0.05 || v < -0.05 {
			return true
		}
	}
	return false
}
func (loudGate) Reset() {}
func (loudGate) Close() {}

type fixedASR struct {
	mu    sync.Mutex
	heard []float32
}

func (a *fixedASR) TranscribeSamples(_ context.Context, samples []float32, _ int, _ ai.JobOptions) (string, error) {
	a.mu.Lock()
	a.heard = append([]float32(nil), samples...)
	a.mu.Unlock()
	return "abre la puerta", nil
}
func (a *fixedASR) TranscribeFile(context.Context, string, ai.JobOptions) (string, error) {
	return "", nil
}
func (a *fixedASR) CreateStream() (ai.TranscriptionStream, error) { return nil, nil }
func (a *fixedASR) Close()                                        {}

// rig is a gocalis with a door node, the plugin's node "telegram", and a fake
// Telegram.
type rig struct {
	t        *testing.T
	brain    *brain.Brain
	door     *audionode.Stub
	doorNode *node.PhysicalNode
	engine   *intercom.Engine
	ask      *ask.Engine
	asr      *fixedASR
	telegram *fakeTelegram
	plugin   *Plugin
	offline  context.CancelFunc // loses the connection to Telegram
	stop     context.CancelFunc // shuts gocalis down
	stopped  chan struct{}

	mu       sync.Mutex
	events   []plugin.Event
	intercom []intercom.Event
	states   []string // the states the telegram node went through
	media    []*fakeMedia
}

// Publish records the node's state changes.
func (r *rig) Publish(ev protocol.Response) {
	if ev.Event == "state_changed" && ev.NodeID == "telegram" {
		r.mu.Lock()
		r.states = append(r.states, ev.State)
		r.mu.Unlock()
	}
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
	r := &rig{t: t, brain: brain.New(toneTTS{}), door: &audionode.Stub{}, asr: &fixedASR{}, telegram: newFakeTelegram(), stopped: make(chan struct{})}
	r.doorNode = node.NewPhysicalNode("door", "local")
	t.Cleanup(r.doorNode.Close)
	r.brain.RegisterNode("door", &brain.NodeHandle{Node: r.doorNode, Audio: r.door, Config: config.NodeConfig{NodeID: "door", Type: "local"}})
	r.engine = intercom.NewEngine(r.brain, config.IntercomConfig{DefaultTimeoutSeconds: 30}, func(ev intercom.Event) {
		r.mu.Lock()
		r.intercom = append(r.intercom, ev)
		r.mu.Unlock()
	}, nil)
	t.Cleanup(r.engine.Shutdown)
	r.ask = ask.NewEngine(r.brain, r.asr, nil, config.SpeakerIDConfig{})
	endpoints := runtime.NewEndpointsWithGate(r, r.brain, func() (runtime.SpeechGate, error) { return loudGate{}, nil })

	session := filepath.Join(t.TempDir(), "telegram.session")
	if err := os.WriteFile(session, []byte("{}"), 0o600); err != nil {
		t.Fatal(err)
	}
	p, err := New(pluginConfig(t, "api_id: 1\napi_hash: h\nchat: Casa\ntitle: Timbre\nring_seconds: 5\nidle_seconds: 60\nsession_file: "+session), plugin.Host{
		ID: "telegram",
		AddNode: func(audio audionode.AudioNode) (func(), error) {
			return endpoints.Add("telegram", "telegram_call", audio)
		},
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

// call is the intercom event with that name, once it happened.
func (r *rig) call(name string) intercom.Event {
	r.t.Helper()
	var found intercom.Event
	r.until("the intercom event "+name, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		for _, ev := range r.intercom {
			if ev.Event == name {
				found = ev
				return true
			}
		}
		return false
	})
	return found
}

func (r *rig) was(state string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Contains(r.states, state)
}

func (r *rig) conn() *fakeMedia {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.media[len(r.media)-1]
}

func (r *rig) voiceChats() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.media)
}

func (r *rig) played() []int16 {
	var out []int16
	for _, buf := range r.door.Played {
		out = append(out, buf...)
	}
	return out
}

func (r *rig) telegramState() node.NodeState {
	return r.brain.GetNodeHandle("telegram").Node.GetState()
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

// Saying something to the node rings the group: the voice chat opens and
// gocalis waits in it. Whoever comes in hears it; a second thing said soon
// after does not ring again, and with nothing more to say gocalis leaves.
func TestSayingRingsTheGroupAndSpeaksToWhoeverComesIn(t *testing.T) {
	r := newRig(t)
	r.plugin.idleFor = 300 * time.Millisecond

	said := make(chan error, 1)
	go func() {
		said <- r.brain.Speak(context.Background(), "telegram", "han llamado a la puerta", 0, brain.CallReject)
	}()

	ringing := r.event("call_ringing")
	if ringing.NodeID != "telegram" || ringing.Data["chat"] != "Casa" || ringing.Data["started_voice_chat"] != true || ringing.Data["ring_seconds"] != 5.0 {
		t.Fatalf("unexpected ringing event: %+v", ringing)
	}
	if !r.telegram.did("started:Timbre") || !r.telegram.did("joined:payload") {
		t.Fatalf("the voice chat was not started and joined: %v", r.telegram.log)
	}
	r.until("the node to be RINGING", func() bool { return r.was("RINGING") })
	time.Sleep(100 * time.Millisecond)
	if len(r.conn().got()) != 0 {
		t.Fatal("spoke into a voice chat with nobody in it")
	}
	if r.plugin.Answered() {
		t.Fatal("answered before anybody came in")
	}

	r.telegram.set(func() { r.telegram.others = []person{ana, luis} })
	if err := <-said; err != nil {
		t.Fatal(err)
	}
	answered := r.event("call_answered")
	if !slices.Equal(answered.Data["answered"].([]string), []string{"Ana", "Luis"}) {
		t.Fatalf("unexpected answered event: %+v", answered)
	}
	sent := r.conn().got()
	if len(sent) != 25*frameSamples || tonePower(sent, 660) < 4000 {
		t.Fatalf("the voice chat got %d samples with %.0f of the spoken tone", len(sent), tonePower(sent, 660))
	}

	// They are still there: no second ring.
	if err := r.brain.Speak(context.Background(), "telegram", "y otra cosa", 0, brain.CallReject); err != nil {
		t.Fatal(err)
	}
	if r.voiceChats() != 1 || len(r.conn().got()) != 50*frameSamples {
		t.Fatalf("the second sentence went wrong: %d voice chats, %d samples", r.voiceChats(), len(r.conn().got()))
	}

	// Nothing more to say: gocalis leaves and closes what it opened.
	ended := r.event("call_ended")
	if ended.Reason != "completed" || ended.Status != "success" || ended.Data["answered"] != true {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	if !r.telegram.did("left") || !r.telegram.did("ended") {
		t.Fatalf("the voice chat was not left and closed: %v", r.telegram.log)
	}
	if r.plugin.Answered() || r.telegramState() != node.StateIdle {
		t.Fatalf("after the call: answered=%v state=%s", r.plugin.Answered(), r.telegramState())
	}
}

func TestSayingFailsWhenNobodyComesIn(t *testing.T) {
	r := newRig(t)
	r.plugin.ringFor = 200 * time.Millisecond

	err := r.brain.Speak(context.Background(), "telegram", "hola", 0, brain.CallReject)
	if !errors.Is(err, audionode.ErrUnanswered) {
		t.Fatalf("speak = %v, want ErrUnanswered", err)
	}
	ended := r.event("call_ended")
	if ended.Reason != "unanswered" || ended.Status != "success" || ended.Data["answered"] != false {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	if !r.telegram.did("left") || !r.telegram.did("ended") {
		t.Fatalf("the voice chat was not left and closed: %v", r.telegram.log)
	}
	if r.telegramState() != node.StateIdle {
		t.Fatalf("state = %s", r.telegramState())
	}
}

// An ask rings, puts its question to whoever comes in and transcribes what
// they say in the voice chat.
func TestAskingHearsTheAnswerFromTheVoiceChat(t *testing.T) {
	r := newRig(t)
	done := make(chan ask.Result, 1)
	go func() {
		done <- r.ask.Run(context.Background(), ask.Config{NodeID: "telegram", TTSText: "quien es?", VADTimeoutSeconds: 5, PostSpeechSilenceSeconds: 0.2})
	}()
	r.event("call_ringing")
	r.telegram.set(func() { r.telegram.others = []person{ana} })
	r.until("the ask to listen", func() bool { return r.telegramState() == node.StateListening })
	if tonePower(r.conn().got(), 660) < 2000 {
		t.Fatal("the question was not put to the voice chat")
	}

	conn := r.conn()
	next := time.Now()
	for _, frame := range tone(300, 9000, 30) {
		conn.heard.push(1, frame)
		next = next.Add(20 * time.Millisecond)
		time.Sleep(time.Until(next))
	}
	select {
	case result := <-done:
		if result.Status != "success" || result.Transcription != "abre la puerta" {
			t.Fatalf("result = %+v", result)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the ask did not finish")
	}
	r.asr.mu.Lock()
	heard := audioPCM(r.asr.heard)
	r.asr.mu.Unlock()
	if tonePower(heard, 300) < 3000 {
		t.Fatalf("the transcriber got %d samples with %.0f of what Ana said", len(heard), tonePower(heard, 300))
	}
}

func audioPCM(samples []float32) []int16 {
	out := make([]int16, len(samples))
	for i, v := range samples {
		out[i] = int16(v * 32767)
	}
	return out
}

// The doorbell use: an intercom call between the door and the node rings the
// group and leaves the door alone; it starts when somebody comes in, both
// sides hear each other, and it is over when the people on Telegram are gone.
func TestAnIntercomCallStartsWhenSomebodyComesIn(t *testing.T) {
	r := newRig(t)
	if err := r.engine.Connect([]string{"door", "telegram"}, 0); err != nil {
		t.Fatal(err)
	}
	r.call("intercom_ringing")
	r.event("call_ringing")
	time.Sleep(150 * time.Millisecond)
	if state := r.doorNode.GetState(); state != node.StateIdle {
		t.Fatalf("the door is %s while nobody has answered", state)
	}

	r.telegram.set(func() { r.telegram.others = []person{ana, luis} })
	r.call("intercom_started")
	r.until("the door to be in the call", func() bool { return r.doorNode.GetState() == node.StateIntercom })
	r.until("the node to be in the call", func() bool { return r.telegramState() == node.StateIntercom })

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
	r.until("the voice chat to hear the door", func() bool { return tonePower(conn.got(), 440) > 1000 })

	// They leave: the call is over and everything is put back.
	r.telegram.set(func() { r.telegram.others = nil })
	left := r.call("intercom_left")
	if left.ParticipantID != "telegram" || left.Reason != "hung_up" {
		t.Fatalf("unexpected left event: %+v", left)
	}
	if ended := r.call("intercom_ended"); ended.Reason != "left" {
		t.Fatalf("unexpected intercom end: %+v", ended)
	}
	if ended := r.event("call_ended"); ended.Reason != "hung_up" || ended.Data["answered"] != true {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	r.until("the door to be free", func() bool { return r.doorNode.GetState() == node.StateIdle })
	heard := r.played()
	if tonePower(heard, 1000) < 500 || tonePower(heard, 600) < 500 {
		t.Fatalf("the door did not hear both people (%.0f and %.0f)", tonePower(heard, 1000), tonePower(heard, 600))
	}
	if tonePower(heard, 440) > 200 {
		t.Fatalf("the door heard itself (%.0f)", tonePower(heard, 440))
	}
	if !r.telegram.did("left") || !r.telegram.did("ended") {
		t.Fatalf("the voice chat was not left and closed: %v", r.telegram.log)
	}
}

func TestAnIntercomCallNobodyAnswers(t *testing.T) {
	r := newRig(t)
	r.plugin.ringFor = 200 * time.Millisecond
	if err := r.engine.Connect([]string{"door", "telegram"}, 0); err != nil {
		t.Fatal(err)
	}
	if ended := r.call("intercom_ended"); ended.Reason != "unanswered" || ended.Status != "success" {
		t.Fatalf("unexpected intercom end: %+v", ended)
	}
	if ended := r.event("call_ended"); ended.Reason != "unanswered" {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	if len(r.door.Played) != 0 || r.doorNode.GetState() != node.StateIdle {
		t.Fatal("the door was used by a call nobody answered")
	}
}

// When the intercom call is over gocalis hangs up at once: it does not sit in
// the voice chat as it does after something it said.
func TestTheEndOfAnIntercomCallHangsUp(t *testing.T) {
	r := newRig(t)
	r.telegram.running, r.telegram.others = true, []person{ana}
	if err := r.engine.Connect([]string{"door", "telegram"}, 0); err != nil {
		t.Fatal(err)
	}
	if ringing := r.event("call_ringing"); ringing.Data["started_voice_chat"] != false {
		t.Fatalf("unexpected ringing event: %+v", ringing)
	}
	r.until("the door to be in the call", func() bool { return r.doorNode.GetState() == node.StateIntercom })

	if !r.engine.Stop("door") {
		t.Fatal("the door is in no call")
	}
	if ended := r.event("call_ended"); ended.Reason != "completed" || ended.Data["answered"] != true {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	if !r.telegram.did("left") || r.telegram.did("ended") || r.telegram.did("started") {
		t.Fatalf("a voice chat that was already going must be left, not closed: %v", r.telegram.log)
	}
}

func TestClosingTheVoiceChatEndsTheCall(t *testing.T) {
	r := newRig(t)
	r.telegram.others = []person{ana}
	if err := r.engine.Connect([]string{"door", "telegram"}, 0); err != nil {
		t.Fatal(err)
	}
	r.until("the door to be in the call", func() bool { return r.doorNode.GetState() == node.StateIntercom })

	r.telegram.set(func() { r.telegram.running = false })
	if ended := r.event("call_ended"); ended.Reason != "closed" || ended.Status != "success" {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	if left := r.call("intercom_left"); left.ParticipantID != "telegram" || left.Reason != "hung_up" {
		t.Fatalf("unexpected left event: %+v", left)
	}
	r.until("the door to be free", func() bool { return r.doorNode.GetState() == node.StateIdle })
}

// "hangup" takes gocalis out at whatever point it is.
func TestHangupWhileRinging(t *testing.T) {
	r := newRig(t)
	if err := r.plugin.Command(context.Background(), plugin.Command{Name: "hangup"}); err == nil {
		t.Fatal("hangup with no call was accepted")
	}
	if err := r.plugin.Command(context.Background(), plugin.Command{Name: "call"}); err == nil || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("an unknown command was not refused: %v", err)
	}

	said := make(chan error, 1)
	go func() { said <- r.brain.Speak(context.Background(), "telegram", "hola", 0, brain.CallReject) }()
	r.event("call_ringing")
	if err := r.plugin.Command(context.Background(), plugin.Command{Name: "hangup"}); err != nil {
		t.Fatal(err)
	}
	if err := <-said; err == nil || errors.Is(err, audionode.ErrUnanswered) || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("speak = %v, want it to say the call was stopped", err)
	}
	if ended := r.event("call_ended"); ended.Reason != "stopped" || ended.Data["answered"] != false {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	if !r.telegram.did("left") || !r.telegram.did("ended") {
		t.Fatalf("the voice chat was not left and closed: %v", r.telegram.log)
	}
}

// A request that is given up while it rings does not leave the group ringing.
func TestGivingUpStopsTheRinging(t *testing.T) {
	r := newRig(t)
	ctx, cancel := context.WithCancel(context.Background())
	said := make(chan error, 1)
	go func() { said <- r.brain.Speak(ctx, "telegram", "hola", 0, brain.CallReject) }()
	r.event("call_ringing")
	cancel()
	if err := <-said; !errors.Is(err, context.Canceled) {
		t.Fatalf("speak = %v", err)
	}
	if ended := r.event("call_ended"); ended.Reason != "cancelled" {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
}

func TestFailuresReachWhoeverCalls(t *testing.T) {
	r := newRig(t)
	r.telegram.failOpen = errors.New("no group \"Casa\" among the account's chats")
	err := r.brain.Speak(context.Background(), "telegram", "hola", 0, brain.CallReject)
	if err == nil || !strings.Contains(err.Error(), "no group") {
		t.Fatalf("speak = %v, want the reason", err)
	}
	if ended := r.event("call_ended"); ended.Status != "error" || ended.Reason != "error" || !strings.Contains(ended.Message, "no group") {
		t.Fatalf("unexpected ended event: %+v", ended)
	}

	// Without Telegram the node says so.
	r.offline()
	r.until("the plugin to notice", func() bool { return !r.plugin.Status().Ready })
	err = r.brain.Speak(context.Background(), "telegram", "hola", 0, brain.CallReject)
	if err == nil || !strings.Contains(err.Error(), "Telegram is not available") {
		t.Fatalf("speak offline = %v", err)
	}
}

func TestRunWaitsForTheLogin(t *testing.T) {
	session := filepath.Join(t.TempDir(), "telegram.session")
	b := brain.New(toneTTS{})
	endpoints := runtime.NewEndpointsWithGate(protocol.NewMultiPublisher(), b, func() (runtime.SpeechGate, error) { return loudGate{}, nil })
	p, err := New(pluginConfig(t, "api_id: 1\napi_hash: h\nchat: Casa\nsession_file: "+session), plugin.Host{
		ID:      "tg",
		AddNode: func(audio audionode.AudioNode) (func(), error) { return endpoints.Add("tg", "telegram_call", audio) },
		Emit:    func(plugin.Event) {},
	})
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
	// The node is there, and says why it cannot be called.
	if err := b.Speak(context.Background(), "tg", "hola", 0, brain.CallReject); err == nil || !strings.Contains(err.Error(), "not logged in") {
		t.Fatalf("expected speaking without a login to say so, got %v", err)
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
	r.telegram.others = []person{ana}
	if err := r.engine.Connect([]string{"door", "telegram"}, 0); err != nil {
		t.Fatal(err)
	}
	r.event("call_answered")

	r.stop()
	<-r.stopped
	ended, ok := r.find("call_ended")
	if !ok || ended.Reason != "shutdown" {
		t.Fatalf("expected the call to be over by the time the plugin stopped, got %+v (found=%v)", ended, ok)
	}
	if !r.telegram.did("left") || !r.telegram.did("ended") {
		t.Fatalf("the voice chat was not left and closed: %v", r.telegram.log)
	}
	if r.brain.GetNodeHandle("telegram") != nil {
		t.Fatal("the node outlived the plugin")
	}
}

func TestSettingsAreChecked(t *testing.T) {
	for name, settings := range map[string]string{
		"no api id":     "api_hash: h",
		"no api hash":   "api_id: 1",
		"ring too long": "api_id: 1\napi_hash: h\nring_seconds: 5000",
		"idle too long": "api_id: 1\napi_hash: h\nidle_seconds: 5000",
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
// on silence when nobody speaks.
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
	if len(out) != 1 || len(out[0]) != frameSamples || out[0][0] != 0 {
		t.Fatal("silence did not come out as a frame of silence")
	}
	m.push(1, flat(1000))
	m.tick()
	if len(out) != 2 || out[1][0] != 0 {
		t.Fatal("a speaker was played before its cushion was there")
	}
	m.push(1, flat(1000))
	m.push(2, flat(200))
	m.push(2, flat(200))
	m.tick()
	if len(out) != 3 || out[2][0] != 1200 || out[2][frameSamples-1] != 1200 {
		t.Fatalf("expected the sum of both speakers, got %v", out[len(out)-1][:4])
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
