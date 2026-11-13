package intercom

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	"gocalis/internal/ai"
	"gocalis/internal/ask"
	"gocalis/internal/audio"
	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/node"
)

// eventCollector records intercom lifecycle events for assertions.
type eventCollector struct {
	mu     sync.Mutex
	events []Event
}

func (c *eventCollector) emit(ev Event) {
	c.mu.Lock()
	c.events = append(c.events, ev)
	c.mu.Unlock()
}

// find returns the first recorded event with that name about that participant
// ("" for any).
func (c *eventCollector) find(name, participant string) (Event, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ev := range c.events {
		if ev.Event == name && (participant == "" || ev.ParticipantID == participant) {
			return ev, true
		}
	}
	return Event{}, false
}

// testRig wires a brain with stub audio nodes A, B and C and an intercom engine
// whose chimes are silent, so tests can follow microphone audio verbatim.
type testRig struct {
	brain  *brain.Brain
	engine *Engine
	events *eventCollector
	stubs  map[string]*audionode.Stub
	nodes  map[string]*node.PhysicalNode
}

func newTestRig(t *testing.T, cfg config.IntercomConfig) *testRig {
	t.Helper()
	rig := &testRig{
		brain:  brain.New(nil),
		events: &eventCollector{},
		stubs:  map[string]*audionode.Stub{},
		nodes:  map[string]*node.PhysicalNode{},
	}
	for _, id := range []string{"A", "B", "C"} {
		rig.stubs[id] = &audionode.Stub{}
		rig.nodes[id] = node.NewPhysicalNode(id, "local")
		t.Cleanup(rig.nodes[id].Close)
		rig.brain.RegisterNode(id, &brain.NodeHandle{Node: rig.nodes[id], Audio: rig.stubs[id], Config: config.NodeConfig{NodeID: id, Type: "local"}})
	}
	rig.engine = NewEngine(rig.brain, cfg, rig.events.emit, nil) // nil AEC: verbatim bridge
	rig.engine.joinChime, rig.engine.leaveChime = nil, nil
	t.Cleanup(rig.engine.Shutdown)
	return rig
}

func (rig *testRig) waitForState(t *testing.T, id string, want node.NodeState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rig.nodes[id].GetState() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %s did not reach state %s (still %s)", id, want, rig.nodes[id].GetState())
}

func (rig *testRig) waitForEvent(t *testing.T, name, participant string) Event {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if ev, ok := rig.events.find(name, participant); ok {
			return ev
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("event %q (participant %q) not observed", name, participant)
	return Event{}
}

// played returns everything the node's speaker has finished playing.
func (rig *testRig) played(id string) []int16 {
	var out []int16
	for _, buf := range rig.stubs[id].Played {
		out = append(out, buf...)
	}
	return out
}

// tone returns n samples of a recognisable ramp starting at base.
func tone(base int16, n int) []int16 {
	out := make([]int16, n)
	for i := range out {
		out[i] = base + int16(i%7)
	}
	return out
}

// contains reports whether want appears as a contiguous run in got.
func contains(got, want []int16) bool {
	for i := 0; i+len(want) <= len(got); i++ {
		if slices.Equal(got[i:i+len(want)], want) {
			return true
		}
	}
	return len(want) == 0
}

// readRemote collects what the page is sent until stop reports true or the
// page is out of the call.
func readRemote(t *testing.T, r *Remote, stop func(got []int16) bool) []int16 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got []int16
	for {
		pcm, err := r.Read(ctx)
		got = append(got, pcm...)
		if stop != nil && stop(got) {
			return got
		}
		if errors.Is(err, io.EOF) {
			return got
		}
		if err != nil {
			t.Fatalf("remote read: %v (got %d samples)", err, len(got))
		}
	}
}

// TestIntercomBridgesMicToPeer verifies A's mic reaches B's speaker and the call
// ends on its timeout deadline.
func TestIntercomBridgesMicToPeer(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 5})

	if err := rig.engine.Connect([]string{"A", "B"}, 300*time.Millisecond); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	started := rig.waitForEvent(t, "intercom_started", "")
	if !slices.Equal(started.NodeIDs, []string{"A", "B"}) || started.RemainingSeconds <= 0 {
		t.Fatalf("unexpected started event: %+v", started)
	}

	// Once both nodes are INTERCOM the taps are installed and streaming.
	rig.waitForState(t, "A", node.StateIntercom)
	rig.waitForState(t, "B", node.StateIntercom)

	micA := []float32{0.1, -0.2, 0.3, -0.4, 0.5}
	rig.brain.ForwardRawAudio("A", micA)

	ended := rig.waitForEvent(t, "intercom_ended", "")
	if ended.Reason != "timeout" || ended.Status != "success" || !slices.Equal(ended.NodeIDs, []string{"A", "B"}) {
		t.Fatalf("unexpected ended event: %+v", ended)
	}

	// B's stub should have played A's mic audio (converted to PCM16).
	if want := audio.FloatToPCM16(micA); !contains(rig.played("B"), want) {
		t.Fatalf("node B did not play A's mic audio; played=%v want-subseq=%v", rig.played("B"), want)
	}
	// A's mix-minus feed (everyone but A) had no other talkers, so A played only
	// silence: never its own microphone.
	if slices.ContainsFunc(rig.played("A"), func(v int16) bool { return v != 0 }) {
		t.Fatal("node A heard itself")
	}

	// Both nodes return to IDLE after the call.
	rig.waitForState(t, "A", node.StateIdle)
	rig.waitForState(t, "B", node.StateIdle)
}

// TestIntercomRejectsSpeak verifies a speak/play on a participating node fails
// right away with ErrIntercomActive while the call holds it, and works again
// once the call is over.
func TestIntercomRejectsSpeak(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 5})

	if err := rig.engine.Connect([]string{"A", "B"}, 250*time.Millisecond); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	rig.waitForState(t, "A", node.StateIntercom)

	err := rig.brain.PlaySamples(context.Background(), "A", []int16{1, 2, 3}, 16000, 0, brain.CallReject)
	if !errors.Is(err, brain.ErrIntercomActive) {
		t.Fatalf("PlaySamples during the call = %v, want ErrIntercomActive", err)
	}
	if _, err := rig.brain.AcquireNode(context.Background(), "B", 1000); !errors.Is(err, brain.ErrIntercomActive) {
		t.Fatalf("AcquireNode during the call = %v, want ErrIntercomActive", err)
	}

	rig.waitForEvent(t, "intercom_ended", "")
	if err := rig.brain.PlaySamples(context.Background(), "A", []int16{1, 2, 3}, 16000, 0, brain.CallReject); err != nil {
		t.Fatalf("PlaySamples after the call: %v", err)
	}
}

// TestIntercomPreemptsTurn verifies a call cuts off the turn in progress (TTS
// or an ask flow) instead of waiting for it: the human voice wins.
func TestIntercomPreemptsTurn(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 5})

	turn, err := rig.brain.AcquireNode(context.Background(), "A", 10)
	if err != nil {
		t.Fatal(err)
	}
	endless := newBridgeBuffer(16000, 16000) // a TTS stream that never ends
	turnDone := make(chan error, 1)
	go func() {
		defer turn.Release()
		turnDone <- turn.PlayStream(turn.Context(), endless)
	}()

	if err := rig.engine.Connect([]string{"A", "B"}, 300*time.Millisecond); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	select {
	case <-turnDone:
	case <-time.After(time.Second):
		t.Fatal("the turn in progress was not preempted")
	}
	if !turn.Preempted() {
		t.Fatal("turn not marked preempted")
	}
	rig.waitForState(t, "A", node.StateIntercom)
	rig.waitForEvent(t, "intercom_ended", "")
}

// TestIntercomRemoteAndNode verifies a page and a node hear each other, and that
// the call ends when the page leaves.
func TestIntercomRemoteAndNode(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})

	var notified eventCollector
	remote := rig.engine.NewRemote(notified.emit)
	if err := rig.engine.ConnectRemote(remote, "A", false, 0); err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	started := rig.waitForEvent(t, "intercom_started", "")
	if !slices.Equal(started.NodeIDs, []string{"A"}) || !slices.Equal(started.RemoteIDs, []string{remote.ID}) {
		t.Fatalf("unexpected started event: %+v", started)
	}
	rig.waitForState(t, "A", node.StateIntercom)
	if rig.nodes["B"].GetState() != node.StateIdle {
		t.Fatalf("node B was drawn into the call: %s", rig.nodes["B"].GetState())
	}

	// The node's microphone reaches the page.
	micA := []float32{0.1, -0.2, 0.3, -0.4, 0.5}
	rig.brain.ForwardRawAudio("A", micA)
	want := audio.FloatToPCM16(micA)
	if got := readRemote(t, remote, func(got []int16) bool { return contains(got, want) }); !contains(got, want) {
		t.Fatal("the page did not hear node A")
	}

	// The page's microphone reaches the node, never the page itself.
	voice := tone(1000, 1600) // 100 ms, above the jitter cushion
	remote.Push(voice)
	time.Sleep(150 * time.Millisecond)

	if !rig.engine.Leave(remote.ID) {
		t.Fatal("Leave returned false for a participant")
	}
	select {
	case <-remote.Left():
	case <-time.After(5 * time.Second):
		t.Fatal("the page was not let go")
	}
	if tail := readRemote(t, remote, nil); slices.Contains(tail, voice[0]) {
		t.Fatal("the page heard itself")
	}
	if _, ok := notified.find("intercom_left", remote.ID); !ok {
		t.Fatal("the page was not told it left")
	}
	ended := rig.waitForEvent(t, "intercom_ended", "")
	if ended.Reason != "left" || !slices.Equal(ended.NodeIDs, []string{"A"}) || len(ended.RemoteIDs) != 0 {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
	if !contains(rig.played("A"), voice[:320]) {
		t.Fatal("node A did not play the page's voice")
	}
	rig.waitForState(t, "A", node.StateIdle)
	if err := rig.brain.PlaySamples(context.Background(), "A", []int16{1}, 16000, 0, brain.CallReject); err != nil {
		t.Fatalf("PlaySamples after the call: %v", err)
	}
}

// TestIntercomRemotePlaysNodeItself verifies a page that plays the node from its
// own viewer is not sent that node again, while it still gets everyone else.
func TestIntercomRemotePlaysNodeItself(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})

	viewer := rig.engine.NewRemote(nil)
	if err := rig.engine.ConnectRemote(viewer, "A", true, 0); err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	rig.waitForState(t, "A", node.StateIntercom)

	// A second page joins the same call instead of being refused.
	other := rig.engine.NewRemote(nil)
	if err := rig.engine.ConnectRemote(other, "A", true, 0); err != nil {
		t.Fatalf("second ConnectRemote failed: %v", err)
	}
	joined := rig.waitForEvent(t, "intercom_joined", other.ID)
	if !slices.Equal(joined.RemoteIDs, []string{viewer.ID, other.ID}) {
		t.Fatalf("unexpected joined event: %+v", joined)
	}

	doorbell := audio.FloatToPCM16([]float32{0.9})[0]
	rig.brain.ForwardRawAudio("A", []float32{0.9, 0.9, 0.9})
	voice := tone(2000, 1600)
	other.Push(voice)

	got := readRemote(t, viewer, func(got []int16) bool { return contains(got, voice[:320]) })
	if !contains(got, voice[:320]) {
		t.Fatal("the first page did not hear the second one")
	}
	if slices.Contains(got, doorbell) {
		t.Fatal("the page was sent the node it plays itself")
	}

	// One page leaves: the call goes on for the other one and the node.
	rig.engine.Leave(other.ID)
	left := rig.waitForEvent(t, "intercom_left", other.ID)
	if !slices.Equal(left.RemoteIDs, []string{viewer.ID}) || !slices.Equal(left.NodeIDs, []string{"A"}) {
		t.Fatalf("unexpected left event: %+v", left)
	}
	time.Sleep(50 * time.Millisecond)
	if _, ok := rig.events.find("intercom_ended", ""); ok {
		t.Fatal("the call ended although two participants remain")
	}
	if rig.nodes["A"].GetState() != node.StateIntercom {
		t.Fatal("node A dropped out of the call")
	}

	rig.engine.Leave(viewer.ID)
	if ended := rig.waitForEvent(t, "intercom_ended", ""); ended.Reason != "left" {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
}

// TestIntercomNodeJoinsAndLeaves verifies a node can join a running call and
// leave it again without ending it for the others.
func TestIntercomNodeJoinsAndLeaves(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})

	if err := rig.engine.Connect([]string{"A", "B"}, 0); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	rig.waitForState(t, "B", node.StateIntercom)

	// Naming a node of the running call brings the other ones into it.
	if err := rig.engine.Connect([]string{"B", "C"}, 0); err != nil {
		t.Fatalf("joining Connect failed: %v", err)
	}
	joined := rig.waitForEvent(t, "intercom_joined", "C")
	if !slices.Equal(joined.NodeIDs, []string{"A", "B", "C"}) {
		t.Fatalf("unexpected joined event: %+v", joined)
	}
	rig.waitForState(t, "C", node.StateIntercom)
	if err := rig.engine.Connect([]string{"A", "C"}, 0); err == nil {
		t.Fatal("expected an error connecting nodes that already share the call")
	}

	micA := []float32{0.2, -0.3, 0.4, -0.5, 0.6}
	rig.brain.ForwardRawAudio("A", micA)
	time.Sleep(100 * time.Millisecond)

	if !rig.engine.Leave("C") {
		t.Fatal("Leave returned false for a participant")
	}
	left := rig.waitForEvent(t, "intercom_left", "C")
	if !slices.Equal(left.NodeIDs, []string{"A", "B"}) {
		t.Fatalf("unexpected left event: %+v", left)
	}
	// C is free again and played what A said while it was in the call.
	rig.waitForState(t, "C", node.StateIdle)
	if err := rig.brain.PlaySamples(context.Background(), "C", []int16{1}, 16000, 0, brain.CallReject); err != nil {
		t.Fatalf("PlaySamples on the node that left: %v", err)
	}
	if !contains(rig.played("C"), audio.FloatToPCM16(micA)) {
		t.Fatal("node C did not hear A while it was in the call")
	}
	if rig.nodes["A"].GetState() != node.StateIntercom || rig.nodes["B"].GetState() != node.StateIntercom {
		t.Fatal("the call did not go on for A and B")
	}

	rig.engine.Stop("A")
	if ended := rig.waitForEvent(t, "intercom_ended", ""); ended.Reason != "stopped" || !slices.Equal(ended.NodeIDs, []string{"A", "B"}) {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
}

// TestIntercomJoinPushesDeadlineBack verifies a call lasts at least the asked
// duration from the last join.
func TestIntercomJoinPushesDeadlineBack(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})

	if err := rig.engine.Connect([]string{"A", "B"}, 200*time.Millisecond); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	rig.waitForState(t, "A", node.StateIntercom)
	if err := rig.engine.Connect([]string{"A", "C"}, 700*time.Millisecond); err != nil {
		t.Fatalf("joining Connect failed: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, ok := rig.events.find("intercom_ended", ""); ok {
		t.Fatal("the call ended on the deadline it had before the join")
	}
	ended := rig.waitForEvent(t, "intercom_ended", "")
	if ended.Reason != "timeout" || ended.DurationSeconds < 0.7 {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
}

// TestIntercomChimes verifies the call rings when someone joins or leaves, and
// when it ends.
func TestIntercomChimes(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})
	rig.engine.joinChime, rig.engine.leaveChime = tone(5000, 640), tone(-5000, 640)
	join, leave := rig.engine.joinChime, rig.engine.leaveChime

	if err := rig.engine.Connect([]string{"A", "B"}, 0); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	rig.waitForState(t, "B", node.StateIntercom)
	time.Sleep(100 * time.Millisecond) // the start chime plays out

	page := rig.engine.NewRemote(nil)
	if err := rig.engine.ConnectRemote(page, "A", false, 0); err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	if got := readRemote(t, page, func(got []int16) bool { return contains(got, join) }); !contains(got, join) {
		t.Fatal("the page did not hear the chime of its own join")
	}

	rig.engine.Leave("B")
	if got := readRemote(t, page, func(got []int16) bool { return contains(got, leave) }); !contains(got, leave) {
		t.Fatal("the page did not hear a node leave")
	}
	rig.waitForState(t, "B", node.StateIdle)
	// B heard the call start, the page join, and its own leave.
	b := rig.played("B")
	if i := slices.Index(b, join[0]); i < 0 || !contains(b[i+len(join):], join) {
		t.Fatal("node B did not hear two join chimes")
	}
	if !contains(b, leave) {
		t.Fatal("node B did not hear its own leave chime")
	}

	rig.engine.Stop("A")
	rig.waitForEvent(t, "intercom_ended", "")
	if !contains(rig.played("A"), leave) {
		t.Fatal("node A did not hear the call end")
	}
}

// TestIntercomPlaysInCall verifies a clip played for a node in a call goes
// through the call: to that node alone, or to everyone.
func TestIntercomPlaysInCall(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})
	if err := rig.engine.Connect([]string{"A", "B"}, 0); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	rig.waitForState(t, "B", node.StateIntercom)
	page := rig.engine.NewRemote(nil)
	if err := rig.engine.ConnectRemote(page, "A", false, 0); err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}

	private, public := tone(4000, 640), tone(-4000, 640)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := rig.brain.PlaySamples(ctx, "A", private, 16000, 0, brain.CallNode); err != nil {
		t.Fatalf("PlaySamples for the node in its call: %v", err)
	}
	if err := rig.brain.PlaySamples(ctx, "A", public, 16000, 0, brain.CallAll); err != nil {
		t.Fatalf("PlaySamples for the whole call: %v", err)
	}
	heard := readRemote(t, page, func(got []int16) bool { return contains(got, public) })
	if !contains(heard, public) || contains(heard, private[:20]) {
		t.Fatal("the page did not get exactly the clip that was for everyone")
	}

	rig.engine.Stop("A")
	rig.waitForEvent(t, "intercom_ended", "")
	if a := rig.played("A"); !contains(a, private) || !contains(a, public) {
		t.Fatal("node A did not hear both clips")
	}
	if b := rig.played("B"); contains(b, private[:20]) || !contains(b, public) {
		t.Fatal("node B did not get exactly the clip that was for everyone")
	}
	// Once out of the call the node plays by itself again.
	if rig.brain.CallLine("A") != nil {
		t.Fatal("node A kept its line after the call")
	}
	if err := rig.brain.PlaySamples(ctx, "A", private, 16000, 0, brain.CallNode); err != nil {
		t.Fatalf("PlaySamples after the call: %v", err)
	}
}

// TestIntercomHoldSetsNodeAside verifies a held node neither talks to the call
// nor hears it, still hears what is played for it, and is back in the
// conversation once released.
func TestIntercomHoldSetsNodeAside(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})
	if err := rig.engine.Connect([]string{"A", "B"}, 0); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	rig.waitForState(t, "A", node.StateIntercom)
	rig.waitForState(t, "B", node.StateIntercom)
	say := func(id string, level float32) int16 {
		rig.brain.ForwardRawAudio(id, []float32{level, level, level})
		time.Sleep(80 * time.Millisecond)
		return audio.FloatToPCM16([]float32{level})[0]
	}

	hold, err := rig.brain.CallLine("A").Hold(context.Background())
	if err != nil {
		t.Fatalf("Hold failed: %v", err)
	}
	secret := say("A", 0.3) // the command, for the assistant only
	missed := say("B", 0.5) // said to a node that is not listening
	cue := tone(6000, 320)  // the assistant's chime
	if err := hold.Play(cue, 16000); err != nil {
		t.Fatalf("Play on the hold: %v", err)
	}
	// A spoken reply waits for the ask to be over: the line is busy.
	waited := make(chan error, 1)
	go func() {
		waited <- rig.brain.PlaySamples(context.Background(), "A", tone(7000, 320), 16000, 0, brain.CallNode)
	}()
	select {
	case err := <-waited:
		t.Fatalf("a clip played on a held line (err=%v)", err)
	case <-time.After(100 * time.Millisecond):
	}
	hold.Release()
	hold.Release() // harmless
	if err := <-waited; err != nil {
		t.Fatalf("the clip that waited for the line: %v", err)
	}
	back := say("A", 0.7)

	// A hold ends with the node's place in the call.
	hold, err = rig.brain.CallLine("A").Hold(context.Background())
	if err != nil {
		t.Fatalf("second Hold failed: %v", err)
	}
	rig.engine.Stop("A")
	select {
	case <-hold.Context().Done():
	case <-time.After(2 * time.Second):
		t.Fatal("the hold outlived the node's place in the call")
	}
	if cause := context.Cause(hold.Context()); !errors.Is(cause, brain.ErrCallLeft) {
		t.Fatalf("hold ended with %v, want ErrCallLeft", cause)
	}
	hold.Release()

	rig.waitForEvent(t, "intercom_ended", "")
	a, b := rig.played("A"), rig.played("B")
	if slices.Contains(b, secret) {
		t.Fatal("the call heard a node that was set aside")
	}
	if slices.Contains(a, missed) {
		t.Fatal("a node that was set aside heard the call")
	}
	if !contains(a, cue) || !contains(a, tone(7000, 320)) {
		t.Fatal("the held node did not hear what was played for it")
	}
	if !slices.Contains(b, back) {
		t.Fatal("the node was not back in the conversation after the hold")
	}
}

// heardASR is a transcriber that records what it was given.
type heardASR struct{ samples []float32 }

func (s *heardASR) TranscribeSamples(_ context.Context, samples []float32, _ int, _ ai.JobOptions) (string, error) {
	s.samples = append([]float32(nil), samples...)
	return "cuelga", nil
}
func (*heardASR) TranscribeFile(context.Context, string, ai.JobOptions) (string, error) {
	return "", nil
}
func (*heardASR) CreateStream() (ai.TranscriptionStream, error) { return nil, nil }
func (*heardASR) Close()                                        {}

// TestIntercomAskInCall runs a whole ask inside a call, as the wake word does on
// a node with wake_in_call: the command reaches the assistant and not the call,
// and the node is back in the conversation afterwards.
func TestIntercomAskInCall(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})
	asr := &heardASR{}
	asker := ask.NewEngine(rig.brain, asr, nil, config.SpeakerIDConfig{})
	if err := rig.engine.Connect([]string{"A", "B"}, 0); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	rig.waitForState(t, "A", node.StateIntercom)
	rig.waitForState(t, "B", node.StateIntercom)

	// mic stands in for A's capture path: a node in a call sends its microphone
	// to the call and, while an ask listens to it, to that ask.
	mic := func(level float32, speech bool) int16 {
		chunk := []float32{level, level, level}
		rig.brain.ForwardRawAudio("A", chunk)
		rig.brain.Sessions().FeedPCM("A", chunk, speech)
		return audio.FloatToPCM16(chunk)[0]
	}
	done := make(chan ask.Result, 1)
	go func() {
		done <- asker.Run(context.Background(), ask.Config{NodeID: "A", VADTimeoutSeconds: 2, PostSpeechSilenceSeconds: 0.1, InCall: true})
	}()
	// The command: said for a while once the ask is under way, so that some of
	// it falls after the listening chime, then silence ends the turn.
	for rig.brain.Sessions().ActiveCount("A") == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	var command int16
	for until := time.Now().Add(700 * time.Millisecond); time.Now().Before(until); {
		command = mic(0.3, true)
		time.Sleep(20 * time.Millisecond)
	}
	var result ask.Result
	select {
	case result = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the ask did not finish")
	}
	if result.Status != "success" || result.Transcription != "cuelga" || !result.InCall {
		t.Fatalf("result = %+v", result)
	}
	if len(asr.samples) == 0 {
		t.Fatal("the command did not reach ASR")
	}
	back := mic(0.7, false)
	time.Sleep(80 * time.Millisecond)

	if rig.nodes["A"].GetState() != node.StateIntercom {
		t.Fatalf("the ask took node A out of the call: %s", rig.nodes["A"].GetState())
	}
	rig.engine.Stop("A")
	rig.waitForEvent(t, "intercom_ended", "")
	b := rig.played("B")
	if slices.Contains(b, command) {
		t.Fatal("the call heard the command")
	}
	if !slices.Contains(b, back) {
		t.Fatal("node A was not back in the call after the ask")
	}
	if !slices.ContainsFunc(rig.played("A"), func(v int16) bool { return v != 0 }) {
		t.Fatal("node A did not hear the ask's chimes")
	}
}

// TestIntercomStopImmediatelyAfterStart verifies Stop is safe before the call's
// background goroutine has run.
func TestIntercomStopImmediatelyAfterStart(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})

	if err := rig.engine.Connect([]string{"A", "B"}, 30*time.Second); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if !rig.engine.Stop("A") {
		t.Fatal("Stop returned false for a just-started call")
	}
	ended := rig.waitForEvent(t, "intercom_ended", "")
	if ended.Reason != "stopped" || ended.Status != "success" {
		t.Fatalf("expected stopped/success, got %q/%q", ended.Reason, ended.Status)
	}
	rig.waitForState(t, "A", node.StateIdle)
	if err := rig.brain.PlaySamples(context.Background(), "A", []int16{1}, 16000, 0, brain.CallReject); err != nil {
		t.Fatalf("PlaySamples after the call: %v", err)
	}
	if rig.engine.Stop("A") || rig.engine.Leave("A") {
		t.Fatal("Stop/Leave found a call that is over")
	}
}

// TestIntercomKeepsCallsApart verifies two calls cannot be merged by naming a
// node of each.
func TestIntercomKeepsCallsApart(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})

	if err := rig.engine.Connect([]string{"A", "B"}, 0); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	if err := rig.engine.ConnectRemote(rig.engine.NewRemote(nil), "C", false, 0); err != nil {
		t.Fatalf("ConnectRemote failed: %v", err)
	}
	if err := rig.engine.Connect([]string{"A", "C"}, 0); err == nil {
		t.Fatal("expected an error connecting nodes of two different calls")
	}
}

func TestIntercomRejectsInvalid(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})
	if err := rig.engine.Connect([]string{"A", "A"}, time.Second); err == nil {
		t.Fatal("expected error for identical nodes")
	}
	if err := rig.engine.Connect([]string{"A"}, time.Second); err == nil {
		t.Fatal("expected error for a single node")
	}
	if err := rig.engine.Connect([]string{"A", "ghost"}, time.Second); err == nil {
		t.Fatal("expected error for unregistered node")
	}
	if err := rig.engine.ConnectRemote(rig.engine.NewRemote(nil), "ghost", false, 0); err == nil {
		t.Fatal("expected error for a page calling an unregistered node")
	}
}

// failingNode is an audio node whose playback fails at once.
type failingNode struct{ audionode.Stub }

func (*failingNode) PlayStream(context.Context, audionode.PCM16Source) error {
	return errors.New("speaker unplugged")
}

// TestIntercomDropsFailingNode verifies a node whose playback fails is dropped
// from the call, which goes on for the others and ends in error only when that
// leaves a single participant.
func TestIntercomDropsFailingNode(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})
	broken := node.NewPhysicalNode("X", "local")
	t.Cleanup(broken.Close)
	rig.brain.RegisterNode("X", &brain.NodeHandle{Node: broken, Audio: &failingNode{}, Config: config.NodeConfig{NodeID: "X", Type: "local"}})

	if err := rig.engine.Connect([]string{"A", "B", "X"}, 0); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	left := rig.waitForEvent(t, "intercom_left", "X")
	if left.Status != "error" || left.Message == "" || !slices.Equal(left.NodeIDs, []string{"A", "B"}) {
		t.Fatalf("unexpected left event: %+v", left)
	}
	rig.waitForState(t, "A", node.StateIntercom)
	rig.engine.Stop("A")
	rig.waitForEvent(t, "intercom_ended", "")
	rig.waitForState(t, "A", node.StateIdle)

	rig.events = &eventCollector{}
	rig.engine.emit = rig.events.emit
	if err := rig.engine.Connect([]string{"A", "X"}, 0); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	ended := rig.waitForEvent(t, "intercom_ended", "")
	if ended.Reason != "error" || ended.Status != "error" || ended.Message == "" {
		t.Fatalf("unexpected ended event: %+v", ended)
	}
}

// fakeCanceller is a test EchoCanceller that can transform the near-end so
// tests can prove the AEC actually sits in the stream.
type fakeCanceller struct {
	transform func([]int16) []int16
}

func (f *fakeCanceller) Playback(_ []int16) {}
func (f *fakeCanceller) Capture(near []int16) []int16 {
	if f.transform != nil {
		return f.transform(near)
	}
	return near
}
func (f *fakeCanceller) Close() {}

// TestIntercomAECDisabledBypasses proves that with echo cancellation disabled the
// canceller factory is never invoked and audio is bridged verbatim, even when a
// factory is provided.
func TestIntercomAECDisabledBypasses(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 5}) // Enabled=false
	var built int
	rig.engine.newAEC = func(_, _, _ int) (EchoCanceller, error) {
		built++
		return &fakeCanceller{}, nil
	}

	if err := rig.engine.Connect([]string{"A", "B"}, 250*time.Millisecond); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	rig.waitForState(t, "B", node.StateIntercom)

	micA := []float32{0.25, -0.25, 0.5}
	rig.brain.ForwardRawAudio("A", micA)
	rig.waitForEvent(t, "intercom_ended", "")

	if built != 0 {
		t.Fatalf("AEC factory was invoked %d times while disabled", built)
	}
	if !contains(rig.played("B"), audio.FloatToPCM16(micA)) {
		t.Fatalf("disabled AEC did not bridge audio verbatim; played=%v", rig.played("B"))
	}
}

// TestIntercomAECEnabledEngaged proves that with echo cancellation enabled a
// per-node canceller is built and processes each node's mic before bridging.
func TestIntercomAECEnabledEngaged(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{
		DefaultTimeoutSeconds: 5,
		EchoCancellation:      config.IntercomAECConfig{Enabled: true, TailMs: 100, FrameMs: 20},
	})
	var built int
	// Transform zeroes the near-end so we can detect the AEC output in playback.
	rig.engine.newAEC = func(_, _, _ int) (EchoCanceller, error) {
		built++
		return &fakeCanceller{transform: func(s []int16) []int16 { return make([]int16, len(s)) }}, nil
	}

	if err := rig.engine.Connect([]string{"A", "B"}, 250*time.Millisecond); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	rig.waitForState(t, "B", node.StateIntercom)

	rig.brain.ForwardRawAudio("A", []float32{0.4, 0.4, 0.4})
	rig.waitForEvent(t, "intercom_ended", "")

	if built != 2 {
		t.Fatalf("expected an AEC per node (2), built %d", built)
	}
	// The AEC zeroed A's mic, so B must have played only zeros (or nothing).
	if slices.ContainsFunc(rig.played("B"), func(v int16) bool { return v != 0 }) {
		t.Fatalf("expected AEC-processed (zeroed) audio on B, got %v", rig.played("B"))
	}
}

// TestIntercomThreeNodeMixMinus verifies N-way mix-minus: with three nodes and
// only A talking, both B and C hear A while A (the only talker) hears silence.
func TestIntercomThreeNodeMixMinus(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 5})

	if err := rig.engine.Connect([]string{"A", "B", "C"}, 300*time.Millisecond); err != nil {
		t.Fatalf("Connect failed: %v", err)
	}
	for _, id := range []string{"A", "B", "C"} {
		rig.waitForState(t, id, node.StateIntercom)
	}

	micA := []float32{0.2, -0.3, 0.4, -0.5, 0.6}
	rig.brain.ForwardRawAudio("A", micA)

	ended := rig.waitForEvent(t, "intercom_ended", "")
	if len(ended.NodeIDs) != 3 {
		t.Fatalf("expected 3 participants in event, got %v", ended.NodeIDs)
	}

	want := audio.FloatToPCM16(micA)
	if !contains(rig.played("B"), want) {
		t.Fatalf("node B did not hear A: %v", rig.played("B"))
	}
	if !contains(rig.played("C"), want) {
		t.Fatalf("node C did not hear A: %v", rig.played("C"))
	}
	if slices.ContainsFunc(rig.played("A"), func(v int16) bool { return v != 0 }) {
		t.Fatal("node A (sole talker) heard itself")
	}
}

// TestMixerClampsAndSkips verifies the mix saturates instead of wrapping, that a
// listener never gets itself or the talker it skips, and that a clip reaches
// everyone once.
func TestMixerClampsAndSkips(t *testing.T) {
	m := newMixer(1000) // 20-sample frames
	loud := func(v int16) *bridge {
		b := newBridgeBuffer(1000, 1000)
		b.push(slices.Repeat([]int16{v}, 20))
		return b
	}
	m.addTalker("a", loud(30000))
	m.addTalker("b", loud(30000))
	m.addTalker("c", loud(-100))
	outs := map[string]*bridge{}
	for id, skip := range map[string]string{"a": "", "c": "", "page": "c"} {
		outs[id] = newBridgeBuffer(1000, 1000)
		m.addListener(id, outs[id], skip)
	}
	m.play([]int16{7, 7, 7}, "")
	m.tick()
	m.tick()

	first := func(id string) []int16 {
		got, err := outs[id].ReadPCM16(context.Background(), 20)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	// a hears b + c + clip; c hears a + b (saturated); the page hears a + b only.
	if got := first("a"); got[0] != 30000-100+7 || got[3] != 30000-100 {
		t.Fatalf("listener a got %v", got[:4])
	}
	if got := first("c"); got[0] != 32767 {
		t.Fatalf("listener c got %d, want saturation", got[0])
	}
	if got := first("page"); got[5] != 32767 {
		t.Fatalf("the page got %d, want a+b without the skipped talker", got[5])
	}
	// Second tick: the talkers ran dry and the clip is over.
	if got := first("a"); slices.ContainsFunc(got, func(v int16) bool { return v != 0 }) {
		t.Fatalf("second frame is not silent: %v", got)
	}
}
