package intercom

import (
	"context"
	"sync"
	"testing"
	"time"

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

func (c *eventCollector) find(name string) (Event, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ev := range c.events {
		if ev.Event == name {
			return ev, true
		}
	}
	return Event{}, false
}

// testRig wires a brain with two stub audio nodes and an intercom engine.
type testRig struct {
	brain  *brain.Brain
	engine *Engine
	events *eventCollector
	stubA  *audionode.Stub
	stubB  *audionode.Stub
	nodeA  *node.PhysicalNode
	nodeB  *node.PhysicalNode
}

func newTestRig(t *testing.T, cfg config.IntercomConfig) *testRig {
	t.Helper()
	b := brain.New(nil)
	stubA, stubB := &audionode.Stub{}, &audionode.Stub{}
	pA := node.NewPhysicalNode("A", "local")
	pB := node.NewPhysicalNode("B", "local")
	b.RegisterNode("A", &brain.NodeHandle{Node: pA, Audio: stubA, Config: config.NodeConfig{NodeID: "A", Type: "local"}})
	b.RegisterNode("B", &brain.NodeHandle{Node: pB, Audio: stubB, Config: config.NodeConfig{NodeID: "B", Type: "local"}})

	ec := &eventCollector{}
	eng := NewEngine(b, cfg, ec.emit, nil) // nil AEC: verbatim bridge
	return &testRig{brain: b, engine: eng, events: ec, stubA: stubA, stubB: stubB, nodeA: pA, nodeB: pB}
}

func waitForState(t *testing.T, n *node.PhysicalNode, want node.NodeState, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n.GetState() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("node %s did not reach state %s (still %s)", n.NodeID, want, n.GetState())
}

func waitForEvent(t *testing.T, ec *eventCollector, name string, timeout time.Duration) Event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ev, ok := ec.find(name); ok {
			return ev
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("event %q not observed within %v", name, timeout)
	return Event{}
}

// TestIntercomBridgesMicToPeer verifies A's mic reaches B's speaker and the call
// ends on its timeout deadline.
func TestIntercomBridgesMicToPeer(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 5})

	if err := rig.engine.Start([]string{"A", "B"}, 300*time.Millisecond); err != nil {
		t.Fatalf("Start failed: %v", err)
	}

	// Once both nodes are INTERCOM the taps are installed and streaming.
	waitForState(t, rig.nodeA, node.StateIntercom, time.Second)
	waitForState(t, rig.nodeB, node.StateIntercom, time.Second)

	micA := []float32{0.1, -0.2, 0.3, -0.4, 0.5}
	rig.brain.ForwardRawAudio("A", micA)

	ended := waitForEvent(t, rig.events, "intercom_ended", 2*time.Second)
	if ended.Reason != "timeout" {
		t.Fatalf("expected reason timeout, got %q", ended.Reason)
	}
	if ended.NodeID != "A" || ended.PeerNodeID != "B" {
		t.Fatalf("unexpected participants: %+v", ended)
	}

	// B's stub should have played A's mic audio (converted to PCM16).
	want := audio.FloatToPCM16(micA)
	if !stubPlayedContains(rig.stubB, want) {
		t.Fatalf("node B did not play A's mic audio; played=%v want-subseq=%v", flatten(rig.stubB), want)
	}
	// A's mix-minus feed (everyone but A) had no other talkers, so A played only
	// silence — never any leaked audio.
	if n := nonZeroCount(flatten(rig.stubA)); n != 0 {
		t.Fatalf("node A played %d non-silent samples unexpectedly", n)
	}

	// Both nodes return to IDLE after the call.
	waitForState(t, rig.nodeA, node.StateIdle, time.Second)
	waitForState(t, rig.nodeB, node.StateIdle, time.Second)
}

// TestIntercomGatesSpeak verifies a speak/play on a participating node is queued
// until the intercom call ends.
func TestIntercomGatesSpeak(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 5})

	if err := rig.engine.Start([]string{"A", "B"}, 250*time.Millisecond); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	waitForState(t, rig.nodeA, node.StateIntercom, time.Second)

	// PlaySamples acquires node A's turn slot, which the intercom call holds.
	played := make(chan error, 1)
	go func() {
		played <- rig.brain.PlaySamples(context.Background(), "A", []int16{1, 2, 3}, 16000, 0)
	}()

	select {
	case <-played:
		t.Fatal("PlaySamples completed while intercom held node A")
	case <-time.After(120 * time.Millisecond):
		// Still blocked — correct.
	}

	// After the call times out and releases the slot, the play proceeds.
	waitForEvent(t, rig.events, "intercom_ended", 2*time.Second)
	select {
	case err := <-played:
		if err != nil {
			t.Fatalf("queued PlaySamples failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued PlaySamples did not proceed after intercom ended")
	}
}

// TestIntercomStop verifies an external stop ends the call early with the
// "stopped" reason.
func TestIntercomStop(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})

	if err := rig.engine.Start([]string{"A", "B"}, 30*time.Second); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	waitForState(t, rig.nodeA, node.StateIntercom, time.Second)

	if !rig.engine.Stop("B") {
		t.Fatal("Stop returned false for an active call")
	}
	ended := waitForEvent(t, rig.events, "intercom_ended", 2*time.Second)
	if ended.Reason != "stopped" {
		t.Fatalf("expected reason stopped, got %q", ended.Reason)
	}
}

// TestIntercomRejectsBusyNode verifies a node already in a call cannot join another.
func TestIntercomRejectsBusyNode(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})
	// Third node so the second Start has a valid partner.
	rig.brain.RegisterNode("C", &brain.NodeHandle{Node: node.NewPhysicalNode("C", "local"), Audio: &audionode.Stub{}, Config: config.NodeConfig{NodeID: "C", Type: "local"}})

	if err := rig.engine.Start([]string{"A", "B"}, 5*time.Second); err != nil {
		t.Fatalf("first Start failed: %v", err)
	}
	waitForState(t, rig.nodeA, node.StateIntercom, time.Second)

	if err := rig.engine.Start([]string{"A", "C"}, 5*time.Second); err == nil {
		t.Fatal("expected error starting a call with a node already in a call")
	}
	rig.engine.Stop("A")
	waitForEvent(t, rig.events, "intercom_ended", 2*time.Second)
}

func TestIntercomRejectsInvalid(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{DefaultTimeoutSeconds: 30})
	if err := rig.engine.Start([]string{"A", "A"}, time.Second); err == nil {
		t.Fatal("expected error for identical nodes")
	}
	if err := rig.engine.Start([]string{"A", "ghost"}, time.Second); err == nil {
		t.Fatal("expected error for unregistered node")
	}
}

// fakeCanceller is a test EchoCanceller that records use and can transform the
// near-end so tests can prove the AEC actually sits in the stream.
type fakeCanceller struct {
	mu        sync.Mutex
	captures  int
	transform func([]int16) []int16
}

func (f *fakeCanceller) Playback(_ []int16) {}
func (f *fakeCanceller) Capture(near []int16) []int16 {
	f.mu.Lock()
	f.captures++
	f.mu.Unlock()
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

	if err := rig.engine.Start([]string{"A", "B"}, 250*time.Millisecond); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	waitForState(t, rig.nodeB, node.StateIntercom, time.Second)

	micA := []float32{0.25, -0.25, 0.5}
	rig.brain.ForwardRawAudio("A", micA)
	waitForEvent(t, rig.events, "intercom_ended", 2*time.Second)

	if built != 0 {
		t.Fatalf("AEC factory was invoked %d times while disabled", built)
	}
	if !stubPlayedContains(rig.stubB, audio.FloatToPCM16(micA)) {
		t.Fatalf("disabled AEC did not bridge audio verbatim; played=%v", flatten(rig.stubB))
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
		return &fakeCanceller{transform: func(s []int16) []int16 {
			out := make([]int16, len(s))
			return out
		}}, nil
	}

	if err := rig.engine.Start([]string{"A", "B"}, 250*time.Millisecond); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	waitForState(t, rig.nodeB, node.StateIntercom, time.Second)

	rig.brain.ForwardRawAudio("A", []float32{0.4, 0.4, 0.4})
	waitForEvent(t, rig.events, "intercom_ended", 2*time.Second)

	if built != 2 {
		t.Fatalf("expected an AEC per node (2), built %d", built)
	}
	// The AEC zeroed A's mic, so B must have played only zeros (or nothing).
	for _, v := range flatten(rig.stubB) {
		if v != 0 {
			t.Fatalf("expected AEC-processed (zeroed) audio on B, got %v", flatten(rig.stubB))
		}
	}
}

// TestIntercomThreeNodeMixMinus verifies N-way mix-minus: with three nodes and
// only A talking, both B and C hear A while A (the only talker) hears silence.
func TestIntercomThreeNodeMixMinus(t *testing.T) {
	b := brain.New(nil)
	stubs := map[string]*audionode.Stub{"A": {}, "B": {}, "C": {}}
	pnodes := map[string]*node.PhysicalNode{}
	for _, id := range []string{"A", "B", "C"} {
		p := node.NewPhysicalNode(id, "local")
		pnodes[id] = p
		b.RegisterNode(id, &brain.NodeHandle{Node: p, Audio: stubs[id], Config: config.NodeConfig{NodeID: id, Type: "local"}})
	}

	ec := &eventCollector{}
	eng := NewEngine(b, config.IntercomConfig{DefaultTimeoutSeconds: 5}, ec.emit, nil)

	if err := eng.Start([]string{"A", "B", "C"}, 300*time.Millisecond); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	for _, id := range []string{"A", "B", "C"} {
		waitForState(t, pnodes[id], node.StateIntercom, time.Second)
	}

	micA := []float32{0.2, -0.3, 0.4, -0.5, 0.6}
	b.ForwardRawAudio("A", micA)

	ended := waitForEvent(t, ec, "intercom_ended", 2*time.Second)
	if len(ended.NodeIDs) != 3 {
		t.Fatalf("expected 3 participants in event, got %v", ended.NodeIDs)
	}

	want := audio.FloatToPCM16(micA)
	if !stubPlayedContains(stubs["B"], want) {
		t.Fatalf("node B did not hear A: %v", flatten(stubs["B"]))
	}
	if !stubPlayedContains(stubs["C"], want) {
		t.Fatalf("node C did not hear A: %v", flatten(stubs["C"]))
	}
	if n := nonZeroCount(flatten(stubs["A"])); n != 0 {
		t.Fatalf("node A (sole talker) heard %d non-silent samples", n)
	}
}

func TestClampAddInt16(t *testing.T) {
	cases := []struct{ a, b, want int16 }{
		{100, 200, 300},
		{-100, -200, -300},
		{30000, 30000, 32767},    // positive saturation
		{-30000, -30000, -32768}, // negative saturation
		{0, 0, 0},
	}
	for _, c := range cases {
		if got := clampAddInt16(c.a, c.b); got != c.want {
			t.Fatalf("clampAddInt16(%d,%d)=%d, want %d", c.a, c.b, got, c.want)
		}
	}
}

// nonZeroCount returns how many samples are non-silent.
func nonZeroCount(samples []int16) int {
	n := 0
	for _, v := range samples {
		if v != 0 {
			n++
		}
	}
	return n
}

// flatten concatenates all recorded stub playbacks into one slice.
func flatten(s *audionode.Stub) []int16 {
	var out []int16
	for _, buf := range s.Played {
		out = append(out, buf...)
	}
	return out
}

// stubPlayedContains reports whether want appears as a contiguous subsequence of
// the stub's flattened playback.
func stubPlayedContains(s *audionode.Stub, want []int16) bool {
	got := flatten(s)
	if len(want) == 0 {
		return true
	}
	for i := 0; i+len(want) <= len(got); i++ {
		match := true
		for j := range want {
			if got[i+j] != want[j] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
