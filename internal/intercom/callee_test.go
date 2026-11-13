package intercom

import (
	"sync"
	"testing"
	"time"

	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/node"
)

// addPhone registers node P, which somebody has to answer, and records every
// state node A goes through.
func (rig *testRig) addPhone(t *testing.T, ringTime time.Duration) (*audionode.PhoneStub, func() []node.NodeState) {
	t.Helper()
	phone := &audionode.PhoneStub{RingTime: ringTime}
	rig.nodes["P"] = node.NewPhysicalNode("P", "phone")
	t.Cleanup(rig.nodes["P"].Close)
	rig.brain.RegisterNode("P", &brain.NodeHandle{Node: rig.nodes["P"], Audio: phone, Config: config.NodeConfig{NodeID: "P"}})

	var mu sync.Mutex
	var states []node.NodeState
	rig.nodes["A"].OnStateChanged(func(_, now node.NodeState) {
		mu.Lock()
		states = append(states, now)
		mu.Unlock()
	})
	return phone, func() []node.NodeState {
		mu.Lock()
		defer mu.Unlock()
		return append([]node.NodeState(nil), states...)
	}
}

func (rig *testRig) never(t *testing.T, name, participant string) {
	t.Helper()
	if ev, ok := rig.events.find(name, participant); ok {
		t.Fatalf("unexpected event %+v", ev)
	}
}

func TestACallStartsWhenThePhoneIsAnswered(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{})
	phone, statesOfA := rig.addPhone(t, 0)

	if err := rig.engine.Connect([]string{"A", "P"}, 0); err != nil {
		t.Fatal(err)
	}
	ringing := rig.waitForEvent(t, "intercom_ringing", "")
	if len(ringing.NodeIDs) != 2 {
		t.Fatalf("ringing event lists %v", ringing.NodeIDs)
	}
	rig.waitForState(t, "P", node.StateRinging)
	time.Sleep(50 * time.Millisecond)
	rig.never(t, "intercom_started", "")
	if got := statesOfA(); len(got) != 0 {
		t.Fatalf("node A was touched while the phone rang: %v", got)
	}

	phone.PickUp()
	rig.waitForEvent(t, "intercom_started", "")
	rig.waitForState(t, "A", node.StateIntercom)
	rig.waitForState(t, "P", node.StateIntercom)

	phone.HangUp()
	left := rig.waitForEvent(t, "intercom_left", "P")
	if left.Reason != "hung_up" || left.Status != "success" {
		t.Fatalf("left event = %+v, want reason hung_up", left)
	}
	ended := rig.waitForEvent(t, "intercom_ended", "")
	if ended.Reason != "left" || ended.Status != "success" {
		t.Fatalf("ended event = %+v", ended)
	}
	rig.waitForState(t, "A", node.StateIdle)
	rig.waitForState(t, "P", node.StateIdle)
}

func TestACallNobodyAnswersNeverStarts(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{})
	_, statesOfA := rig.addPhone(t, 60*time.Millisecond)

	if err := rig.engine.Connect([]string{"A", "P"}, 0); err != nil {
		t.Fatal(err)
	}
	rig.waitForEvent(t, "intercom_ringing", "")
	left := rig.waitForEvent(t, "intercom_left", "P")
	if left.Reason != "unanswered" || left.Status != "success" {
		t.Fatalf("left event = %+v, want reason unanswered", left)
	}
	ended := rig.waitForEvent(t, "intercom_ended", "")
	if ended.Reason != "unanswered" || ended.Status != "success" {
		t.Fatalf("ended event = %+v, want reason unanswered", ended)
	}
	rig.never(t, "intercom_started", "")
	if got := statesOfA(); len(got) != 0 {
		t.Fatalf("node A was touched by a call nobody answered: %v", got)
	}
	rig.waitForState(t, "P", node.StateIdle)

	// Both are free for a new call.
	if err := rig.engine.Connect([]string{"A", "B"}, 0); err != nil {
		t.Fatal(err)
	}
	rig.waitForState(t, "A", node.StateIntercom)
}

func TestACallTimeRunsFromTheAnswer(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{})
	phone, _ := rig.addPhone(t, 0)

	if err := rig.engine.Connect([]string{"A", "P"}, 400*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	rig.waitForEvent(t, "intercom_ringing", "")
	time.Sleep(300 * time.Millisecond)
	phone.PickUp()
	started := rig.waitForEvent(t, "intercom_started", "")
	if started.RemainingSeconds < 0.3 {
		t.Fatalf("the ringing ate the call: %.2fs left at the start", started.RemainingSeconds)
	}
	ended := rig.waitForEvent(t, "intercom_ended", "")
	if ended.Reason != "timeout" {
		t.Fatalf("ended event = %+v, want timeout", ended)
	}
	if ended.DurationSeconds > 0.7 {
		t.Fatalf("the call counts the ringing: lasted %.2fs", ended.DurationSeconds)
	}
	if phone.HungUpBy() != 1 {
		t.Fatalf("the call did not hang up the phone (hung up by %d)", phone.HungUpBy())
	}
}

func TestStoppingACallThatRings(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{})
	phone, statesOfA := rig.addPhone(t, 0)

	if err := rig.engine.Connect([]string{"A", "P"}, 0); err != nil {
		t.Fatal(err)
	}
	rig.waitForState(t, "P", node.StateRinging)
	if !rig.engine.Stop("A") {
		t.Fatal("node A is not in the ringing call")
	}
	ended := rig.waitForEvent(t, "intercom_ended", "")
	if ended.Reason != "stopped" {
		t.Fatalf("ended event = %+v, want stopped", ended)
	}
	rig.waitForState(t, "P", node.StateIdle)
	if phone.Ringing() {
		t.Fatal("the phone still rings")
	}
	if got := statesOfA(); len(got) != 0 {
		t.Fatalf("node A was touched: %v", got)
	}
}

func TestAPhoneJoinsARunningCall(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{})
	phone, _ := rig.addPhone(t, 0)

	if err := rig.engine.Connect([]string{"A", "B"}, 0); err != nil {
		t.Fatal(err)
	}
	rig.waitForEvent(t, "intercom_started", "")
	rig.waitForState(t, "B", node.StateIntercom)

	if err := rig.engine.Connect([]string{"A", "P"}, 0); err != nil {
		t.Fatal(err)
	}
	rig.waitForEvent(t, "intercom_ringing", "P")
	time.Sleep(30 * time.Millisecond)
	rig.never(t, "intercom_joined", "P")

	phone.PickUp()
	joined := rig.waitForEvent(t, "intercom_joined", "P")
	if len(joined.NodeIDs) != 3 {
		t.Fatalf("joined event lists %v", joined.NodeIDs)
	}
	rig.waitForState(t, "P", node.StateIntercom)

	// The phone hangs up; the others go on.
	phone.HangUp()
	rig.waitForEvent(t, "intercom_left", "P")
	time.Sleep(30 * time.Millisecond)
	rig.never(t, "intercom_ended", "")
	rig.waitForState(t, "A", node.StateIntercom)
}

func TestAPhoneNobodyAnswersLeavesARunningCallAlone(t *testing.T) {
	rig := newTestRig(t, config.IntercomConfig{})
	rig.addPhone(t, 60*time.Millisecond)

	if err := rig.engine.Connect([]string{"A", "B"}, 0); err != nil {
		t.Fatal(err)
	}
	rig.waitForState(t, "B", node.StateIntercom)
	if err := rig.engine.Connect([]string{"A", "P"}, 0); err != nil {
		t.Fatal(err)
	}
	left := rig.waitForEvent(t, "intercom_left", "P")
	if left.Reason != "unanswered" {
		t.Fatalf("left event = %+v, want unanswered", left)
	}
	time.Sleep(30 * time.Millisecond)
	rig.never(t, "intercom_ended", "")
	rig.never(t, "intercom_joined", "P")
	rig.waitForState(t, "A", node.StateIntercom)
}
