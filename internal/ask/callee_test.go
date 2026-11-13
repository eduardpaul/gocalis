package ask

import (
	"context"
	"testing"
	"time"

	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/node"
)

// phoneRig is an ask engine over a single node somebody has to answer.
func phoneRig(t *testing.T, ringTime time.Duration) (*Engine, *audionode.PhoneStub, *node.PhysicalNode, *testASR) {
	t.Helper()
	b := brain.New(testTTS{})
	phone := &audionode.PhoneStub{RingTime: ringTime}
	p := node.NewPhysicalNode("phone", "phone")
	b.RegisterNode("phone", &brain.NodeHandle{Node: p, Audio: phone})
	t.Cleanup(func() {
		b.UnregisterNode("phone")
		p.Close()
	})
	asr := &testASR{}
	return NewEngine(b, asr, nil, config.SpeakerIDConfig{}), phone, p, asr
}

func waitState(t *testing.T, p *node.PhysicalNode, want node.NodeState) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for p.GetState() != want {
		if time.Now().After(deadline) {
			t.Fatalf("node never reached %s (state %s)", want, p.GetState())
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestAskRingsAndListensToWhoeverAnswers(t *testing.T) {
	e, phone, p, asr := phoneRig(t, 0)
	done := make(chan Result, 1)
	go func() {
		done <- e.Run(context.Background(), Config{NodeID: "phone", TTSText: "who is it?", VADTimeoutSeconds: 5, PostSpeechSilenceSeconds: 0.05})
	}()
	waitState(t, p, node.StateRinging)
	if len(phone.Played) != 0 {
		t.Fatal("asked before anybody answered")
	}
	phone.PickUp()
	waitState(t, p, node.StateListening)
	e.Brain.Sessions().FeedPCM("phone", []float32{0.2, 0.3, 0.2}, true)

	select {
	case result := <-done:
		if result.Status != "success" || result.Transcription != "heard speech" {
			t.Fatalf("result = %+v", result)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("ask did not finish")
	}
	if len(asr.samples) != 3 {
		t.Fatalf("ASR got %d samples", len(asr.samples))
	}
	if phone.Holds() != 0 || phone.HungUpBy() != 0 {
		t.Fatalf("after the ask: %d lines out, hung up by %d", phone.Holds(), phone.HungUpBy())
	}
}

func TestAskReportsThatNobodyAnswered(t *testing.T) {
	e, phone, p, asr := phoneRig(t, 40*time.Millisecond)
	result := e.Run(context.Background(), Config{NodeID: "phone", TTSText: "who is it?"})
	if result.Status != "unanswered" {
		t.Fatalf("result = %+v, want unanswered", result)
	}
	if len(phone.Played) != 0 || len(asr.samples) != 0 {
		t.Fatal("an unanswered ask played or transcribed something")
	}
	if p.GetState() != node.StateIdle {
		t.Fatalf("state = %s", p.GetState())
	}
}

func TestAskReportsAHangUp(t *testing.T) {
	e, phone, p, asr := phoneRig(t, 0)
	phone.PickUp()
	done := make(chan Result, 1)
	go func() {
		done <- e.Run(context.Background(), Config{NodeID: "phone", VADTimeoutSeconds: 5})
	}()
	waitState(t, p, node.StateListening)
	phone.HangUp()
	select {
	case result := <-done:
		if result.Status != "hung_up" {
			t.Fatalf("result = %+v, want hung_up", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ask went on listening to nobody")
	}
	if len(asr.samples) != 0 {
		t.Fatal("ASR ran on nothing")
	}
}

func TestAnAnswerGivenBeforeHangingUpCounts(t *testing.T) {
	e, phone, p, asr := phoneRig(t, 0)
	phone.PickUp()
	done := make(chan Result, 1)
	go func() {
		// The long silence would keep the ask listening: the hang-up ends it.
		done <- e.Run(context.Background(), Config{NodeID: "phone", VADTimeoutSeconds: 5, PostSpeechSilenceSeconds: 5})
	}()
	waitState(t, p, node.StateListening)
	e.Brain.Sessions().FeedPCM("phone", []float32{0.2, 0.3}, true)
	phone.HangUp()
	select {
	case result := <-done:
		if result.Status != "success" || result.Transcription != "heard speech" {
			t.Fatalf("result = %+v, want the answer", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ask did not end with the hang-up")
	}
	if len(asr.samples) != 2 {
		t.Fatalf("ASR got %d samples", len(asr.samples))
	}
}
