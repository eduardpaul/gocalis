package brain

import (
	"context"
	"errors"
	"testing"
	"time"

	"gocalis/internal/audionode"
	"gocalis/internal/node"
)

// slowPhone is a phone whose playback lasts until its context ends.
type slowPhone struct{ audionode.PhoneStub }

func (p *slowPhone) PlayStream(ctx context.Context, _ audionode.PCM16Source) error {
	<-ctx.Done()
	return ctx.Err()
}

func registerPhone(t *testing.T, b *Brain, id string, audio audionode.AudioNode) *node.PhysicalNode {
	t.Helper()
	p := node.NewPhysicalNode(id, "phone")
	b.RegisterNode(id, &NodeHandle{Node: p, Audio: audio})
	t.Cleanup(func() {
		b.UnregisterNode(id)
		p.Close()
	})
	return p
}

func eventually(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("never happened: %s", what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestSpeakRingsANodeSomebodyHasToAnswer(t *testing.T) {
	b := New(&countingTTS{})
	phone := &audionode.PhoneStub{}
	p := registerPhone(t, b, "phone", phone)

	done := make(chan error, 1)
	go func() { done <- b.Speak(context.Background(), "phone", "hello", 0, CallReject) }()
	eventually(t, "the phone rings", phone.Ringing)
	eventually(t, "the node is RINGING", func() bool { return p.GetState() == node.StateRinging })
	if len(phone.Played) != 0 {
		t.Fatal("spoke before anybody answered")
	}

	phone.PickUp()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(phone.Played) != 1 {
		t.Fatalf("played %d utterances, want 1", len(phone.Played))
	}
	if phone.Holds() != 0 || phone.HungUpBy() != 0 {
		t.Fatalf("after the turn: %d lines out, %d hung up; want the line let go without hanging up", phone.Holds(), phone.HungUpBy())
	}
	if p.GetState() != node.StateIdle {
		t.Fatalf("state after the turn = %s", p.GetState())
	}

	// Whoever answered is still there: the next turn does not ring.
	if err := b.Speak(context.Background(), "phone", "and again", 0, CallReject); err != nil {
		t.Fatal(err)
	}
	if phone.Rings() != 1 {
		t.Fatalf("rang %d times, want 1", phone.Rings())
	}
}

func TestSpeakFailsWhenNobodyAnswers(t *testing.T) {
	b := New(&countingTTS{})
	phone := &audionode.PhoneStub{RingTime: 30 * time.Millisecond}
	p := registerPhone(t, b, "phone", phone)

	err := b.Speak(context.Background(), "phone", "hello", 0, CallReject)
	if !errors.Is(err, audionode.ErrUnanswered) {
		t.Fatalf("speak = %v, want ErrUnanswered", err)
	}
	if len(phone.Played) != 0 {
		t.Fatal("spoke to nobody")
	}
	if p.GetState() != node.StateIdle {
		t.Fatalf("state = %s, want IDLE", p.GetState())
	}
	// The node is free again.
	phone.PickUp()
	if err := b.PlaySamples(context.Background(), "phone", []int16{1, 2}, 16000, 0, CallReject); err != nil {
		t.Fatal(err)
	}
}

func TestHangingUpEndsTheTurn(t *testing.T) {
	b := New(&countingTTS{})
	phone := &slowPhone{}
	registerPhone(t, b, "phone", phone)
	phone.PickUp()

	done := make(chan error, 1)
	go func() { done <- b.Speak(context.Background(), "phone", "a long story", 0, CallReject) }()
	eventually(t, "the turn holds the line", func() bool { return phone.Holds() == 1 })
	phone.HangUp()
	select {
	case err := <-done:
		if !errors.Is(err, audionode.ErrHungUp) {
			t.Fatalf("speak = %v, want ErrHungUp", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the turn went on after the hang-up")
	}

	// Nobody is on the phone any more: the next turn rings again.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	turn, err := b.AcquireNode(ctx, "phone", 0)
	if err == nil {
		turn.Release()
		t.Fatal("a turn started with nobody on the phone and nobody answering")
	}
	if phone.Rings() != 1 {
		t.Fatalf("rang %d times after the hang-up, want 1", phone.Rings())
	}
}

func TestAnIntercomCallHangsUpWhenItIsOver(t *testing.T) {
	b := New(&countingTTS{})
	phone := &audionode.PhoneStub{}
	registerPhone(t, b, "phone", phone)
	phone.PickUp()

	turn, err := b.AcquireNodeIntercom(context.Background(), "phone")
	if err != nil {
		t.Fatal(err)
	}
	if turn.HungUp() {
		t.Fatal("hung up before the call")
	}
	turn.Release()
	if phone.HungUpBy() != 1 || phone.Answered() {
		t.Fatalf("after the call: hung up by %d turns, answered=%v", phone.HungUpBy(), phone.Answered())
	}
}

func TestABroadcastDoesNotRing(t *testing.T) {
	b := New(&countingTTS{})
	phone := &audionode.PhoneStub{}
	registerPhone(t, b, "phone", phone)
	speaker := &audionode.Stub{}
	registerPhone(t, b, "speaker", speaker)

	if err := b.SpeakAll(context.Background(), "dinner is ready", 0, CallReject); err != nil {
		t.Fatal(err)
	}
	if err := b.PlaySamplesAll(context.Background(), []int16{1, 2, 3}, 16000, 0, CallReject); err != nil {
		t.Fatal(err)
	}
	if len(speaker.Played) != 2 {
		t.Fatalf("the speaker played %d clips, want 2", len(speaker.Played))
	}
	if phone.Rings() != 0 || len(phone.Played) != 0 {
		t.Fatalf("the broadcast reached the phone (rings=%d, played=%d)", phone.Rings(), len(phone.Played))
	}
	for _, info := range b.ListNodes() {
		if want := map[string]string{"phone": "call", "speaker": "auto"}[info.NodeID]; info.Answer != want {
			t.Errorf("node %s answer = %q, want %q", info.NodeID, info.Answer, want)
		}
	}
}
