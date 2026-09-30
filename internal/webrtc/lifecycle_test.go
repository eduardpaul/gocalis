package webrtc

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestClientCloseJoinsAudioCallback(t *testing.T) {
	c, err := NewClientWithConfig(Config{SignalingURL: "ws://localhost/api/ws"})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	c.OnAudio(func([]float32) { close(entered); <-release })
	go c.deliver([]float32{0.1})
	<-entered
	closed := make(chan struct{})
	go func() { _ = c.Close(); close(closed) }()
	<-c.ctx.Done()
	select {
	case <-closed:
		t.Fatal("Close returned during callback")
	default:
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not finish")
	}
	_ = c.Close()
	c.deliver([]float32{0.2}) // A stopped client never invokes the callback again.
}

func TestTalkbackBackpressureAndCancellation(t *testing.T) {
	sender := &talkbackSender{}
	sender.cond = sync.NewCond(&sender.mu)
	for i := 0; i < 100; i++ {
		if err := sender.enqueue(context.Background(), []byte{1}); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := sender.enqueue(ctx, []byte{2}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("enqueue = %v", err)
	}
	if len(sender.queue) != 100 {
		t.Fatalf("buffer grew to %d frames", len(sender.queue))
	}
	sender.clearPending()
	if len(sender.queue) != 0 {
		t.Fatal("canceled playback retained frames")
	}
}

func TestTalkbackWaitIncludesInFlightFrame(t *testing.T) {
	sender := &talkbackSender{inFlight: true}
	sender.cond = sync.NewCond(&sender.mu)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := sender.waitDrained(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait ignored in-flight audio: %v", err)
	}
	sender.mu.Lock()
	sender.inFlight = false
	sender.mu.Unlock()
	if err := sender.waitDrained(context.Background()); err != nil {
		t.Fatal(err)
	}
}
