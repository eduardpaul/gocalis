package webrtc

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// A first connect that fails (e.g. the host network is not up yet at boot) must
// not leave the node without audio for good: Connect hands over to the reconnect
// loop, which keeps trying in the background.
func TestConnectFailureKeepsRetrying(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there now: the dial is refused

	c, err := NewClient("ws://" + addr + "/api/ws?src=doorbell")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatalf("Connect returned %v; want nil and a background retry", err)
	}
	c.reconnectMu.Lock()
	reconnecting := c.reconnecting
	c.reconnectMu.Unlock()
	if !reconnecting {
		t.Fatal("no reconnect loop running after a failed first connect")
	}
}

func TestRouteBackoff(t *testing.T) {
	var b routeBackoff
	now := time.Unix(1000, 0)
	if !b.ready(now) {
		t.Fatal("a fresh backoff must allow an attempt")
	}
	fail := errors.New("route failed")
	want := []time.Duration{3 * time.Second, 6 * time.Second, 12 * time.Second, 24 * time.Second, 48 * time.Second, time.Minute, time.Minute}
	for i, w := range want {
		if got := b.done(fail, now); got != w {
			t.Fatalf("failure %d: wait %v, want %v", i+1, got, w)
		}
		if b.ready(now.Add(w - time.Millisecond)) {
			t.Fatalf("failure %d: ready before the wait elapsed", i+1)
		}
		if !b.ready(now.Add(w)) {
			t.Fatalf("failure %d: not ready after the wait", i+1)
		}
	}
	if got := b.done(nil, now); got != 0 || !b.ready(now) || b.failures != 0 {
		t.Fatalf("a success must reset the backoff (wait %v, failures %d)", got, b.failures)
	}
}

// The talkback watchdog must not touch the go2rtc route while the doorbell sends
// no audio.
func TestReceivingNeedsRecentAudio(t *testing.T) {
	c, err := NewClient("ws://127.0.0.1:1/api/ws?src=doorbell")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	now := time.Now()
	if c.receiving(now) {
		t.Fatal("receiving before any RTP arrived")
	}
	c.pcMu.Lock()
	gen := c.gen
	c.pcMu.Unlock()
	c.rtpGen.Store(gen)
	c.lastRTP.Store(now.Add(-time.Second).UnixNano())
	if !c.receiving(now) {
		t.Fatal("not receiving with RTP one second ago")
	}
	c.lastRTP.Store(now.Add(-rxStallTimeout - time.Second).UnixNano())
	if c.receiving(now) {
		t.Fatal("still receiving after the stall timeout")
	}
}
