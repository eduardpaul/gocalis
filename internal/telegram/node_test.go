package telegram

import (
	"context"
	"sync"
	"testing"
	"time"

	"gocalis/internal/config"
)

// fakeCall is a controllable Call for exercising the node lifecycle without a
// real Telegram backend. Peer presence is gated on the peerReady channel.
type fakeCall struct {
	mu        sync.Mutex
	joined    int
	left      int
	writes    int
	written   int // total samples written
	onFrame   func(pcm48 []int16)
	joinErr   error
	peerReady chan struct{}
}

func newFakeCall() *fakeCall { return &fakeCall{peerReady: make(chan struct{})} }

func (f *fakeCall) Join(ctx context.Context) error {
	f.mu.Lock()
	f.joined++
	err := f.joinErr
	f.mu.Unlock()
	return err
}

func (f *fakeCall) WaitPeer(ctx context.Context) error {
	select {
	case <-f.peerReady:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (f *fakeCall) Write(pcm48 []int16) error {
	f.mu.Lock()
	f.writes++
	f.written += len(pcm48)
	f.mu.Unlock()
	return nil
}

func (f *fakeCall) OnFrame(fn func(pcm48 []int16)) {
	f.mu.Lock()
	f.onFrame = fn
	f.mu.Unlock()
}

func (f *fakeCall) Leave() error {
	f.mu.Lock()
	f.left++
	f.mu.Unlock()
	return nil
}

func (f *fakeCall) counts() (joined, left, writes, written int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.joined, f.left, f.writes, f.written
}

func (f *fakeCall) signalPeer() { close(f.peerReady) }

func (f *fakeCall) pushInbound(pcm48 []int16) {
	f.mu.Lock()
	cb := f.onFrame
	f.mu.Unlock()
	if cb != nil {
		cb(pcm48)
	}
}

// fakeManager hands out a preset call.
type fakeManager struct{ call Call }

func (m *fakeManager) NewCall(target Target) (Call, error) { return m.call, nil }
func (m *fakeManager) Close() error                        { return nil }

func newTestNode(t *testing.T, cfg config.TelegramNodeConfig, call Call, lim *Limiter) *TelegramNode {
	t.Helper()
	n, err := NewNode("tg_test", cfg, &fakeManager{call: call}, lim)
	if err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	return n
}

func TestOnDemandEnsureReadyPlacesCallThenPlays(t *testing.T) {
	fc := newFakeCall()
	fc.signalPeer() // peer already present
	n := newTestNode(t, config.TelegramNodeConfig{
		TargetType: "group", AutoWake: false,
		ReadyTimeoutSeconds: 2, IdleTimeoutSeconds: 10,
	}, fc, NewLimiter())
	if err := n.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer n.Close()

	// On-demand: no call until something targets the node.
	if joined, _, _, _ := fc.counts(); joined != 0 {
		t.Fatalf("expected no call before use, joined=%d", joined)
	}

	// 100ms of 16kHz audio -> resampled to 48kHz and paced out as frames.
	clip := make([]int16, 1600)
	if err := n.Play(context.Background(), clip, nodeRate); err != nil {
		t.Fatalf("Play: %v", err)
	}
	joined, _, writes, written := fc.counts()
	if joined != 1 {
		t.Fatalf("expected exactly one join, got %d", joined)
	}
	if writes == 0 || written < 4000 {
		t.Fatalf("expected ~4800 samples written across frames, got writes=%d written=%d", writes, written)
	}
}

func TestEnsureReadyBlocksUntilPeerJoins(t *testing.T) {
	fc := newFakeCall() // peer NOT ready yet
	n := newTestNode(t, config.TelegramNodeConfig{
		TargetType: "group", ReadyTimeoutSeconds: 2, IdleTimeoutSeconds: 10,
	}, fc, NewLimiter())
	_ = n.Connect(context.Background())
	defer n.Close()

	done := make(chan error, 1)
	go func() { done <- n.EnsureReady(context.Background()) }()

	select {
	case <-done:
		t.Fatal("EnsureReady returned before a peer joined")
	case <-time.After(150 * time.Millisecond):
	}

	fc.signalPeer()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("EnsureReady after peer joined: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("EnsureReady did not return after peer joined")
	}
	if joined, _, _, _ := fc.counts(); joined != 1 {
		t.Fatalf("expected one join, got %d", joined)
	}
}

func TestEnsureReadyTimesOutWithoutPeer(t *testing.T) {
	fc := newFakeCall() // peer never joins
	n := newTestNode(t, config.TelegramNodeConfig{
		TargetType: "group", ReadyTimeoutSeconds: 0.15, IdleTimeoutSeconds: 10,
	}, fc, NewLimiter())
	_ = n.Connect(context.Background())
	defer n.Close()

	if err := n.EnsureReady(context.Background()); err == nil {
		t.Fatal("expected EnsureReady to fail when no peer joins")
	}
	// A failed attempt must leave the transient call and reset to idle so a later
	// attempt can retry.
	if _, left, _, _ := fc.counts(); left == 0 {
		t.Fatal("expected the timed-out call to be left")
	}
}

func TestAutowakeConnectRoutesInboundToOnAudio(t *testing.T) {
	fc := newFakeCall()
	fc.signalPeer()
	n := newTestNode(t, config.TelegramNodeConfig{
		TargetType: "group", AutoWake: true,
		ReadyTimeoutSeconds: 2, IdleTimeoutSeconds: 10,
	}, fc, NewLimiter())

	var mu sync.Mutex
	var got []float32
	n.OnAudio(func(s []float32) { mu.Lock(); got = append(got, s...); mu.Unlock() })

	if err := n.Connect(context.Background()); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer n.Close()

	// Autowake places the call in the background; wait for it to join.
	waitFor(t, time.Second, func() bool { j, _, _, _ := fc.counts(); return j == 1 })

	// One 10ms 48kHz frame (480) -> 160 samples at 16kHz on the OnAudio path.
	fc.pushInbound(make([]int16, 480))
	waitFor(t, time.Second, func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 160 })
}

func TestOnDemandLeavesAfterIdle(t *testing.T) {
	fc := newFakeCall()
	fc.signalPeer()
	n := newTestNode(t, config.TelegramNodeConfig{
		TargetType: "group", ReadyTimeoutSeconds: 2, IdleTimeoutSeconds: 0.2,
	}, fc, NewLimiter())
	_ = n.Connect(context.Background())
	defer n.Close()

	if err := n.EnsureReady(context.Background()); err != nil {
		t.Fatalf("EnsureReady: %v", err)
	}
	// After the idle window with no activity the node leaves the call.
	waitFor(t, 3*time.Second, func() bool { _, left, _, _ := fc.counts(); return left >= 1 })
}

func TestSingle1to1CallSerialized(t *testing.T) {
	lim := NewLimiter()
	ca, cb := newFakeCall(), newFakeCall()
	ca.signalPeer()
	cb.signalPeer()
	na := newTestNode(t, config.TelegramNodeConfig{TargetType: "contact", ReadyTimeoutSeconds: 3, IdleTimeoutSeconds: 10}, ca, lim)
	nb := newTestNode(t, config.TelegramNodeConfig{TargetType: "contact", ReadyTimeoutSeconds: 3, IdleTimeoutSeconds: 10}, cb, lim)
	_ = na.Connect(context.Background())
	_ = nb.Connect(context.Background())
	defer nb.Close()

	if err := na.EnsureReady(context.Background()); err != nil {
		t.Fatalf("A EnsureReady: %v", err)
	}

	// B must wait: the single 1:1 slot is held by A.
	done := make(chan error, 1)
	go func() { done <- nb.EnsureReady(context.Background()) }()
	select {
	case <-done:
		t.Fatal("B became ready while A held the only 1:1 call slot")
	case <-time.After(150 * time.Millisecond):
	}

	// Releasing A frees the slot for B.
	na.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("B EnsureReady after A released: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("B did not acquire the 1:1 slot after A released it")
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", d)
}
