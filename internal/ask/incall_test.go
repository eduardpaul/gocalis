package ask

import (
	"context"
	"sync"
	"testing"
	"time"

	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/node"
)

// fakeLine is a node's line into a call: it records what is played for the node
// and whether the node is set aside.
type fakeLine struct {
	mu     sync.Mutex
	held   bool
	holds  int
	clips  [][]int16
	seat   context.Context
	played chan struct{} // receives after every clip
}

func (l *fakeLine) Play(context.Context, []int16, int, bool) error { return nil }

func (l *fakeLine) Hold(ctx context.Context) (brain.CallHold, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.held = true
	l.holds++
	hctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(l.seat, func() { cancel(context.Cause(l.seat)) })
	return &fakeHold{line: l, ctx: hctx, cancel: func() { stop(); cancel(nil) }}, nil
}

type fakeHold struct {
	line   *fakeLine
	ctx    context.Context
	cancel func()
}

func (h *fakeHold) Context() context.Context { return h.ctx }
func (h *fakeHold) Play(pcm []int16, _ int) error {
	if err := h.ctx.Err(); err != nil {
		return context.Cause(h.ctx)
	}
	h.line.mu.Lock()
	h.line.clips = append(h.line.clips, pcm)
	h.line.mu.Unlock()
	h.line.played <- struct{}{}
	return nil
}
func (h *fakeHold) Release() {
	h.line.mu.Lock()
	h.line.held = false
	h.line.mu.Unlock()
	h.cancel()
}

func inCallRig(t *testing.T) (*Engine, *brain.Brain, *testASR, *audionode.Stub, *node.PhysicalNode, *fakeLine, context.CancelCauseFunc) {
	t.Helper()
	b := brain.New(testTTS{})
	asr := &testASR{}
	p := node.NewPhysicalNode("test", "local")
	t.Cleanup(p.Close)
	stub := &audionode.Stub{}
	b.RegisterNode("test", &brain.NodeHandle{Node: p, Audio: stub})
	t.Cleanup(func() { b.UnregisterNode("test") })
	p.SetState(node.StateIntercom)
	seat, unseat := context.WithCancelCause(context.Background())
	line := &fakeLine{seat: seat, played: make(chan struct{}, 8)}
	b.EnterCall("test", func([]float32) {}, line)
	return NewEngine(b, asr, nil, config.SpeakerIDConfig{}), b, asr, stub, p, line, unseat
}

// TestAskInCallGoesThroughTheLine verifies an ask for a node in a call sets the
// node aside, prompts and chimes on its line only, captures what it says and
// puts it back in the call, without ever taking the node from the call.
func TestAskInCallGoesThroughTheLine(t *testing.T) {
	e, b, asr, stub, p, line, _ := inCallRig(t)

	// Speak once the prompt and its chime have played.
	go func() {
		<-line.played
		line.mu.Lock()
		held := line.held
		line.mu.Unlock()
		if !held {
			t.Error("the node was not set aside while the ask prompted it")
		}
		time.Sleep(20 * time.Millisecond)
		b.Sessions().FeedPCM("test", []float32{0.1, 0.2, 0.3}, true)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := e.Run(ctx, Config{NodeID: "test", TTSText: "prompt", VADTimeoutSeconds: 1, PostSpeechSilenceSeconds: 0.05, InCall: true})
	if result.Status != "success" || result.Transcription != "heard speech" || !result.InCall {
		t.Fatalf("result = %+v", result)
	}
	if len(asr.samples) != 3 {
		t.Fatalf("captured speech lost: %v", asr.samples)
	}
	line.mu.Lock()
	defer line.mu.Unlock()
	if line.holds != 1 || line.held {
		t.Fatalf("holds = %d, still held = %v", line.holds, line.held)
	}
	// The prompt (2 samples from the test TTS) leads the start chime; the stop
	// chime follows the capture.
	if len(line.clips) != 2 || line.clips[0][0] != 1 || line.clips[0][1] != 2 || len(line.clips[0]) <= 2 {
		t.Fatalf("clips on the line: %d", len(line.clips))
	}
	if len(stub.Played) != 0 {
		t.Fatal("the ask played on the node directly, behind the call's back")
	}
	if p.GetState() != node.StateIntercom {
		t.Fatalf("the ask changed the node's state to %s", p.GetState())
	}
}

// TestAskInCallEndsWhenTheNodeLeaves verifies an ask stops, saying why, when its
// node goes out of the call while it listens.
func TestAskInCallEndsWhenTheNodeLeaves(t *testing.T) {
	e, _, asr, _, _, line, unseat := inCallRig(t)
	go func() {
		<-line.played
		time.Sleep(30 * time.Millisecond)
		unseat(brain.ErrCallLeft)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := e.Run(ctx, Config{NodeID: "test", VADTimeoutSeconds: 2, InCall: true})
	if result.Status != "error" || result.ErrorMessage != brain.ErrCallLeft.Error() || !result.InCall {
		t.Fatalf("result = %+v", result)
	}
	if asr.samples != nil {
		t.Fatal("ASR ran for an ask that was cut off")
	}
}

// TestAskInCallNeedsTheOption verifies an ask without the option still fails for
// a node in a call, and that the option changes nothing for a node in none.
func TestAskInCallNeedsTheOption(t *testing.T) {
	e, b, _, stub, p, line, _ := inCallRig(t)
	b.LeaveCall("test")
	p.SetState(node.StateIdle)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result := e.Run(ctx, Config{NodeID: "test", VADTimeoutSeconds: 0.05, InCall: true})
	if result.Status != "silence_timeout" || result.InCall {
		t.Fatalf("result = %+v", result)
	}
	if line.holds != 0 || len(stub.Played) == 0 {
		t.Fatal("an ask outside a call did not run on the node itself")
	}
}
