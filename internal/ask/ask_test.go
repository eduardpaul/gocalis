package ask

import (
	"context"
	"errors"
	"testing"
	"time"

	"gocalis/internal/ai"
	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/node"
	"gocalis/internal/session"
)

type testTTS struct{ err error }

func (s testTTS) SynthesizeToStream(context.Context, string, ai.JobOptions) (ai.AudioStream, error) {
	if s.err != nil {
		return nil, s.err
	}
	return audionode.NewSliceSource([]int16{1, 2}, 16000), nil
}
func (s testTTS) SynthesizeToFile(context.Context, string, string, ai.JobOptions) error { return s.err }
func (s testTTS) Close()                                                                {}

type testASR struct{ samples []float32 }

func (s *testASR) TranscribeSamples(ctx context.Context, samples []float32, _ int, _ ai.JobOptions) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s.samples = append([]float32(nil), samples...)
	return "heard speech", nil
}
func (s *testASR) TranscribeFile(context.Context, string, ai.JobOptions) (string, error) {
	return "", nil
}
func (s *testASR) CreateStream() (ai.TranscriptionStream, error) { return nil, nil }
func (s *testASR) Close()                                        {}

type interruptedAudio struct {
	audionode.Stub
	feed   func([]float32)
	prompt bool
}

func (a *interruptedAudio) Play(ctx context.Context, _ []int16, _ int) error {
	if !a.prompt { // Feed speech during the prompt, then await playback-only cancellation.
		a.prompt = true
		a.feed([]float32{0.1, 0.2, 0.3})
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
			return errors.New("prompt was not interrupted")
		}
	}
	return ctx.Err()
}

func TestBargeInPreservesSpeechAndContinuesTurn(t *testing.T) {
	b := brain.New(testTTS{})
	asr := &testASR{}
	p := node.NewPhysicalNode("test", "local")
	defer p.Close()
	a := &interruptedAudio{feed: func(samples []float32) { b.FeedAudio("test", samples) }}
	b.RegisterNode("test", &brain.NodeHandle{Node: p, Audio: a})
	defer b.UnregisterNode("test")
	e := NewEngine(b, asr, nil, config.SpeakerIDConfig{})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	result := e.Run(ctx, Config{NodeID: "test", TTSText: "prompt", BargeIn: true, VADTimeoutSeconds: 0.3, PostSpeechSilenceSeconds: 0.02})
	if result.Status != "success" || result.Transcription != "heard speech" {
		t.Fatalf("result = %+v", result)
	}
	if len(asr.samples) != 3 || asr.samples[0] != 0.1 {
		t.Fatalf("triggering speech lost: %v", asr.samples)
	}
	if p.GetState() != node.StateIdle {
		t.Fatalf("terminal state = %s", p.GetState())
	}
	if b.Sessions().ActiveCount("test") != 0 {
		t.Fatal("session not deregistered")
	}
}

func TestPromptFailureReleasesNodeAndRestoresState(t *testing.T) {
	b := brain.New(testTTS{err: errors.New("synthesis failed")})
	p := node.NewPhysicalNode("test", "local")
	defer p.Close()
	b.RegisterNode("test", &brain.NodeHandle{Node: p, Audio: &audionode.Stub{}})
	defer b.UnregisterNode("test")
	e := NewEngine(b, &testASR{}, nil, config.SpeakerIDConfig{})
	result := e.Run(context.Background(), Config{NodeID: "test", TTSText: "prompt"})
	if result.Status != "error" || p.GetState() != node.StateIdle {
		t.Fatalf("result/state = %+v/%s", result, p.GetState())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	turn, err := b.AcquireNode(ctx, "test", 0)
	if err != nil {
		t.Fatal("turn was not released:", err)
	}
	turn.Release()
}

func TestParentCancellationDoesNotTranscribe(t *testing.T) {
	b := brain.New(testTTS{})
	p := node.NewPhysicalNode("test", "local")
	defer p.Close()
	b.RegisterNode("test", &brain.NodeHandle{Node: p, Audio: &audionode.Stub{}})
	defer b.UnregisterNode("test")
	asr := &testASR{}
	e := NewEngine(b, asr, nil, config.SpeakerIDConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	result := e.Run(ctx, Config{NodeID: "test"})
	if result.Status != "error" || len(asr.samples) != 0 {
		t.Fatalf("cancelled turn = %+v", result)
	}
}

func (a *interruptedAudio) PlayStream(ctx context.Context, src audionode.PCM16Source) error {
	pcm, err := src.ReadPCM16(ctx, 2048)
	if err != nil {
		return err
	}
	return a.Play(ctx, pcm, src.SampleRate())
}

func TestLiveSpeechOutlastsOnsetTimeout(t *testing.T) {
	sess := session.New("turn", "node")
	sess.StartCapture()
	sess.FeedPCM([]float32{1}, true)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { waitForEndpoint(ctx, sess, 20*time.Millisecond, 80*time.Millisecond); close(done) }()
	for i := 0; i < 8; i++ {
		time.Sleep(10 * time.Millisecond)
		sess.FeedPCM([]float32{1}, true)
	}
	select {
	case <-done:
		t.Fatal("onset timeout cut active speech")
	default:
	}
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("endpoint never closed")
	}
}
