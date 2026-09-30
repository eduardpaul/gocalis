package brain

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"gocalis/internal/ai"
	"gocalis/internal/audionode"
	"gocalis/internal/config"
	"gocalis/internal/node"
)

type countingTTS struct{ calls atomic.Int32 }

func (s *countingTTS) SynthesizeToStream(context.Context, string, ai.JobOptions) (ai.AudioStream, error) {
	s.calls.Add(1)
	return audionode.NewSliceSource([]int16{1000, -1000}, 16000), nil
}
func (*countingTTS) SynthesizeToFile(context.Context, string, string, ai.JobOptions) error {
	return nil
}
func (*countingTTS) Close() {}

type failedAudio struct {
	audionode.Stub
	err error
}

func (a *failedAudio) Play(context.Context, []int16, int) error { return a.err }

func TestBroadcastSynthesizesOnceAndKeepsSharedPCMImmutable(t *testing.T) {
	tts := &countingTTS{}
	b := New(tts)
	first, second := &audionode.Stub{}, &audionode.Stub{}
	for i, a := range []*audionode.Stub{first, second} {
		id := []string{"first", "second"}[i]
		p := node.NewPhysicalNode(id, "local")
		defer p.Close()
		b.RegisterNode(id, &NodeHandle{Node: p, Audio: a, Config: config.NodeConfig{RTCStream: config.RTCStreamConfig{OutputGainDb: float32(i) * 6}}})
		defer b.UnregisterNode(id)
	}
	if err := b.SpeakAll(context.Background(), "one phrase", 0); err != nil {
		t.Fatal(err)
	}
	if tts.calls.Load() != 1 {
		t.Fatalf("synthesis count = %d", tts.calls.Load())
	}
	if len(first.Played) != 1 || len(second.Played) != 1 {
		t.Fatal("a node did not play")
	}
	if first.Played[0][0] != 1000 || second.Played[0][0] <= 1000 {
		t.Fatalf("gain mutated shared PCM: %v, %v", first.Played, second.Played)
	}
}

func TestBroadcastReturnsPlaybackFailures(t *testing.T) {
	b := New(&countingTTS{})
	want := errors.New("device failed")
	p := node.NewPhysicalNode("failed", "local")
	defer p.Close()
	b.RegisterNode("failed", &NodeHandle{Node: p, Audio: &failedAudio{err: want}})
	defer b.UnregisterNode("failed")
	if err := b.SpeakAll(context.Background(), "hello", 0); !errors.Is(err, want) {
		t.Fatalf("broadcast error = %v", err)
	}
	if err := b.PlaySamplesAll(context.Background(), []int16{1}, 16000, 0); !errors.Is(err, want) {
		t.Fatalf("recording broadcast error = %v", err)
	}
}
