package runtime

import (
	"fmt"
	"log"
	"sync"

	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/node"
	"gocalis/internal/protocol"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

// SpeechGate tells speech from the rest of what a node captures.
type SpeechGate interface {
	// Speech takes the next chunk of audio (16 kHz mono) and tells whether
	// somebody is speaking in it.
	Speech(samples []float32) bool
	// Reset forgets what it heard, before audio that does not follow on.
	Reset()
	Close()
}

// Endpoints runs the nodes that are not in the "nodes:" configuration: the
// ones plugins bring. Such a node has a transport of its own and nothing of
// what a configured node is set up with (wake word, gain, diagnostics); what it
// gets is a place in the brain, so tts, play, ask and intercom reach it, its
// state on the event bus, and its captured audio taken to the asks and calls
// on it.
type Endpoints struct {
	brain   *brain.Brain
	events  protocol.EventPublisher
	newGate func() (SpeechGate, error)
}

// NewEndpoints prepares to run plugin nodes with the configured VAD model.
func NewEndpoints(global *config.Config, events protocol.EventPublisher, b *brain.Brain, threads int) *Endpoints {
	return &Endpoints{brain: b, events: events, newGate: func() (SpeechGate, error) {
		cfg := vadModelConfig(global, threads)
		vad := sherpa.NewVoiceActivityDetector(&cfg, 10.0)
		if vad == nil {
			return nil, fmt.Errorf("failed to initialize VAD")
		}
		return &sherpaGate{vad: vad}, nil
	}}
}

// NewEndpointsWithGate is NewEndpoints with a speech gate of the caller's.
func NewEndpointsWithGate(events protocol.EventPublisher, b *brain.Brain, newGate func() (SpeechGate, error)) *Endpoints {
	return &Endpoints{brain: b, events: events, newGate: newGate}
}

// Add registers audio as the node id, of the given type, and returns what takes
// it out again. Whoever adds the node owns its transport: Add neither connects
// nor closes it, and no audio may arrive from it once the returned function is
// called.
func (e *Endpoints) Add(id, typeName string, audio audionode.AudioNode) (func(), error) {
	if e.brain.GetNodeHandle(id) != nil {
		return nil, fmt.Errorf("a node '%s' already exists", id)
	}
	gate, err := e.newGate()
	if err != nil {
		return nil, fmt.Errorf("node '%s': %w", id, err)
	}
	pNode := node.NewPhysicalNode(id, typeName)
	pNode.OnStateChanged(func(oldState, newState node.NodeState) {
		log.Printf("[Node:%s] State changed: %s -> %s\n", id, oldState, newState)
		e.events.Publish(protocol.Response{Event: "state_changed", NodeID: id, State: string(newState)})
	})

	// The gate only runs while a turn is listening to the node: the rest of
	// the time what the node captures is for the call it is in, or for nobody.
	var mu sync.Mutex
	listening := false
	audio.OnAudio(func(samples []float32) {
		if len(samples) == 0 {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		if pNode.GetState() == node.StateIntercom {
			e.brain.ForwardRawAudio(id, samples)
		}
		now := e.brain.Sessions().ActiveCount(id) > 0
		if now {
			if !listening {
				gate.Reset()
			}
			e.brain.Sessions().FeedPCM(id, samples, gate.Speech(samples))
		}
		listening = now
	})

	e.brain.RegisterNode(id, &brain.NodeHandle{Node: pNode, Audio: audio, Config: config.NodeConfig{NodeID: id, Type: typeName}})
	log.Printf("[Node:%s] Registered (%s)\n", id, typeName)
	return func() {
		e.brain.UnregisterNode(id)
		audio.OnAudio(nil)
		// Wait out a chunk that is still being taken in.
		mu.Lock()
		gate.Close()
		mu.Unlock()
		pNode.Close()
	}, nil
}

// sherpaGate is the Silero VAD as a SpeechGate.
type sherpaGate struct {
	vad *sherpa.VoiceActivityDetector
}

func (g *sherpaGate) Speech(samples []float32) bool {
	g.vad.AcceptWaveform(samples)
	speech := g.vad.IsSpeech()
	for !g.vad.IsEmpty() {
		g.vad.Pop()
	}
	return speech
}

func (g *sherpaGate) Reset() { g.vad.Reset() }

func (g *sherpaGate) Close() { sherpa.DeleteVoiceActivityDetector(g.vad) }
