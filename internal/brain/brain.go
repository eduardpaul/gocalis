// Package brain implements the central orchestrator that owns all audio nodes,
// enforces turn-taking, and routes global commands such as TTS to one or all devices.
package brain

import (
	"context"
	"fmt"
	"io"
	"log"
	"strings"
	"sync"

	"gocalis/internal/ai"
	"gocalis/internal/audio"
	"gocalis/internal/audionode"
	"gocalis/internal/config"
	"gocalis/internal/node"
	"gocalis/internal/session"
)

// NodeHandle bundles everything the brain needs to control a single physical audio node.
type NodeHandle struct {
	Node       *node.PhysicalNode
	Audio      audionode.AudioNode
	Config     config.NodeConfig
	speakMutex sync.Mutex
	// queue serializes turns (speak utterances and full ask flows) on this node
	// so a lower-priority speak waits for an in-progress higher-priority ask
	// instead of cutting into it. Assigned by RegisterNode.
	queue *nodeQueue
}

// NodeInfo is a read-only snapshot of a registered node for dashboards/APIs.
type NodeInfo struct {
	NodeID string `json:"node_id"`
	Type   string `json:"type"`
	State  string `json:"state"`
}

// Brain is the central orchestrator for all audio streams / physical devices.
type Brain struct {
	ttsEngine  ai.Synthesizer
	nodes      map[string]*NodeHandle
	nodesMutex sync.RWMutex
	sessions   *session.Registry
}

// New creates a new Brain backed by the given TTS engine.
func New(ttsEngine ai.Synthesizer) *Brain {
	return &Brain{
		ttsEngine: ttsEngine,
		nodes:     make(map[string]*NodeHandle),
		sessions:  session.NewRegistry(),
	}
}

// Sessions returns the registry of active interaction turns. The audio ingestion
// path feeds captured speech into it, and the ask engine registers turns on it.
func (b *Brain) Sessions() *session.Registry {
	return b.sessions
}

// FeedAudio delivers a detected-speech segment for a node to every active
// session on that node.
func (b *Brain) FeedAudio(nodeID string, samples []float32) {
	b.sessions.Feed(nodeID, samples)
}

// RegisterNode registers a physical node so the brain can route commands to it.
func (b *Brain) RegisterNode(nodeID string, handle *NodeHandle) {
	if handle.queue == nil {
		handle.queue = newNodeQueue()
	}
	b.nodesMutex.Lock()
	defer b.nodesMutex.Unlock()
	b.nodes[nodeID] = handle
}

// UnregisterNode removes a physical node from the brain's routing table.
func (b *Brain) UnregisterNode(nodeID string) {
	b.nodesMutex.Lock()
	defer b.nodesMutex.Unlock()
	delete(b.nodes, nodeID)
}

// GetNodeHandle returns the registered handle for a node, or nil if not found.
func (b *Brain) GetNodeHandle(nodeID string) *NodeHandle {
	b.nodesMutex.RLock()
	defer b.nodesMutex.RUnlock()
	return b.nodes[nodeID]
}

// NodeCount returns the number of currently registered nodes.
func (b *Brain) NodeCount() int {
	b.nodesMutex.RLock()
	defer b.nodesMutex.RUnlock()
	return len(b.nodes)
}

// ListNodes returns a snapshot of all registered nodes.
func (b *Brain) ListNodes() []NodeInfo {
	b.nodesMutex.RLock()
	defer b.nodesMutex.RUnlock()

	infos := make([]NodeInfo, 0, len(b.nodes))
	for _, h := range b.nodes {
		infos = append(infos, NodeInfo{
			NodeID: h.Node.NodeID,
			Type:   h.Node.Type,
			State:  string(h.Node.GetState()),
		})
	}
	return infos
}

// Speak routes a TTS utterance to a single node.
func (b *Brain) Speak(ctx context.Context, nodeID string, text string, priority int) error {
	b.nodesMutex.RLock()
	handle, ok := b.nodes[nodeID]
	b.nodesMutex.RUnlock()

	if !ok {
		return fmt.Errorf("node '%s' not registered", nodeID)
	}

	// Empty text is a no-op: never take the node's turn slot for nothing.
	if strings.TrimSpace(text) == "" {
		return nil
	}

	// Wait for this node's turn (higher-priority work runs first) instead of
	// being rejected when the node is busy.
	release, err := handle.queue.acquire(ctx, priority)
	if err != nil {
		return err
	}
	defer release()

	return b.speakToHandle(ctx, handle, text, priority)
}

// PlaySamples plays a pre-recorded PCM16 clip on a single node, honoring the
// node's turn queue and priority exactly like Speak. Unlike SpeakSamples it
// does not reject a busy node: it waits for its turn on the queue.
func (b *Brain) PlaySamples(ctx context.Context, nodeID string, samples []int16, sampleRate int, priority int) error {
	b.nodesMutex.RLock()
	handle, ok := b.nodes[nodeID]
	b.nodesMutex.RUnlock()

	if !ok {
		return fmt.Errorf("node '%s' not registered", nodeID)
	}

	// Empty audio is a no-op: never take the node's turn slot for nothing.
	if len(samples) == 0 {
		return nil
	}

	release, err := handle.queue.acquire(ctx, priority)
	if err != nil {
		return err
	}
	defer release()

	return b.playSamplesToHandle(ctx, handle, samples, sampleRate)
}

// PlaySamplesAll plays the same recording on every registered node. Each node is
// driven independently through its own turn queue at the given priority, exactly
// like SpeakAll: free nodes play immediately, busy nodes queue behind their
// current turn, and nodes never block one another.
func (b *Brain) PlaySamplesAll(ctx context.Context, samples []int16, sampleRate int, priority int) error {
	if len(samples) == 0 {
		return nil
	}

	handles := b.snapshotHandles()
	if len(handles) == 0 {
		return fmt.Errorf("no nodes registered")
	}

	var wg sync.WaitGroup
	for _, h := range handles {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if err := b.PlaySamples(ctx, nodeID, samples, sampleRate, priority); err != nil {
				log.Printf("[Brain] Broadcast play to node '%s' failed: %v\n", nodeID, err)
			}
		}(h.Node.NodeID)
	}
	wg.Wait()

	return nil
}

// playSamplesToHandle plays pre-recorded samples on a single node with the same
// state transitions and output-gain handling as speakToHandle. The caller must
// already hold the node's turn slot (via the queue).
func (b *Brain) playSamplesToHandle(ctx context.Context, handle *NodeHandle, baseSamples []int16, sampleRate int) error {
	if len(baseSamples) == 0 {
		return nil
	}

	handle.speakMutex.Lock()
	defer handle.speakMutex.Unlock()

	handle.Node.SetState(node.StateProcessing)

	samples := baseSamples
	if handle.Config.RTCStream.OutputGainDb != 0 {
		samples = audio.ApplyGainPCM16(baseSamples, handle.Config.RTCStream.OutputGainDb)
	}

	handle.Node.SetState(node.StateSpeaking)
	err := handle.Audio.Play(ctx, samples, sampleRate)
	handle.Node.SetState(node.StateIdle)
	if err != nil {
		return fmt.Errorf("failed to play audio: %w", err)
	}
	return nil
}

// AcquireNode reserves a node for an exclusive turn, blocking until the caller
// owns it (higher priority first) or ctx is cancelled. The returned release
// func MUST be called when the turn ends. It is used by the ask flow, which
// holds the node for its whole prompt -> listen -> ASR turn so a queued speak
// waits until the turn completes.
func (b *Brain) AcquireNode(ctx context.Context, nodeID string, priority int) (func(), error) {
	b.nodesMutex.RLock()
	handle, ok := b.nodes[nodeID]
	b.nodesMutex.RUnlock()

	if !ok {
		return nil, fmt.Errorf("node '%s' not registered", nodeID)
	}
	return handle.queue.acquire(ctx, priority)
}

// SpeakAll routes the same TTS utterance to every registered node.
//
// Each node is driven independently through its own turn queue at the given
// priority: a node that is free plays right away, while a node busy with a
// higher-priority turn (e.g. an ask) queues the utterance and plays it once
// that turn finishes. Nodes never block one another.
func (b *Brain) SpeakAll(ctx context.Context, text string, priority int) error {
	if strings.TrimSpace(text) == "" {
		return nil
	}

	handles := b.snapshotHandles()
	if len(handles) == 0 {
		return fmt.Errorf("no nodes registered")
	}

	var wg sync.WaitGroup
	for _, h := range handles {
		wg.Add(1)
		go func(nodeID string) {
			defer wg.Done()
			if err := b.Speak(ctx, nodeID, text, priority); err != nil {
				log.Printf("[Brain] Broadcast to node '%s' failed: %v\n", nodeID, err)
			}
		}(h.Node.NodeID)
	}
	wg.Wait()

	return nil
}

// speakToHandle synthesizes audio for a single node and streams it as it is
// generated, so playback can begin before synthesis completes.
func (b *Brain) speakToHandle(ctx context.Context, handle *NodeHandle, text string, priority int) error {
	// Empty text is a no-op: never spin up the TTS pipeline for nothing.
	if strings.TrimSpace(text) == "" {
		return nil
	}

	// The caller already holds this node's turn slot (via the queue), so this is
	// the only turn touching the device; speakMutex just guards against a stray
	// concurrent low-level Play (e.g. a chime) on the same handle.
	handle.speakMutex.Lock()
	defer handle.speakMutex.Unlock()

	handle.Node.SetState(node.StateProcessing)

	audioStream, err := b.ttsEngine.SynthesizeToStream(text, ai.JobOptions{Priority: priority})
	if err != nil {
		handle.Node.SetState(node.StateIdle)
		return fmt.Errorf("TTS synthesis failed: %w", err)
	}

	var src audionode.PCM16Source = audioStream
	if handle.Config.RTCStream.OutputGainDb != 0 {
		src = &gainSource{src: audioStream, gainDb: handle.Config.RTCStream.OutputGainDb}
	}

	handle.Node.SetState(node.StateSpeaking)
	err = handle.Audio.PlayStream(ctx, src)
	handle.Node.SetState(node.StateIdle)
	if err != nil {
		return fmt.Errorf("failed to stream audio: %w", err)
	}
	return nil
}

// Synthesize converts text to PCM16 samples using the brain's TTS engine.
func (b *Brain) Synthesize(text string, priority int) ([]int16, int, error) {
	// Empty text is a no-op: never spin up the TTS pipeline for nothing.
	if strings.TrimSpace(text) == "" {
		return nil, 0, nil
	}

	audioStream, err := b.ttsEngine.SynthesizeToStream(text, ai.JobOptions{Priority: priority})
	if err != nil {
		return nil, 0, fmt.Errorf("TTS synthesis failed: %w", err)
	}
	return readAllSamples(audioStream)
}

// SpeakSamples plays pre-synthesized PCM16 samples on a single node.
func (b *Brain) SpeakSamples(ctx context.Context, nodeID string, samples []int16, sampleRate int) error {
	b.nodesMutex.RLock()
	handle, ok := b.nodes[nodeID]
	b.nodesMutex.RUnlock()

	if !ok {
		return fmt.Errorf("node '%s' not registered", nodeID)
	}

	return b.sendSamplesToHandle(ctx, handle, samples, sampleRate)
}

// sendSamplesToHandle plays pre-synthesized audio on a single node.
func (b *Brain) sendSamplesToHandle(ctx context.Context, handle *NodeHandle, baseSamples []int16, sampleRate int) error {
	handle.speakMutex.Lock()
	defer handle.speakMutex.Unlock()

	if handle.Node.GetState() == node.StateSpeaking {
		return fmt.Errorf("node '%s' is already speaking", handle.Node.NodeID)
	}

	handle.Node.SetState(node.StateProcessing)

	samples := baseSamples
	if handle.Config.RTCStream.OutputGainDb != 0 {
		samples = audio.ApplyGainPCM16(baseSamples, handle.Config.RTCStream.OutputGainDb)
	}

	return b.playSamplesLocked(ctx, handle, samples, sampleRate)
}

// playSamplesLocked sends already-prepared samples to the node's audio output.
// The caller must hold handle.speakMutex.
func (b *Brain) playSamplesLocked(ctx context.Context, handle *NodeHandle, samples []int16, sampleRate int) error {
	handle.Node.SetState(node.StateSpeaking)
	err := handle.Audio.Play(ctx, samples, sampleRate)
	handle.Node.SetState(node.StateIdle)
	if err != nil {
		return fmt.Errorf("failed to send audio: %w", err)
	}
	return nil
}

// PlayAudio plays pre-synthesized samples on a node WITHOUT changing the node's
// reported state. It is used for cosmetic audio (UI chimes) and for the ask
// flow's prompt, where the caller drives the node's state machine explicitly so
// the event stream stays clean (e.g. idle -> speaking -> listening) instead of
// emitting spurious PROCESSING/SPEAKING/IDLE transitions per clip.
func (b *Brain) PlayAudio(ctx context.Context, nodeID string, samples []int16, sampleRate int) error {
	b.nodesMutex.RLock()
	handle, ok := b.nodes[nodeID]
	b.nodesMutex.RUnlock()

	if !ok {
		return fmt.Errorf("node '%s' not registered", nodeID)
	}

	handle.speakMutex.Lock()
	defer handle.speakMutex.Unlock()

	if handle.Config.RTCStream.OutputGainDb != 0 {
		samples = audio.ApplyGainPCM16(samples, handle.Config.RTCStream.OutputGainDb)
	}

	return handle.Audio.Play(ctx, samples, sampleRate)
}

// snapshotHandles returns a copy of the current node handles slice.
func (b *Brain) snapshotHandles() []*NodeHandle {
	b.nodesMutex.RLock()
	defer b.nodesMutex.RUnlock()

	handles := make([]*NodeHandle, 0, len(b.nodes))
	for _, h := range b.nodes {
		handles = append(handles, h)
	}
	return handles
}

// readAllSamples drains an AudioStream into a single int16 slice.
func readAllSamples(stream ai.AudioStream) ([]int16, int, error) {
	var samples []int16
	chunkSize := 1024

	for {
		chunk, err := stream.ReadPCM16(chunkSize)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, 0, err
		}
		samples = append(samples, chunk...)
	}

	return samples, stream.SampleRate(), nil
}

// gainSource wraps a PCM16 source, applying an output dB gain to each chunk as it
// is pulled. It lets streamed playback apply per-node gain without buffering the
// whole utterance first.
type gainSource struct {
	src    audionode.PCM16Source
	gainDb float32
}

func (g *gainSource) SampleRate() int {
	return g.src.SampleRate()
}

func (g *gainSource) ReadPCM16(chunkSize int) ([]int16, error) {
	chunk, err := g.src.ReadPCM16(chunkSize)
	if len(chunk) > 0 {
		chunk = audio.ApplyGainPCM16(chunk, g.gainDb)
	}
	return chunk, err
}
