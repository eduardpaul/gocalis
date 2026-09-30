// Package brain implements the central orchestrator that owns all audio nodes,
// enforces turn-taking, and routes global commands such as TTS to one or all devices.
package brain

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
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
	Node   *node.PhysicalNode
	Audio  audionode.AudioNode
	Config config.NodeConfig
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
	if h := b.nodes[nodeID]; h != nil {
		h.queue.close()
	}
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
	sort.Slice(infos, func(i, j int) bool { return infos[i].NodeID < infos[j].NodeID })
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
// node's turn queue and priority exactly like Speak.
func (b *Brain) PlaySamples(ctx context.Context, nodeID string, samples []int16, sampleRate int, priority int) error {
	b.nodesMutex.RLock()
	handle, ok := b.nodes[nodeID]
	b.nodesMutex.RUnlock()

	if !ok {
		return fmt.Errorf("node '%s' not registered", nodeID)
	}

	if sampleRate <= 0 || sampleRate > 192000 || len(samples) > sampleRate*ai.MaxAudioSeconds {
		return fmt.Errorf("invalid audio rate or duration")
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

	return b.playAll(ctx, handles, samples, sampleRate, priority)
}

// playSamplesToHandle plays pre-recorded samples on a single node with the same
// state transitions and output-gain handling as speakToHandle. The caller must
// already hold the node's turn slot (via the queue).
func (b *Brain) playSamplesToHandle(ctx context.Context, handle *NodeHandle, baseSamples []int16, sampleRate int) error {
	if len(baseSamples) == 0 {
		return nil
	}

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

// AcquireNode reserves a node for an exclusive turn, in priority/FIFO order.
func (b *Brain) AcquireNode(ctx context.Context, nodeID string, priority int) (*Turn, error) {
	handle := b.GetNodeHandle(nodeID)
	if handle == nil {
		return nil, fmt.Errorf("node '%s' not registered", nodeID)
	}
	release, err := handle.queue.acquire(ctx, priority)
	if err != nil {
		return nil, err
	}
	return &Turn{Handle: handle, release: release}, nil
}

// SpeakAll routes the same TTS utterance to every registered node.
//
// The utterance is synthesized once. Each node then waits independently for
// its turn and applies its own gain without changing the shared recording.
func (b *Brain) SpeakAll(ctx context.Context, text string, priority int) error {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	handles := b.snapshotHandles()
	if len(handles) == 0 {
		return fmt.Errorf("no nodes registered")
	}
	samples, rate, err := b.Synthesize(ctx, text, priority)
	if err != nil {
		return err
	}
	return b.playAll(ctx, handles, samples, rate, priority)
}

func (b *Brain) playAll(ctx context.Context, handles []*NodeHandle, samples []int16, rate, priority int) error {
	errs := make([]error, len(handles))
	var wg sync.WaitGroup
	for i, handle := range handles {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := handle.queue.acquire(ctx, priority)
			if err == nil {
				defer release()
				err = b.playSamplesToHandle(ctx, handle, samples, rate)
			}
			if err != nil {
				errs[i] = fmt.Errorf("node %s: %w", handle.Node.NodeID, err)
			}
		}()
	}
	wg.Wait()
	return errors.Join(errs...)
}

// speakToHandle synthesizes audio for a single node and streams it as it is
// generated, so playback can begin before synthesis completes.
func (b *Brain) speakToHandle(ctx context.Context, handle *NodeHandle, text string, priority int) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Empty text is a no-op: never spin up the TTS pipeline for nothing.
	if strings.TrimSpace(text) == "" {
		return nil
	}

	handle.Node.SetState(node.StateProcessing)

	audioStream, err := b.ttsEngine.SynthesizeToStream(ctx, text, ai.JobOptions{Priority: priority})
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
func (b *Brain) Synthesize(ctx context.Context, text string, priority int) ([]int16, int, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Empty text is a no-op: never spin up the TTS pipeline for nothing.
	if strings.TrimSpace(text) == "" {
		return nil, 0, nil
	}

	audioStream, err := b.ttsEngine.SynthesizeToStream(ctx, text, ai.JobOptions{Priority: priority})
	if err != nil {
		return nil, 0, fmt.Errorf("TTS synthesis failed: %w", err)
	}
	return readAllSamples(ctx, audioStream)
}

// Turn holds exclusive ownership of one node until Release is called.
// State-neutral playback is available only through a reserved turn.
type Turn struct {
	Handle  *NodeHandle
	release func()
}

func (t *Turn) Release() { t.release() }

func (t *Turn) Play(ctx context.Context, samples []int16, sampleRate int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if sampleRate <= 0 {
		return fmt.Errorf("invalid sample rate")
	}
	if t.Handle.Config.RTCStream.OutputGainDb != 0 {
		samples = audio.ApplyGainPCM16(samples, t.Handle.Config.RTCStream.OutputGainDb)
	}
	return t.Handle.Audio.Play(ctx, samples, sampleRate)
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
func readAllSamples(ctx context.Context, stream ai.AudioStream) ([]int16, int, error) {
	var samples []int16
	chunkSize := 1024

	for {
		chunk, err := stream.ReadPCM16(ctx, chunkSize)
		samples = append(samples, chunk...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, 0, err
		}
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

func (g *gainSource) ReadPCM16(ctx context.Context, chunkSize int) ([]int16, error) {
	chunk, err := g.src.ReadPCM16(ctx, chunkSize)
	if len(chunk) > 0 {
		chunk = audio.ApplyGainPCM16(chunk, g.gainDb)
	}
	return chunk, err
}
