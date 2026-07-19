package intercom

import (
	"time"
)

// mixFrameMs is the mixer's processing cadence: every tick it consumes one frame
// of this length from each input and emits one to each output.
const mixFrameMs = 20

// mixer implements N-way mix-minus for a group intercom call. Each participant
// pushes its (echo-cancelled) mic into a per-node input buffer; on a fixed
// wall-clock cadence the mixer pulls one frame from every input and, for each
// participant, sums every OTHER participant's frame (so a node never hears
// itself) with hard clamping, then pushes the result to that node's output
// buffer, which its StreamOut drains to the device.
//
// A single mix clock is what keeps N independent, drifting capture streams time
// aligned: inputs are read non-blocking with silence substituted on underrun,
// and the drop-oldest input buffers bound any overrun.
type mixer struct {
	frame   int
	nodes   []string
	inputs  map[string]*bridge
	outputs map[string]*bridge
	quit    chan struct{}
	done    chan struct{}
}

// newMixer builds a mixer for the given participants. outputCapSamples bounds
// each output buffer (drop-oldest) so end-to-end latency stays bounded.
func newMixer(rate int, nodes []string, outputCapSamples int) *mixer {
	m := &mixer{
		frame:   rate * mixFrameMs / 1000,
		nodes:   append([]string(nil), nodes...),
		inputs:  make(map[string]*bridge, len(nodes)),
		outputs: make(map[string]*bridge, len(nodes)),
		quit:    make(chan struct{}),
		done:    make(chan struct{}),
	}
	for _, n := range nodes {
		m.inputs[n] = newBridgeBuffer(rate, rate/2)
		m.outputs[n] = newBridgeBuffer(rate, outputCapSamples)
	}
	return m
}

// input returns the mic buffer a node's tap pushes into.
func (m *mixer) input(node string) *bridge { return m.inputs[node] }

// output returns the mix-minus source a node's StreamOut consumes.
func (m *mixer) output(node string) *bridge { return m.outputs[node] }

// run mixes on a fixed cadence until stop is called.
func (m *mixer) run() {
	defer close(m.done)
	ticker := time.NewTicker(mixFrameMs * time.Millisecond)
	defer ticker.Stop()

	frames := make(map[string][]int16, len(m.nodes))
	for {
		select {
		case <-m.quit:
			return
		case <-ticker.C:
			for _, n := range m.nodes {
				frames[n] = m.inputs[n].takeFrame(m.frame)
			}
			for _, out := range m.nodes {
				mixed := make([]int16, m.frame)
				for _, in := range m.nodes {
					if in == out {
						continue // mix-minus: a node never hears itself
					}
					src := frames[in]
					for i := 0; i < m.frame; i++ {
						mixed[i] = clampAddInt16(mixed[i], src[i])
					}
				}
				m.outputs[out].push(mixed)
			}
		}
	}
}

// stop halts the mix loop and closes every output so the StreamOut readers
// unwind with io.EOF.
func (m *mixer) stop() {
	close(m.quit)
	<-m.done
	for _, out := range m.outputs {
		out.close()
	}
}

// clampAddInt16 sums two samples in int32 and hard-clamps to the int16 range.
func clampAddInt16(a, b int16) int16 {
	s := int32(a) + int32(b)
	if s > 32767 {
		return 32767
	}
	if s < -32768 {
		return -32768
	}
	return int16(s)
}
