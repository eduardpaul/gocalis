package intercom

import (
	"sort"
	"time"
)

// mixFrameMs is the mixer's processing cadence: every tick it consumes one frame
// of this length from each input and emits one to each output.
const mixFrameMs = 20

// mixer implements N-way mix-minus for a group intercom call. Each talker pushes
// its (echo-cancelled) mic into an input buffer; on a fixed wall-clock cadence
// the mixer pulls one frame from every input and, for each listener, sums every
// OTHER talker's frame (so a participant never hears itself) with hard clamping,
// then pushes the result to that listener's output buffer, which its StreamOut
// drains to the device.
//
// Talkers and listeners are separate sets: a node participant is both, while a
// remote talker (the intercom page's microphone) only talks, because the page
// plays the doorbell straight from go2rtc and gocalis has nothing to send it.
//
// A single mix clock is what keeps N independent, drifting capture streams time
// aligned: inputs are read non-blocking with silence substituted on underrun,
// and the drop-oldest input buffers bound any overrun.
type mixer struct {
	frame     int
	talkers   []string
	listeners []string
	inputs    map[string]*bridge
	outputs   map[string]*bridge
	quit      chan struct{}
	done      chan struct{}
}

// newMixer builds a mixer over the given talker inputs and listener ids.
// outputCapSamples bounds each output buffer (drop-oldest) so end-to-end latency
// stays bounded.
func newMixer(rate int, inputs map[string]*bridge, listeners []string, outputCapSamples int) *mixer {
	m := &mixer{
		frame:     rate * mixFrameMs / 1000,
		listeners: append([]string(nil), listeners...),
		inputs:    inputs,
		outputs:   make(map[string]*bridge, len(listeners)),
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
	}
	for id := range inputs {
		m.talkers = append(m.talkers, id)
	}
	sort.Strings(m.talkers)
	for _, n := range listeners {
		m.outputs[n] = newBridgeBuffer(rate, outputCapSamples)
	}
	return m
}

// input returns the mic buffer a talker pushes into (nil if not a talker).
func (m *mixer) input(id string) *bridge { return m.inputs[id] }

// output returns the mix-minus source a listener's StreamOut consumes.
func (m *mixer) output(id string) *bridge { return m.outputs[id] }

// run mixes on a fixed cadence until stop is called.
func (m *mixer) run() {
	defer close(m.done)
	ticker := time.NewTicker(mixFrameMs * time.Millisecond)
	defer ticker.Stop()

	frames := make(map[string][]int16, len(m.talkers))
	for {
		select {
		case <-m.quit:
			return
		case <-ticker.C:
			for _, n := range m.talkers {
				frames[n] = m.inputs[n].takeFrame(m.frame)
			}
			for _, out := range m.listeners {
				mixed := make([]int16, m.frame)
				for _, in := range m.talkers {
					if in == out {
						continue // mix-minus: a participant never hears itself
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
