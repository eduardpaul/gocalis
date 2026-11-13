package intercom

import (
	"sync"
	"time"
)

// mixFrameMs is the mixer's processing cadence: every tick it consumes one frame
// of this length from each talker and emits one to each listener.
const mixFrameMs = 20

// mixer implements N-way mix-minus for an intercom call whose participants come
// and go. Each talker pushes its microphone into an input buffer; on a fixed
// wall-clock cadence the mixer pulls one frame from every talker and, for each
// listener, sums every OTHER talker's frame (a participant never hears itself)
// plus the clips being played (join/leave chimes), then pushes the result to
// that listener's output buffer, which its transport drains.
//
// A single mix clock is what keeps N independent, drifting capture streams time
// aligned: inputs are read non-blocking with silence substituted on underrun,
// and the drop-oldest input buffers bound any overrun.
type mixer struct {
	frame int

	mu        sync.Mutex
	talkers   map[string]*bridge
	listeners map[string]*listener
	clips     []*clip

	quit chan struct{}
	done chan struct{}
}

// listener is one participant's output.
type listener struct {
	out *bridge
	// skip is a talker this listener must not get from the mix because it
	// already hears it elsewhere (the intercom page plays the node it shows
	// straight from go2rtc). Empty for none.
	skip string
}

// clip is a one-shot sound mixed to every listener until it runs out.
type clip struct {
	pcm []int16
	pos int
}

func newMixer(rate int) *mixer {
	return &mixer{
		frame:     rate * mixFrameMs / 1000,
		talkers:   make(map[string]*bridge),
		listeners: make(map[string]*listener),
		quit:      make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// addTalker starts mixing the microphone buffer in under the participant id.
func (m *mixer) addTalker(id string, in *bridge) {
	m.mu.Lock()
	m.talkers[id] = in
	m.mu.Unlock()
}

// addListener starts feeding out with the mix of every talker but id and skip.
func (m *mixer) addListener(id string, out *bridge, skip string) {
	m.mu.Lock()
	m.listeners[id] = &listener{out: out, skip: skip}
	m.mu.Unlock()
}

// remove drops the participant from the mix, as a talker and as a listener. Its
// output buffer is left to the caller, which closes it once the tail is in.
func (m *mixer) remove(id string) {
	m.mu.Lock()
	delete(m.talkers, id)
	delete(m.listeners, id)
	m.mu.Unlock()
}

// play mixes a one-shot sound to every listener, starting on the next tick.
func (m *mixer) play(pcm []int16) {
	m.mu.Lock()
	m.clips = append(m.clips, &clip{pcm: pcm})
	m.mu.Unlock()
}

// run mixes on a fixed cadence until stop is called.
func (m *mixer) run() {
	defer close(m.done)
	ticker := time.NewTicker(mixFrameMs * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-m.quit:
			return
		case <-ticker.C:
			m.tick()
		}
	}
}

// stop halts the mix loop.
func (m *mixer) stop() {
	close(m.quit)
	<-m.done
}

// tick mixes one frame for every listener.
func (m *mixer) tick() {
	m.mu.Lock()
	defer m.mu.Unlock()

	frames := make(map[string][]int16, len(m.talkers))
	for id, in := range m.talkers {
		frames[id] = in.takeFrame(m.frame)
	}

	// The clips sound the same for everyone: sum them once.
	var shared []int32
	if len(m.clips) > 0 {
		shared = make([]int32, m.frame)
		live := m.clips[:0]
		for _, c := range m.clips {
			c.pos += addInto(shared, c.pcm[c.pos:])
			if c.pos < len(c.pcm) {
				live = append(live, c)
			}
		}
		clear(m.clips[len(live):])
		m.clips = live
	}

	sum := make([]int32, m.frame)
	for id, l := range m.listeners {
		clear(sum)
		if shared != nil {
			copy(sum, shared)
		}
		for talker, frame := range frames {
			if talker == id || talker == l.skip {
				continue
			}
			addInto(sum, frame)
		}
		mixed := make([]int16, m.frame)
		for i, v := range sum {
			mixed[i] = clampInt16(v)
		}
		l.out.push(mixed)
	}
}

// addInto adds src onto dst sample by sample, up to the shorter of the two, and
// returns how many samples it consumed.
func addInto(dst []int32, src []int16) int {
	n := min(len(dst), len(src))
	for i := 0; i < n; i++ {
		dst[i] += int32(src[i])
	}
	return n
}

// clampInt16 hard-clamps a summed sample to the int16 range.
func clampInt16(s int32) int16 {
	if s > 32767 {
		return 32767
	}
	if s < -32768 {
		return -32768
	}
	return int16(s)
}
