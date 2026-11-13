package intercom

import (
	"slices"
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
// plus the clips being played (chimes, the assistant's voice), then pushes the
// result to that listener's output buffer, which its transport drains.
//
// A participant can be set aside for a private exchange with the assistant:
// until it is back, the others do not get its microphone and it gets only the
// clips played for it alone.
//
// A single mix clock is what keeps N independent, drifting capture streams time
// aligned: inputs are read non-blocking with silence substituted on underrun,
// and the drop-oldest input buffers bound any overrun.
type mixer struct {
	frame int

	mu        sync.Mutex
	talkers   map[string]*bridge
	listeners map[string]*listener
	aside     map[string]bool
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

// clip is a one-shot sound mixed in until it runs out.
type clip struct {
	pcm  []int16
	pos  int
	to   string        // the only listener that gets it; empty for everyone
	done chan struct{} // closed once it has played out or was dropped
}

func newMixer(rate int) *mixer {
	return &mixer{
		frame:     rate * mixFrameMs / 1000,
		talkers:   make(map[string]*bridge),
		listeners: make(map[string]*listener),
		aside:     make(map[string]bool),
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

// remove drops the participant from the mix, as a talker and as a listener,
// along with the clips that were for it alone. Its output buffer is left to the
// caller, which closes it once the tail is in.
func (m *mixer) remove(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.talkers, id)
	delete(m.listeners, id)
	delete(m.aside, id)
	m.clips = slices.DeleteFunc(m.clips, func(c *clip) bool {
		if c.to != id {
			return false
		}
		close(c.done)
		return true
	})
}

// setAside takes the participant out of the conversation (or puts it back)
// without taking it out of the mix.
func (m *mixer) setAside(id string, aside bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if aside {
		m.aside[id] = true
	} else {
		delete(m.aside, id)
	}
}

// play mixes a one-shot sound in, starting on the next tick: for the listener
// `to` alone, or for every listener when `to` is empty. The returned clip's done
// channel closes when it has played out.
func (m *mixer) play(pcm []int16, to string) *clip {
	c := &clip{pcm: pcm, to: to, done: make(chan struct{})}
	m.mu.Lock()
	m.clips = append(m.clips, c)
	m.mu.Unlock()
	return c
}

// drop stops a clip that is still playing.
func (m *mixer) drop(c *clip) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if i := slices.Index(m.clips, c); i >= 0 {
		m.clips = slices.Delete(m.clips, i, i+1)
		close(c.done)
	}
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

	// Sum the clips once: the ones for everyone, and the ones for one listener.
	shared := make([]int32, m.frame)
	private := make(map[string][]int32)
	live := m.clips[:0]
	for _, c := range m.clips {
		into := shared
		if c.to != "" {
			if private[c.to] == nil {
				private[c.to] = make([]int32, m.frame)
			}
			into = private[c.to]
		}
		c.pos += addInto(into, c.pcm[c.pos:])
		if c.pos < len(c.pcm) {
			live = append(live, c)
		} else {
			close(c.done)
		}
	}
	clear(m.clips[len(live):])
	m.clips = live

	sum := make([]int32, m.frame)
	for id, l := range m.listeners {
		copy(sum, shared)
		for i, v := range private[id] {
			sum[i] += v
		}
		for talker, frame := range frames {
			// Mix-minus, and nothing to or from whoever is set aside.
			if talker == id || talker == l.skip || m.aside[talker] || m.aside[id] {
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
