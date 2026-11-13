package telegramcall

import (
	"sync"
	"time"
)

const (
	// sourcePrimeMs is the cushion a speaker's stream builds before it is
	// played (and rebuilds after it runs dry), to ride out network jitter.
	sourcePrimeMs = 40
	// sourceMaxMs bounds what one speaker can have waiting, so a burst after a
	// stall cannot leave lasting delay.
	sourceMaxMs = 240
)

// mixdown makes one stream of the voice chat: Telegram delivers every
// speaker separately, the intercom call wants what they say together. Each
// speaker's audio queues up as it arrives; on a 20 ms clock one frame is
// taken from each and their sum handed on.
type mixdown struct {
	mu      sync.Mutex
	sources map[uint32]*source
	out     func(pcm []int16) // nil while nobody is listening yet
}

type source struct {
	buf    []int16
	primed bool
}

func newMixdown() *mixdown {
	return &mixdown{sources: make(map[uint32]*source)}
}

// to sets where the mix goes; nil drops it.
func (m *mixdown) to(out func(pcm []int16)) {
	m.mu.Lock()
	m.out = out
	m.mu.Unlock()
}

// push queues audio from one speaker.
func (m *mixdown) push(id uint32, pcm []int16) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.out == nil {
		return
	}
	s := m.sources[id]
	if s == nil {
		s = &source{}
		m.sources[id] = s
	}
	s.buf = append(s.buf, pcm...)
	if over := len(s.buf) - pcmRate*sourceMaxMs/1000; over > 0 {
		s.buf = s.buf[over:]
	}
}

// drop forgets a speaker whose stream ended.
func (m *mixdown) drop(id uint32) {
	m.mu.Lock()
	delete(m.sources, id)
	m.mu.Unlock()
}

// run mixes until stop is closed.
func (m *mixdown) run(stop <-chan struct{}) {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			m.tick()
		}
	}
}

// tick hands on one frame of everybody who has audio ready, or nothing when
// nobody does.
func (m *mixdown) tick() {
	m.mu.Lock()
	out := m.out
	var sum []int32
	for _, s := range m.sources {
		if !s.primed {
			if len(s.buf) < pcmRate*sourcePrimeMs/1000 {
				continue
			}
			s.primed = true
		}
		if len(s.buf) == 0 {
			s.primed = false
			continue
		}
		if sum == nil {
			sum = make([]int32, frameSamples)
		}
		n := min(len(s.buf), frameSamples)
		for i, v := range s.buf[:n] {
			sum[i] += int32(v)
		}
		s.buf = s.buf[n:]
	}
	m.mu.Unlock()
	if sum == nil || out == nil {
		return
	}
	frame := make([]int16, frameSamples)
	for i, v := range sum {
		frame[i] = int16(min(max(v, -32768), 32767))
	}
	out(frame)
}
