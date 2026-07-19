package intercom

import (
	"io"
	"sync"

	"gocalis/internal/audionode"
)

// bridge is a live PCM16 audio conduit between two intercom nodes. One node's
// mic pushes samples in; the peer node's StreamOut pulls them out via the
// audionode.PCM16Source interface. It is a drop-oldest buffer: if the consumer
// falls behind the producer, the oldest samples are discarded so end-to-end
// latency stays bounded instead of growing without limit.
type bridge struct {
	mu     sync.Mutex
	cond   *sync.Cond
	buf    []int16
	maxLen int
	rate   int
	closed bool
}

var _ audionode.PCM16Source = (*bridge)(nil)

// newBridgeBuffer creates a bridge carrying PCM16 at the given sample rate,
// buffering at most maxSamples before dropping the oldest audio.
func newBridgeBuffer(rate, maxSamples int) *bridge {
	if maxSamples <= 0 {
		maxSamples = rate // 1s default cap
	}
	b := &bridge{rate: rate, maxLen: maxSamples}
	b.cond = sync.NewCond(&b.mu)
	return b
}

// SampleRate reports the bridge's fixed PCM16 sample rate.
func (b *bridge) SampleRate() int { return b.rate }

// push appends samples, dropping the oldest audio to stay within maxLen.
func (b *bridge) push(samples []int16) {
	if len(samples) == 0 {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.buf = append(b.buf, samples...)
	if len(b.buf) > b.maxLen {
		b.buf = b.buf[len(b.buf)-b.maxLen:]
	}
	b.cond.Signal()
}

// ReadPCM16 returns up to chunkSize buffered samples, blocking until audio is
// available or the bridge is closed. It returns io.EOF once closed and drained.
func (b *bridge) ReadPCM16(chunkSize int) ([]int16, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for len(b.buf) == 0 && !b.closed {
		b.cond.Wait()
	}
	if len(b.buf) == 0 && b.closed {
		return nil, io.EOF
	}
	n := chunkSize
	if n > len(b.buf) {
		n = len(b.buf)
	}
	out := make([]int16, n)
	copy(out, b.buf[:n])
	b.buf = b.buf[n:]
	return out, nil
}

// close wakes any blocked reader so PlayStream unwinds and returns io.EOF.
func (b *bridge) close() {
	b.mu.Lock()
	b.closed = true
	b.cond.Broadcast()
	b.mu.Unlock()
}

// takeFrame removes and returns exactly n samples, zero-padding with silence
// when fewer are buffered. It never blocks — an empty buffer yields a frame of
// silence. The mixer uses it to pull one fixed frame per input each cycle.
func (b *bridge) takeFrame(n int) []int16 {
	out := make([]int16, n)
	b.mu.Lock()
	defer b.mu.Unlock()
	m := n
	if m > len(b.buf) {
		m = len(b.buf)
	}
	copy(out, b.buf[:m])
	b.buf = b.buf[m:]
	return out
}
