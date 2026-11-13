package telegramcall

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"gocalis/internal/audio"
	"gocalis/internal/audionode"
)

// The plugin is the audio node it adds to gocalis, and a node somebody has to
// answer.
var (
	_ audionode.AudioNode = (*Plugin)(nil)
	_ audionode.Callee    = (*Plugin)(nil)
)

// Answered implements audionode.Callee: somebody of the group is in the voice
// chat with gocalis.
func (p *Plugin) Answered() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.call != nil && !p.call.over() && p.call.isAnswered()
}

// Call implements audionode.Callee. With nobody on the line it goes into the
// group's voice chat (starting it when none runs) and waits there for somebody
// to come in, for the configured ring time.
func (p *Plugin) Call(ctx context.Context) (audionode.Line, error) {
	p.mu.Lock()
	if p.api == nil {
		detail := p.detail
		p.mu.Unlock()
		return nil, fmt.Errorf("Telegram is not available (%s)", detail)
	}
	c := p.call
	if c == nil || c.over() {
		c = p.newCall(c)
		p.call = c
		go c.run()
	}
	c.users++
	if c.idle != nil {
		c.idle.Stop()
		c.idle = nil
	}
	p.mu.Unlock()

	select {
	case <-c.answered:
		select {
		case <-c.gone:
			// They were gone again before the turn got to them.
			p.unuse(c, false)
			return nil, audionode.ErrHungUp
		default:
		}
		return &line{call: c}, nil
	case <-c.gone:
		p.unuse(c, false)
		return nil, c.err()
	case <-ctx.Done():
		p.unuse(c, false)
		return nil, ctx.Err()
	}
}

// newCall prepares a call that waits for the one before it to be out of the
// voice chat. The caller holds mu.
func (p *Plugin) newCall(prev *call) *call {
	ctx, stop := context.WithCancelCause(context.Background())
	c := &call{
		p:        p,
		api:      p.api,
		ctx:      ctx,
		stop:     stop,
		answered: make(chan struct{}),
		gone:     make(chan struct{}),
		finished: make(chan struct{}),
	}
	if prev != nil {
		c.prev = prev.finished
	}
	return c
}

// unuse counts a turn out of the call: it gave up waiting, or its hold of the
// line is over. With hangUp gocalis leaves now. Otherwise a ringing call nobody
// waits for any more is dropped, and an answered one stays for the idle time in
// case another turn follows.
func (p *Plugin) unuse(c *call, hangUp bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	c.users--
	switch {
	case hangUp:
		c.stop(errDone)
	case c.users > 0 || c.over():
	case !c.isAnswered():
		c.stop(errAbandoned)
	default:
		c.idle = time.AfterFunc(p.idleFor, func() {
			p.mu.Lock()
			defer p.mu.Unlock()
			if c.users == 0 {
				c.stop(errDone)
			}
		})
	}
}

// release is called by a call that is out of the voice chat.
func (p *Plugin) release(c *call) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c.idle != nil {
		c.idle.Stop()
	}
	if p.call == c {
		p.call = nil
	}
}

// endCall ends the call in progress, if any, and waits until gocalis is out of
// the voice chat. It tells whether there was one.
func (p *Plugin) endCall(cause error) bool {
	p.mu.Lock()
	c := p.call
	p.mu.Unlock()
	if c == nil {
		return false
	}
	c.stop(cause)
	<-c.finished
	return true
}

// line is a turn's hold on an answered call.
type line struct {
	call *call
	once sync.Once
}

func (l *line) Gone() <-chan struct{} { return l.call.gone }

func (l *line) Release(hangUp bool) {
	l.once.Do(func() { l.call.p.unuse(l.call, hangUp) })
}

// Connect implements audionode.AudioNode. The node's transport is the Telegram
// login, which Run keeps up, and the voice chat, which each call enters.
func (p *Plugin) Connect(context.Context) error { return nil }

// Close implements audionode.AudioNode: it hangs up.
func (p *Plugin) Close() error {
	p.endCall(errShutdown)
	return nil
}

// OnAudio implements audionode.AudioNode: callback gets what the people in the
// voice chat say, mixed together, while a call is answered.
func (p *Plugin) OnAudio(callback func(samples []float32)) {
	p.audioMu.Lock()
	p.onAudio = callback
	p.audioMu.Unlock()
}

// heard takes one frame of the voice chat's mixdown to the node's listener.
func (p *Plugin) heard(pcm []int16) {
	p.audioMu.Lock()
	defer p.audioMu.Unlock()
	if p.onAudio != nil {
		p.onAudio(audio.PCM16ToFloat(pcm))
	}
}

// Play implements audionode.AudioNode.
func (p *Plugin) Play(ctx context.Context, pcm16 []int16, sampleRate int) error {
	return p.PlayStream(ctx, audionode.NewSliceSource(pcm16, sampleRate))
}

// PlayStream implements audionode.AudioNode: it sends the audio to the voice
// chat at the pace it plays, and returns once the last of it is sent. With
// nobody on the line it fails with audionode.ErrHungUp.
func (p *Plugin) PlayStream(ctx context.Context, src audionode.PCM16Source) error {
	p.mu.Lock()
	c := p.call
	p.mu.Unlock()
	if c == nil || !c.isAnswered() {
		return audionode.ErrHungUp
	}
	rate := src.SampleRate()
	if rate <= 0 {
		return errors.New("invalid sample rate")
	}
	var resampler *audio.PCMResampler
	if rate != pcmRate {
		resampler = audio.NewPCMResampler(rate, pcmRate)
	}

	var pending []int16
	next := time.Now()
	// send plays out the whole frames that are waiting, no closer together
	// than they play.
	send := func() error {
		for len(pending) >= frameSamples {
			if wait := time.Until(next); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-c.gone:
					timer.Stop()
					return audionode.ErrHungUp
				case <-ctx.Done():
					timer.Stop()
					return ctx.Err()
				}
			}
			if now := time.Now(); now.After(next.Add(100 * time.Millisecond)) {
				next = now
			}
			next = next.Add(20 * time.Millisecond)
			select {
			case <-c.gone:
				return audionode.ErrHungUp
			default:
			}
			if err := c.conn.send(pending[:frameSamples]); err != nil {
				return err
			}
			pending = pending[frameSamples:]
		}
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		pcm, err := src.ReadPCM16(ctx, rate/50)
		if resampler != nil {
			pcm = resampler.Push(pcm)
		}
		pending = append(pending, pcm...)
		if err == io.EOF {
			if resampler != nil {
				pending = append(pending, resampler.Flush()...)
			}
			// The last frame is filled up with silence.
			if rest := len(pending) % frameSamples; rest != 0 {
				pending = append(pending, make([]int16, frameSamples-rest)...)
			}
			return send()
		}
		if err != nil {
			return err
		}
		if err := send(); err != nil {
			return err
		}
	}
}
