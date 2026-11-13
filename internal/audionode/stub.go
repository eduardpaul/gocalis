package audionode

import (
	"context"
	"io"
	"sync"
	"time"
)

// Stub is an in-memory AudioNode implementation for tests and for node types
// that do not yet have a real transport. It records played audio and lets
// callers inject captured microphone audio via Emit.
type Stub struct {
	mu         sync.Mutex
	callbackMu sync.Mutex
	onAudio    func(samples []float32)

	Played    [][]int16
	Connected bool
	Closed    bool
}

var _ AudioNode = (*Stub)(nil)

// Connect marks the stub as connected.
func (s *Stub) Connect(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Connected = true
	return nil
}

// Play records a copy of the played samples.
func (s *Stub) Play(_ context.Context, pcm16 []int16, _ int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	buf := make([]int16, len(pcm16))
	copy(buf, pcm16)
	s.Played = append(s.Played, buf)
	return nil
}

// PlayStream drains src and records the concatenated samples as one entry.
func (s *Stub) PlayStream(ctx context.Context, src PCM16Source) error {
	var buf []int16
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		chunk, err := src.ReadPCM16(ctx, 1024)
		buf = append(buf, chunk...)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.Played = append(s.Played, buf)
	s.mu.Unlock()
	return nil
}

// OnAudio registers the microphone callback.
func (s *Stub) OnAudio(callback func(samples []float32)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.onAudio = callback
}

// Close marks the stub as closed.
func (s *Stub) Close() error {
	s.callbackMu.Lock()
	defer s.callbackMu.Unlock()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.Closed = true
	return nil
}

// Emit delivers samples to the registered OnAudio callback, simulating mic input.
func (s *Stub) Emit(samples []float32) {
	s.callbackMu.Lock()
	defer s.callbackMu.Unlock()
	s.mu.Lock()
	cb := s.onAudio
	if s.Closed {
		cb = nil
	}
	s.mu.Unlock()
	if cb != nil {
		cb(samples)
	}
}

// PhoneStub is a Stub somebody has to answer: an in-memory Callee for tests.
// Nobody is on it until PickUp; HangUp takes them off again.
type PhoneStub struct {
	Stub

	// RingTime is how long Call rings before it gives up with ErrUnanswered;
	// zero rings until the caller's context ends.
	RingTime time.Duration

	phoneMu  sync.Mutex
	gone     chan struct{} // not nil while somebody is on the phone
	changed  chan struct{} // closed, and replaced, whenever that changes
	ringing  int           // calls waiting for an answer
	rings    int           // times it started ringing
	holds    int           // lines out
	hungUpBy int           // lines released with hangUp
}

var _ Callee = (*PhoneStub)(nil)

func (p *PhoneStub) change() {
	if p.changed != nil {
		close(p.changed)
	}
	p.changed = make(chan struct{})
}

// PickUp answers the phone.
func (p *PhoneStub) PickUp() {
	p.phoneMu.Lock()
	defer p.phoneMu.Unlock()
	if p.gone == nil {
		p.gone = make(chan struct{})
		p.change()
	}
}

// HangUp takes whoever answered off the phone.
func (p *PhoneStub) HangUp() {
	p.phoneMu.Lock()
	defer p.phoneMu.Unlock()
	if p.gone != nil {
		close(p.gone)
		p.gone = nil
		p.change()
	}
}

// Answered implements Callee.
func (p *PhoneStub) Answered() bool {
	p.phoneMu.Lock()
	defer p.phoneMu.Unlock()
	return p.gone != nil
}

// Rings is how many times the phone started ringing, Ringing whether it is
// ringing now, Holds how many lines are out, and HungUpBy how many lines were
// released with hangUp.
func (p *PhoneStub) Rings() int    { p.phoneMu.Lock(); defer p.phoneMu.Unlock(); return p.rings }
func (p *PhoneStub) Ringing() bool { p.phoneMu.Lock(); defer p.phoneMu.Unlock(); return p.ringing > 0 }
func (p *PhoneStub) Holds() int    { p.phoneMu.Lock(); defer p.phoneMu.Unlock(); return p.holds }
func (p *PhoneStub) HungUpBy() int { p.phoneMu.Lock(); defer p.phoneMu.Unlock(); return p.hungUpBy }

// Call implements Callee.
func (p *PhoneStub) Call(ctx context.Context) (Line, error) {
	var timeout <-chan time.Time
	if p.RingTime > 0 {
		timer := time.NewTimer(p.RingTime)
		defer timer.Stop()
		timeout = timer.C
	}
	p.phoneMu.Lock()
	if p.gone == nil {
		if p.ringing == 0 {
			p.rings++
		}
		p.ringing++
		defer func() {
			p.phoneMu.Lock()
			p.ringing--
			p.phoneMu.Unlock()
		}()
	}
	for {
		if p.gone != nil {
			p.holds++
			line := &phoneLine{phone: p, gone: p.gone}
			p.phoneMu.Unlock()
			return line, nil
		}
		if p.changed == nil {
			p.changed = make(chan struct{})
		}
		changed := p.changed
		p.phoneMu.Unlock()
		select {
		case <-changed:
		case <-timeout:
			return nil, ErrUnanswered
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		p.phoneMu.Lock()
	}
}

type phoneLine struct {
	phone *PhoneStub
	gone  chan struct{}
	once  sync.Once
}

func (l *phoneLine) Gone() <-chan struct{} { return l.gone }

func (l *phoneLine) Release(hangUp bool) {
	l.once.Do(func() {
		l.phone.phoneMu.Lock()
		l.phone.holds--
		if hangUp {
			l.phone.hungUpBy++
		}
		l.phone.phoneMu.Unlock()
		if hangUp {
			l.phone.HangUp()
		}
	})
}
