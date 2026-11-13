package intercom

import (
	"context"

	"gocalis/internal/audio"
	"gocalis/internal/brain"
)

// line is a node's brain.CallLine: how the assistant reaches the node, or the
// whole call, while the node is in the mix.
type line struct {
	mix *mixer
	id  string

	// seat ends (cause brain.ErrCallLeft) when the node goes out of the mix.
	seat context.Context

	// busy holds a token while something uses the line, so a spoken reply and
	// an ask on the same node follow each other instead of overlapping.
	busy chan struct{}
}

var _ brain.CallLine = (*line)(nil)

func newLine(mix *mixer, id string, seat context.Context) *line {
	return &line{mix: mix, id: id, seat: seat, busy: make(chan struct{}, 1)}
}

// take waits for the line to be free.
func (l *line) take(ctx context.Context) error {
	select {
	case l.busy <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-l.seat.Done():
		return context.Cause(l.seat)
	}
}

func (l *line) free() { <-l.busy }

// playClip mixes the clip in for one listener (or everyone when `to` is empty)
// and waits until it has played.
func (l *line) playClip(ctx context.Context, pcm []int16, sampleRate int, to string) error {
	c := l.mix.play(audio.ResampleInt16(pcm, sampleRate, bridgeRate), to)
	select {
	case <-c.done:
		// The mixer also drops a node's clips when the node goes out of the
		// mix: that is not having played.
		if l.seat.Err() != nil {
			return context.Cause(l.seat)
		}
		return nil
	case <-ctx.Done():
		l.mix.drop(c)
		return context.Cause(ctx)
	case <-l.seat.Done():
		l.mix.drop(c)
		return context.Cause(l.seat)
	}
}

func (l *line) Play(ctx context.Context, pcm []int16, sampleRate int, everyone bool) error {
	if err := l.take(ctx); err != nil {
		return err
	}
	defer l.free()
	to := l.id
	if everyone {
		to = ""
	}
	return l.playClip(ctx, pcm, sampleRate, to)
}

func (l *line) Hold(ctx context.Context) (brain.CallHold, error) {
	if err := l.take(ctx); err != nil {
		return nil, err
	}
	hctx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(l.seat, func() { cancel(context.Cause(l.seat)) })
	l.mix.setAside(l.id, true)
	return &hold{line: l, ctx: hctx, release: func() {
		l.mix.setAside(l.id, false)
		stop()
		cancel(nil)
		l.free()
	}}, nil
}

// hold is a node set aside from its call, holding the node's line.
type hold struct {
	line    *line
	ctx     context.Context
	release func()
	done    bool
}

func (h *hold) Context() context.Context { return h.ctx }

func (h *hold) Play(pcm []int16, sampleRate int) error {
	return h.line.playClip(h.ctx, pcm, sampleRate, h.line.id)
}

func (h *hold) Release() {
	if !h.done {
		h.done = true
		h.release()
	}
}
