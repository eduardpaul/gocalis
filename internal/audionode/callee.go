package audionode

import (
	"context"
	"errors"
)

// ErrUnanswered is returned by Callee.Call when nobody answered the node in
// its ring time.
var ErrUnanswered = errors.New("nobody answered")

// ErrHungUp ends a turn on a node whose far side is gone: they hung up, or the
// call was lost.
var ErrHungUp = errors.New("the call was hung up")

// Callee is implemented by the audio node of an endpoint nobody is at until
// they answer: a call into a Telegram voice chat, as opposed to a speaker or a
// doorbell, which are always there. The brain calls such a node before every
// turn on it (speech, a clip, an ask, an intercom call), so the turn only
// starts once somebody is listening.
type Callee interface {
	// Answered tells whether somebody is on the node right now.
	Answered() bool

	// Call makes sure somebody is on the node: it rings when nobody is and
	// returns once somebody answered, with the line the turn holds until it is
	// over. It fails with ErrUnanswered when nobody answered in the node's ring
	// time, and with ctx's error when ctx ends first.
	Call(ctx context.Context) (Line, error)
}

// Line is a turn's hold on an answered Callee.
type Line interface {
	// Gone is closed once nobody is on the node any more.
	Gone() <-chan struct{}

	// Release ends the hold. With hangUp the node hangs up now (an intercom
	// call that is over); without, it may stay on the line for a moment in
	// case another turn follows (an answer to what was just asked).
	Release(hangUp bool)
}
