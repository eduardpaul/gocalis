// Package session models a single voice interaction turn as an explicit finite
// state machine. Each Session owns its own capture buffer and barge-in signaling,
// so multiple turns can run concurrently on the same node without sharing mutable
// per-turn state. A Registry fans captured microphone audio out to every active
// session on a node.
package session

import (
	"sync"
)

// State is a stage in a turn's lifecycle.
type State string

const (
	// StatePrompting: the node is playing the TTS prompt (barge-in may be armed).
	StatePrompting State = "PROMPTING"
	// StateListening: capturing the user's spoken response.
	StateListening State = "LISTENING"
	// StateProcessing: running ASR / speaker-ID on the captured audio.
	StateProcessing State = "PROCESSING"
	// StateDone: the turn has completed (terminal).
	StateDone State = "DONE"
)

// Session owns the per-turn state for one interaction: an explicit lifecycle
// FSM, an isolated capture buffer, and barge-in signaling. It is safe for
// concurrent use.
type Session struct {
	ID     string
	NodeID string

	mu            sync.Mutex
	state         State
	captureActive bool
	captured      []float32

	barged     bool
	bargeArmed bool
	bargeCh    chan struct{}
}

// New creates a Session in the Prompting state.
func New(id, nodeID string) *Session {
	return &Session{ID: id, NodeID: nodeID, state: StatePrompting}
}

// State returns the current lifecycle state.
func (s *Session) State() State {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state
}

func (s *Session) setState(st State) {
	s.mu.Lock()
	s.state = st
	s.mu.Unlock()
}

// ToListening transitions the session into the capture phase.
func (s *Session) ToListening() { s.setState(StateListening) }

// ToProcessing transitions the session into the post-capture analysis phase.
func (s *Session) ToProcessing() { s.setState(StateProcessing) }

// ToDone marks the session terminal.
func (s *Session) ToDone() { s.setState(StateDone) }

// ArmBargeIn enables barge-in detection and returns a channel that receives a
// single signal the first time speech is fed to the session.
func (s *Session) ArmBargeIn() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bargeCh = make(chan struct{}, 1)
	s.bargeArmed = true
	s.barged = false
	return s.bargeCh
}

// DisarmBargeIn disables barge-in detection.
func (s *Session) DisarmBargeIn() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bargeArmed = false
	s.bargeCh = nil
	return s.barged
}

// StartCapture begins accumulating speech samples, discarding any prior buffer.
func (s *Session) StartCapture() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.captureActive = true
	s.captured = nil
}

// StopCapture stops accumulating samples and returns what was captured.
func (s *Session) StopCapture() []float32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.captureActive = false
	return s.captured
}

// CapturedCount returns the number of samples captured so far.
func (s *Session) CapturedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.captured)
}

// Feed delivers a detected-speech segment to the session. It signals barge-in
// (when armed) and appends to the capture buffer (when capturing).
func (s *Session) Feed(samples []float32) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(samples) == 0 {
		return
	}
	if s.bargeArmed && !s.barged {
		s.barged = true
		s.captureActive = true
		s.captured = nil
		s.state = StateListening
		s.bargeCh <- struct{}{}
	}

	if s.captureActive {
		remaining := 16000*120 - len(s.captured)
		if len(samples) > remaining {
			samples = samples[:remaining]
		}
		s.captured = append(s.captured, samples...)
	}
}
