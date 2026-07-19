package intercom

/*
#cgo LDFLAGS: -l:libspeexdsp.so.1

#include <stdlib.h>

// Minimal speexdsp acoustic-echo-canceller prototypes. Declared here instead of
// including <speex/speex_echo.h> so the build only needs the runtime shared
// library (libspeexdsp.so.1), not the -dev headers. The API is stable.
typedef struct SpeexEchoState_ SpeexEchoState;

SpeexEchoState *speex_echo_state_init(int frame_size, int filter_length);
void speex_echo_state_destroy(SpeexEchoState *st);
void speex_echo_cancellation(SpeexEchoState *st, const short *rec, const short *play, short *out);
void speex_echo_capture(SpeexEchoState *st, const short *rec, short *out);
void speex_echo_playback(SpeexEchoState *st, const short *play);
void speex_echo_state_reset(SpeexEchoState *st);
int speex_echo_ctl(SpeexEchoState *st, int request, void *ptr);

// SPEEX_ECHO_SET_SAMPLING_RATE from speex_echo.h.
#define GOCALIS_SPEEX_ECHO_SET_SAMPLING_RATE 24
*/
import "C"

import (
	"fmt"
	"sync"
	"unsafe"
)

// speexCanceller is a libspeexdsp acoustic echo canceller. It uses the split
// playback/capture API so the far-end reference (fed as audio is played) and
// the near-end mic (fed as it is captured) can arrive on different goroutines;
// speexdsp aligns them internally against its adaptive filter tail. A mutex
// serializes all speex calls because the echo state is not thread-safe.
type speexCanceller struct {
	mu      sync.Mutex
	st      *C.SpeexEchoState
	frame   int     // required samples per speex call
	nearBuf []int16 // near-end (mic) samples awaiting a full frame
	farBuf  []int16 // far-end (played) samples awaiting a full frame
	out     []int16 // reusable single-frame output scratch
	closed  bool
}

var _ EchoCanceller = (*speexCanceller)(nil)

// NewSpeexEchoCanceller builds a canceller at the given sample rate, processing
// frameSamples per step with an adaptive filter of tailSamples length. It
// satisfies the intercom.Engine newAEC signature.
func NewSpeexEchoCanceller(rate, frameSamples, tailSamples int) (EchoCanceller, error) {
	if frameSamples <= 0 {
		return nil, fmt.Errorf("aec: frame size must be positive")
	}
	if tailSamples < frameSamples {
		tailSamples = frameSamples
	}
	st := C.speex_echo_state_init(C.int(frameSamples), C.int(tailSamples))
	if st == nil {
		return nil, fmt.Errorf("aec: speex_echo_state_init failed")
	}
	if rate > 0 {
		r := C.int(rate)
		C.speex_echo_ctl(st, C.GOCALIS_SPEEX_ECHO_SET_SAMPLING_RATE, unsafe.Pointer(&r))
	}
	return &speexCanceller{
		st:    st,
		frame: frameSamples,
		out:   make([]int16, frameSamples),
	}, nil
}

// Playback feeds the far-end reference (audio being played on the node) in
// frame-sized steps.
func (s *speexCanceller) Playback(farPCM []int16) {
	if len(farPCM) == 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.farBuf = append(s.farBuf, farPCM...)
	for len(s.farBuf) >= s.frame {
		C.speex_echo_playback(s.st, (*C.short)(unsafe.Pointer(&s.farBuf[0])))
		s.farBuf = s.farBuf[s.frame:]
	}
}

// Capture returns the near-end mic with the echo of the far-end removed. Input
// is buffered to frame boundaries, so the returned slice may be shorter than the
// input by up to one frame; the remainder is emitted on the next call.
func (s *speexCanceller) Capture(nearPCM []int16) []int16 {
	if len(nearPCM) == 0 {
		return nearPCM
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nearPCM
	}
	s.nearBuf = append(s.nearBuf, nearPCM...)
	var result []int16
	for len(s.nearBuf) >= s.frame {
		C.speex_echo_capture(
			s.st,
			(*C.short)(unsafe.Pointer(&s.nearBuf[0])),
			(*C.short)(unsafe.Pointer(&s.out[0])),
		)
		result = append(result, s.out...)
		s.nearBuf = s.nearBuf[s.frame:]
	}
	return result
}

// Close destroys the underlying speex state. Further calls are no-ops.
func (s *speexCanceller) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	C.speex_echo_state_destroy(s.st)
	s.st = nil
}
