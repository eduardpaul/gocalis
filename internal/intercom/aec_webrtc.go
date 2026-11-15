package intercom

/*
#cgo pkg-config: webrtc-audio-processing-1
#cgo CXXFLAGS: -std=c++17
#include <stdlib.h>
#include "aec_webrtc.h"
*/
import "C"

import (
	"fmt"
	"log"
	"math"
	"sync"
	"time"
	"unsafe"
)

// AECOptions configures the echo canceller of one node in a call.
type AECOptions struct {
	// Node is the node the canceller is for (used in its log lines).
	Node string
	// SampleRate is the rate of the PCM16 fed to Playback and Capture.
	SampleRate int
	// Delay is how long the far-end reference is held back before the
	// canceller sees it: a hint for the part of the echo delay that is known to
	// be there (network, transcoding, device buffers). AEC3 estimates the rest
	// itself, within a window of about half a second past Delay.
	Delay time.Duration
	// NoiseSuppression switches WebRTC's noise suppressor on as well.
	NoiseSuppression bool
}

// webrtcCanceller is an EchoCanceller backed by WebRTC's audio processing
// module (AEC3: an adaptive linear filter with its own delay estimator plus a
// non-linear residual echo suppressor).
//
// Playback and Capture arrive on different goroutines. Playback only queues
// the far end, stamped with its arrival time; Capture hands the queued far end
// that is at least Delay old to the module and then processes the mic, so the
// module is only ever driven from one goroutine at a time, under mu.
type webrtcCanceller struct {
	mu     sync.Mutex
	apm    *C.gocalis_apm
	node   string
	frame  int           // samples per 10 ms frame
	delay  time.Duration // far-end hold-back
	now    func() time.Time
	far    []farChunk // far end not yet due, oldest first
	farLen int        // samples in far
	farMax int        // far is trimmed (oldest first) beyond this
	render []int16    // far end due, awaiting a full frame
	near   []int16    // mic awaiting a full frame
	frames int        // mic frames processed
	inE    float64    // mic energy into the canceller since the last stats log
	outE   float64    // and out of it
	errLog bool       // an error code has been logged
	closed bool
}

type farChunk struct {
	at  time.Time
	pcm []int16
}

var _ EchoCanceller = (*webrtcCanceller)(nil)

// statsEvery is how often (in processed mic audio) a canceller logs its echo
// delay and suppression, for tuning a node's delay hint.
const statsEvery = 10 * time.Second

// NewWebRTCEchoCanceller builds a WebRTC AEC3 echo canceller. It satisfies the
// intercom.Engine newAEC signature.
func NewWebRTCEchoCanceller(opts AECOptions) (EchoCanceller, error) {
	switch opts.SampleRate {
	case 8000, 16000, 32000, 48000:
	default:
		return nil, fmt.Errorf("aec: unsupported sample rate %d", opts.SampleRate)
	}
	if opts.Delay < 0 {
		return nil, fmt.Errorf("aec: negative delay %v", opts.Delay)
	}
	ns := C.int(0)
	if opts.NoiseSuppression {
		ns = 1
	}
	apm := C.gocalis_apm_new(C.int(opts.SampleRate), ns)
	if apm == nil {
		return nil, fmt.Errorf("aec: creating the WebRTC audio processing module failed")
	}
	return &webrtcCanceller{
		apm:   apm,
		node:  opts.Node,
		frame: int(C.gocalis_apm_frame_samples(apm)),
		delay: opts.Delay,
		now:   time.Now,
		// Hold at most the delay plus two seconds: if the mic stops, the far
		// end must not pile up without bound.
		farMax: opts.SampleRate * int((opts.Delay+2*time.Second)/time.Millisecond) / 1000,
	}, nil
}

// Playback queues the far-end reference: the audio being played on the node.
func (w *webrtcCanceller) Playback(farPCM []int16) {
	if len(farPCM) == 0 {
		return
	}
	pcm := make([]int16, len(farPCM))
	copy(pcm, farPCM)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.far = append(w.far, farChunk{at: w.now(), pcm: pcm})
	w.farLen += len(pcm)
	for w.farLen > w.farMax && len(w.far) > 1 {
		w.farLen -= len(w.far[0].pcm)
		w.far[0] = farChunk{}
		w.far = w.far[1:]
	}
}

// Capture returns the near-end mic with the echo of the far end removed. Input
// is processed in 10 ms frames, so the returned slice may be shorter than the
// input by up to one frame; the remainder comes out of the next call.
func (w *webrtcCanceller) Capture(nearPCM []int16) []int16 {
	if len(nearPCM) == 0 {
		return nearPCM
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nearPCM
	}
	w.feedRender()

	w.near = append(w.near, nearPCM...)
	n := len(w.near) / w.frame * w.frame
	if n == 0 {
		return nil
	}
	out := make([]int16, n)
	copy(out, w.near[:n])
	w.near = append(w.near[:0], w.near[n:]...)
	w.inE += pcmEnergy(out)
	for i := 0; i < n; i += w.frame {
		w.check("capture", C.gocalis_apm_capture(w.apm, (*C.int16_t)(unsafe.Pointer(&out[i]))))
		w.outE += pcmEnergy(out[i : i+w.frame])
		w.frames++
		if w.frames%int(statsEvery/(10*time.Millisecond)) == 0 {
			w.logStats()
		}
	}
	return out
}

// feedRender hands the module every queued far-end chunk that is due, in whole
// 10 ms frames. The caller holds mu.
func (w *webrtcCanceller) feedRender() {
	due := w.now().Add(-w.delay)
	i := 0
	for ; i < len(w.far) && !w.far[i].at.After(due); i++ {
		w.render = append(w.render, w.far[i].pcm...)
		w.farLen -= len(w.far[i].pcm)
		w.far[i] = farChunk{}
	}
	w.far = w.far[i:]
	n := len(w.render) / w.frame * w.frame
	for j := 0; j < n; j += w.frame {
		w.check("render", C.gocalis_apm_render(w.apm, (*C.int16_t)(unsafe.Pointer(&w.render[j]))))
	}
	w.render = append(w.render[:0], w.render[n:]...)
}

// check logs the first error the module reports; it keeps processing.
func (w *webrtcCanceller) check(stream string, code C.int) {
	if code != 0 && !w.errLog {
		w.errLog = true
		log.Printf("[Intercom] echo canceller for '%s': %s error %d\n", w.node, stream, int(code))
	}
}

// echoDelay returns the module's current echo delay estimate, including the
// hold-back, when it has one. The caller holds mu.
func (w *webrtcCanceller) echoDelay() (time.Duration, bool) {
	var ms C.int
	if C.gocalis_apm_delay_ms(w.apm, &ms) == 0 {
		return 0, false
	}
	return w.delay + time.Duration(ms)*time.Millisecond, true
}

// logStats logs the echo delay estimate and how much quieter the mic came out
// since the last log (while only the far end talks, that is the echo
// reduction), then starts a new measurement. The caller holds mu.
func (w *webrtcCanceller) logStats() {
	d := "unknown"
	if delay, ok := w.echoDelay(); ok {
		d = fmt.Sprintf("%d ms", delay.Milliseconds())
	}
	log.Printf("[Intercom] echo canceller for '%s': echo delay estimate %s (hint %d ms), mic level %+.1f dB after cancellation\n",
		w.node, d, w.delay.Milliseconds(), 10*math.Log10((w.outE+1)/(w.inE+1)))
	w.inE, w.outE = 0, 0
}

func pcmEnergy(pcm []int16) float64 {
	e := 0.0
	for _, v := range pcm {
		e += float64(v) * float64(v)
	}
	return e
}

// Close logs the final statistics and frees the module. Further calls are
// no-ops.
func (w *webrtcCanceller) Close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return
	}
	w.closed = true
	if w.frames > 0 {
		w.logStats()
	}
	C.gocalis_apm_free(w.apm)
	w.apm = nil
	w.far, w.render, w.near = nil, nil, nil
}
