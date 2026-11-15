package intercom

import (
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"
)

// fakeClock is a manually advanced clock for the canceller's far-end hold-back.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestCanceller(t *testing.T, delay time.Duration) (*webrtcCanceller, *fakeClock) {
	t.Helper()
	ec, err := NewWebRTCEchoCanceller(AECOptions{Node: "test", SampleRate: bridgeRate, Delay: delay})
	if err != nil {
		t.Fatalf("NewWebRTCEchoCanceller: %v", err)
	}
	w := ec.(*webrtcCanceller)
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	w.now = clk.now
	t.Cleanup(w.Close)
	return w, clk
}

// speechLike returns n samples of noise shaped like speech: low-passed noise
// in syllable-length bursts (about 4 per second) with pauses between phrases.
func speechLike(rng *rand.Rand, n int, amp float64) []float64 {
	out := make([]float64, n)
	var lp1, lp2 float64
	for i := range out {
		lp1 += 0.35 * (rng.NormFloat64() - lp1)
		lp2 += 0.35 * (lp1 - lp2)
		sec := float64(i) / bridgeRate
		syl := math.Abs(math.Sin(2 * math.Pi * 2 * sec)) // 4 bursts/s
		if math.Mod(sec, 3) > 2.4 {                      // 0.6 s pause every 3 s
			syl = 0
		}
		out[i] = amp * syl * lp2 * 3
	}
	return out
}

// voiced returns n samples of a voiced, pitch-wobbling harmonic signal (a
// stand-in for the near-end talker), with syllabic bursts.
func voiced(n int, amp float64) []float64 {
	out := make([]float64, n)
	phase := 0.0
	for i := range out {
		sec := float64(i) / bridgeRate
		f0 := 140 + 25*math.Sin(2*math.Pi*0.7*sec)
		phase += 2 * math.Pi * f0 / bridgeRate
		v := 0.0
		for h := 1; h <= 8; h++ {
			v += math.Sin(float64(h)*phase) / float64(h)
		}
		out[i] = amp * math.Abs(math.Sin(2*math.Pi*1.5*sec)) * v / 2
	}
	return out
}

// echoPath models the doorbell's speaker-to-mic path: a delay, attenuation, a
// short reflection tail and, when saturate is set, the soft clipping of a
// small speaker driven hard (the non-linear part a linear filter cannot model).
func echoPath(far []float64, delaySamples int, gain float64, saturate bool) []float64 {
	taps := []struct {
		at int
		g  float64
	}{{0, 1}, {24, 0.45}, {70, -0.25}, {130, 0.12}}
	out := make([]float64, len(far))
	for i := range out {
		v := 0.0
		for _, tp := range taps {
			if j := i - delaySamples - tp.at; j >= 0 {
				v += tp.g * far[j]
			}
		}
		v *= gain
		if saturate {
			v = 6000 * math.Tanh(v/6000)
		}
		out[i] = v
	}
	return out
}

func toPCM(x []float64) []int16 {
	out := make([]int16, len(x))
	for i, v := range x {
		out[i] = int16(max(-32768, min(32767, math.Round(v))))
	}
	return out
}

func db(num, den float64) float64 { return 10 * math.Log10((num+1)/(den+1)) }

// simulate runs a call through w: the far end is tapped in 2048-sample chunks
// as gocalis reads the mix (ahead of playback), the mic arrives in 20 ms
// chunks. It returns what Capture produced, aligned with near.
func simulate(t *testing.T, w *webrtcCanceller, clk *fakeClock, far, near []int16) []int16 {
	t.Helper()
	const farChunk, micChunk = 2048, bridgeRate * 20 / 1000
	out := make([]int16, 0, len(near))
	nextFar := 0
	for pos := 0; pos+micChunk <= len(near); pos += micChunk {
		for nextFar < len(far) && nextFar <= pos {
			end := min(nextFar+farChunk, len(far))
			w.Playback(far[nextFar:end])
			nextFar = end
		}
		clk.advance(20 * time.Millisecond)
		out = append(out, w.Capture(near[pos:pos+micChunk])...)
	}
	if len(out) != len(near)/micChunk*micChunk {
		t.Fatalf("capture returned %d samples for %d", len(out), len(near))
	}
	return out
}

type aecResult struct {
	erle       float64 // echo-only stretch: echo in vs residual out, dB
	nearKept   float64 // near-only stretch: out vs in, dB (0 = untouched)
	doubleKept float64 // double talk: out vs near-end speech in, dB
}

// runScenario: 0-12 s far end only (convergence, ERLE measured over 6-12 s),
// 12-15 s near end only, 15-18 s both talking.
func runScenario(t *testing.T, echoDelay, hint time.Duration, saturate bool) aecResult {
	t.Helper()
	w, clk := newTestCanceller(t, hint)
	rng := rand.New(rand.NewPCG(1, 2))
	sec := func(s float64) int { return int(s * bridgeRate) }
	n := sec(18)

	farF := speechLike(rng, n, 6000)
	for i := sec(12); i < sec(15); i++ {
		farF[i] = 0
	}
	echoF := echoPath(farF, int(echoDelay.Seconds()*bridgeRate), 0.5, saturate)
	talkF := voiced(n, 3000)
	for i := 0; i < sec(12); i++ {
		talkF[i] = 0
	}
	nearF := make([]float64, n)
	for i := range nearF {
		nearF[i] = echoF[i] + talkF[i] + 20*rng.NormFloat64() // mic noise floor
	}
	far, near, echo, talk := toPCM(farF), toPCM(nearF), toPCM(echoF), toPCM(talkF)
	out := simulate(t, w, clk, far, near)

	r := aecResult{
		erle:       db(pcmEnergy(echo[sec(6):sec(12)]), pcmEnergy(out[sec(6):sec(12)])),
		nearKept:   db(pcmEnergy(out[sec(12):sec(15)]), pcmEnergy(talk[sec(12):sec(15)])),
		doubleKept: db(pcmEnergy(out[sec(15):sec(18)]), pcmEnergy(talk[sec(15):sec(18)])),
	}
	w.mu.Lock()
	estimate, _ := w.echoDelay()
	w.mu.Unlock()
	t.Logf("echo delay %v, hint %v, saturate %v: echo reduced %.1f dB, near end alone %+.1f dB, double talk out/near %+.1f dB; delay estimate %v",
		echoDelay, hint, saturate, r.erle, r.nearKept, r.doubleKept, estimate)
	return r
}

func checkAEC(t *testing.T, r aecResult) {
	t.Helper()
	if r.erle < 20 {
		t.Errorf("echo reduced by %.1f dB after convergence, want >= 20 dB", r.erle)
	}
	if r.nearKept < -3 || r.nearKept > 3 {
		t.Errorf("near-end speech alone came out at %.1f dB of its input, want within 3 dB", r.nearKept)
	}
}

// TestWebRTCAECCancelsDelayedEcho: an echo 300 ms behind the reference, with
// no delay hint, is found and cancelled by AEC3's own delay estimator.
func TestWebRTCAECCancelsDelayedEcho(t *testing.T) {
	checkAEC(t, runScenario(t, 300*time.Millisecond, 0, false))
}

// TestWebRTCAECCancelsLongDelayWithHint: a 700 ms echo (the doorbell's range)
// is out of AEC3's reach alone; holding the reference back 400 ms brings it in.
func TestWebRTCAECCancelsLongDelayWithHint(t *testing.T) {
	checkAEC(t, runScenario(t, 700*time.Millisecond, 400*time.Millisecond, false))
}

// TestWebRTCAECNeedsHintBeyondWindow: without the hint the same 700 ms echo is
// beyond AEC3's delay search and passes through, which is what the per-node
// delay_ms setting is for.
func TestWebRTCAECNeedsHintBeyondWindow(t *testing.T) {
	if r := runScenario(t, 700*time.Millisecond, 0, false); r.erle > 6 {
		t.Errorf("700 ms echo reduced by %.1f dB with no hint; the delay window assumption is off", r.erle)
	}
}

// TestWebRTCAECNonLinearEcho: a saturating speaker on the echo path leaves a
// non-linear residual the suppressor has to take care of.
func TestWebRTCAECNonLinearEcho(t *testing.T) {
	checkAEC(t, runScenario(t, 500*time.Millisecond, 300*time.Millisecond, true))
}

// TestWebRTCAECConcurrentFeeds drives Playback and Capture from two goroutines
// (as a call does) and closes the canceller while they run.
func TestWebRTCAECConcurrentFeeds(t *testing.T) {
	ec, err := NewWebRTCEchoCanceller(AECOptions{Node: "test", SampleRate: bridgeRate, Delay: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("NewWebRTCEchoCanceller: %v", err)
	}
	rng := rand.New(rand.NewPCG(3, 4))
	pcm := toPCM(speechLike(rng, bridgeRate, 6000))
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			ec.Playback(pcm[:2048])
			time.Sleep(time.Millisecond)
		}
	}()
	got := 0
	go func() {
		defer wg.Done()
		for i := 0; i < 400; i++ {
			got += len(ec.Capture(pcm[:333]))
			time.Sleep(500 * time.Microsecond)
		}
	}()
	time.Sleep(50 * time.Millisecond)
	ec.Close()
	wg.Wait()
	ec.Close()
	if got == 0 {
		t.Fatal("capture produced nothing before Close")
	}
	if out := ec.Capture(pcm[:320]); len(out) != 320 {
		t.Fatalf("capture after Close returned %d samples, want the input back", len(out))
	}
}

// TestWebRTCAECFramesCapture checks the 10 ms framing: odd-sized input comes
// out in whole frames, nothing is lost or duplicated.
func TestWebRTCAECFramesCapture(t *testing.T) {
	w, _ := newTestCanceller(t, 0)
	total := 0
	for _, n := range []int{100, 100, 100, 37, 500, 3} { // 840 samples
		total += len(w.Capture(make([]int16, n)))
	}
	if total != 800 {
		t.Fatalf("got %d samples out, want 800 (5 frames, 40 pending)", total)
	}
	if len(w.near) != 40 {
		t.Fatalf("pending %d samples, want 40", len(w.near))
	}
}

func TestWebRTCAECRejectsBadOptions(t *testing.T) {
	if _, err := NewWebRTCEchoCanceller(AECOptions{SampleRate: 22050}); err == nil {
		t.Error("accepted 22050 Hz")
	}
	if _, err := NewWebRTCEchoCanceller(AECOptions{SampleRate: bridgeRate, Delay: -time.Millisecond}); err == nil {
		t.Error("accepted a negative delay")
	}
}
