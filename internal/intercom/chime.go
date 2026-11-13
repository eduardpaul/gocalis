package intercom

import "math"

// The call chimes: a bell that rises when someone joins the call and falls when
// someone leaves. They are rendered once, at the rate the call is mixed at. The
// bell timbre (sharp attack, ringing decay) sets them apart from the ask flow's
// soft "listening" tones.
var (
	joinChime  = renderBells(bridgeRate, 783.99, 1174.66) // G5 -> D6
	leaveChime = renderBells(bridgeRate, 1174.66, 783.99) // D6 -> G5
)

// renderBells synthesizes the given notes as overlapping bell strikes, mono
// PCM16 at sampleRate.
func renderBells(sampleRate int, notes ...float64) []int16 {
	const (
		strikeMs = 110 // time between strikes
		ringMs   = 320 // how long a strike rings
		attackMs = 4   // ramp that avoids a click on the strike
		decayMs  = 70  // time constant of the ring
		amp      = 0.28
	)
	sr := float64(sampleRate)
	strikeN := int(sr * strikeMs / 1000)
	ringN := int(sr * ringMs / 1000)
	attackN := sr * attackMs / 1000

	mix := make([]float64, strikeN*(len(notes)-1)+ringN)
	for k, f := range notes {
		for i := 0; i < ringN; i++ {
			t := float64(i) / sr
			env := math.Exp(-t * 1000 / decayMs)
			if a := float64(i) / attackN; a < 1 {
				env *= a
			}
			// Fade the last milliseconds to exactly zero so the end does not click.
			if left := float64(ringN-i) / attackN; left < 1 {
				env *= left
			}
			// A second partial an octave up gives the strike its brightness.
			v := math.Sin(2*math.Pi*f*t) + 0.35*math.Sin(2*math.Pi*2*f*t)
			mix[k*strikeN+i] += amp * env * v
		}
	}

	out := make([]int16, len(mix))
	for i, v := range mix {
		out[i] = int16(math.Max(-1, math.Min(1, v)) * 32767)
	}
	return out
}
