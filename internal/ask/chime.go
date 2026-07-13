package ask

import "math"

// chimeKind selects which UI chime to render.
type chimeKind int

const (
	// chimeStart is played just before capture begins ("I'm listening").
	chimeStart chimeKind = iota
	// chimeStop is played right after capture ends ("done listening").
	chimeStop
)

// chimeSampleRate is the rate at which chimes are rendered. The per-node audio
// output resamples this to the device rate, so the same PCM works everywhere.
const chimeSampleRate = 24000

// Precomputed chime PCM. The tones are constant, so render them once at startup.
var (
	chimeStartPCM = renderChime(chimeStart)
	chimeStopPCM  = renderChime(chimeStop)
)

// chimePCM returns the precomputed samples for a chime kind.
func chimePCM(kind chimeKind) []int16 {
	if kind == chimeStop {
		return chimeStopPCM
	}
	return chimeStartPCM
}

// renderChime synthesizes a short two-tone Alexa-style chime as mono PCM16 at
// chimeSampleRate.
func renderChime(kind chimeKind) []int16 {
	return renderChimeAt(kind, chimeSampleRate)
}

// renderChimeAt synthesizes the chime at an arbitrary sample rate. Rendering at
// the prompt's rate lets the chime be concatenated onto the prompt so both play
// in a single, gapless audio transmission (avoids a second route-assert/drain
// cycle that would otherwise leave an audible silence between them).
// The start chime rises (ready to listen); the stop chime falls (done).
func renderChimeAt(kind chimeKind, sampleRate int) []int16 {
	var tones []float64
	switch kind {
	case chimeStop:
		tones = []float64{659.25, 523.25} // E5 -> C5 falling
	default:
		tones = []float64{523.25, 659.25} // C5 -> E5 rising
	}

	const (
		toneMs = 110
		gapMs  = 15
		amp    = 0.30 // leaves headroom for per-node output gain
	)

	sr := float64(sampleRate)
	toneN := int(sr * toneMs / 1000)
	gapN := int(sr * gapMs / 1000)

	out := make([]int16, 0, len(tones)*(toneN+gapN))
	for _, f := range tones {
		for i := 0; i < toneN; i++ {
			t := float64(i) / sr
			// Hann envelope avoids start/end clicks.
			env := 0.5 * (1 - math.Cos(2*math.Pi*float64(i)/float64(toneN-1)))
			v := amp * env * math.Sin(2*math.Pi*f*t)
			out = append(out, int16(v*32767))
		}
		for i := 0; i < gapN; i++ {
			out = append(out, 0)
		}
	}
	return out
}
