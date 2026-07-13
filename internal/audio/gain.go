package audio

import "math"

// ApplyGainPCM16 applies a dB gain to PCM16 samples, returning a new slice.
// A gain of 0 dB returns the input unchanged. Results are clamped to the int16
// range to prevent wrap-around distortion.
func ApplyGainPCM16(samples []int16, gainDb float32) []int16 {
	if gainDb == 0 {
		return samples
	}

	gainFactor := math.Pow(10.0, float64(gainDb)/20.0)
	out := make([]int16, len(samples))

	for i, val := range samples {
		newVal := float64(val) * gainFactor
		if newVal > 32767.0 {
			newVal = 32767.0
		} else if newVal < -32768.0 {
			newVal = -32768.0
		}
		out[i] = int16(newVal)
	}

	return out
}
