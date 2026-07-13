// Package audio centralizes low-level audio DSP primitives (sample-type
// conversion, WAV encoding, G.711 mu-law codec, linear resampling and gain)
// that were previously hand-rolled and duplicated across the webrtc, ask,
// webserver and brain packages. Keeping a single implementation avoids the
// drift and rounding inconsistencies that come from copy-pasted DSP code.
package audio

// FloatToPCM16 converts float32 samples in the range [-1.0, 1.0] to signed
// 16-bit PCM. Values outside the range are clamped to avoid integer wrap-around.
func FloatToPCM16(samples []float32) []int16 {
	out := make([]int16, len(samples))
	for i, s := range samples {
		out[i] = floatToInt16(s)
	}
	return out
}

// PCM16ToFloat converts signed 16-bit PCM samples to float32 in the range
// [-1.0, 1.0].
func PCM16ToFloat(samples []int16) []float32 {
	out := make([]float32, len(samples))
	for i, s := range samples {
		out[i] = float32(s) / 32767.0
	}
	return out
}

// floatToInt16 converts a single float32 sample to int16 with clamping.
func floatToInt16(s float32) int16 {
	v := s * 32767.0
	if v > 32767.0 {
		v = 32767.0
	} else if v < -32768.0 {
		v = -32768.0
	}
	return int16(v)
}
