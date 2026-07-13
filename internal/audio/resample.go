package audio

// ResampleFloat32 resamples float32 samples from srcRate to dstRate using linear
// interpolation. When the rates are equal the input is returned unchanged.
func ResampleFloat32(samples []float32, srcRate, dstRate int) []float32 {
	if srcRate == dstRate || len(samples) == 0 {
		return samples
	}

	outLen := len(samples) * dstRate / srcRate
	out := make([]float32, outLen)
	ratio := float64(srcRate) / float64(dstRate)

	for i := 0; i < outLen; i++ {
		pos := float64(i) * ratio
		idx := int(pos)
		frac := float32(pos - float64(idx))

		if idx+1 < len(samples) {
			out[i] = samples[idx]*(1.0-frac) + samples[idx+1]*frac
		} else if idx < len(samples) {
			out[i] = samples[idx]
		}
	}
	return out
}

// ResampleInt16 resamples PCM16 samples from srcRate to dstRate using linear
// interpolation. When the rates are equal the input is returned unchanged.
func ResampleInt16(samples []int16, srcRate, dstRate int) []int16 {
	if srcRate == dstRate || len(samples) == 0 {
		return samples
	}

	outLen := len(samples) * dstRate / srcRate
	out := make([]int16, outLen)
	ratio := float64(srcRate) / float64(dstRate)

	for i := 0; i < outLen; i++ {
		pos := float64(i) * ratio
		idx := int(pos)
		frac := pos - float64(idx)

		if idx+1 < len(samples) {
			s0 := float64(samples[idx])
			s1 := float64(samples[idx+1])
			out[i] = int16(s0*(1.0-frac) + s1*frac)
		} else if idx < len(samples) {
			out[i] = samples[idx]
		}
	}
	return out
}
