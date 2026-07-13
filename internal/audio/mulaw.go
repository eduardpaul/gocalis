package audio

// G.711 mu-law codec. This is the codec used on the WebRTC PCMU backchannel to
// speaker/mic nodes.

const muLawBias = 0x84 // 132

// EncodeMuLaw encodes a single signed 16-bit PCM sample to a G.711 mu-law byte.
func EncodeMuLaw(sample int16) byte {
	const clip = 32635

	var sign int16 = 0
	if sample < 0 {
		sample = -sample
		sign = 0x80
	}

	if sample > clip {
		sample = clip
	}
	sample += muLawBias

	exponent := int16(7)
	for mask := int16(0x4000); (sample&mask) == 0 && exponent > 0; mask >>= 1 {
		exponent--
	}

	mantissa := (sample >> (exponent + 3)) & 0x0F
	muVal := byte(sign | (exponent << 4) | mantissa)
	return ^muVal
}

// DecodeMuLaw decodes a single G.711 mu-law byte to a signed 16-bit PCM sample,
// following the standard ITU-T G.711 reference implementation.
func DecodeMuLaw(muVal byte) int16 {
	muVal = ^muVal
	sign := muVal & 0x80
	exponent := (muVal >> 4) & 0x07
	mantissa := muVal & 0x0F

	sample := (int32(mantissa)<<3 + muLawBias) << exponent
	if sign != 0 {
		return int16(muLawBias - sample)
	}
	return int16(sample - muLawBias)
}

// EncodePCM16ToMuLaw resamples PCM16 from sourceRate to 8000Hz (when needed) and
// encodes the result to G.711 mu-law bytes suitable for the PCMU backchannel.
func EncodePCM16ToMuLaw(pcm16 []int16, sourceRate int) []byte {
	resampled := ResampleInt16(pcm16, sourceRate, 8000)
	out := make([]byte, len(resampled))
	for i, v := range resampled {
		out[i] = EncodeMuLaw(v)
	}
	return out
}

// DecodeMuLawToFloat decodes G.711 mu-law bytes to float32 samples in the range
// [-1.0, 1.0] (still at the source 8000Hz rate).
func DecodeMuLawToFloat(data []byte) []float32 {
	out := make([]float32, len(data))
	for i, b := range data {
		out[i] = float32(DecodeMuLaw(b)) / 32767.0
	}
	return out
}
