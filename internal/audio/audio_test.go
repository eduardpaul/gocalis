package audio

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestFloatPCM16RoundTrip(t *testing.T) {
	in := []float32{0, 0.5, -0.5, 1.0, -1.0}
	pcm := FloatToPCM16(in)
	back := PCM16ToFloat(pcm)
	for i := range in {
		if diff := back[i] - in[i]; diff > 0.001 || diff < -0.001 {
			t.Errorf("round trip mismatch at %d: got %f want %f", i, back[i], in[i])
		}
	}
}

func TestFloatToPCM16Clamps(t *testing.T) {
	out := FloatToPCM16([]float32{2.0, -2.0})
	if out[0] != 32767 {
		t.Errorf("positive clamp: got %d want 32767", out[0])
	}
	if out[1] != -32768 {
		t.Errorf("negative clamp: got %d want -32768", out[1])
	}
}

func TestEncodeWAVPCM16Header(t *testing.T) {
	samples := []int16{0, 1, -1, 100}
	wav := EncodeWAVPCM16(samples, 16000)

	if len(wav) != 44+len(samples)*2 {
		t.Fatalf("unexpected length: got %d", len(wav))
	}
	if !bytes.Equal(wav[0:4], []byte("RIFF")) {
		t.Errorf("missing RIFF magic")
	}
	if !bytes.Equal(wav[8:12], []byte("WAVE")) {
		t.Errorf("missing WAVE magic")
	}
	if !bytes.Equal(wav[36:40], []byte("data")) {
		t.Errorf("missing data chunk")
	}
	if got := binary.LittleEndian.Uint32(wav[24:28]); got != 16000 {
		t.Errorf("sample rate: got %d want 16000", got)
	}
	if got := binary.LittleEndian.Uint16(wav[22:24]); got != 1 {
		t.Errorf("channels: got %d want 1", got)
	}
	if got := binary.LittleEndian.Uint32(wav[40:44]); int(got) != len(samples)*2 {
		t.Errorf("data size: got %d want %d", got, len(samples)*2)
	}
	// verify first sample payload round trips little-endian
	if got := int16(binary.LittleEndian.Uint16(wav[46:48])); got != 1 {
		t.Errorf("sample[1]: got %d want 1", got)
	}
}

func TestMuLawRoundTripApproximation(t *testing.T) {
	// mu-law is lossy; encode/decode should stay reasonably close for mid values.
	for _, v := range []int16{0, 100, -100, 1000, -1000, 10000, -10000} {
		dec := DecodeMuLaw(EncodeMuLaw(v))
		diff := int(dec) - int(v)
		if diff < 0 {
			diff = -diff
		}
		tol := int(v)
		if tol < 0 {
			tol = -tol
		}
		tol = tol/8 + 64
		if diff > tol {
			t.Errorf("mu-law %d -> %d, diff %d exceeds tol %d", v, dec, diff, tol)
		}
	}
}

func TestResampleInt16Upsample2x(t *testing.T) {
	in := []int16{0, 100, 200, 300}
	out := ResampleInt16(in, 8000, 16000)
	if len(out) != 8 {
		t.Fatalf("expected 8 samples, got %d", len(out))
	}
	if out[0] != 0 {
		t.Errorf("out[0]: got %d want 0", out[0])
	}
}

func TestResampleSameRateReturnsInput(t *testing.T) {
	in := []int16{1, 2, 3}
	out := ResampleInt16(in, 16000, 16000)
	if &out[0] != &in[0] {
		t.Errorf("expected same-rate resample to return input slice unchanged")
	}
}

func TestResampleFloat32Downsample(t *testing.T) {
	in := make([]float32, 16)
	out := ResampleFloat32(in, 16000, 8000)
	if len(out) != 8 {
		t.Errorf("expected 8 samples, got %d", len(out))
	}
}

func TestApplyGainZeroReturnsInput(t *testing.T) {
	in := []int16{1, 2, 3}
	out := ApplyGainPCM16(in, 0)
	if &out[0] != &in[0] {
		t.Errorf("expected 0 dB gain to return input slice unchanged")
	}
}

func TestApplyGainClamps(t *testing.T) {
	out := ApplyGainPCM16([]int16{30000, -30000}, 20) // +20 dB = x10
	if out[0] != 32767 {
		t.Errorf("positive clamp: got %d", out[0])
	}
	if out[1] != -32768 {
		t.Errorf("negative clamp: got %d", out[1])
	}
}

func TestEncodePCM16ToMuLawResamples(t *testing.T) {
	// 16 samples at 16000 -> 8 mu-law bytes at 8000
	in := make([]int16, 16)
	out := EncodePCM16ToMuLaw(in, 16000)
	if len(out) != 8 {
		t.Errorf("expected 8 mu-law bytes, got %d", len(out))
	}
}
