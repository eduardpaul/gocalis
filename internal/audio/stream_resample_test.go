package audio

import "testing"

func TestChunkedResamplingMatchesClip(t *testing.T) {
	for _, rate := range []int{16000, 22050, 24000, 44100, 48000} {
		input := make([]int16, 12347)
		for i := range input {
			input[i] = int16(i%3000 - 1500)
		}
		want := ResampleInt16(input, rate, 48000)
		for _, size := range []int{1, 127, 2048} {
			r := NewPCMResampler(rate, 48000)
			var got []int16
			for i := 0; i < len(input); i += size {
				got = append(got, r.Push(input[i:min(i+size, len(input))])...)
			}
			got = append(got, r.Flush()...)
			if len(got) != len(want) {
				t.Fatalf("rate %d size %d: lengths %d/%d", rate, size, len(got), len(want))
			}
			for i := range got {
				if d := int(got[i]) - int(want[i]); d < -1 || d > 1 {
					t.Fatalf("rate %d sample %d mismatch", rate, i)
				}
			}
		}
	}
}
