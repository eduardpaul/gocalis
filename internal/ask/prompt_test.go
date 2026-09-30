package ask

import (
	"context"
	"gocalis/internal/audionode"
	"io"
	"testing"
)

func TestStreamedPromptPrecedesChime(t *testing.T) {
	s := &promptSource{prompt: audionode.NewSliceSource([]int16{1, 2}, 16000), tail: audionode.NewSliceSource([]int16{3, 4}, 16000)}
	var got []int16
	for {
		chunk, err := s.ReadPCM16(context.Background(), 1)
		got = append(got, chunk...)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(got) != 4 {
		t.Fatal(got)
	}
	for i, v := range got {
		if v != int16(i+1) {
			t.Fatal("chime reordered", got)
		}
	}
}
