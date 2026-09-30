package diagnostics

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRecordingOptInAndBounds(t *testing.T) {
	var disabled *Recorder
	disabled.Add([]float32{1})
	if err := disabled.Save(nil); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	r := NewRecorder(dir, "../../doorbell")
	r.Add(make([]float32, 16000*121))
	if len(r.mic) != 16000*120 {
		t.Fatal("unbounded microphone history")
	}
	for i := 0; i < 3; i++ {
		if err := r.Save([]float32{0.5}); err != nil {
			t.Fatal(err)
		}
	}
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 2 {
		t.Fatalf("recordings accumulated: %v %v", files, err)
	}
	for _, file := range files {
		info, err := os.Stat(filepath.Join(dir, file.Name()))
		if err != nil || info.Mode().Perm() != 0600 {
			t.Fatalf("recording permissions: %v", err)
		}
	}
}
