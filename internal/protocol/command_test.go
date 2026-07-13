package protocol

import (
	"path/filepath"
	"testing"
)

func TestResolveAudioFile(t *testing.T) {
	base := t.TempDir()
	e := &Executor{AudioBaseDir: base}

	tests := []struct {
		name    string
		input   string
		wantErr bool
	}{
		{"empty", "", true},
		{"simple relative", "received_audio.wav", false},
		{"nested relative", "sub/clip.wav", false},
		{"parent traversal", "../secret.wav", true},
		{"deep traversal", "a/../../secret.wav", true},
		{"absolute outside", "/etc/passwd", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := e.resolveAudioFile(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q, got path %q", tc.input, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tc.input, err)
			}
			baseAbs, _ := filepath.Abs(base)
			rel, relErr := filepath.Rel(baseAbs, got)
			if relErr != nil || rel == ".." || filepath.IsAbs(rel) {
				t.Fatalf("resolved path %q escaped base %q", got, baseAbs)
			}
		})
	}
}
