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

// Validate gates Execute, so every dispatched action must be accepted here or
// it is unreachable over the API.
func TestValidateIntercomActions(t *testing.T) {
	tests := []struct {
		name    string
		req     Request
		wantErr bool
	}{
		{"intercom node_ids", Request{Action: "intercom", NodeIDs: []string{"a", "b"}}, false},
		{"intercom node_id+peer", Request{Action: "intercom", NodeID: "a", PeerNodeID: "b"}, false},
		{"intercom with duration", Request{Action: "intercom", NodeIDs: []string{"a", "b"}, DurationSeconds: 30}, false},
		{"intercom negative duration", Request{Action: "intercom", NodeIDs: []string{"a", "b"}, DurationSeconds: -1}, true},
		{"intercom too many nodes", Request{Action: "intercom", NodeIDs: make([]string, maxIntercomNodes+1)}, true},
		{"intercom_stop", Request{Action: "intercom_stop", NodeID: "a"}, false},
		{"intercom_stop without node", Request{Action: "intercom_stop"}, true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.req.Validate()
			if (err != nil) != tc.wantErr {
				t.Fatalf("Validate() error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}
