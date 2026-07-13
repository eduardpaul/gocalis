package runtime

import "testing"

func TestSignalingURL(t *testing.T) {
	tests := []struct {
		name    string
		apiURL  string
		stream  string
		want    string
		wantErr bool
	}{
		{"http maps to ws", "http://192.168.50.5:1984", "doorbell", "ws://192.168.50.5:1984/api/ws?src=doorbell", false},
		{"https maps to wss", "https://cam.local:1984", "front", "wss://cam.local:1984/api/ws?src=front", false},
		{"host without port", "http://go2rtc", "s1", "ws://go2rtc/api/ws?src=s1", false},
		{"invalid url", "http://[::1", "s1", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := signalingURL(tt.apiURL, tt.stream)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error for %q", tt.apiURL)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("signalingURL(%q,%q) = %q, want %q", tt.apiURL, tt.stream, got, tt.want)
			}
		})
	}
}
