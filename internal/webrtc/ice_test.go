package webrtc

import (
	"gocalis/internal/config"
	"testing"
)

func TestICEServersOmittedAndEmpty(t *testing.T) {
	if got := iceServers(nil); len(got) != 1 || got[0].URLs[0] != "stun:stun.l.google.com:19302" {
		t.Fatalf("default ICE servers: %+v", got)
	}
	if got := iceServers([]config.ICEServer{}); got == nil || len(got) != 0 {
		t.Fatalf("explicit empty ICE servers: %+v", got)
	}
	got := iceServers([]config.ICEServer{{URLs: []string{"turn:localhost:3478"}, Username: "test", Credential: "secret"}})
	if got[0].Username != "test" || got[0].Credential != "secret" {
		t.Fatal("TURN credentials lost")
	}
}
