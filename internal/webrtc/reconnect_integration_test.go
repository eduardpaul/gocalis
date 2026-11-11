//go:build integration

package webrtc

// Reconnect test against a real go2rtc that gets restarted under the client.
//
//	GOCALIS_IT_WS='ws://127.0.0.1:11984/api/ws?src=it_tone' \
//	GOCALIS_IT_RESTART='<shell command that restarts that go2rtc>' \
//	go test -tags integration -run TestReconnectAfterGo2rtcRestart -v ./internal/webrtc
//
// The stream must deliver continuous audio (e.g. an ffmpeg sine tone).

import (
	"context"
	"os"
	"os/exec"
	"sync/atomic"
	"testing"
	"time"
)

func TestReconnectAfterGo2rtcRestart(t *testing.T) {
	ws, restart := os.Getenv("GOCALIS_IT_WS"), os.Getenv("GOCALIS_IT_RESTART")
	if ws == "" || restart == "" {
		t.Skip("set GOCALIS_IT_WS and GOCALIS_IT_RESTART")
	}

	c, err := NewClientWithConfig(Config{SignalingURL: ws, SendCodec: "opus"})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var samples atomic.Int64
	c.OnAudio(func(s []float32) { samples.Add(int64(len(s))) })

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	// waitAudio waits until at least `secs` seconds of audio arrive after now.
	waitAudio := func(what string, secs float64, timeout time.Duration) {
		t.Helper()
		start := samples.Load()
		t0 := time.Now()
		for time.Since(t0) < timeout {
			if float64(samples.Load()-start)/recvModelRate >= secs {
				t.Logf("%s: %.1fs of audio after %v", what, secs, time.Since(t0).Round(100*time.Millisecond))
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("%s: audio did not flow within %v (got %.2fs)", what, timeout,
			float64(samples.Load()-start)/recvModelRate)
	}

	waitAudio("initial connect", 2, 20*time.Second)

	for i := 1; i <= 2; i++ {
		t.Logf("restarting go2rtc (%d/2)", i)
		if out, err := exec.Command("sh", "-c", restart).CombinedOutput(); err != nil {
			t.Fatalf("restart: %v: %s", err, out)
		}
		// must recover by itself: watchdog (10 s) + reconnect backoff
		waitAudio("after restart", 3, 60*time.Second)
	}
}
