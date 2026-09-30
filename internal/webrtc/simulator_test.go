package webrtc

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"gocalis/internal/config"
	"io"
	"math"
	"net/http"
	"os"
	"sync"
	"testing"
	"time"
)

// Opt-in transport proof against tools/simulator's real go2rtc instance.
func TestSimulatorTransport(t *testing.T) {
	base := os.Getenv("GOCALIS_SIMULATOR")
	if base == "" {
		t.Skip("set GOCALIS_SIMULATOR to run the container transport proof")
	}
	started := time.Now().UTC()
	t.Cleanup(func() {
		b, _ := json.Marshal(map[string]any{"name": "model-free-transport", "passed": !t.Failed(), "started": started})
		res, err := http.Post(base+"/api/export", "application/json", bytes.NewReader(b))
		if err != nil {
			t.Error(err)
			return
		}
		defer res.Body.Close()
		if res.StatusCode != 200 {
			t.Errorf("artifact export returned %s", res.Status)
		} else {
			body, _ := io.ReadAll(res.Body)
			t.Logf("transport report: %s", body)
		}
	})
	type spec struct {
		ID      string `json:"id"`
		Profile string `json:"profile"`
	}
	res, e := http.Get(base + "/api/devices")
	if e != nil {
		t.Fatal(e)
	}
	var devices []spec
	e = json.NewDecoder(res.Body).Decode(&devices)
	res.Body.Close()
	if e != nil {
		t.Fatal(e)
	}
	if len(devices) == 0 {
		t.Fatal("simulator has no devices")
	}
	var wg sync.WaitGroup
	for i, d := range devices {
		i, d := i, d
		wg.Add(1)
		go func() {
			defer wg.Done()
			t.Run(d.ID, func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
				defer cancel()
				post := func(action string, p any) {
					t.Helper()
					b, _ := json.Marshal(p)
					req, _ := http.NewRequestWithContext(ctx, "POST", base+"/api/devices/"+d.ID+"/"+action, bytes.NewReader(b))
					res, e := http.DefaultClient.Do(req)
					if e != nil {
						t.Fatal(e)
					}
					defer res.Body.Close()
					if res.StatusCode != 200 {
						b, _ := io.ReadAll(res.Body)
						t.Fatalf("%s: %s", action, b)
					}
				}
				cfg := Config{SignalingURL: "ws://127.0.0.1:11984/api/ws?src=" + d.ID, ICEServers: []config.ICEServer{}}
				if d.Profile == "doorbell" {
					cfg.APIBaseURL = "http://127.0.0.1:11984"
					cfg.TalkbackStream = d.ID
					cfg.TalkbackIn = d.ID + "_in"
				}
				for cycle := 0; cycle < 2; cycle++ {
					post("reset", nil)
					post("reconnect", nil)
					c, e := NewClientWithConfig(cfg)
					if e != nil {
						t.Fatal(e)
					}
					defer c.Close()
					var mu sync.Mutex
					var received []float32
					c.OnAudio(func(p []float32) {
						mu.Lock()
						if len(received) < 16000*40 {
							received = append(received, p...)
						}
						mu.Unlock()
					})
					if e = c.Connect(ctx); e != nil {
						c.Close()
						t.Fatal(e)
					}
					select {
					case <-c.connected:
					case <-ctx.Done():
						c.Close()
						t.Fatal(ctx.Err())
					}
					micHz := float64(400 + i*200)
					speakerHz := float64(1300 + i*200)
					post("tone", map[string]any{"hz": micHz, "seconds": 1.2})
					pcm := make([]int16, 16000*2)
					for j := range pcm {
						pcm[j] = int16(10000 * math.Sin(2*math.Pi*speakerHz*float64(j)/16000))
					}
					if e = c.Play(ctx, pcm, 16000); e != nil {
						c.Close()
						t.Fatal(e)
					}
					timer := time.NewTimer(2 * time.Second)
					select {
					case <-timer.C:
					case <-ctx.Done():
						timer.Stop()
					}
					mu.Lock()
					rx := append([]float32(nil), received...)
					mu.Unlock()
					checkTone := func(p []float64, hz float64, minSeconds float64) {
						t.Helper()
						active := 0
						var sin, cos, energy float64
						for j, v := range p {
							if math.Abs(v) > 0.02 {
								active++
							}
							sin += v * math.Sin(2*math.Pi*hz*float64(j)/16000)
							cos += v * math.Cos(2*math.Pi*hz*float64(j)/16000)
							energy += v * v
						}
						if float64(active)/16000 < minSeconds || energy == 0 || 2*(sin*sin+cos*cos)/(float64(len(p))*energy) < 0.05 {
							t.Fatalf("decoded marker %.0f missing: active %.2fs samples %d", hz, float64(active)/16000, len(p))
						}
					}
					floats := make([]float64, len(rx))
					for j, v := range rx {
						floats[j] = float64(v)
					}
					checkTone(floats, micHz, 0.7)
					res, e := http.Get(base + "/api/devices/" + d.ID + "/recording")
					if e != nil {
						c.Close()
						t.Fatal(e)
					}
					b, e := io.ReadAll(res.Body)
					res.Body.Close()
					if e != nil || len(b) < 44 {
						c.Close()
						t.Fatal("invalid recording")
					}
					p := make([]int16, (len(b)-44)/2)
					binary.Read(bytes.NewReader(b[44:]), binary.LittleEndian, p)
					floats = make([]float64, len(p))
					for j, v := range p {
						floats[j] = float64(v) / 32768
					}
					checkTone(floats, speakerHz, 1.3)
					// Markers from another device must be absent.
					for k := range devices {
						if k == i {
							continue
						}
						var sin, cos, energy float64
						hz := float64(1300 + k*200)
						for j, v := range floats {
							sin += v * math.Sin(2*math.Pi*hz*float64(j)/16000)
							cos += v * math.Cos(2*math.Pi*hz*float64(j)/16000)
							energy += v * v
						}
						if energy > 0 && 2*(sin*sin+cos*cos)/(float64(len(floats))*energy) > 0.01 {
							t.Fatal("cross-device audio leak")
						}
					}
					if e = c.Close(); e != nil {
						t.Fatal(e)
					}
					post("disconnect", nil)
				}
				post("reconnect", nil)
			})
		}()
	}
	wg.Wait()
}
