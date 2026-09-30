package main

import (
	"bytes"
	"github.com/gorilla/websocket"
	"net"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestWAVValidation(t *testing.T) {
	p := tone(600, 0.1)
	got, e := parseWAV(wav(p))
	if e != nil || len(got) != len(p) {
		t.Fatal(e)
	}
	for _, b := range [][]byte{nil, []byte("RIFF"), wav(p)[:40], append([]byte(nil), wav(p)...)} {
		if len(b) > 44 {
			b[34] = 8
		}
		if _, e := parseWAV(b); e == nil {
			t.Fatal("accepted malformed WAV")
		}
	}
}
func TestDeviceIsolationStopAndLimit(t *testing.T) {
	a := newDevice(DeviceSpec{ID: "a", Profile: "bidirectional"})
	b := newDevice(DeviceSpec{ID: "b", Profile: "doorbell"})
	a.play(tone(600, 1))
	if level(a.next()) == 0 || level(b.next()) != 0 {
		t.Fatal("input isolation")
	}
	a.stop()
	if level(a.next()) != 0 {
		t.Fatal("stop retained input")
	}
	a.capture([]int16{100}, nil)
	if len(b.audio()) != 0 {
		t.Fatal("recording isolation")
	}
	a.recording = make([]int16, maxSamples)
	a.capture([]int16{100}, nil)
	if a.snapshot().Overruns != 1 || len(a.audio()) != maxSamples {
		t.Fatal("recording unbounded")
	}
}
func TestManifestValidationAndGenerate(t *testing.T) {
	dir := t.TempDir()
	file := filepath.Join(dir, "devices.yaml")
	for _, text := range []string{"devices: []", "devices: [{id: ../x, profile: doorbell}]", "devices: [{id: x, profile: other}]", "devices: [{id: x, profile: doorbell}, {id: x, profile: doorbell}]"} {
		os.WriteFile(file, []byte(text), 0600)
		if _, e := loadManifest(file); e == nil {
			t.Fatal("accepted invalid manifest")
		}
	}
	os.WriteFile(file, []byte("devices: [{id: x, profile: doorbell}]"), 0600)
	m, e := loadManifest(file)
	if e != nil {
		t.Fatal(e)
	}
	if e = generate(m, dir, ""); e != nil {
		t.Fatal(e)
	}
	b, _ := os.ReadFile(filepath.Join(dir, "go2rtc.yaml"))
	if !bytes.Contains(b, []byte("aac_eld")) {
		t.Fatal("missing ELD")
	}
}
func TestLiveBoundedAndExclusive(t *testing.T) {
	m := Manifest{Devices: []DeviceSpec{{ID: "a", Profile: "bidirectional"}}}
	e := newEngine(m)
	defer e.close()
	s := newServer(e, m, t.TempDir())
	defer s.cancel()
	h := httptest.NewServer(s)
	defer h.Close()
	c, _, err := websocket.DefaultDialer.Dial("ws"+h.URL[4:]+"/api/devices/a/live", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	d := e.devices["a"]
	if err = d.play(tone(600, 1)); err == nil {
		t.Fatal("fixture accepted during live input")
	}
	for i := 0; i < 20; i++ {
		if err = c.WriteMessage(websocket.BinaryMessage, make([]byte, 6400)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for d.snapshot().Overruns == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	d.mu.Lock()
	n := len(d.live)
	d.mu.Unlock()
	if n > rate*2 || d.snapshot().Overruns == 0 {
		t.Fatal("live queue unbounded or overruns unreported")
	}
	s.closeSockets()
}
func TestRepeatedShutdownWithSlowRTSP(t *testing.T) {
	for i := 0; i < 5; i++ {
		e := newEngine(Manifest{Devices: []DeviceSpec{{ID: "a", Profile: "bidirectional"}}})
		if err := e.start("127.0.0.1:0"); err != nil {
			t.Fatal(err)
		}
		c, err := net.Dial("tcp", e.listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { e.close(); close(done) }()
		select {
		case <-done:
		case <-time.After(6 * time.Second):
			t.Fatal("shutdown did not join slow RTSP session")
		}
		c.Close()
	}
}
func TestParallelDeviceAccess(t *testing.T) {
	d := newDevice(DeviceSpec{ID: "x"})
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				d.play(tone(500, 0.01))
				d.next()
				d.snapshot()
				d.capture([]int16{1}, nil)
				d.audio()
				d.stop()
			}
		}()
	}
	wg.Wait()
}
