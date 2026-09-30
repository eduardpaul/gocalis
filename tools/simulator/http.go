package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"net/http"
	"net/url"
	"path/filepath"
	"sync"
	"time"
)

type Server struct {
	engine   *Engine
	manifest Manifest
	out      string
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
	mu       sync.Mutex
	sockets  map[*websocket.Conn]bool
	runMu    sync.Mutex
	closing  bool
}

func newServer(e *Engine, m Manifest, out string) *Server {
	ctx, c := context.WithCancel(context.Background())
	return &Server{engine: e, manifest: m, out: out, ctx: ctx, cancel: c, sockets: map[*websocket.Conn]bool{}}
}
func (s *Server) closeSockets() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closing = true
	for c := range s.sockets {
		c.Close()
	}
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Origin") != "" {
		u, e := url.Parse(r.Header.Get("Origin"))
		if e != nil || u.Host != r.Host {
			http.Error(w, "same origin required", 403)
			return
		}
	}
	if r.URL.Path == "/api/devices" && r.Method == "GET" {
		a := []Status{}
		for _, id := range s.engine.order {
			a = append(a, s.engine.devices[id].snapshot())
		}
		writeJSON(w, a)
		return
	}
	if r.URL.Path == "/api/export" && r.Method == "POST" {
		var report Report
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&report); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if report.Started.IsZero() {
			report.Started = time.Now().UTC()
		}
		report.Finished = time.Now().UTC()
		report.ArtifactDir = filepath.Join(s.out, report.Started.Format("20060102T150405.000000000Z"))
		if err := saveArtifacts(report.ArtifactDir, s.engine, report); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		writeJSON(w, report)
		return
	}
	if r.URL.Path == "/api/scenarios" && r.Method == "POST" {
		s.scenarioHTTP(w, r)
		return
	}
	if r.URL.Path == "/" || r.URL.Path == "/app.js" || r.URL.Path == "/worklet.js" {
		serveUI(w, r)
		return
	}
	id := r.PathValue("id")
	_ = id
	parts := splitPath(r.URL.Path)
	if len(parts) != 4 || parts[0] != "api" || parts[1] != "devices" {
		http.NotFound(w, r)
		return
	}
	d := s.engine.devices[parts[2]]
	if d == nil {
		http.NotFound(w, r)
		return
	}
	action := parts[3]
	if action == "recording" && r.Method == "GET" {
		w.Header().Set("Content-Type", "audio/wav")
		w.Write(wav(d.audio()))
		return
	}
	if action == "live" && r.Method == "GET" {
		s.live(w, r, d)
		return
	}
	if r.Method != "POST" {
		http.Error(w, "POST required", 405)
		return
	}
	var err error
	switch action {
	case "play":
		var b []byte
		b, err = readLimited(r, maxSamples*2+65536)
		if err == nil {
			var p []int16
			p, err = parseWAV(b)
			if err == nil {
				err = d.play(p)
			}
		}
	case "tone":
		var v struct {
			Hz      float64 `json:"hz"`
			Seconds float64 `json:"seconds"`
		}
		err = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024)).Decode(&v)
		if err == nil {
			if v.Hz < 100 || v.Hz > 7000 || v.Seconds <= 0 || v.Seconds > 300 {
				err = fmt.Errorf("invalid tone")
			} else {
				err = d.play(tone(v.Hz, v.Seconds))
			}
		}
	case "stop":
		d.stop()
	case "disconnect":
		d.online(false)
	case "reconnect":
		d.online(true)
	case "reset":
		d.reset()
	default:
		http.NotFound(w, r)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	writeJSON(w, d.snapshot())
}
func splitPath(p string) []string {
	var a []string
	start := 1
	for i := 1; i <= len(p); i++ {
		if i == len(p) || p[i] == '/' {
			if i > start {
				a = append(a, p[start:i])
			}
			start = i + 1
		}
	}
	return a
}
func (s *Server) live(w http.ResponseWriter, r *http.Request, d *Device) {
	d.mu.Lock()
	if d.status.Input != "silence" {
		d.mu.Unlock()
		http.Error(w, "device input is busy", 409)
		return
	}
	d.status.Input = "live"
	d.inputEpoch++
	epoch := d.inputEpoch
	d.mu.Unlock()
	defer func() {
		d.mu.Lock()
		if d.inputEpoch == epoch && d.status.Input == "live" {
			d.live = nil
			d.status.Input = "silence"
		}
		d.mu.Unlock()
	}()
	c, e := (&websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}).Upgrade(w, r, nil)
	if e != nil {
		return
	}
	defer c.Close()
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return
	}
	s.wg.Add(1)
	defer s.wg.Done()
	s.sockets[c] = true
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.sockets, c); s.mu.Unlock() }()
	c.SetReadLimit(6400)
	for {
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		kind, b, e := c.ReadMessage()
		if e != nil {
			return
		}
		if kind != websocket.BinaryMessage || len(b)%2 != 0 || len(b) == 0 {
			return
		}
		p := make([]int16, len(b)/2)
		for i := range p {
			p[i] = int16(binary.LittleEndian.Uint16(b[i*2:]))
		}
		d.mu.Lock()
		if d.status.Input != "live" || d.inputEpoch != epoch {
			d.mu.Unlock()
			return
		}
		if len(d.live)+len(p) > rate*2 {
			d.status.Overruns++
		} else {
			d.live = append(d.live, p...)
		}
		d.mu.Unlock()
	}
}
