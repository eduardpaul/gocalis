package main

import (
	"context"
	"encoding/json"
	"github.com/gorilla/websocket"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestScenarioDeadlinesAndAssertions(t *testing.T) {
	for _, body := range []string{
		"name: x\nsequences: {a: [{action: wait_event, event: wake}]}",
		"name: x\nsequences: {a: [{action: wait, timeout: 1h}]}",
		"name: x\nsequences: {a: [{action: tone, hz: .nan}]}",
		"name: x\nsequences: {a: [{action: invented}]}",
	} {
		if _, e := parseScenario([]byte(body)); e == nil {
			t.Fatal("accepted invalid scenario")
		}
	}
	if e := assertOutput(tone(600, 1), Step{Hz: 600, MinActiveSeconds: 0.8}, Status{}); e != nil {
		t.Fatal(e)
	}
	if e := assertOutput(tone(700, 1), Step{Hz: 600, MinActiveSeconds: 0.8}, Status{}); e == nil {
		t.Fatal("accepted wrong decoded content")
	}
	if e := assertOutput(tone(600, 0.1), Step{Hz: 600, MinActiveSeconds: 0.8}, Status{}); e == nil {
		t.Fatal("accepted short output")
	}
}
func TestScenarioParallelAndArtifacts(t *testing.T) {
	m := Manifest{Devices: []DeviceSpec{{ID: "a"}, {ID: "b"}}}
	e := newEngine(m)
	defer e.close()
	s := newServer(e, m, t.TempDir())
	defer s.cancel()
	sc := Scenario{Name: "parallel", Sequences: map[string][]Step{"a": {{Action: "wait", Seconds: 0.08, Timeout: "1s"}}, "b": {{Action: "wait", Seconds: 0.08, Timeout: "1s"}}}}
	start := time.Now()
	report := s.executeScenario(context.Background(), sc)
	if !report.Passed || len(report.Steps) != 2 || time.Since(start) > 150*time.Millisecond {
		t.Fatalf("parallel run: %+v", report)
	}
	for _, name := range []string{"a.wav", "b.wav", "report.json", "events.json", "diagnostics.json"} {
		if _, err := os.Stat(filepath.Join(report.ArtifactDir, name)); err != nil {
			t.Fatal(err)
		}
	}
}
func TestScenarioFailedDeadlineAndCLIExit(t *testing.T) {
	m := Manifest{Devices: []DeviceSpec{{ID: "a"}}}
	e := newEngine(m)
	defer e.close()
	s := newServer(e, m, t.TempDir())
	defer s.cancel()
	h := httptest.NewServer(s)
	defer h.Close()
	path := filepath.Join(t.TempDir(), "bad.yaml")
	os.WriteFile(path, []byte("name: fail\nsequences:\n  a:\n    - {action: wait_activity, timeout: 30ms}\n"), 0600)
	out := t.TempDir()
	if err := scenarioCLI(h.URL, path, out); err == nil {
		t.Fatal("failed scenario returned success")
	}
	b, err := os.ReadFile(filepath.Join(out, "report.json"))
	if err != nil {
		t.Fatal(err)
	}
	var report Report
	if err = json.Unmarshal(b, &report); err != nil || report.Passed || len(report.Steps) != 1 {
		t.Fatalf("bad failure report: %s", b)
	}
}
func TestAppCommandsEventsAndSerialization(t *testing.T) {
	var mu sync.Mutex
	var socket *websocket.Conn
	connected := make(chan struct{})
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test" {
			http.Error(w, "unauthorized", 401)
			return
		}
		if r.URL.Path == "/api/events" {
			c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
			if err != nil {
				return
			}
			mu.Lock()
			socket = c
			mu.Unlock()
			close(connected)
			for {
				if _, _, err = c.ReadMessage(); err != nil {
					return
				}
			}
		}
		if r.URL.Path == "/api/execute" {
			var command map[string]any
			json.NewDecoder(r.Body).Decode(&command)
			w.WriteHeader(202)
			mu.Lock()
			socket.WriteJSON(map[string]any{"event": "tts_completed", "node_id": "a", "text": "reply"})
			mu.Unlock()
			return
		}
		http.NotFound(w, r)
	}))
	defer app.Close()
	m := Manifest{Devices: []DeviceSpec{{ID: "a"}}, App: app.URL, Token: "test"}
	e := newEngine(m)
	defer e.close()
	s := newServer(e, m, t.TempDir())
	defer s.cancel()
	sc := Scenario{Name: "app", Sequences: map[string][]Step{"a": {{Action: "command", Command: map[string]any{"action": "tts", "text": "hello"}, Timeout: "1s"}, {Action: "wait_event", Event: "tts_completed", Contains: "reply", Timeout: "1s"}}}}
	report := s.executeScenario(context.Background(), sc)
	if !report.Passed {
		t.Fatalf("app scenario failed: %+v", report)
	}
	// Direct execution refuses a second command while completion is outstanding.
	pending := "tts_completed"
	err := s.executeStep(context.Background(), e.devices["a"], Step{Action: "command", Timeout: "1s"}, nil, &pending)
	if err == nil || !strings.Contains(err.Error(), "wait for") {
		t.Fatalf("serialization: %v", err)
	}
}
func TestMissingPrerequisites(t *testing.T) {
	m := Manifest{Devices: []DeviceSpec{{ID: "a"}}}
	e := newEngine(m)
	defer e.close()
	s := newServer(e, m, t.TempDir())
	defer s.cancel()
	report := s.executeScenario(context.Background(), Scenario{Name: "missing", Requires: []string{"absent.wav"}, Sequences: map[string][]Step{"a": {{Action: "stop"}}}})
	if report.Passed || !strings.Contains(report.Error, "missing prerequisite") {
		t.Fatalf("missing prerequisite report: %+v", report)
	}
}
