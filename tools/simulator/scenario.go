package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gorilla/websocket"
	"gopkg.in/yaml.v3"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type Scenario struct {
	Name      string            `yaml:"name"`
	Sequences map[string][]Step `yaml:"sequences"`
	Requires  []string          `yaml:"requires"`
}
type Step struct {
	Action           string         `yaml:"action"`
	Timeout          string         `yaml:"timeout"`
	Replay           string         `yaml:"replay"`
	Audio            string         `yaml:"audio"`
	Seconds          float64        `yaml:"seconds"`
	Hz               float64        `yaml:"hz"`
	Command          map[string]any `yaml:"command"`
	Event            string         `yaml:"event"`
	Field            string         `yaml:"field"`
	Contains         string         `yaml:"contains"`
	Direction        string         `yaml:"direction"`
	MinActiveSeconds float64        `yaml:"min_active_seconds"`
	MaxSeconds       float64        `yaml:"max_seconds"`
	MinRMS           float64        `yaml:"min_rms"`
	MinMarker        float64        `yaml:"min_marker"`
}
type StepResult struct {
	Device   string    `json:"device"`
	Index    int       `json:"index"`
	Action   string    `json:"action"`
	Started  time.Time `json:"started"`
	Duration float64   `json:"duration_seconds"`
	Error    string    `json:"error,omitempty"`
}
type Report struct {
	Name        string       `json:"name"`
	Passed      bool         `json:"passed"`
	Started     time.Time    `json:"started"`
	Finished    time.Time    `json:"finished"`
	ArtifactDir string       `json:"artifact_dir"`
	Error       string       `json:"error,omitempty"`
	Steps       []StepResult `json:"steps"`
}

func parseScenario(b []byte) (Scenario, error) {
	var sc Scenario
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	err := d.Decode(&sc)
	if err != nil {
		return sc, err
	}
	if d.Decode(new(any)) != io.EOF {
		return sc, fmt.Errorf("one scenario document required")
	}
	if sc.Name == "" || len(sc.Sequences) == 0 || len(sc.Sequences) > 16 {
		return sc, fmt.Errorf("name and 1–16 sequences required")
	}
	for id, steps := range sc.Sequences {
		if len(steps) == 0 || len(steps) > 100 {
			return sc, fmt.Errorf("%s: needs 1–100 steps", id)
		}
		for _, step := range steps {
			for _, v := range []float64{step.Hz, step.Seconds, step.MinActiveSeconds, step.MaxSeconds, step.MinRMS, step.MinMarker} {
				if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
					return sc, fmt.Errorf("scenario values must be finite and nonnegative")
				}
			}
			switch step.Action {
			case "replay", "tone", "stop", "reset", "disconnect", "reconnect", "assert_output":
			case "wait", "wait_input_idle", "wait_activity", "wait_event", "command":
				if _, err := stepDeadline(step); err != nil {
					return sc, err
				}
			default:
				return sc, fmt.Errorf("unknown action %q", step.Action)
			}
		}
	}
	return sc, nil
}
func stepDeadline(step Step) (time.Duration, error) {
	d, e := time.ParseDuration(step.Timeout)
	if e != nil || d <= 0 || d > 5*time.Minute {
		return 0, fmt.Errorf("%s requires timeout in (0,5m]", step.Action)
	}
	return d, nil
}
func fixture(path string) ([]int16, error) {
	root, e := filepath.Abs("fixtures")
	if e != nil {
		return nil, e
	}
	target, e := filepath.EvalSymlinks(filepath.Join(root, path))
	if e != nil {
		return nil, fmt.Errorf("missing speech fixture %s: %w", path, e)
	}
	rel, e := filepath.Rel(root, target)
	if e != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return nil, fmt.Errorf("fixture must be inside fixtures directory")
	}
	f, e := os.Open(target)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, maxSamples*2+65537))
	if e != nil {
		return nil, e
	}
	return parseWAV(b)
}
func (s *Server) scenarioHTTP(w http.ResponseWriter, r *http.Request) {
	b, e := readLimited(r, 16<<20)
	if e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	sc, e := parseScenario(b)
	if e != nil {
		http.Error(w, e.Error(), 400)
		return
	}
	if !s.runMu.TryLock() {
		http.Error(w, "another scenario is running", 409)
		return
	}
	defer s.runMu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Minute)
	defer cancel()
	stop := context.AfterFunc(s.ctx, cancel)
	defer stop()
	report := s.executeScenario(ctx, sc)
	writeJSON(w, report)
}
func (s *Server) executeScenario(ctx context.Context, sc Scenario) (report Report) {
	report = Report{Name: sc.Name, Passed: true, Started: time.Now().UTC(), Steps: []StepResult{}}
	report.ArtifactDir = filepath.Join(s.out, report.Started.Format("20060102T150405.000000000Z"))
	defer func() {
		report.Finished = time.Now().UTC()
		if e := saveArtifacts(report.ArtifactDir, s.engine, report); e != nil {
			report.Passed = false
			report.Error = "write artifacts: " + e.Error()
		}
	}()
	for id := range sc.Sequences {
		if s.engine.devices[id] == nil {
			report.Passed = false
			report.Error = "unknown device " + id
			return
		}
	}
	for _, path := range sc.Requires {
		if _, e := fixture(path); e != nil {
			report.Passed = false
			report.Error = "missing prerequisite: " + e.Error()
			return
		}
	}
	for _, d := range s.engine.devices {
		d.mu.Lock()
		busy := d.status.Input == "live"
		d.mu.Unlock()
		if busy {
			report.Passed = false
			report.Error = "stop browser microphone before running scenarios"
			return
		}
	}
	for _, d := range s.engine.devices {
		d.stop()
		d.reset()
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var mu sync.Mutex
	var wg sync.WaitGroup
	// One event socket per sequence provides an independent, bounded event history.
	for id, steps := range sc.Sequences {
		id, steps := id, steps
		wg.Add(1)
		go func() {
			defer wg.Done()
			d := s.engine.devices[id]
			var events *appEvents
			pending := ""
			needsApp := false
			for _, step := range steps {
				if step.Action == "command" || step.Action == "wait_event" {
					needsApp = true
				}
			}
			if needsApp {
				var e error
				events, e = connectEvents(ctx, s.manifest, id, d)
				if e != nil {
					mu.Lock()
					report.Passed = false
					report.Error = "Gocalis prerequisite unavailable: " + e.Error()
					mu.Unlock()
					cancel()
					return
				}
				defer events.close()
			}
			for index, step := range steps {
				start := time.Now().UTC()
				err := s.executeStep(ctx, d, step, events, &pending)
				result := StepResult{id, index, step.Action, start, time.Since(start).Seconds(), ""}
				if err != nil {
					result.Error = err.Error()
				}
				d.mu.Lock()
				d.event("scenario_step", result)
				d.mu.Unlock()
				mu.Lock()
				report.Steps = append(report.Steps, result)
				if err != nil {
					report.Passed = false
				}
				mu.Unlock()
				if err != nil {
					cancel()
					return
				}
			}
			if pending != "" {
				mu.Lock()
				report.Passed = false
				report.Error = "sequence " + id + " ended without waiting for " + pending
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	for _, d := range s.engine.devices {
		d.stop()
	}
	return
}
func (s *Server) executeStep(ctx context.Context, d *Device, step Step, events *appEvents, pending *string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch step.Action {
	case "replay":
		var p []int16
		var e error
		if step.Audio != "" {
			var b []byte
			b, e = base64.StdEncoding.DecodeString(step.Audio)
			if e == nil {
				p, e = parseWAV(b)
			}
		} else {
			p, e = fixture(step.Replay)
		}
		if e != nil {
			return e
		}
		return d.play(p)
	case "tone":
		if step.Hz < 100 || step.Hz > 7000 || step.Seconds <= 0 || step.Seconds > 300 {
			return fmt.Errorf("invalid tone")
		}
		return d.play(tone(step.Hz, step.Seconds))
	case "stop":
		d.stop()
		return nil
	case "reset":
		d.reset()
		return nil
	case "disconnect":
		d.online(false)
		return nil
	case "reconnect":
		d.online(true)
		return nil
	case "assert_output":
		return assertOutput(d.audio(), step, d.snapshot())
	}
	timeout, e := stepDeadline(step)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	switch step.Action {
	case "command":
		if *pending != "" {
			return fmt.Errorf("wait for %s before issuing another command", *pending)
		}
		command := map[string]any{}
		for k, v := range step.Command {
			command[k] = v
		}
		action, ok := command["action"].(string)
		if !ok || action == "" {
			return fmt.Errorf("command action required")
		}
		target, ok := command["node_id"].(string)
		if !ok {
			target = d.status.ID
			command["node_id"] = target
		}
		if target != d.status.ID && target != "all" {
			return fmt.Errorf("command node must match sequence device or all")
		}
		events.clear()
		b, e := json.Marshal(command)
		if e != nil {
			return e
		}
		req, e := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(s.manifest.App, "/")+"/api/execute", bytes.NewReader(b))
		if e != nil {
			return e
		}
		req.Header.Set("Content-Type", "application/json")
		if s.manifest.Token != "" {
			req.Header.Set("Authorization", "Bearer "+s.manifest.Token)
		}
		res, e := http.DefaultClient.Do(req)
		if e != nil {
			return e
		}
		defer res.Body.Close()
		b, e = io.ReadAll(io.LimitReader(res.Body, 1<<20))
		if e != nil {
			return e
		}
		if res.StatusCode != http.StatusAccepted {
			return fmt.Errorf("app command HTTP %d: %s", res.StatusCode, b)
		}
		*pending = action + "_completed"
		return nil
	case "wait_event":
		if events == nil {
			return fmt.Errorf("event socket unavailable")
		}
		e := events.wait(ctx, step)
		if e == nil && step.Event == *pending {
			*pending = ""
		}
		return e
	case "wait":
		timer := time.NewTimer(time.Duration(step.Seconds * float64(time.Second)))
		defer timer.Stop()
		if step.Seconds < 0 || step.Seconds > 300 {
			return fmt.Errorf("invalid wait duration")
		}
		select {
		case <-timer.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	case "wait_input_idle", "wait_activity":
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			status := d.snapshot()
			if step.Action == "wait_input_idle" && status.Input == "silence" {
				return nil
			}
			if step.Action == "wait_activity" {
				threshold := step.MinRMS
				if threshold <= 0 {
					threshold = 0.02
				}
				v := status.OutputLevel
				if step.Direction == "input" {
					v = status.InputLevel
				}
				if v >= threshold {
					return nil
				}
			}
			select {
			case <-ctx.Done():
				return fmt.Errorf("%s: %w", step.Action, ctx.Err())
			case <-ticker.C:
			}
		}
	}
	return fmt.Errorf("unsupported action")
}
func assertOutput(p []int16, step Step, status Status) error {
	if status.DecodeErrors > 0 || status.Overruns > 0 {
		return fmt.Errorf("codec or buffer errors: %+v", status)
	}
	if step.MinActiveSeconds <= 0 {
		return fmt.Errorf("assert_output requires min_active_seconds")
	}
	threshold := step.MinRMS
	if threshold <= 0 {
		threshold = 0.02
	}
	active := 0
	for i := 0; i < len(p); i += 320 {
		end := i + 320
		if end > len(p) {
			end = len(p)
		}
		if level(p[i:end]) >= threshold {
			active += end - i
		}
	}
	seconds := float64(active) / rate
	if seconds < step.MinActiveSeconds {
		return fmt.Errorf("active output %.3fs < %.3fs", seconds, step.MinActiveSeconds)
	}
	if step.MaxSeconds > 0 && float64(len(p))/rate > step.MaxSeconds {
		return fmt.Errorf("recording duration exceeds %.3fs", step.MaxSeconds)
	}
	if step.Hz > 0 {
		minimum := step.MinMarker
		if minimum <= 0 {
			minimum = 0.05
		}
		if score := marker(p, step.Hz); score < minimum {
			return fmt.Errorf("decoded %.0f Hz marker score %.3f < %.3f", step.Hz, score, minimum)
		}
	}
	return nil
}

type appEvents struct {
	conn   *websocket.Conn
	queue  chan map[string]any
	done   chan struct{}
	mu     sync.Mutex
	err    error
	device *Device
	id     string
}

func connectEvents(ctx context.Context, m Manifest, id string, d *Device) (*appEvents, error) {
	u, e := url.Parse(strings.TrimRight(m.App, "/") + "/api/events")
	if e != nil {
		return nil, e
	}
	if u.Scheme == "https" {
		u.Scheme = "wss"
	} else {
		u.Scheme = "ws"
	}
	header := http.Header{}
	if m.Token != "" {
		header.Set("Authorization", "Bearer "+m.Token)
	}
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	c, _, e := websocket.DefaultDialer.DialContext(dialCtx, u.String(), header)
	if e != nil {
		return nil, e
	}
	a := &appEvents{conn: c, queue: make(chan map[string]any, 128), done: make(chan struct{}), device: d, id: id}
	c.SetReadLimit(4 << 20)
	go func() {
		defer close(a.done)
		stop := context.AfterFunc(ctx, func() { c.Close() })
		defer stop()
		for {
			var event map[string]any
			if e := c.ReadJSON(&event); e != nil {
				a.mu.Lock()
				a.err = e
				a.mu.Unlock()
				return
			}
			node, _ := event["node_id"].(string)
			if node != id && node != "all" {
				continue
			}
			d.mu.Lock()
			d.event("app_event", event)
			d.mu.Unlock()
			select {
			case a.queue <- event:
			default:
				a.mu.Lock()
				a.err = fmt.Errorf("app event queue overrun")
				a.mu.Unlock()
				c.Close()
				return
			}
		}
	}()
	return a, nil
}
func (a *appEvents) close() { a.conn.Close(); <-a.done }
func (a *appEvents) clear() {
	for {
		select {
		case <-a.queue:
		default:
			return
		}
	}
}
func (a *appEvents) wait(ctx context.Context, step Step) error {
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("event %s: %w", step.Event, ctx.Err())
		case <-a.done:
			a.mu.Lock()
			e := a.err
			a.mu.Unlock()
			return fmt.Errorf("event socket ended: %v", e)
		case event := <-a.queue:
			if event["event"] == "error" || event["status"] == "error" {
				return fmt.Errorf("app error: %v", event)
			}
			if event["event"] != step.Event {
				continue
			}
			field := step.Field
			if field == "" {
				field = "text"
			}
			value, _ := event[field].(string)
			if step.Contains != "" && !strings.Contains(strings.ToLower(value), strings.ToLower(step.Contains)) {
				return fmt.Errorf("%s event %s does not contain %q", step.Event, field, step.Contains)
			}
			return nil
		}
	}
}
func scenarioCLI(endpoint, path, out string) (runErr error) {
	written := false
	defer func() {
		if runErr != nil && !written {
			report := Report{Name: filepath.Base(path), Passed: false, Started: time.Now().UTC(), Finished: time.Now().UTC(), Error: runErr.Error(), Steps: []StepResult{}}
			b, _ := json.MarshalIndent(report, "", "  ")
			if e := os.MkdirAll(out, 0755); e == nil {
				_ = os.WriteFile(filepath.Join(out, "report.json"), b, 0644)
			}
		}
	}()

	b, e := os.ReadFile(path)
	if e != nil {
		return e
	}
	sc, e := parseScenario(b)
	if e != nil {
		return e
	}
	// Resolve CLI fixture paths next to the YAML and send their contents, so the
	// UI and CLI use the same execution endpoint without sharing a filesystem.
	for id, steps := range sc.Sequences {
		for i := range steps {
			if steps[i].Action == "replay" && steps[i].Replay != "" {
				b, e = os.ReadFile(filepath.Join(filepath.Dir(path), steps[i].Replay))
				if e != nil {
					return fmt.Errorf("missing speech fixture: %w", e)
				}
				if _, e = parseWAV(b); e != nil {
					return e
				}
				steps[i].Audio = base64.StdEncoding.EncodeToString(b)
				steps[i].Replay = ""
			}
		}
		sc.Sequences[id] = steps
	}
	// CLI prerequisites are verified locally; the server verifies app availability.
	for _, pathName := range sc.Requires {
		if _, e = os.Stat(filepath.Join(filepath.Dir(path), pathName)); e != nil {
			return fmt.Errorf("missing prerequisite %s: %w", pathName, e)
		}
	}
	sc.Requires = nil
	b, e = yaml.Marshal(sc)
	if e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 310*time.Second)
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, "POST", strings.TrimRight(endpoint, "/")+"/api/scenarios", bytes.NewReader(b))
	if e != nil {
		return e
	}
	res, e := http.DefaultClient.Do(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode != 200 {
		b, _ = io.ReadAll(io.LimitReader(res.Body, 1<<20))
		return fmt.Errorf("scenario HTTP %d: %s", res.StatusCode, b)
	}
	var report Report
	if e = json.NewDecoder(res.Body).Decode(&report); e != nil {
		return e
	}
	b, _ = json.MarshalIndent(report, "", "  ")
	if e = os.MkdirAll(out, 0755); e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(out, "report.json"), b, 0644); e != nil {
		return e
	}
	written = true
	fmt.Println(string(b))
	if !report.Passed {
		return fmt.Errorf("scenario failed; artifacts: %s", report.ArtifactDir)
	}
	return nil
}
