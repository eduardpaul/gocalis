package protocol

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/intercom"
	"gocalis/internal/node"
	"gocalis/internal/plugin"
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
		{"intercom single node", Request{Action: "intercom", NodeIDs: []string{"a"}}, true},
		{"intercom empty node id", Request{Action: "intercom", NodeIDs: []string{"a", ""}}, true},
		{"intercom with duration", Request{Action: "intercom", NodeIDs: []string{"a", "b"}, DurationSeconds: 30}, false},
		{"intercom negative duration", Request{Action: "intercom", NodeIDs: []string{"a", "b"}, DurationSeconds: -1}, true},
		{"intercom too many nodes", Request{Action: "intercom", NodeIDs: make([]string, intercom.MaxParticipants+1)}, true},
		{"intercom_leave", Request{Action: "intercom_leave", NodeID: "a"}, false},
		{"intercom_leave without node", Request{Action: "intercom_leave"}, true},
		{"intercom_stop", Request{Action: "intercom_stop", NodeID: "a"}, false},
		{"intercom_stop without node", Request{Action: "intercom_stop"}, true},
		{"tts in call for the node", Request{Action: "tts", NodeID: "a", Text: "hi", CallMode: "node"}, false},
		{"tts in call for all", Request{Action: "tts", NodeID: "a", Text: "hi", CallMode: "all"}, false},
		{"tts unknown call mode", Request{Action: "tts", NodeID: "a", Text: "hi", CallMode: "everyone"}, true},
		{"ask in call", Request{Action: "ask", NodeID: "a", CallMode: "node"}, false},
		{"ask in call for all", Request{Action: "ask", NodeID: "a", CallMode: "all"}, true},
		{"plugin command", Request{Action: "plugin", Plugin: "tg", Command: "call", NodeID: "a", Args: []byte(`{"ring_seconds":30}`)}, false},
		{"plugin command without args", Request{Action: "plugin", Plugin: "tg", Command: "hangup"}, false},
		{"plugin without command", Request{Action: "plugin", Plugin: "tg"}, true},
		{"plugin without plugin", Request{Action: "plugin", Command: "call"}, true},
		{"plugin args not an object", Request{Action: "plugin", Plugin: "tg", Command: "call", Args: []byte(`[1]`)}, true},
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

// An omitted capture_delay_seconds takes the node's configured default; an
// explicit value (including 0) always wins.
func TestExecutorAskConfigCaptureDelay(t *testing.T) {
	b := brain.New(nil)
	cfg := config.NodeConfig{NodeID: "door"}
	cfg.KWS.AutoAskCaptureDelaySeconds = 0.8
	b.RegisterNode("door", &brain.NodeHandle{Node: node.NewPhysicalNode("door", "rtc_stream"), Audio: &audionode.Stub{}, Config: cfg})
	e := &Executor{Brain: b}

	zero, one := 0.0, 1.0
	tests := []struct {
		name string
		req  Request
		want float64
	}{
		{"omitted uses node default", Request{NodeID: "door"}, 0.8},
		{"explicit zero wins", Request{NodeID: "door", CaptureDelaySeconds: &zero}, 0},
		{"explicit value wins", Request{NodeID: "door", CaptureDelaySeconds: &one}, 1},
		{"unknown node", Request{NodeID: "nope"}, 0},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := e.AskConfig(tc.req).CaptureDelaySeconds; got != tc.want {
				t.Fatalf("CaptureDelaySeconds = %v, want %v", got, tc.want)
			}
		})
	}
}

// recorder collects what the executor publishes.
type recorder struct {
	mu     sync.Mutex
	events []Response
}

func (r *recorder) Publish(ev Response) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func (r *recorder) last(t *testing.T) Response {
	t.Helper()
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.events) == 0 {
		t.Fatal("nothing was published")
	}
	return r.events[len(r.events)-1]
}

// knock is a plugin type for the test: "knock" reports on the node it names,
// if gocalis has it.
type knock struct{ host plugin.Host }

func init() {
	plugin.Register("knock", func(_ config.PluginConfig, host plugin.Host) (plugin.Plugin, error) {
		return &knock{host: host}, nil
	})
}

func (k *knock) Run(ctx context.Context) error { <-ctx.Done(); return nil }
func (k *knock) Status() plugin.Status         { return plugin.Status{Ready: true} }
func (k *knock) Command(_ context.Context, cmd plugin.Command) error {
	if cmd.Name != "knock" {
		return fmt.Errorf("unknown command: %s", cmd.Name)
	}
	if !k.host.HasNode(cmd.NodeID) {
		return fmt.Errorf("node '%s' not registered", cmd.NodeID)
	}
	var args struct {
		Times int `json:"times"`
	}
	if err := cmd.Decode(&args); err != nil {
		return err
	}
	k.host.Emit(plugin.Event{Name: "knock_heard", NodeID: cmd.NodeID, Status: "success", Data: map[string]any{"times": args.Times}})
	return nil
}

// A "plugin" request reaches the plugin it names; the plugin's events go out
// marked with its id, and so does the error for a command it refuses.
func TestExecutorRunsPluginCommands(t *testing.T) {
	b := brain.New(nil)
	b.RegisterNode("door", &brain.NodeHandle{Node: node.NewPhysicalNode("door", "rtc_stream"), Audio: &audionode.Stub{}, Config: config.NodeConfig{NodeID: "door"}})
	out := &recorder{}
	e := &Executor{Brain: b, Publisher: out}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Without plugins the request is refused.
	e.Execute(ctx, Request{Action: "plugin", Plugin: "bell", Command: "knock", NodeID: "door"})
	if ev := out.last(t); ev.Event != "error" || ev.Plugin != "bell" {
		t.Fatalf("expected an error for the unknown plugin, got %+v", ev)
	}

	if err := e.StartPlugins(ctx, []config.PluginConfig{{ID: "bell", Type: "knock"}}); err != nil {
		t.Fatal(err)
	}

	e.Execute(ctx, Request{Action: "plugin", Plugin: "bell", Command: "knock", NodeID: "door", Args: []byte(`{"times":2}`)})
	if ev := out.last(t); ev.Event != "knock_heard" || ev.Plugin != "bell" || ev.NodeID != "door" || ev.Status != "success" || ev.Data["times"] != 2 {
		t.Fatalf("unexpected plugin event: %+v", ev)
	}

	e.Execute(ctx, Request{Action: "plugin", Plugin: "bell", Command: "knock", NodeID: "garden"})
	if ev := out.last(t); ev.Event != "error" || ev.Plugin != "bell" || ev.NodeID != "garden" || !strings.Contains(ev.Message, "not registered") {
		t.Fatalf("expected the refusal as an error event, got %+v", ev)
	}

	cancel()
	e.Plugins.Wait()
}
