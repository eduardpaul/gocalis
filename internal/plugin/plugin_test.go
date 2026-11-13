package plugin

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"gocalis/internal/config"
)

// echo is a plugin type for the tests: "say" emits an event with the word it
// was given, "quit" makes Run return.
type echo struct {
	host   Host
	prefix string
	quit   chan error
}

type echoSettings struct {
	Prefix string `yaml:"prefix"`
}

func init() {
	Register("echo", func(cfg config.PluginConfig, host Host) (Plugin, error) {
		var s echoSettings
		if err := cfg.DecodeSettings(&s); err != nil {
			return nil, err
		}
		if s.Prefix == "bad" {
			return nil, errors.New("prefix must not be bad")
		}
		return &echo{host: host, prefix: s.Prefix, quit: make(chan error, 1)}, nil
	})
}

func (e *echo) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return nil
	case err := <-e.quit:
		return err
	}
}

func (e *echo) Command(_ context.Context, cmd Command) error {
	switch cmd.Name {
	case "say":
		var args struct {
			Word string `json:"word"`
		}
		if err := cmd.Decode(&args); err != nil {
			return err
		}
		e.host.Emit(Event{Name: "echo_said", NodeID: cmd.NodeID, Data: map[string]any{"word": e.prefix + args.Word}})
		return nil
	case "quit":
		e.quit <- errors.New("asked to quit")
		return nil
	}
	return errors.New("unknown command: " + cmd.Name)
}

func (e *echo) Status() Status { return Status{Ready: true, Detail: "echoing"} }

type published struct {
	mu     sync.Mutex
	events []string
}

func (p *published) publish(pluginID string, ev Event) {
	p.mu.Lock()
	defer p.mu.Unlock()
	word, _ := ev.Data["word"].(string)
	p.events = append(p.events, pluginID+":"+ev.Name+":"+ev.NodeID+":"+word)
}

func (p *published) all() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.events...)
}

func pluginConfigs(t *testing.T, yamlText string) []config.PluginConfig {
	t.Helper()
	var cfgs []config.PluginConfig
	if err := yaml.Unmarshal([]byte(yamlText), &cfgs); err != nil {
		t.Fatal(err)
	}
	return cfgs
}

func TestManagerRoutesCommandsAndMarksEvents(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := &published{}
	m, err := Start(ctx, pluginConfigs(t, `
- id: one
  type: echo
  settings:
    prefix: "1-"
- id: two
  type: echo
`), Services{Publish: out.publish})
	if err != nil {
		t.Fatal(err)
	}

	if err := m.Command(ctx, "one", Command{Name: "say", NodeID: "door", Args: []byte(`{"word":"hola"}`)}); err != nil {
		t.Fatal(err)
	}
	if err := m.Command(ctx, "two", Command{Name: "say", Args: []byte(`{"word":"adios"}`)}); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(out.all(), " "); got != "one:echo_said:door:1-hola two:echo_said::adios" {
		t.Fatalf("unexpected events: %s", got)
	}

	// Refusals come back as errors: an unknown plugin, an unknown command, a
	// misspelled arg.
	if err := m.Command(ctx, "three", Command{Name: "say"}); err == nil {
		t.Fatal("expected a command for an unknown plugin to fail")
	}
	if err := m.Command(ctx, "one", Command{Name: "sing"}); err == nil {
		t.Fatal("expected an unknown command to fail")
	}
	if err := m.Command(ctx, "one", Command{Name: "say", Args: []byte(`{"wrd":"x"}`)}); err == nil || !strings.Contains(err.Error(), "wrd") {
		t.Fatalf("expected the misspelled arg to be refused, got %v", err)
	}

	infos := m.List()
	if len(infos) != 2 || infos[0].ID != "one" || infos[0].Type != "echo" || !infos[0].Ready || infos[0].Detail != "echoing" {
		t.Fatalf("unexpected list: %+v", infos)
	}

	cancel()
	m.Wait()
}

func TestManagerRefusesCommandsForAStoppedPlugin(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m, err := Start(ctx, pluginConfigs(t, "- id: one\n  type: echo\n"), Services{Publish: (&published{}).publish})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Command(ctx, "one", Command{Name: "quit"}); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		err := m.Command(ctx, "one", Command{Name: "say"})
		if err != nil && strings.Contains(err.Error(), "asked to quit") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("expected commands to be refused once the plugin stopped, got %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if info := m.List()[0]; info.Ready || !strings.Contains(info.Detail, "asked to quit") {
		t.Fatalf("expected the list to show the plugin stopped, got %+v", info)
	}
}

func TestStartFailsOnBadConfiguration(t *testing.T) {
	for name, text := range map[string]string{
		"unknown type":    "- id: one\n  type: nope\n",
		"refused setting": "- id: one\n  type: echo\n  settings:\n    prefix: bad\n",
		"unknown setting": "- id: one\n  type: echo\n  settings:\n    colour: red\n",
	} {
		if _, err := Start(context.Background(), pluginConfigs(t, text), Services{}); err == nil || !strings.Contains(err.Error(), "one") {
			t.Errorf("%s: expected an error naming the plugin, got %v", name, err)
		}
	}
}
