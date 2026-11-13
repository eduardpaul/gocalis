// Package plugin lets optional parts of gocalis be compiled in and switched on
// from config.yaml. A plugin is one entry of the "plugins" list: it has an id,
// a type that selects the code, and settings of its own. While gocalis runs it
// takes commands sent through the API (action "plugin") and reports with
// events.
//
// A plugin type lives in its own package under internal/plugins, registers
// itself from init with Register, and is linked in by a blank import in
// cmd/main.go.
package plugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"

	"gocalis/internal/config"
	"gocalis/internal/intercom"
)

// Plugin is one configured plugin while gocalis runs.
type Plugin interface {
	// Run keeps the plugin working until ctx ends. It is called once, on its
	// own goroutine. Returning before ctx ends stops the plugin: from then on
	// its commands are refused with the returned error.
	Run(ctx context.Context) error

	// Command carries out one command sent to the plugin. It returns once the
	// command is accepted or refused; what comes of an accepted command is
	// reported with events. A returned error reaches the sender as an "error"
	// event.
	Command(ctx context.Context, cmd Command) error

	// Status says whether the plugin can take commands right now and, in a
	// few words for a person, what it is doing or what it is missing.
	Status() Status
}

// Setup is implemented by plugins that need a step with a person at the
// terminal before they can run (a login). `gocalis -plugin-setup <id>` runs it
// and exits.
type Setup interface {
	Setup(ctx context.Context, in io.Reader, out io.Writer) error
}

// Status is a plugin's condition at one moment.
type Status struct {
	Ready  bool   `json:"ready"`
	Detail string `json:"detail"`
}

// Command is a request for a plugin.
type Command struct {
	Name   string          // what to do; each plugin type defines its commands
	NodeID string          // the node the command is about, when it is about one
	Args   json.RawMessage // the command's own parameters, a JSON object; empty for none
}

// Decode reads the command's args into a struct. An arg the struct does not
// have is an error, so a misspelled one is not silently ignored. Without args
// the struct is left as it is.
func (c Command) Decode(into any) error {
	if len(bytes.TrimSpace(c.Args)) == 0 {
		return nil
	}
	dec := json.NewDecoder(bytes.NewReader(c.Args))
	dec.DisallowUnknownFields()
	if err := dec.Decode(into); err != nil {
		return fmt.Errorf("invalid args for '%s': %w", c.Name, err)
	}
	return nil
}

// Event is something a plugin reports. It goes out on every transport, like
// the events of gocalis itself, marked with the plugin's id.
type Event struct {
	Name    string         // the event name; each plugin type defines its events
	NodeID  string         // the node the event is about, when it is about one
	Status  string         // "success" | "error", when the event closes something
	Reason  string         // why, in one word, when the event closes something
	Message string         // error detail, when Status == "error"
	Data    map[string]any // the event's own fields
}

// Host is what gocalis gives a plugin to work with.
type Host struct {
	// ID is the plugin's id from the configuration.
	ID string
	// Intercom is the call engine: a plugin brings an outside party into a
	// call with a node as a remote participant.
	Intercom *intercom.Engine
	// HasNode tells whether a node with that id is registered right now.
	HasNode func(nodeID string) bool
	// Emit reports an event. It does not block.
	Emit func(Event)
}

// Factory builds a plugin of one type from its entry in the configuration.
// It only checks the settings and prepares: the work starts in Run. A Host
// without an intercom engine means the plugin is being built for Setup only.
type Factory func(cfg config.PluginConfig, host Host) (Plugin, error)

var (
	typesMu sync.RWMutex
	types   = map[string]Factory{}
)

// Register makes a plugin type available under the name the configuration
// selects it with. It is meant to be called from the init of the type's
// package.
func Register(typeName string, factory Factory) {
	typesMu.Lock()
	defer typesMu.Unlock()
	if _, taken := types[typeName]; taken {
		panic("plugin: type registered twice: " + typeName)
	}
	types[typeName] = factory
}

// Types lists the registered plugin types.
func Types() []string {
	typesMu.RLock()
	defer typesMu.RUnlock()
	names := make([]string, 0, len(types))
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// New builds the plugin for one configuration entry.
func New(cfg config.PluginConfig, host Host) (Plugin, error) {
	typesMu.RLock()
	factory := types[cfg.Type]
	typesMu.RUnlock()
	if factory == nil {
		return nil, fmt.Errorf("plugin '%s': unknown type '%s' (this build has: %v)", cfg.ID, cfg.Type, Types())
	}
	host.ID = cfg.ID
	p, err := factory(cfg, host)
	if err != nil {
		return nil, fmt.Errorf("plugin '%s': %w", cfg.ID, err)
	}
	return p, nil
}

// Services is what the plugins of one gocalis process share.
type Services struct {
	Intercom *intercom.Engine
	HasNode  func(nodeID string) bool
	// Publish sends a plugin's event out on the transports.
	Publish func(pluginID string, ev Event)
}

// Info describes one running plugin for the API.
type Info struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Status
}

// Manager runs the configured plugins and routes commands to them.
type Manager struct {
	order   []string
	plugins map[string]*running
	wg      sync.WaitGroup
}

type running struct {
	typeName string
	plugin   Plugin

	mu      sync.Mutex
	stopped bool
	failure error
}

// Start builds every configured plugin and sets it running until ctx ends. A
// plugin that cannot be built is a configuration error and fails the start; one
// that stops later only takes itself out.
func Start(ctx context.Context, cfgs []config.PluginConfig, services Services) (*Manager, error) {
	m := &Manager{plugins: make(map[string]*running)}
	for _, cfg := range cfgs {
		id := cfg.ID
		p, err := New(cfg, Host{
			Intercom: services.Intercom,
			HasNode:  services.HasNode,
			Emit:     func(ev Event) { services.Publish(id, ev) },
		})
		if err != nil {
			return nil, err
		}
		m.order = append(m.order, id)
		m.plugins[id] = &running{typeName: cfg.Type, plugin: p}
	}
	for _, id := range m.order {
		r := m.plugins[id]
		m.wg.Add(1)
		go func() {
			defer m.wg.Done()
			log.Printf("[Plugin %s] Started (%s)\n", id, r.typeName)
			err := r.plugin.Run(ctx)
			r.mu.Lock()
			r.stopped, r.failure = true, err
			r.mu.Unlock()
			if err != nil && ctx.Err() == nil {
				log.Printf("[Plugin %s] Stopped: %v\n", id, err)
			}
		}()
	}
	return m, nil
}

// Wait returns once every plugin has stopped, which they do when the context
// given to Start ends.
func (m *Manager) Wait() { m.wg.Wait() }

// Command passes a command to the plugin with that id.
func (m *Manager) Command(ctx context.Context, id string, cmd Command) error {
	r := m.plugins[id]
	if r == nil {
		return fmt.Errorf("no plugin '%s'", id)
	}
	if stopped, failure := r.state(); stopped {
		if failure != nil {
			return fmt.Errorf("plugin '%s' has stopped: %w", id, failure)
		}
		return fmt.Errorf("plugin '%s' has stopped", id)
	}
	return r.plugin.Command(ctx, cmd)
}

// List describes the plugins, in configuration order.
func (m *Manager) List() []Info {
	infos := make([]Info, 0, len(m.order))
	for _, id := range m.order {
		r := m.plugins[id]
		info := Info{ID: id, Type: r.typeName}
		if stopped, failure := r.state(); stopped {
			info.Detail = "stopped"
			if failure != nil {
				info.Detail = "stopped: " + failure.Error()
			}
		} else {
			info.Status = r.plugin.Status()
		}
		infos = append(infos, info)
	}
	return infos
}

func (r *running) state() (stopped bool, failure error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped, r.failure
}
