// Package telegramcall is the "telegram_call" plugin: it brings a Telegram
// group's voice chat into an intercom call with a node.
//
// The use it is made for is a doorbell. When somebody rings, a flow sends the
// plugin the "call" command for the doorbell node. The plugin, logged in as a
// Telegram user, opens the voice chat of the configured group and waits in it.
// The moment somebody from the group comes in, the intercom call starts: the
// node is taken over and the two sides talk. It is over when the people on
// Telegram are gone, when the intercom call ends, or when nobody came in time;
// the plugin then leaves, and closes the voice chat if it opened it.
//
// In the intercom call the voice chat is one remote participant, like an
// intercom page: everybody in it hears the call and is heard by it, and
// intercom pages and other nodes can be in the same call.
//
// Only a user account can be in a voice chat (a bot cannot), so the plugin
// needs an api_id and api_hash from my.telegram.org and a login done once with
// `gocalis -plugin-setup <id>`.
package telegramcall

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gocalis/internal/config"
	"gocalis/internal/plugin"
)

func init() {
	plugin.Register("telegram_call", func(cfg config.PluginConfig, host plugin.Host) (plugin.Plugin, error) {
		return New(cfg, host)
	})
}

// Settings is the plugin's part of the configuration.
type Settings struct {
	// APIID and APIHash identify the application to Telegram. They come from
	// https://my.telegram.org ("API development tools").
	APIID   int    `yaml:"api_id"`
	APIHash string `yaml:"api_hash"`
	// Phone is the number of the account gocalis logs in with, in
	// international form. Only the setup uses it; left out, the setup asks.
	Phone string `yaml:"phone"`
	// SessionFile is where the login is kept. Whoever has the file has the
	// account: keep it out of backups and repositories, and on a volume that
	// outlives the container.
	SessionFile string `yaml:"session_file"`
	// Chat is the group whose voice chat is used when a command does not
	// name one: its @username, its numeric id, or its exact title.
	Chat string `yaml:"chat"`
	// Title is given to a voice chat the plugin starts.
	Title string `yaml:"title"`
	// RingSeconds is how long the plugin waits in the voice chat for
	// somebody to come in when a command does not say.
	RingSeconds float64 `yaml:"ring_seconds"`
}

const (
	defaultRingSeconds = 60
	maxRingSeconds     = 600
)

// connectFunc logs in to Telegram and calls ready with the account once it is
// usable. The connection lasts until ready returns, or until it is lost for
// good, which ends the context ready was given. errNotLoggedIn means the
// session file holds no login.
type connectFunc func(ready func(ctx context.Context, api telegramAPI, account string) error) error

var errNotLoggedIn = errors.New("not logged in to Telegram")

// Plugin is one configured telegram_call plugin.
type Plugin struct {
	host     plugin.Host
	settings Settings
	connect  connectFunc
	newMedia func(heard sink) (media, error)
	// lookEvery is how often a call looks at who is in the voice chat when
	// Telegram does not report a change by itself.
	lookEvery time.Duration

	mu     sync.Mutex
	api    telegramAPI // nil while not connected
	detail string
	ring   *ring // the call in progress, if any
}

// New checks the settings and prepares the plugin.
func New(cfg config.PluginConfig, host plugin.Host) (*Plugin, error) {
	settings := Settings{SessionFile: "telegram.session", RingSeconds: defaultRingSeconds}
	if err := cfg.DecodeSettings(&settings); err != nil {
		return nil, err
	}
	if settings.APIID <= 0 || settings.APIHash == "" {
		return nil, errors.New("settings: api_id and api_hash are required (from https://my.telegram.org)")
	}
	if settings.SessionFile == "" {
		return nil, errors.New("settings: session_file must not be empty")
	}
	if settings.RingSeconds <= 0 || settings.RingSeconds > maxRingSeconds {
		return nil, fmt.Errorf("settings: ring_seconds must be in (0,%d]", maxRingSeconds)
	}
	p := &Plugin{
		host:      host,
		settings:  settings,
		newMedia:  func(heard sink) (media, error) { return newLink(heard) },
		lookEvery: 3 * time.Second,
		detail:    "starting",
	}
	p.connect = func(ready func(context.Context, telegramAPI, string) error) error {
		return connectTelegram(context.Background(), settings, nil, ready)
	}
	return p, nil
}

// Run keeps the plugin logged in to Telegram until ctx ends. Without a login
// it waits for one to be made with -plugin-setup, which can happen while
// gocalis runs.
func (p *Plugin) Run(ctx context.Context) error {
	retry := 5 * time.Second
	for ctx.Err() == nil {
		if _, err := os.Stat(p.settings.SessionFile); err != nil {
			p.setDetail(fmt.Sprintf("not logged in: run gocalis -plugin-setup %s", p.host.ID))
			sleep(ctx, 2*time.Second)
			continue
		}
		p.setDetail("connecting to Telegram")
		err := p.connect(func(online context.Context, api telegramAPI, account string) error {
			log.Printf("[Plugin %s] Logged in to Telegram as %s\n", p.host.ID, account)
			retry = 5 * time.Second
			p.setAPI(api, "logged in as "+account)
			defer p.setAPI(nil, "disconnected")
			select {
			case <-ctx.Done():
				p.stopRing(errShutdown)
				return nil
			case <-online.Done():
				p.stopRing(errOffline)
				return online.Err()
			}
		})
		if ctx.Err() != nil {
			return nil
		}
		if errors.Is(err, errNotLoggedIn) {
			log.Printf("[Plugin %s] Not logged in to Telegram: run gocalis -plugin-setup %s\n", p.host.ID, p.host.ID)
			p.setDetail(fmt.Sprintf("not logged in: run gocalis -plugin-setup %s", p.host.ID))
			p.waitForLogin(ctx)
			continue
		}
		log.Printf("[Plugin %s] Telegram connection ended (%v); again in %v\n", p.host.ID, err, retry)
		p.setDetail(fmt.Sprintf("disconnected: %v", err))
		sleep(ctx, retry)
		retry = min(retry*2, time.Minute)
	}
	return nil
}

// waitForLogin returns when the session file changes, which is what a setup
// run does.
func (p *Plugin) waitForLogin(ctx context.Context) {
	stamp := func() time.Time {
		info, err := os.Stat(p.settings.SessionFile)
		if err != nil {
			return time.Time{}
		}
		return info.ModTime()
	}
	// The failed attempt itself may still be writing the file.
	sleep(ctx, 2*time.Second)
	before := stamp()
	for ctx.Err() == nil && stamp().Equal(before) {
		sleep(ctx, 5*time.Second)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// Status implements plugin.Plugin.
func (p *Plugin) Status() plugin.Status {
	p.mu.Lock()
	defer p.mu.Unlock()
	status := plugin.Status{Ready: p.api != nil, Detail: p.detail}
	if p.ring != nil {
		status.Detail += "; call for " + p.ring.node
	}
	return status
}

// callArgs are the args of the "call" command.
type callArgs struct {
	// Chat and Title replace the configured ones for this call.
	Chat  string `json:"chat"`
	Title string `json:"title"`
	// RingSeconds is how long to wait for somebody to come in; 0 for the
	// configured time.
	RingSeconds float64 `json:"ring_seconds"`
	// DurationSeconds is how long the intercom call lasts once it starts; 0
	// for the intercom default.
	DurationSeconds float64 `json:"duration_seconds"`
}

// Command implements plugin.Plugin. "call" opens the voice chat for the node
// and waits for somebody to come in; "hangup" ends that, at whatever point it
// is.
func (p *Plugin) Command(_ context.Context, cmd plugin.Command) error {
	switch cmd.Name {
	case "call":
		return p.call(cmd)
	case "hangup":
		return p.hangup(cmd)
	}
	return fmt.Errorf("unknown command '%s' (telegram_call has: call, hangup)", cmd.Name)
}

func (p *Plugin) call(cmd plugin.Command) error {
	args := callArgs{Chat: p.settings.Chat, Title: p.settings.Title, RingSeconds: p.settings.RingSeconds}
	if err := cmd.Decode(&args); err != nil {
		return err
	}
	if args.RingSeconds == 0 {
		args.RingSeconds = p.settings.RingSeconds
	}
	switch {
	case cmd.NodeID == "":
		return errors.New("call requires node_id")
	case !p.host.HasNode(cmd.NodeID):
		return fmt.Errorf("node '%s' not registered", cmd.NodeID)
	case args.Chat == "":
		return errors.New("call requires a chat: set 'chat' in the plugin settings or in the args")
	case args.RingSeconds < 0 || args.RingSeconds > maxRingSeconds:
		return fmt.Errorf("ring_seconds must be in (0,%d]", maxRingSeconds)
	case args.DurationSeconds < 0:
		return errors.New("duration_seconds must not be negative")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.api == nil {
		return fmt.Errorf("not connected to Telegram (%s)", p.detail)
	}
	if p.ring != nil {
		if p.ring.node != cmd.NodeID {
			return fmt.Errorf("busy with a call for node '%s'", p.ring.node)
		}
		// The bell again: whoever is waiting waits from now.
		select {
		case p.ring.again <- struct{}{}:
		default:
		}
		return nil
	}

	ctx, stop := context.WithCancelCause(context.Background())
	p.ring = &ring{
		p:        p,
		api:      p.api,
		node:     cmd.NodeID,
		chat:     args.Chat,
		title:    args.Title,
		ringFor:  time.Duration(args.RingSeconds * float64(time.Second)),
		callFor:  time.Duration(args.DurationSeconds * float64(time.Second)),
		ctx:      ctx,
		stop:     stop,
		again:    make(chan struct{}, 1),
		finished: make(chan struct{}),
	}
	go p.ring.run()
	return nil
}

func (p *Plugin) hangup(cmd plugin.Command) error {
	if err := cmd.Decode(&struct{}{}); err != nil {
		return err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ring == nil {
		return errors.New("no Telegram call in progress")
	}
	if cmd.NodeID != "" && cmd.NodeID != p.ring.node {
		return fmt.Errorf("the Telegram call in progress is for node '%s'", p.ring.node)
	}
	p.ring.stop(errHangup)
	return nil
}

// stopRing ends the call in progress, if any, and waits until it is over.
func (p *Plugin) stopRing(cause error) {
	p.mu.Lock()
	r := p.ring
	p.mu.Unlock()
	if r == nil {
		return
	}
	r.stop(cause)
	<-r.finished
}

// release is called by a ring that is over, before it reports so.
func (p *Plugin) release(r *ring) {
	p.mu.Lock()
	if p.ring == r {
		p.ring = nil
	}
	p.mu.Unlock()
	r.stop(nil)
}

func (p *Plugin) setAPI(api telegramAPI, detail string) {
	p.mu.Lock()
	p.api, p.detail = api, detail
	p.mu.Unlock()
}

func (p *Plugin) setDetail(detail string) {
	p.mu.Lock()
	p.detail = detail
	p.mu.Unlock()
}

// Setup implements plugin.Setup: it logs the account in, asking at the
// terminal for what Telegram wants (the code it sends, the two-step password
// if the account has one), and leaves the login in the session file.
func (p *Plugin) Setup(ctx context.Context, in io.Reader, out io.Writer) error {
	if dir := filepath.Dir(p.settings.SessionFile); dir != "." {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("create the session directory: %w", err)
		}
	}
	fmt.Fprintf(out, "Logging in to Telegram (session file: %s)\n", p.settings.SessionFile)
	ask := newPrompter(in, out)
	return connectTelegram(ctx, p.settings, ask, func(ctx context.Context, api telegramAPI, account string) error {
		fmt.Fprintf(out, "Logged in as %s.\n", account)
		if p.settings.Chat == "" {
			return nil
		}
		vc, err := api.(*client).describe(ctx, p.settings.Chat)
		if err != nil {
			fmt.Fprintf(out, "The chat %q was not found: %v\n", p.settings.Chat, err)
			return nil
		}
		fmt.Fprintf(out, "The chat %q is %s.\n", p.settings.Chat, vc)
		return nil
	})
}
