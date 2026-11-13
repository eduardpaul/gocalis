// Package telegramcall is the "telegram_call" plugin: it makes a Telegram
// group a node of gocalis, one that somebody has to answer.
//
// The node's id is the plugin's. Say something to it, ask it something or put
// it in an intercom call, like any other node, and gocalis, logged in as a
// Telegram user, goes into the voice chat of the configured group (starting it
// when none runs) and waits there. The moment somebody of the group comes in,
// whatever was asked for happens: they hear what is said, are asked and
// listened to, or talk with the doorbell. When nobody comes in the ring time,
// it fails as "unanswered". gocalis leaves when they are gone, when an intercom
// call is over, or a little after the last thing it had to say, and closes the
// voice chat if it opened it.
//
// The use it is made for is a doorbell: when somebody rings, a flow starts an
// intercom call between the doorbell node and this one. The doorbell is left
// alone until somebody on Telegram answers.
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
	// Chat is the group whose voice chat is the node: its @username, its
	// numeric id, or its exact title.
	Chat string `yaml:"chat"`
	// Title is given to a voice chat the plugin starts.
	Title string `yaml:"title"`
	// RingSeconds is how long gocalis waits in the voice chat for somebody to
	// come in before the turn that rang fails as unanswered.
	RingSeconds float64 `yaml:"ring_seconds"`
	// IdleSeconds is how long gocalis stays in an answered voice chat after a
	// tts, play or ask, in case another follows (the reply to an answer). An
	// intercom call hangs up when it is over.
	IdleSeconds float64 `yaml:"idle_seconds"`
}

const (
	defaultRingSeconds = 60
	// A tts, play or ask request lives five minutes at most, ring included.
	maxRingSeconds     = 240
	defaultIdleSeconds = 15
	maxIdleSeconds     = 300
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
	ringFor   time.Duration
	idleFor   time.Duration

	mu     sync.Mutex
	api    telegramAPI // nil while not connected
	detail string
	call   *call // the stay in the voice chat, from the ring until gocalis is out

	audioMu sync.Mutex
	onAudio func(samples []float32)
}

// New checks the settings and prepares the plugin.
func New(cfg config.PluginConfig, host plugin.Host) (*Plugin, error) {
	settings := Settings{SessionFile: "telegram.session", RingSeconds: defaultRingSeconds, IdleSeconds: defaultIdleSeconds}
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
	if settings.IdleSeconds <= 0 || settings.IdleSeconds > maxIdleSeconds {
		return nil, fmt.Errorf("settings: idle_seconds must be in (0,%d]", maxIdleSeconds)
	}
	p := &Plugin{
		host:      host,
		settings:  settings,
		newMedia:  func(heard sink) (media, error) { return newLink(heard) },
		lookEvery: 3 * time.Second,
		ringFor:   time.Duration(settings.RingSeconds * float64(time.Second)),
		idleFor:   time.Duration(settings.IdleSeconds * float64(time.Second)),
		detail:    "starting",
	}
	p.connect = func(ready func(context.Context, telegramAPI, string) error) error {
		return connectTelegram(context.Background(), settings, nil, ready)
	}
	return p, nil
}

// Run adds the node and keeps the plugin logged in to Telegram until ctx ends.
// Without a login it waits for one to be made with -plugin-setup, which can
// happen while gocalis runs; the node is there meanwhile and tells whoever
// calls it why it cannot.
func (p *Plugin) Run(ctx context.Context) error {
	if p.host.AddNode == nil {
		return errors.New("no way to add the node")
	}
	remove, err := p.host.AddNode(p)
	if err != nil {
		return err
	}
	defer remove()

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
				p.endCall(errShutdown)
				return nil
			case <-online.Done():
				p.endCall(errOffline)
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
	if c := p.call; c != nil && !c.over() {
		if c.isAnswered() {
			status.Detail += "; in a call"
		} else {
			status.Detail += "; ringing"
		}
	}
	return status
}

// Command implements plugin.Plugin. The node is used with tts, play, ask and
// intercom; the one thing left to command is "hangup", which takes gocalis out
// of the voice chat at whatever point it is: ringing, or with somebody on the
// line.
func (p *Plugin) Command(_ context.Context, cmd plugin.Command) error {
	if cmd.Name != "hangup" {
		return fmt.Errorf("unknown command '%s' (telegram_call has: hangup)", cmd.Name)
	}
	if err := cmd.Decode(&struct{}{}); err != nil {
		return err
	}
	p.mu.Lock()
	c := p.call
	p.mu.Unlock()
	if c == nil || c.over() {
		return errors.New("no Telegram call in progress")
	}
	c.stop(errHangup)
	return nil
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
		telegram := api.(*client)
		if p.settings.Chat != "" {
			group, err := telegram.resolve(ctx, p.settings.Chat)
			if err == nil {
				fmt.Fprintf(out, "The chat %q is %s.\n", p.settings.Chat, group)
				return nil
			}
			fmt.Fprintf(out, "The chat %q cannot be used: %v\n", p.settings.Chat, err)
		}
		groups, err := telegram.groups(ctx)
		if err != nil {
			return fmt.Errorf("list the account's groups: %w", err)
		}
		if len(groups) == 0 {
			fmt.Fprintln(out, "The account is in no group yet.", errNotAGroup)
			return nil
		}
		fmt.Fprintln(out, "Groups the account is in (\"chat\" takes a title or an id):")
		for _, group := range groups {
			fmt.Fprintf(out, "  %s\n", group)
		}
		return nil
	})
}
