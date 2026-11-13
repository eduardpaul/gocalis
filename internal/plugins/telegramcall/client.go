package telegramcall

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/query"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"golang.org/x/term"
)

// client is the logged-in Telegram account: telegramAPI over MTProto.
type client struct {
	api    *tg.Client
	selfID int64
	nudge  chan struct{}

	mu    sync.Mutex
	chats map[string]*chat // resolved chats, by how they were asked for
}

// chat is a group resolved to what the API wants to address it.
type chat struct {
	title   string
	peer    tg.InputPeerClass
	channel *tg.InputChannel // set for supergroups and channels
	id      int64            // the basic group's id otherwise
	manages bool             // the account may start a voice chat there
}

func (c *chat) String() string {
	rights := "the account cannot start a voice chat there, only join one"
	if c.manages {
		rights = "the account can start voice chats there"
	}
	if c.channel != nil {
		return fmt.Sprintf("the supergroup %q (id %d; %s)", c.title, c.channel.ChannelID, rights)
	}
	return fmt.Sprintf("the group %q (id %d; %s)", c.title, c.id, rights)
}

// connectTelegram runs the account's connection: it logs in from the session
// file and calls ready once the account is usable. With ask, an account that
// is not logged in is logged in at the terminal first; without, that is
// errNotLoggedIn. The connection is closed when ready returns or ctx ends.
func connectTelegram(ctx context.Context, settings Settings, ask *prompter, ready func(ctx context.Context, api telegramAPI, account string) error) error {
	c := &client{nudge: make(chan struct{}, 1), chats: make(map[string]*chat)}
	poke := func() {
		select {
		case c.nudge <- struct{}{}:
		default:
		}
	}
	// Updates only hurry the next look at a voice chat; what is true is
	// always asked for, so a missed update costs a few seconds at most.
	updates := tg.NewUpdateDispatcher()
	updates.OnGroupCallParticipants(func(context.Context, tg.Entities, *tg.UpdateGroupCallParticipants) error {
		poke()
		return nil
	})
	updates.OnGroupCall(func(context.Context, tg.Entities, *tg.UpdateGroupCall) error {
		poke()
		return nil
	})

	conn := telegram.NewClient(settings.APIID, settings.APIHash, telegram.Options{
		SessionStorage: &session.FileStorage{Path: settings.SessionFile},
		UpdateHandler:  updates,
		Device: telegram.DeviceConfig{
			DeviceModel:   "gocalis",
			SystemVersion: "linux",
			AppVersion:    "1",
		},
	})
	return conn.Run(ctx, func(ctx context.Context) error {
		status, err := conn.Auth().Status(ctx)
		if err != nil {
			return fmt.Errorf("check the login: %w", err)
		}
		if !status.Authorized {
			if ask == nil {
				return errNotLoggedIn
			}
			flow := auth.NewFlow(loginPrompts{ask: ask, phone: settings.Phone}, auth.SendCodeOptions{})
			if err := conn.Auth().IfNecessary(ctx, flow); err != nil {
				return fmt.Errorf("log in: %w", err)
			}
			if status, err = conn.Auth().Status(ctx); err != nil || !status.Authorized {
				return fmt.Errorf("the login did not hold: %v", err)
			}
		}
		c.api = conn.API()
		c.selfID = status.User.ID
		// Telegram sends updates to a session once it has asked for their
		// state.
		if _, err := c.api.UpdatesGetState(ctx); err != nil {
			log.Printf("[telegram_call] updates.getState: %v\n", err)
		}
		return ready(ctx, c, userName(status.User))
	})
}

func (c *client) changes() <-chan struct{} { return c.nudge }

// open implements telegramAPI.
func (c *client) open(ctx context.Context, name, title string) (voiceChat, bool, error) {
	group, err := c.resolve(ctx, name)
	if err != nil {
		return voiceChat{}, false, err
	}
	vc, err := c.current(ctx, group)
	if err != nil {
		return voiceChat{}, false, err
	}
	if vc != nil {
		return *vc, false, nil
	}

	result, err := c.api.PhoneCreateGroupCall(ctx, &tg.PhoneCreateGroupCallRequest{
		Peer:     group.peer,
		RandomID: int(rand.Int32()),
		Title:    title,
	})
	if err != nil {
		if tgerr.Is(err, "CHAT_ADMIN_REQUIRED") {
			return voiceChat{}, false, fmt.Errorf("the account may not start a voice chat in %q: make it an admin that can manage voice chats", group.title)
		}
		return voiceChat{}, false, fmt.Errorf("start the voice chat: %w", err)
	}
	for _, update := range updatesOf(result) {
		if u, ok := update.(*tg.UpdateGroupCall); ok {
			if call, ok := u.Call.(*tg.GroupCall); ok {
				return voiceChat{ID: call.ID, AccessHash: call.AccessHash, Chat: group.title}, true, nil
			}
		}
	}
	// The answer did not carry it: the group does by now.
	if vc, err = c.current(ctx, group); err != nil {
		return voiceChat{}, true, err
	}
	if vc == nil {
		return voiceChat{}, true, errors.New("the voice chat was started but the group does not show it")
	}
	return *vc, true, nil
}

// current is the group's voice chat, or nil when it has none.
func (c *client) current(ctx context.Context, group *chat) (*voiceChat, error) {
	var (
		full *tg.MessagesChatFull
		err  error
	)
	if group.channel != nil {
		full, err = c.api.ChannelsGetFullChannel(ctx, group.channel)
	} else {
		full, err = c.api.MessagesGetFullChat(ctx, group.id)
	}
	if err != nil {
		return nil, fmt.Errorf("read the group %q: %w", group.title, err)
	}
	holder, ok := full.FullChat.(interface {
		GetCall() (tg.InputGroupCallClass, bool)
	})
	if !ok {
		return nil, fmt.Errorf("the group %q has an unexpected form (%T)", group.title, full.FullChat)
	}
	call, has := holder.GetCall()
	if !has {
		return nil, nil
	}
	input, ok := call.(*tg.InputGroupCall)
	if !ok {
		return nil, fmt.Errorf("the voice chat of %q has an unexpected form (%T)", group.title, call)
	}
	return &voiceChat{ID: input.ID, AccessHash: input.AccessHash, Chat: group.title}, nil
}

// A join the media server refuses for no reason of ours is tried this many
// times in all, a little longer apart each time.
const (
	joinAttempts   = 5
	joinRetryDelay = 500 * time.Millisecond
)

// join implements telegramAPI.
func (c *client) join(ctx context.Context, vc voiceChat, payload string) (string, error) {
	var (
		result tg.UpdatesClass
		err    error
	)
	for attempt := 1; ; attempt++ {
		result, err = c.api.PhoneJoinGroupCall(ctx, &tg.PhoneJoinGroupCallRequest{
			Call:         vc.input(),
			JoinAs:       &tg.InputPeerSelf{},
			VideoStopped: true, // audio only
			Params:       tg.DataJSON{Data: payload},
		})
		// A voice chat started a moment ago can refuse its first participant
		// with this (a 500); the same request is taken shortly after.
		if !tgerr.Is(err, "GROUPCALL_ADD_PARTICIPANTS_FAILED") || attempt == joinAttempts {
			break
		}
		log.Printf("[telegram_call] The voice chat of %q is not taking participants yet, trying again (%d/%d)\n", vc.Chat, attempt, joinAttempts-1)
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(time.Duration(attempt) * joinRetryDelay):
		}
	}
	if err != nil {
		return "", closedOr(err)
	}
	params := ""
	for _, update := range updatesOf(result) {
		if u, ok := update.(*tg.UpdateGroupCallConnection); ok {
			params = u.Params.Data
		}
	}
	if params == "" {
		return "", errors.New("Telegram did not return the connection for the voice chat")
	}

	// A voice chat can bring newcomers in muted. Speak up; where that takes
	// rights the account lacks, say so, because nobody would hear the node.
	unmute := &tg.PhoneEditGroupCallParticipantRequest{Call: vc.input(), Participant: &tg.InputPeerSelf{}}
	unmute.SetMuted(false)
	if _, err := c.api.PhoneEditGroupCallParticipant(ctx, unmute); err != nil && !tgerr.Is(err, "PARTICIPANT_NOT_MODIFIED", "GROUPCALL_NOT_MODIFIED") {
		log.Printf("[telegram_call] Could not unmute in the voice chat of %q (%v): if nobody hears the node, let members speak or make the account an admin\n", vc.Chat, err)
	}
	return params, nil
}

// present implements telegramAPI.
func (c *client) present(ctx context.Context, vc voiceChat, source uint32) ([]person, bool, error) {
	signed := int(int32(source))
	sources, err := c.api.PhoneCheckGroupCall(ctx, &tg.PhoneCheckGroupCallRequest{Call: vc.input(), Sources: []int{signed}})
	if err != nil {
		return nil, false, closedOr(err)
	}
	in := false
	for _, s := range sources {
		in = in || s == signed
	}

	result, err := c.api.PhoneGetGroupParticipants(ctx, &tg.PhoneGetGroupParticipantsRequest{
		Call:    vc.input(),
		IDs:     []tg.InputPeerClass{},
		Sources: []int{},
		Limit:   100,
	})
	if err != nil {
		return nil, false, closedOr(err)
	}
	names := make(map[int64]string)
	for _, u := range result.Users {
		if user, ok := u.(*tg.User); ok {
			names[user.ID] = userName(user)
		}
	}
	for _, ch := range result.Chats {
		switch ch := ch.(type) {
		case *tg.Channel:
			names[ch.ID] = ch.Title
		case *tg.Chat:
			names[ch.ID] = ch.Title
		}
	}
	var others []person
	for _, p := range result.Participants {
		id := peerID(p.Peer)
		if p.Left || p.Self || id == c.selfID || p.Source == signed {
			continue
		}
		name := names[id]
		if name == "" {
			name = strconv.FormatInt(id, 10)
		}
		others = append(others, person{ID: id, Name: name})
	}
	return others, in, nil
}

// leave implements telegramAPI.
func (c *client) leave(ctx context.Context, vc voiceChat, source uint32) error {
	_, err := c.api.PhoneLeaveGroupCall(ctx, &tg.PhoneLeaveGroupCallRequest{Call: vc.input(), Source: int(int32(source))})
	return closedOr(err)
}

// end implements telegramAPI.
func (c *client) end(ctx context.Context, vc voiceChat) error {
	_, err := c.api.PhoneDiscardGroupCall(ctx, vc.input())
	return closedOr(err)
}

// errNotAGroup is a chat setting that names something without voice chats.
var errNotAGroup = errors.New("a voice chat needs a group: create one with the account and the people who answer the door in it, and name that group")

// resolve finds the group a name stands for: "@username", a numeric id (as
// Telegram clients or bots show it, with or without the -100 or - in front),
// or the exact title, capitals aside.
func (c *client) resolve(ctx context.Context, name string) (*chat, error) {
	name = strings.TrimSpace(name)
	c.mu.Lock()
	known := c.chats[name]
	c.mu.Unlock()
	if known != nil {
		return known, nil
	}

	var found *chat
	if username, ok := strings.CutPrefix(name, "@"); ok {
		resolved, err := c.api.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: username})
		if err != nil {
			return nil, fmt.Errorf("find the chat %s: %w", name, err)
		}
		if _, person := resolved.Peer.(*tg.PeerUser); person {
			return nil, fmt.Errorf("%s is a person: %w", name, errNotAGroup)
		}
		for _, candidate := range resolved.Chats {
			if group := asChat(candidate); group.matches(peerID(resolved.Peer), "") {
				found = group
			}
		}
		if found == nil {
			return nil, fmt.Errorf("the account is not in the group %s", name)
		}
	} else {
		var id int64
		if _, err := strconv.ParseInt(name, 10, 64); err == nil {
			digits := strings.TrimPrefix(name, "-")
			if len(digits) > 10 && strings.HasPrefix(digits, "100") {
				digits = digits[3:]
			}
			id, _ = strconv.ParseInt(digits, 10, 64)
		}
		groups, err := c.groups(ctx)
		if err != nil {
			return nil, fmt.Errorf("look for the chat %q: %w", name, err)
		}
		for _, group := range groups {
			if found == nil && group.matches(id, name) {
				found = group
			}
		}
		if found == nil {
			return nil, fmt.Errorf("no group %q among the account's chats", name)
		}
	}
	c.mu.Lock()
	c.chats[name] = found
	c.mu.Unlock()
	return found, nil
}

// groups lists the groups the account is in, the most recently used first.
func (c *client) groups(ctx context.Context) ([]*chat, error) {
	var groups []*chat
	dialogs := query.GetDialogs(c.api).BatchSize(100).Iter()
	for dialogs.Next(ctx) {
		entities := dialogs.Value().Entities
		var group *chat
		switch peer := dialogs.Value().Peer.(type) {
		case *tg.InputPeerChannel:
			if channel := entities.Channels()[peer.ChannelID]; channel != nil {
				group = asChat(channel)
			}
		case *tg.InputPeerChat:
			if basic := entities.Chats()[peer.ChatID]; basic != nil {
				group = asChat(basic)
			}
		}
		if group != nil {
			groups = append(groups, group)
		}
	}
	return groups, dialogs.Err()
}

// matches tells whether the group has that id or, with no id, that title.
func (c *chat) matches(id int64, title string) bool {
	if c == nil {
		return false
	}
	if id != 0 {
		return (c.channel != nil && c.channel.ChannelID == id) || (c.channel == nil && c.id == id)
	}
	return title != "" && strings.EqualFold(c.title, title)
}

// asChat is the group in an API chat object, or nil for what is not a group
// the account is in.
func asChat(candidate tg.ChatClass) *chat {
	switch ch := candidate.(type) {
	case *tg.Channel:
		if ch == nil || ch.Left {
			return nil
		}
		return &chat{
			title:   ch.Title,
			peer:    &tg.InputPeerChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
			channel: &tg.InputChannel{ChannelID: ch.ID, AccessHash: ch.AccessHash},
			manages: ch.Creator || ch.AdminRights.ManageCall,
		}
	case *tg.Chat:
		if ch == nil || ch.Left || ch.Deactivated {
			return nil
		}
		return &chat{title: ch.Title, peer: &tg.InputPeerChat{ChatID: ch.ID}, id: ch.ID, manages: ch.Creator || ch.AdminRights.ManageCall}
	}
	return nil
}

func (vc voiceChat) input() *tg.InputGroupCall {
	return &tg.InputGroupCall{ID: vc.ID, AccessHash: vc.AccessHash}
}

// closedOr turns the errors Telegram gives for a voice chat that is over into
// errVoiceChatClosed.
func closedOr(err error) error {
	if tgerr.Is(err, "GROUPCALL_INVALID", "GROUPCALL_FORBIDDEN", "GROUPCALL_ALREADY_DISCARDED") {
		return errVoiceChatClosed
	}
	return err
}

func updatesOf(result tg.UpdatesClass) []tg.UpdateClass {
	switch u := result.(type) {
	case *tg.Updates:
		return u.Updates
	case *tg.UpdatesCombined:
		return u.Updates
	}
	return nil
}

func peerID(peer tg.PeerClass) int64 {
	switch p := peer.(type) {
	case *tg.PeerUser:
		return p.UserID
	case *tg.PeerChannel:
		return p.ChannelID
	case *tg.PeerChat:
		return p.ChatID
	}
	return 0
}

func userName(user *tg.User) string {
	if user == nil {
		return "?"
	}
	if name := strings.TrimSpace(user.FirstName + " " + user.LastName); name != "" {
		return name
	}
	if user.Username != "" {
		return "@" + user.Username
	}
	return strconv.FormatInt(user.ID, 10)
}

// prompter asks a person at the terminal.
type prompter struct {
	in  *bufio.Reader
	raw io.Reader
	out io.Writer
}

func newPrompter(in io.Reader, out io.Writer) *prompter {
	return &prompter{in: bufio.NewReader(in), raw: in, out: out}
}

func (p *prompter) line(question string) (string, error) {
	fmt.Fprint(p.out, question)
	text, err := p.in.ReadString('\n')
	if err != nil && text == "" {
		return "", err
	}
	return strings.TrimSpace(text), nil
}

// secret asks without showing what is typed, when the input is a terminal.
func (p *prompter) secret(question string) (string, error) {
	if file, ok := p.raw.(*os.File); ok && term.IsTerminal(int(file.Fd())) {
		fmt.Fprint(p.out, question)
		text, err := term.ReadPassword(int(file.Fd()))
		fmt.Fprintln(p.out)
		return strings.TrimSpace(string(text)), err
	}
	return p.line(question)
}

// loginPrompts answers Telegram's login with what the person types.
type loginPrompts struct {
	ask   *prompter
	phone string
}

func (l loginPrompts) Phone(context.Context) (string, error) {
	if l.phone != "" {
		return l.phone, nil
	}
	return l.ask.line("Phone number of the account (international form, +34...): ")
}

func (l loginPrompts) Code(context.Context, *tg.AuthSentCode) (string, error) {
	return l.ask.line("Code Telegram just sent to the account: ")
}

func (l loginPrompts) Password(context.Context) (string, error) {
	return l.ask.secret("Two-step verification password: ")
}

func (l loginPrompts) AcceptTermsOfService(context.Context, tg.HelpTermsOfService) error {
	return errors.New("the account has terms of service to accept: log in once with a Telegram app")
}

func (l loginPrompts) SignUp(context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, errors.New("the number has no Telegram account: create it with a Telegram app first")
}
