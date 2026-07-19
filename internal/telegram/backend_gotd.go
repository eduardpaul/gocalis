package telegram

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"github.com/gotd/log/logzap"

	"gocalis/internal/config"

	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth"
	"github.com/gotd/td/telegram/calls"
	"github.com/gotd/td/telegram/peers"
	"github.com/gotd/td/session"
	"github.com/gotd/td/tg"
	"github.com/pion/rtp"
	"github.com/pion/webrtc/v4"
	opus "gopkg.in/hraban/opus.v2"
	"golang.org/x/term"
)

type gotdManager struct {
	cfg        config.TelegramConfig
	client     *telegram.Client
	api        *tg.Client
	peers      *peers.Manager
	calls      *calls.Client
	dispatcher tg.UpdateDispatcher

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	callsMap map[string]*gotdCall
	selfUserID int64
}

type resolvedPeer struct {
	user    tg.InputUserClass
	channel tg.InputChannelClass
	chat    tg.InputPeerClass
	isUser  bool
	isChan  bool
	isChat  bool
	userID  int64
	chatID  int64
}

// NewManager creates a concrete gotd-based session Manager.
func NewManager(cfg config.TelegramConfig) (Manager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	dispatcher := tg.NewUpdateDispatcher()

	opts := telegram.Options{
		SessionStorage: &session.FileStorage{Path: cfg.GetSessionFile()},
		UpdateHandler:  dispatcher,
	}

	client := telegram.NewClient(cfg.APIID, cfg.APIHash, opts)
	api := tg.NewClient(client)
	peersMgr := peers.Options{}.Build(api)

	callsClient := calls.NewClient(api, calls.Options{})
	callsClient.Register(dispatcher)

	mgr := &gotdManager{
		cfg:        cfg,
		client:     client,
		api:        api,
		peers:      peersMgr,
		calls:      callsClient,
		dispatcher: dispatcher,
		ctx:        ctx,
		cancel:     cancel,
		callsMap:   make(map[string]*gotdCall),
	}

	// Register incoming call handler
	callsClient.OnIncoming(mgr.handleIncomingCall)

	readyCh := make(chan error, 1)
	mgr.wg.Add(1)
	go func() {
		defer mgr.wg.Done()
		err := client.Run(ctx, func(ctx context.Context) error {
			flow := auth.NewFlow(TerminalAuth{PhoneStr: cfg.Phone}, auth.SendCodeOptions{})
			if err := client.Auth().IfNecessary(ctx, flow); err != nil {
				readyCh <- fmt.Errorf("auth flow: %w", err)
				return err
			}

			if err := peersMgr.Init(ctx); err != nil {
				log.Printf("[Telegram] Warning: peers manager init failed: %v", err)
			}

			self, err := client.Self(ctx)
			if err != nil {
				log.Printf("[Telegram] Warning: failed to fetch self user info: %v", err)
			} else {
				mgr.selfUserID = self.ID
				log.Printf("[Telegram] Authenticated successfully as %s (ID: %d)", self.Username, self.ID)
			}

			readyCh <- nil // authenticated and ready
			<-ctx.Done()
			return nil
		})
		if err != nil && ctx.Err() == nil {
			log.Printf("[Telegram] Client run error: %v", err)
			select {
			case readyCh <- err:
			default:
			}
		}
	}()

	select {
	case err := <-readyCh:
		if err != nil {
			cancel()
			return nil, err
		}
	case <-time.After(3 * time.Minute):
		cancel()
		return nil, fmt.Errorf("timeout waiting for telegram connection/authentication")
	}

	return mgr, nil
}

func (m *gotdManager) NewCall(target Target) (Call, error) {
	// Strip optional @ prefix for Resolve
	peerStr := target.Peer
	if len(peerStr) > 0 && peerStr[0] == '@' {
		peerStr = peerStr[1:]
	}

	var rp *resolvedPeer
	peer, err := m.peers.Resolve(m.ctx, peerStr)
	if err == nil {
		rp = &resolvedPeer{}
		switch p := peer.(type) {
		case peers.User:
			rp.isUser = true
			rp.user = p.InputUser()
			rp.userID = p.ID()
		case peers.Channel:
			rp.isChan = true
			rp.channel = p.InputChannel()
			rp.chatID = p.ID()
		case peers.Chat:
			rp.isChat = true
			rp.chat = p.InputPeer()
			rp.chatID = p.ID()
		default:
			return nil, fmt.Errorf("unsupported peer type: %T", peer)
		}
	} else {
		log.Printf("[Telegram] peers.Resolve failed for %q (%v), falling back to dialogs search...", target.Peer, err)
		dialogsRes, derr := m.api.MessagesGetDialogs(m.ctx, &tg.MessagesGetDialogsRequest{
			Limit:      100,
			OffsetPeer: &tg.InputPeerEmpty{},
		})
		if derr != nil {
			return nil, fmt.Errorf("failed to resolve username %q and dialogs fetch failed: %v (original error: %w)", target.Peer, derr, err)
		}

		var chats []tg.ChatClass
		var users []tg.UserClass
		switch d := dialogsRes.(type) {
		case *tg.MessagesDialogs:
			chats = d.Chats
			users = d.Users
		case *tg.MessagesDialogsSlice:
			chats = d.Chats
			users = d.Users
		}

		var matchedPeer tg.InputPeerClass
		var matchedUser tg.InputUserClass
		isChan, isChat, isUser := false, false, false
		var matchedID int64

		for _, c := range chats {
			switch chat := c.(type) {
			case *tg.Chat:
				if strings.EqualFold(chat.Title, peerStr) {
					matchedPeer = &tg.InputPeerChat{ChatID: chat.ID}
					isChat = true
					matchedID = chat.ID
					break
				}
			case *tg.Channel:
				if strings.EqualFold(chat.Title, peerStr) || strings.EqualFold(chat.Username, peerStr) {
					matchedPeer = &tg.InputPeerChannel{
						ChannelID:  chat.ID,
						AccessHash: chat.AccessHash,
					}
					isChan = true
					matchedID = chat.ID
					break
				}
			}
			if matchedPeer != nil {
				break
			}
		}

		if matchedPeer == nil {
			for _, u := range users {
				if usr, ok := u.(*tg.User); ok {
					if strings.EqualFold(usr.Username, peerStr) || strings.EqualFold(usr.FirstName+" "+usr.LastName, peerStr) || strings.EqualFold(usr.FirstName, peerStr) {
						matchedUser = &tg.InputUser{
							UserID:     usr.ID,
							AccessHash: usr.AccessHash,
						}
						isUser = true
						matchedID = usr.ID
						break
					}
				}
			}
		}

		if matchedPeer == nil && matchedUser == nil {
			return nil, fmt.Errorf("failed to resolve target %q: target not found by username or in recent dialogs (resolve error: %w)", target.Peer, err)
		}

		rp = &resolvedPeer{}
		if isChan {
			rp.isChan = true
			rp.channel = &tg.InputChannel{
				ChannelID:  matchedPeer.(*tg.InputPeerChannel).ChannelID,
				AccessHash: matchedPeer.(*tg.InputPeerChannel).AccessHash,
			}
			rp.chatID = matchedID
		} else if isChat {
			rp.isChat = true
			rp.chat = matchedPeer
			rp.chatID = matchedID
		} else if isUser {
			rp.isUser = true
			rp.user = matchedUser
			rp.userID = matchedID
		}
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	call := &gotdCall{
		mgr:        m,
		target:     target,
		resolved:   rp,
		peerJoined: make(chan struct{}),
	}

	m.callsMap[target.Peer] = call

	return call, nil
}

func (m *gotdManager) Close() error {
	m.cancel()
	m.mu.Lock()
	for _, call := range m.callsMap {
		_ = call.Leave()
	}
	m.mu.Unlock()
	m.wg.Wait()
	return nil
}

func (m *gotdManager) handleIncomingCall(in *calls.IncomingCall) {
	log.Printf("[Telegram] Received incoming 1:1 call from UserID: %d", in.UserID())

	m.mu.Lock()
	var matchingCall *gotdCall
	for _, call := range m.callsMap {
		if call.target.Type == "contact" && call.resolved != nil && call.resolved.userID == in.UserID() {
			matchingCall = call
			break
		}
	}
	m.mu.Unlock()

	if matchingCall == nil {
		log.Printf("[Telegram] Rejecting incoming call from unauthorized user %d", in.UserID())
		ctx, cancel := context.WithTimeout(m.ctx, 10*time.Second)
		defer cancel()
		_ = in.Reject(ctx)
		return
	}

	log.Printf("[Telegram] Auto-answering call from user %s...", matchingCall.target.Peer)
	go func() {
		ctx, cancel := context.WithTimeout(m.ctx, 30*time.Second)
		defer cancel()

		conn, err := in.Accept(ctx)
		if err != nil {
			log.Printf("[Telegram] Failed to accept incoming call: %v", err)
			return
		}

		matchingCall.mu.Lock()
		matchingCall.conn = conn
		select {
		case <-matchingCall.peerJoined:
		default:
			close(matchingCall.peerJoined)
		}

		matchingCall.mixer = NewTrackMixer(matchingCall.handleMixedFrame)
		matchingCall.mixer.Start(matchingCall.mgr.ctx)

		conn.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
			log.Printf("[Telegram] Received incoming audio track for accepted call")
			matchingCall.mixer.AddTrack(track)
			go matchingCall.readRTPTrack(matchingCall.mgr.ctx, track)
		})

		matchingCall.ssrc = conn.AudioSSRC()
		enc, _ := opus.NewEncoder(48000, 1, opus.AppVoIP)
		matchingCall.enc = enc
		matchingCall.mu.Unlock()

		log.Printf("[Telegram] Incoming call connected successfully")
	}()
}

// gotdCall implements Call.
type gotdCall struct {
	mgr      *gotdManager
	target   Target
	resolved *resolvedPeer

	mu      sync.Mutex
	conn    *calls.Conn
	grpCall *calls.GroupCall

	onFrame func([]int16)
	mixer   *TrackMixer

	peerJoined chan struct{}

	enc *opus.Encoder
	writeCount uint32
	channels   int
	lastWriteTime time.Time

	seqNum    uint32
	timestamp uint32
	ssrc      uint32
}

func (c *gotdCall) Join(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil || c.grpCall != nil {
		return nil
	}
	c.peerJoined = make(chan struct{})

	if c.target.Type == "contact" {
		log.Printf("[Telegram] Placing 1:1 call to %s...", c.target.Peer)
		conn, err := c.mgr.calls.Request(ctx, c.resolved.user)
		if err != nil {
			return err
		}
		c.conn = conn

		c.mixer = NewTrackMixer(c.handleMixedFrame)
		c.mixer.Start(c.mgr.ctx)

		conn.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
			log.Printf("[Telegram] Received incoming audio track for 1:1 call")
			c.mixer.AddTrack(track)
			go c.readRTPTrack(c.mgr.ctx, track)
		})

		conn.OnConnected(func() {
			log.Printf("[Telegram] 1:1 call connected")
			select {
			case <-c.peerJoined:
			default:
				close(c.peerJoined)
			}
		})

		c.ssrc = conn.AudioSSRC()

		c.channels = 1
		c.seqNum = uint32(rand.Int31())
		c.timestamp = uint32(rand.Int31())
		enc, err := opus.NewEncoder(48000, 1, opus.AppVoIP)
		if err != nil {
			return fmt.Errorf("create opus encoder: %w", err)
		}
		c.enc = enc

	} else if c.target.Type == "group" {
		log.Printf("[Telegram] Joining group call for %s...", c.target.Peer)

		var inputCall tg.InputGroupCallClass

		if c.resolved.isChan {
			full, err := c.mgr.api.ChannelsGetFullChannel(ctx, c.resolved.channel)
			if err != nil {
				return fmt.Errorf("get full channel: %w", err)
			}
			cf, ok := full.FullChat.(*tg.ChannelFull)
			if !ok {
				return fmt.Errorf("unexpected full chat type: %T", full.FullChat)
			}
			inputCall, _ = cf.GetCall()
		} else if c.resolved.isChat {
			full, err := c.mgr.api.MessagesGetFullChat(ctx, c.resolved.chatID)
			if err != nil {
				return fmt.Errorf("get full chat: %w", err)
			}
			cf, ok := full.FullChat.(*tg.ChatFull)
			if !ok {
				return fmt.Errorf("unexpected full chat type: %T", full.FullChat)
			}
			inputCall, _ = cf.GetCall()
		}

		if inputCall == nil {
			log.Printf("[Telegram] Group call is not active. Creating one...")
			var peer tg.InputPeerClass
			if c.resolved.isChan {
				peer = &tg.InputPeerChannel{
					ChannelID:  c.resolved.channel.(*tg.InputChannel).ChannelID,
					AccessHash: c.resolved.channel.(*tg.InputChannel).AccessHash,
				}
			} else {
				peer = c.resolved.chat
			}

			_, err := c.mgr.api.PhoneCreateGroupCall(ctx, &tg.PhoneCreateGroupCallRequest{
				Peer:     peer,
				RandomID: rand.Int(),
			})
			if err != nil {
				return fmt.Errorf("create group call: %w", err)
			}

			log.Printf("[Telegram] Group call created successfully. Waiting 3 seconds for propagation...")
			time.Sleep(3 * time.Second)

			if c.resolved.isChan {
				full, err := c.mgr.api.ChannelsGetFullChannel(ctx, c.resolved.channel)
				if err != nil {
					return fmt.Errorf("re-get full channel: %w", err)
				}
				cf := full.FullChat.(*tg.ChannelFull)
				inputCall, _ = cf.GetCall()
			} else {
				full, err := c.mgr.api.MessagesGetFullChat(ctx, c.resolved.chatID)
				if err != nil {
					return fmt.Errorf("re-get full chat: %w", err)
				}
				cf := full.FullChat.(*tg.ChatFull)
				inputCall, _ = cf.GetCall()
			}
		}

		if inputCall == nil {
			return fmt.Errorf("failed to obtain group call info for %s", c.target.Peer)
		}

		igc, ok := inputCall.(*tg.InputGroupCall)
		if !ok {
			return fmt.Errorf("unsupported input group call type: %T", inputCall)
		}

		zapLog, _ := zap.NewDevelopment()
		grpCall := calls.NewGroupCall(c.mgr.api, calls.Options{Logger: logzap.New(zapLog)})
		grpCall.Register(c.mgr.dispatcher)

		grpCall.OnConnected(func() {
			log.Printf("[Telegram] Group call WebRTC connection established!")
		})
		grpCall.OnDisconnected(func() {
			log.Printf("[Telegram] Group call WebRTC connection disconnected/failed!")
		})

		var joinAs tg.InputPeerClass = &tg.InputPeerSelf{}

		var err error
		for attempt := 1; attempt <= 3; attempt++ {
			err = grpCall.Join(ctx, igc, joinAs)
			if err == nil {
				break
			}
			log.Printf("[Telegram] Join group call attempt %d failed: %v. Retrying in 3 seconds...", attempt, err)
			time.Sleep(3 * time.Second)
		}
		if err != nil {
			return fmt.Errorf("join group call: %w", err)
		}

		unmuteReq := &tg.PhoneEditGroupCallParticipantRequest{
			Call:        igc,
			Participant: &tg.InputPeerSelf{},
		}
		unmuteReq.SetMuted(false)
		_, err = c.mgr.api.PhoneEditGroupCallParticipant(ctx, unmuteReq)
		if err != nil {
			log.Printf("[Telegram] Warning: failed to explicitly unmute self in group call: %v", err)
		} else {
			log.Printf("[Telegram] Explicitly unmuted self in group call")
		}

		c.grpCall = grpCall

		c.mixer = NewTrackMixer(c.handleMixedFrame)
		c.mixer.Start(c.mgr.ctx)

		grpCall.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
			log.Printf("[Telegram] Received incoming audio track for group call")
			c.mixer.AddTrack(track)
			go c.readRTPTrack(c.mgr.ctx, track)
		})

		checkActivePeers := func(participants []tg.GroupCallParticipant) bool {
			for _, p := range participants {
				isSelf := p.Self
				if !isSelf {
					if userPeer, ok := p.Peer.(*tg.PeerUser); ok {
						isSelf = userPeer.UserID == c.mgr.selfUserID
					}
				}
				if !isSelf && !p.Left {
					return true
				}
			}
			return false
		}

		// Fetch initial participant list to check if someone is already in the call
		gpRes, gpErr := c.mgr.api.PhoneGetGroupParticipants(ctx, &tg.PhoneGetGroupParticipantsRequest{
			Call:  igc,
			Limit: 100,
		})
		if gpErr != nil {
			log.Printf("[Telegram] Warning: failed to fetch initial group call participants: %v", gpErr)
		} else {
			log.Printf("[Telegram] Fetched %d initial participants from group call", len(gpRes.Participants))
			if checkActivePeers(gpRes.Participants) {
				log.Printf("[Telegram] Active peer already present in group call on startup")
				select {
				case <-c.peerJoined:
				default:
					close(c.peerJoined)
				}
			}
		}

		grpCall.OnParticipants(func(participants []tg.GroupCallParticipant) {
			if checkActivePeers(participants) {
				select {
				case <-c.peerJoined:
				default:
					close(c.peerJoined)
				}
			}
		})

		c.ssrc = grpCall.AudioSSRC()

		c.channels = 2
		c.seqNum = uint32(rand.Int31())
		c.timestamp = uint32(rand.Int31())
		enc, err := opus.NewEncoder(48000, 2, opus.AppVoIP)
		if err != nil {
			return fmt.Errorf("create opus encoder: %w", err)
		}
		c.enc = enc
	}
	return nil
}

func (c *gotdCall) WaitPeer(ctx context.Context) error {
	c.mu.Lock()
	wait := c.peerJoined
	c.mu.Unlock()

	if wait == nil {
		return fmt.Errorf("call not joined yet")
	}

	select {
	case <-wait:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *gotdCall) Write(pcm48 []int16) error {
	c.mu.Lock()
	enc := c.enc
	conn := c.conn
	grpCall := c.grpCall
	ssrc := c.ssrc
	channels := c.channels

	now := time.Now()
	isNewTalkspurt := c.lastWriteTime.IsZero() || now.Sub(c.lastWriteTime) > 200*time.Millisecond
	c.lastWriteTime = now

	c.writeCount++
	writeCount := c.writeCount
	c.seqNum++
	c.timestamp += uint32(len(pcm48))
	seqNum := c.seqNum
	timestamp := c.timestamp
	c.mu.Unlock()

	if enc == nil {
		return fmt.Errorf("opus encoder not initialized")
	}

	if ssrc == 0 && grpCall != nil {
		ssrc = grpCall.AudioSSRC()
		c.mu.Lock()
		c.ssrc = ssrc
		c.mu.Unlock()
	}

	nonZero := false
	for _, v := range pcm48 {
		if v != 0 {
			nonZero = true
			break
		}
	}

	var encodeBuf []int16
	if channels == 2 {
		encodeBuf = make([]int16, len(pcm48)*2)
		for i, v := range pcm48 {
			encodeBuf[i*2] = v
			encodeBuf[i*2+1] = v
		}
	} else {
		encodeBuf = pcm48
	}

	opusBuf := make([]byte, 4000)
	n, err := enc.Encode(encodeBuf, opusBuf)
	if err != nil {
		return fmt.Errorf("opus encode error: %w", err)
	}

	if writeCount%100 == 1 {
		log.Printf("[Telegram] Audio write stats: total_frames=%d, last_frame_size=%d, last_encoded_size=%d, non_silent=%v, ssrc=%d, marker=%v", writeCount, len(pcm48), n, nonZero, ssrc, isNewTalkspurt)
	}

	pkt := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			Marker:         isNewTalkspurt,
			PayloadType:    111,
			SequenceNumber: uint16(seqNum),
			Timestamp:      timestamp,
			SSRC:           ssrc,
		},
		Payload: opusBuf[:n],
	}

	if grpCall != nil {
		err = grpCall.WriteAudio(pkt)
		if err != nil {
			log.Printf("[Telegram] grpCall.WriteAudio error: %v", err)
		}
		return err
	} else if conn != nil {
		track := conn.AudioTrack()
		if track == nil {
			return fmt.Errorf("1:1 call audio track is nil")
		}
		err = track.WriteRTP(pkt)
		if err != nil {
			log.Printf("[Telegram] 1:1 write error: %v", err)
		}
		return err
	}

	return fmt.Errorf("call is not joined")
}

func (c *gotdCall) OnFrame(fn func(pcm48 []int16)) {
	c.mu.Lock()
	c.onFrame = fn
	c.mu.Unlock()
}

func (c *gotdCall) Leave() error {
	c.mu.Lock()
	conn := c.conn
	grpCall := c.grpCall
	c.conn = nil
	c.grpCall = nil
	c.mu.Unlock()

	var err error
	if grpCall != nil {
		log.Printf("[Telegram] Leaving group call for %s...", c.target.Peer)
		err = grpCall.Leave(context.Background())
	}
	if conn != nil {
		log.Printf("[Telegram] Leaving 1:1 call...")
		err = conn.Close()
	}
	return err
}

func (c *gotdCall) handleMixedFrame(pcm48 []int16) {
	c.mu.Lock()
	cb := c.onFrame
	c.mu.Unlock()
	if cb != nil {
		cb(pcm48)
	}
}

func (c *gotdCall) readRTPTrack(ctx context.Context, track *webrtc.TrackRemote) {
	defer c.mixer.RemoveTrack(track)

	dec, err := opus.NewDecoder(48000, 1)
	if err != nil {
		log.Printf("[Telegram] Failed to create opus decoder: %v", err)
		return
	}

	pcm := make([]int16, 960)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rtpPkt, _, err := track.ReadRTP()
		if err != nil {
			return
		}

		n, err := dec.Decode(rtpPkt.Payload, pcm)
		if err != nil {
			continue
		}

		c.mixer.Push(track, pcm[:n])
	}
}

// TrackMixer mixes multiple WebRTC remote tracks in real-time.
type TrackMixer struct {
	mu      sync.Mutex
	tracks  map[*webrtc.TrackRemote]*trackBuffer
	onFrame func([]int16)
	active  bool
}

type trackBuffer struct {
	samples []int16
}

func NewTrackMixer(onFrame func([]int16)) *TrackMixer {
	return &TrackMixer{
		tracks:  make(map[*webrtc.TrackRemote]*trackBuffer),
		onFrame: onFrame,
	}
}

func (tm *TrackMixer) AddTrack(track *webrtc.TrackRemote) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tm.tracks[track] = &trackBuffer{}
}

func (tm *TrackMixer) RemoveTrack(track *webrtc.TrackRemote) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	delete(tm.tracks, track)
}

func (tm *TrackMixer) Push(track *webrtc.TrackRemote, samples []int16) {
	tm.mu.Lock()
	defer tm.mu.Unlock()
	tb, ok := tm.tracks[track]
	if !ok {
		return
	}
	tb.samples = append(tb.samples, samples...)
}

func (tm *TrackMixer) Start(ctx context.Context) {
	tm.mu.Lock()
	if tm.active {
		tm.mu.Unlock()
		return
	}
	tm.active = true
	tm.mu.Unlock()

	go func() {
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				tm.tick()
			}
		}
	}()
}

func (tm *TrackMixer) tick() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if len(tm.tracks) == 0 {
		return
	}

	const size = 960
	mixed := make([]int32, size)
	hasAudio := false

	for _, tb := range tm.tracks {
		n := len(tb.samples)
		if n == 0 {
			continue
		}
		if n > size {
			n = size
		}
		for i := 0; i < n; i++ {
			mixed[i] += int32(tb.samples[i])
		}
		tb.samples = tb.samples[n:]
		hasAudio = true
	}

	if !hasAudio {
		return
	}

	out := make([]int16, size)
	for i := 0; i < size; i++ {
		val := mixed[i]
		if val > 32767 {
			val = 32767
		} else if val < -32768 {
			val = -32768
		}
		out[i] = int16(val)
	}

	if tm.onFrame != nil {
		tm.onFrame(out)
	}
}

// TerminalAuth implements auth.UserAuthenticator.
type TerminalAuth struct {
	PhoneStr string
}

func (t TerminalAuth) Phone(ctx context.Context) (string, error) {
	return t.PhoneStr, nil
}

func (t TerminalAuth) Password(ctx context.Context) (string, error) {
	fmt.Print("Enter Telegram 2FA password: ")
	bytePassword, err := term.ReadPassword(int(os.Stdin.Fd()))
	if err != nil {
		return "", err
	}
	fmt.Println()
	return string(bytePassword), nil
}

func (t TerminalAuth) AcceptTermsOfService(ctx context.Context, tos tg.HelpTermsOfService) error {
	return nil
}

func (t TerminalAuth) SignUp(ctx context.Context) (auth.UserInfo, error) {
	return auth.UserInfo{}, fmt.Errorf("signup not supported by gocalis: please sign up with official client first")
}

func (t TerminalAuth) Code(ctx context.Context, sentCode *tg.AuthSentCode) (string, error) {
	fmt.Print("Enter Telegram login code: ")
	var code string
	_, err := fmt.Scanln(&code)
	if err != nil {
		return "", err
	}
	return code, nil
}
