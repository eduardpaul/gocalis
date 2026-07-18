package webrtc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	opus "gopkg.in/hraban/opus.v2"

	"gocalis/internal/audio"
	"gocalis/internal/audionode"
)

// Client satisfies the audionode.AudioNode port.
var _ audionode.AudioNode = (*Client)(nil)

const (
	// frameMs is the RTP packetization interval for every codec below. 10ms
	// divides evenly into the doorbell's 30ms AAC-ELD frame (3 packets in,
	// 1 frame out), avoiding the 20ms/40ms alternating gap that a 20ms
	// packetizer produces against a 30ms target.
	frameMs = 10

	// Opus is negotiated at 48kHz (the WebRTC standard clock) and carries true
	// wideband speech end-to-end (T18): the source TTS/mic PCM is resampled to
	// 48kHz, encoded mono, and go2rtc transcodes it to the doorbell's AAC-ELD.
	opusRate      = 48000
	opusChannels  = 1
	opusFrameSize = opusRate / 1000 * frameMs // 480 samples/frame (mono, 10ms)
	opusBitrate   = 16000                     // lowered from 32kbps to 16kbps for Wi-Fi reliability

	// PCMU (legacy narrowband) path — kept for the diagnostic `-send-codec pcmu`.
	pcmuFrameBytes = 80 // 10ms @ 8kHz

	// recvModelRate is the rate the downstream wake/ASR/speaker models want.
	recvModelRate = 16000
)

// WSMessage represents the JSON signaling messages exchanged with go2rtc.
type WSMessage struct {
	Type  string      `json:"type"`
	Value interface{} `json:"value,omitempty"`
}

// Client represents the WebRTC client connection to go2rtc.
type Client struct {
	signalingURL string
	sendCodec    string // "pcmu" | "opus" | "opus-sendonly"
	pc           *webrtc.PeerConnection
	localTrack   *webrtc.TrackLocalStaticSample
	opusEnc      *opus.Encoder // nil for pcmu
	connected    chan struct{}
	onceConnect  sync.Once
	warmupOnce   sync.Once
	wsWriteMutex sync.Mutex
	onAudioRx    func(samples []float32)
	audioRxMutex sync.RWMutex

	// Talkback (HomeKit doorbell backchannel) routing. When talkbackStream and
	// apiBaseURL are set, outbound TTS is delivered via a dedicated WHIP producer
	// + go2rtc AAC-ELD route (see talkback.go) instead of the receive PeerConnection
	// track, which HomeKit doorbells silently drop.
	apiBaseURL     string
	talkbackStream string
	talkbackIn     string
	tbMu           sync.Mutex
	tb             *talkbackSender
	tbCancel       context.CancelFunc

	reconnectMu  sync.Mutex
	reconnecting bool
	closing      bool
}

// Config configures a WebRTC Client. SignalingURL and SendCodec drive the receive
// PeerConnection (WebSocket signaling to go2rtc); the remaining fields enable the
// HomeKit doorbell talkback backchannel.
type Config struct {
	// SignalingURL is the go2rtc WebSocket signaling URL (ws://.../api/ws?src=...).
	SignalingURL string
	// SendCodec is "opus" (default), "opus-sendonly", or "pcmu".
	SendCodec string
	// APIBaseURL is the go2rtc HTTP base URL (e.g. http://host:1984) used for the
	// talkback control plane. Empty disables talkback (legacy WebRTC-track path).
	APIBaseURL string
	// TalkbackStream is the HomeKit backchannel stream (e.g. doorbell_raw_homekit).
	// Empty disables talkback.
	TalkbackStream string
	// TalkbackIn is the go2rtc stream fed by the WHIP producer. Defaults to
	// "talkback_in" when empty.
	TalkbackIn string
}

// NewClient creates the production WebRTC client, which uses Opus wideband
// (T18) so 16kHz speech is preserved end-to-end instead of the old PCMU 8kHz
// narrowband that degraded talkback quality and wake/ASR accuracy.
func NewClient(signalingURL string) (*Client, error) {
	return NewClientWithConfig(Config{SignalingURL: signalingURL, SendCodec: "opus"})
}

// NewClientWithSendCodec builds a client for a specific send codec:
//   - "opus":          Opus wideband, bidirectional (production + diagnostics).
//   - "opus-sendonly": Opus wideband, send only (no return audio negotiated).
//   - "pcmu":          legacy G.711 8kHz narrowband (diagnostic/compat).
func NewClientWithSendCodec(signalingURL, sendCodec string) (*Client, error) {
	return NewClientWithConfig(Config{SignalingURL: signalingURL, SendCodec: sendCodec})
}

// NewClientWithConfig builds a Client from cfg. When cfg enables talkback, the
// receive PeerConnection still handles inbound doorbell mic audio (wake/ASR) while
// outbound TTS is routed through the go2rtc AAC-ELD backchannel (see talkback.go).
func NewClientWithConfig(cfg Config) (*Client, error) {
	sendCodec := cfg.SendCodec
	if sendCodec == "" {
		sendCodec = "opus"
	}
	switch sendCodec {
	case "pcmu", "opus", "opus-sendonly":
	default:
		return nil, fmt.Errorf("unsupported send codec %q (want pcmu, opus, or opus-sendonly)", sendCodec)
	}

	// Register ONLY the chosen codec so go2rtc negotiates exactly that.
	m := &webrtc.MediaEngine{}
	var (
		capability webrtc.RTPCodecCapability
		payloadPT  webrtc.PayloadType
	)
	if sendCodec == "pcmu" {
		capability = webrtc.RTPCodecCapability{MimeType: webrtc.MimeTypePCMU, ClockRate: 8000, Channels: 1}
		payloadPT = 0
	} else {
		// opus/48000/2 is the de-facto WebRTC SDP form; we encode mono and
		// libopus decodes it transparently on the go2rtc side.
		capability = webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		}
		payloadPT = 111
	}
	if err := m.RegisterCodec(webrtc.RTPCodecParameters{
		RTPCodecCapability: capability,
		PayloadType:        payloadPT,
	}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, err
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))

	config := webrtc.Configuration{
		BundlePolicy: webrtc.BundlePolicyMaxBundle,
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	}

	pc, err := api.NewPeerConnection(config)
	if err != nil {
		return nil, err
	}

	localTrack, err := webrtc.NewTrackLocalStaticSample(capability, "audio", "pion")
	if err != nil {
		pc.Close()
		return nil, err
	}

	direction := webrtc.RTPTransceiverDirectionSendrecv
	if sendCodec == "opus-sendonly" {
		direction = webrtc.RTPTransceiverDirectionSendonly
	}
	if _, err = pc.AddTransceiverFromTrack(localTrack, webrtc.RTPTransceiverInit{
		Direction: direction,
	}); err != nil {
		pc.Close()
		return nil, err
	}

	client := &Client{
		signalingURL:   cfg.SignalingURL,
		sendCodec:      sendCodec,
		pc:             pc,
		localTrack:     localTrack,
		connected:      make(chan struct{}),
		apiBaseURL:     cfg.APIBaseURL,
		talkbackStream: cfg.TalkbackStream,
		talkbackIn:     cfg.TalkbackIn,
	}

	if sendCodec != "pcmu" {
		enc, err := opus.NewEncoder(opusRate, opusChannels, opus.AppVoIP)
		if err != nil {
			pc.Close()
			return nil, fmt.Errorf("create opus encoder: %w", err)
		}
		if err := enc.SetBitrate(opusBitrate); err != nil {
			pc.Close()
			return nil, fmt.Errorf("set opus bitrate: %w", err)
		}
		client.opusEnc = enc
	}

	// Configure PeerConnection state hooks
	pc.OnConnectionStateChange(func(s webrtc.PeerConnectionState) {
		log.Printf("[WebRTC] PeerConnection State: %s\n", s.String())
		if s == webrtc.PeerConnectionStateConnected {
			client.onceConnect.Do(func() {
				close(client.connected)
			})
			return
		}
		if s == webrtc.PeerConnectionStateDisconnected || s == webrtc.PeerConnectionStateFailed {
			client.scheduleReconnect("peer connection state " + s.String())
		}
	})

	pc.OnICEConnectionStateChange(func(s webrtc.ICEConnectionState) {
		log.Printf("[WebRTC] ICE Connection State: %s\n", s.String())
	})

	// Register remote track reader
	pc.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("[WebRTC] Received remote track: MimeType=%s, PayloadType=%d\n", track.Codec().MimeType, track.PayloadType())
		go client.readRemoteTrack(track)
	})

	return client, nil
}

// OnAudio registers a callback to process incoming audio samples (converted to float32).
func (c *Client) OnAudio(callback func(samples []float32)) {
	c.audioRxMutex.Lock()
	defer c.audioRxMutex.Unlock()
	c.onAudioRx = callback
}

// Connect establishes the WebSocket signaling connection and completes the SDP handshake.
func (c *Client) Connect(ctx context.Context) error {
	u, err := url.Parse(c.signalingURL)
	if err != nil {
		return err
	}

	log.Printf("[Signaling] Connecting to WebSocket: %s\n", u.String())
	wsConn, _, err := websocket.DefaultDialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return err
	}

	writeWS := func(msgType int, data []byte) error {
		c.wsWriteMutex.Lock()
		defer c.wsWriteMutex.Unlock()
		return wsConn.WriteMessage(msgType, data)
	}

	// Handle ICE Candidate trickling
	c.pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		candJSON := candidate.ToJSON()
		msg := WSMessage{
			Type:  "webrtc/candidate",
			Value: candJSON.Candidate,
		}
		data, err := json.Marshal(msg)
		if err == nil {
			_ = writeWS(websocket.TextMessage, data)
		}
	})

	// Create and set local Offer. When reconnecting an existing PeerConnection,
	// force ICE restart so media can recover after network/signaling dropouts.
	offerOpts := &webrtc.OfferOptions{}
	if c.pc.RemoteDescription() != nil {
		offerOpts.ICERestart = true
	}
	offer, err := c.pc.CreateOffer(offerOpts)
	if err != nil {
		wsConn.Close()
		return err
	}

	if err := c.pc.SetLocalDescription(offer); err != nil {
		wsConn.Close()
		return err
	}

	// Send Offer via WebSocket
	offerMsg := WSMessage{
		Type:  "webrtc/offer",
		Value: offer.SDP,
	}
	offerData, err := json.Marshal(offerMsg)
	if err != nil {
		wsConn.Close()
		return err
	}

	if err := writeWS(websocket.TextMessage, offerData); err != nil {
		wsConn.Close()
		return err
	}

	// Read SDP Answer and start candidate loop
	go func() {
		defer wsConn.Close()
		for {
			_, message, err := wsConn.ReadMessage()
			if err != nil {
				log.Printf("[Signaling] WebSocket read loop exited: %v\n", err)
				c.scheduleReconnect("signaling read loop exited")
				return
			}

			var wsMsg WSMessage
			if err := json.Unmarshal(message, &wsMsg); err != nil {
				continue
			}

			switch wsMsg.Type {
			case "webrtc/answer":
				sdpStr, ok := wsMsg.Value.(string)
				if !ok {
					continue
				}
				log.Println("[Signaling] Setting remote WebRTC Answer description...")
				err = c.pc.SetRemoteDescription(webrtc.SessionDescription{
					Type: webrtc.SDPTypeAnswer,
					SDP:  sdpStr,
				})
				if err != nil {
					log.Printf("[Signaling] Failed to set remote description: %v\n", err)
					return
				}
			case "webrtc/candidate":
				candStr, ok := wsMsg.Value.(string)
				if !ok {
					continue
				}
				err = c.pc.AddICECandidate(webrtc.ICECandidateInit{
					Candidate: candStr,
				})
				if err != nil {
					log.Printf("[Signaling] Failed to add remote candidate: %v\n", err)
				}
			default:
				log.Printf("[Signaling] UNHANDLED type=%q value=%v", wsMsg.Type, wsMsg.Value)
			}
		}
	}()

	// Warm up the talkback backchannel now (WHIP producer + AAC-ELD route +
	// continuous silence feed) so the first spoken utterance is not clipped by
	// ffmpeg/route spin-up latency. Best-effort: failures are retried on Play.
	if c.talkbackEnabled() {
		if _, err := c.ensureTalkback(); err != nil {
			log.Printf("[Talkback] warm-up deferred: %v", err)
		}
	}

	return nil
}

// Play plays out PCM16 samples over the WebRTC backchannel, executing a warmup silence pre-roll.
func (c *Client) Play(ctx context.Context, pcm16 []int16, sourceSampleRate int) error {
	if c.talkbackEnabled() {
		tb, err := c.ensureTalkback()
		if err != nil {
			return err
		}
		tb.playMu.Lock()
		defer tb.playMu.Unlock()
		// go2rtc drops the idle AAC-ELD ffmpeg bridge over time, so re-assert the
		// route before speaking. Use an independent context: the control-plane POST
		// must not be aborted by a short-lived caller context. Synthesis/buffering
		// latency covers ffmpeg spin-up.
		routeCtx, routeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if rerr := tb.assertRoute(routeCtx); rerr != nil {
			log.Printf("[Talkback] re-assert route: %v", rerr)
		}
		routeCancel()
		log.Println("[WebRTC] Streaming real audio (talkback AAC-ELD)...")
		tb.pushPCM(pcm16, sourceSampleRate)
		tb.flushPCM()
		err = tb.waitDrained(ctx)
		if err == nil {
			log.Println("[WebRTC] Finished audio transmission.")
		}
		return err
	}

	ticker := time.NewTicker(frameMs * time.Millisecond)
	defer ticker.Stop()

	if err := c.waitConnectedAndPreroll(ctx, ticker); err != nil {
		return err
	}

	log.Printf("[WebRTC] Streaming real audio (%s)...", c.sendCodec)
	s := c.newPlaySession(ticker)
	if err := s.push(ctx, pcm16, sourceSampleRate); err != nil {
		return err
	}
	if err := s.flush(ctx); err != nil {
		return err
	}
	log.Println("[WebRTC] Finished audio transmission.")
	return nil
}

// PlayStream plays PCM16 audio pulled incrementally from src, encoding and pacing
// each 20ms frame as audio becomes available. This lets playback begin before the
// whole utterance has been synthesized.
func (c *Client) PlayStream(ctx context.Context, src audionode.PCM16Source) error {
	if c.talkbackEnabled() {
		tb, err := c.ensureTalkback()
		if err != nil {
			return err
		}
		tb.playMu.Lock()
		defer tb.playMu.Unlock()
		// go2rtc drops the idle AAC-ELD ffmpeg bridge over time, so re-assert the
		// route before speaking. Use an independent context: the control-plane POST
		// must not be aborted by a short-lived caller context. Synthesis/buffering
		// latency covers ffmpeg spin-up.
		routeCtx, routeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if rerr := tb.assertRoute(routeCtx); rerr != nil {
			log.Printf("[Talkback] re-assert route: %v", rerr)
		}
		routeCancel()
		log.Println("[WebRTC] Streaming real audio (chunked, talkback AAC-ELD)...")
		rate := src.SampleRate()
		for {
			pcm, readErr := src.ReadPCM16(2048)
			if len(pcm) > 0 {
				tb.pushPCM(pcm, rate)
			}
			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				return readErr
			}
		}
		tb.flushPCM()
		err = tb.waitDrained(ctx)
		if err == nil {
			log.Println("[WebRTC] Finished audio transmission.")
		}
		return err
	}

	ticker := time.NewTicker(frameMs * time.Millisecond)
	defer ticker.Stop()

	if err := c.waitConnectedAndPreroll(ctx, ticker); err != nil {
		return err
	}

	log.Printf("[WebRTC] Streaming real audio (chunked, %s)...", c.sendCodec)
	sourceSampleRate := src.SampleRate()
	s := c.newPlaySession(ticker)

	for {
		pcm, readErr := src.ReadPCM16(2048)
		if len(pcm) > 0 {
			if err := s.push(ctx, pcm, sourceSampleRate); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}

	if err := s.flush(ctx); err != nil {
		return err
	}
	log.Println("[WebRTC] Finished audio transmission.")
	return nil
}

// playSession accumulates encoded frames across source reads so that partial
// frames are only padded once, at the very end (padding every chunk tail would
// inject silence gaps mid-utterance).
type playSession struct {
	c       *Client
	ticker  *time.Ticker
	opusIn  []int16 // leftover resampled-to-48k samples not yet framed (opus)
	pcmuBuf []byte  // leftover mu-law bytes not yet framed (pcmu)
	pkt     []byte  // reusable opus encode scratch buffer
}

func (c *Client) newPlaySession(ticker *time.Ticker) *playSession {
	return &playSession{c: c, ticker: ticker, pkt: make([]byte, 4000)}
}

// push encodes and paces every complete frame contained in pcm16 (sampled at srcRate).
func (s *playSession) push(ctx context.Context, pcm16 []int16, srcRate int) error {
	if s.c.sendCodec == "pcmu" {
		s.pcmuBuf = append(s.pcmuBuf, audio.EncodePCM16ToMuLaw(pcm16, srcRate)...)
		for len(s.pcmuBuf) >= pcmuFrameBytes {
			if err := s.send(ctx, s.pcmuBuf[:pcmuFrameBytes]); err != nil {
				return err
			}
			s.pcmuBuf = s.pcmuBuf[pcmuFrameBytes:]
		}
		return nil
	}

	s.opusIn = append(s.opusIn, audio.ResampleInt16(pcm16, srcRate, opusRate)...)
	for len(s.opusIn) >= opusFrameSize {
		payload, err := s.encodeOpus(s.opusIn[:opusFrameSize])
		if err != nil {
			return err
		}
		if err := s.send(ctx, payload); err != nil {
			return err
		}
		s.opusIn = s.opusIn[opusFrameSize:]
	}
	return nil
}

// flush pads and sends the final partial frame (silence-padded).
func (s *playSession) flush(ctx context.Context) error {
	if s.c.sendCodec == "pcmu" {
		if len(s.pcmuBuf) == 0 {
			return nil
		}
		frame := make([]byte, pcmuFrameBytes)
		copy(frame, s.pcmuBuf)
		for j := len(s.pcmuBuf); j < pcmuFrameBytes; j++ {
			frame[j] = 0xFF // PCMU digital silence
		}
		s.pcmuBuf = nil
		return s.send(ctx, frame)
	}

	if len(s.opusIn) == 0 {
		return nil
	}
	frame := make([]int16, opusFrameSize) // zero-padded (PCM silence)
	copy(frame, s.opusIn)
	s.opusIn = nil
	payload, err := s.encodeOpus(frame)
	if err != nil {
		return err
	}
	return s.send(ctx, payload)
}

// encodeOpus encodes exactly one opusFrameSize mono frame and returns an owned copy
// of the packet (the scratch buffer is reused across calls).
func (s *playSession) encodeOpus(frame []int16) ([]byte, error) {
	n, err := s.c.opusEnc.Encode(frame, s.pkt)
	if err != nil {
		return nil, fmt.Errorf("opus encode: %w", err)
	}
	return append([]byte(nil), s.pkt[:n]...), nil
}

// send paces a single 20ms frame onto the local track.
func (s *playSession) send(ctx context.Context, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ticker.C:
		return s.c.localTrack.WriteSample(media.Sample{
			Data:     payload,
			Duration: frameMs * time.Millisecond,
		})
	}
}

// waitConnectedAndPreroll blocks until the peer connection is established, then
// sends the one-time silence pre-roll used to wake the speaker/transcoder.
func (c *Client) waitConnectedAndPreroll(ctx context.Context, ticker *time.Ticker) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.connected:
		log.Println("[WebRTC] Connected! Preparing audio pipeline...")
	}

	// Warmup pre-roll: send 1.5s of codec silence to wake speaker & transcoder.
	// This is only needed once per connection to prime the audio path; doing it
	// on every utterance would add ~1.5s latency to every spoken turn.
	c.warmupOnce.Do(func() {
		silence := c.silenceFrame()
		if silence == nil {
			return
		}
		log.Println("[WebRTC] Sending 1.5s silence pre-roll to wake speaker (once per connection)...")
		for i := 0; i < 75; i++ {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = c.localTrack.WriteSample(media.Sample{
					Data:     silence,
					Duration: frameMs * time.Millisecond,
				})
			}
		}
	})

	return ctx.Err()
}

// silenceFrame returns one 20ms frame of digital silence for the negotiated codec.
func (c *Client) silenceFrame() []byte {
	if c.sendCodec == "pcmu" {
		f := make([]byte, pcmuFrameBytes)
		for i := range f {
			f[i] = 0xFF
		}
		return f
	}
	pkt := make([]byte, 4000)
	n, err := c.opusEnc.Encode(make([]int16, opusFrameSize), pkt)
	if err != nil {
		log.Printf("[WebRTC] opus silence encode failed: %v", err)
		return nil
	}
	return pkt[:n]
}

// Close closes the PeerConnection.
func (c *Client) Close() error {
	c.reconnectMu.Lock()
	c.closing = true
	c.reconnectMu.Unlock()

	c.tbMu.Lock()
	if c.tbCancel != nil {
		c.tbCancel()
	}
	tb := c.tb
	c.tb = nil
	c.tbMu.Unlock()
	if tb != nil {
		tb.close()
	}
	if c.pc != nil {
		return c.pc.Close()
	}
	return nil
}

// scheduleReconnect starts a best-effort reconnection loop for the receive
// PeerConnection. It is idempotent while a reconnect attempt is in flight.
func (c *Client) scheduleReconnect(reason string) {
	c.reconnectMu.Lock()
	if c.closing || c.reconnecting {
		c.reconnectMu.Unlock()
		return
	}
	c.reconnecting = true
	c.reconnectMu.Unlock()

	go func() {
		defer func() {
			c.reconnectMu.Lock()
			c.reconnecting = false
			c.reconnectMu.Unlock()
		}()

		for attempt := 1; attempt <= 12; attempt++ {
			c.reconnectMu.Lock()
			if c.closing {
				c.reconnectMu.Unlock()
				return
			}
			c.reconnectMu.Unlock()

			state := c.pc.ConnectionState()
			if state == webrtc.PeerConnectionStateConnected {
				return
			}

			log.Printf("[WebRTC] Reconnect attempt %d (%s); state=%s", attempt, reason, state)
			rctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			err := c.Connect(rctx)
			cancel()
			if err == nil {
				log.Printf("[WebRTC] Reconnect succeeded on attempt %d", attempt)
				return
			}

			log.Printf("[WebRTC] Reconnect attempt %d failed: %v", attempt, err)
			time.Sleep(time.Duration(attempt) * time.Second)
		}

		log.Printf("[WebRTC] Reconnect attempts exhausted; manual restart may be required")
	}()
}

// talkbackEnabled reports whether outbound TTS should use the go2rtc AAC-ELD
// backchannel instead of the receive PeerConnection track.
func (c *Client) talkbackEnabled() bool {
	return c.apiBaseURL != "" && c.talkbackStream != ""
}

// ensureTalkback lazily establishes (and caches) the talkback sender. It retries
// on each call until the WHIP producer + AAC-ELD route are up, so a transient
// go2rtc hiccup at startup does not permanently disable the backchannel.
func (c *Client) ensureTalkback() (*talkbackSender, error) {
	c.tbMu.Lock()
	defer c.tbMu.Unlock()
	if c.tb != nil {
		state := c.tb.pc.ConnectionState()
		switch state {
		case webrtc.PeerConnectionStateFailed,
			webrtc.PeerConnectionStateDisconnected,
			webrtc.PeerConnectionStateClosed:
			log.Printf("[Talkback] cached sender is %s; recreating", state)
			if c.tbCancel != nil {
				c.tbCancel()
				c.tbCancel = nil
			}
			c.tb.close()
			c.tb = nil
		default:
			return c.tb, nil
		}
	}

	in := c.talkbackIn
	if in == "" {
		in = "talkback_in"
	}

	ctx, cancel := context.WithCancel(context.Background())
	setupCtx, cancelSetup := context.WithTimeout(ctx, 20*time.Second)
	tb, err := newTalkbackSender(setupCtx, c.apiBaseURL, in, c.talkbackStream)
	cancelSetup()
	if err != nil {
		cancel()
		return nil, err
	}
	go tb.run(ctx)
	c.tb = tb
	c.tbCancel = cancel
	return tb, nil
}

// readRemoteTrack reads incoming RTP packets, decodes them to float32 mono at the
// model rate (16000Hz), and publishes them. It adapts to whichever codec go2rtc
// negotiated on the return path (Opus wideband or legacy PCMU).
func (c *Client) readRemoteTrack(track *webrtc.TrackRemote) {
	mime := track.Codec().MimeType
	log.Printf("[WebRTC] readRemoteTrack started for track ID: %s, Mime: %s\n", track.ID(), mime)

	isOpus := strings.EqualFold(mime, webrtc.MimeTypeOpus)
	var dec *opus.Decoder
	if isOpus {
		var err error
		// Decode straight to mono 16kHz: libopus downmixes/resamples internally,
		// so downstream models get exactly what they want with no extra resample.
		dec, err = opus.NewDecoder(recvModelRate, 1)
		if err != nil {
			log.Printf("[WebRTC] failed to create opus decoder: %v\n", err)
			return
		}
	}
	// Max Opus frame is 120ms; at 16kHz mono that is 1920 samples. Pad for safety.
	pcmBuf := make([]int16, recvModelRate/1000*120)

	packetCount := 0
	for {
		rtpPacket, _, err := track.ReadRTP()
		if err != nil {
			log.Printf("[WebRTC] readRemoteTrack error reading RTP: %v\n", err)
			c.scheduleReconnect("remote RTP read failed")
			return
		}

		packetCount++
		if packetCount == 1 {
			log.Printf("[WebRTC] First incoming RTP packet received! Size: %d\n", len(rtpPacket.Payload))
		}
		if packetCount%250 == 0 { // approx every 5 seconds (50 packets per second)
			log.Printf("[WebRTC] Received %d RTP packets so far\n", packetCount)
		}

		c.audioRxMutex.RLock()
		callback := c.onAudioRx
		c.audioRxMutex.RUnlock()
		if callback == nil {
			continue
		}

		if isOpus {
			n, err := dec.Decode(rtpPacket.Payload, pcmBuf)
			if err != nil {
				continue // drop undecodable packet
			}
			callback(audio.PCM16ToFloat(pcmBuf[:n]))
		} else {
			// PCMU (G.711 mu-law, 8000Hz) -> float32 -> upsample to 16000Hz.
			floatSamples := audio.DecodeMuLawToFloat(rtpPacket.Payload)
			callback(audio.ResampleFloat32(floatSamples, 8000, recvModelRate))
		}
	}
}
