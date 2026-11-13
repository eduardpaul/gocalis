package telegramcall

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
	opus "gopkg.in/hraban/opus.v2"

	"gocalis/internal/intercom"
)

const (
	// pcmRate is the rate of the audio exchanged with the intercom call. Opus
	// codes and decodes at it directly, so nothing is resampled on the way.
	pcmRate = intercom.RemoteSampleRate
	// frameSamples is one 20 ms frame, the size of every packet sent.
	frameSamples = pcmRate / 50
	// rtpFrameTicks is those 20 ms on the RTP clock, which for Opus always
	// runs at 48 kHz whatever the audio inside.
	rtpFrameTicks = 48000 / 50
	// sendBitrate is plenty for 16 kHz mono speech.
	sendBitrate = 32000
	// talkGap is the silence in what we send after which the next packet
	// starts a new talkspurt.
	talkGap = 200 * time.Millisecond
)

// sink takes the audio heard from the voice chat, one stream per speaker.
type sink interface {
	push(source uint32, pcm []int16)
	drop(source uint32)
}

// link is the media connection of one voice chat: a WebRTC transport to
// Telegram's media server that carries our audio up and everybody else's
// down.
//
// Joining a voice chat is not an SDP exchange. The client sends a small JSON
// with its ICE credentials, DTLS fingerprint and the SSRC it will send with
// (joinPayload); the server answers with its own transport parameters
// (connect). The server never declares the streams it forwards: other
// participants' audio simply arrives, each on an SSRC of its own.
type link struct {
	pc     *webrtc.PeerConnection
	track  *webrtc.TrackLocalStaticRTP
	sender *webrtc.RTPSender
	ssrc   uint32
	heard  sink

	up       chan struct{} // closed once the transport is connected
	upOnce   sync.Once
	down     chan struct{} // closed once the transport is lost or closed
	downOnce sync.Once

	sendMu   sync.Mutex
	enc      *opus.Encoder
	seq      uint16
	ts       uint32
	lastSent time.Time
	levelID  uint8 // id of the audio level header extension; 0 when not in use
}

// opusCodec is how Telegram's media server describes voice chat audio.
func opusCodec() webrtc.RTPCodecCapability {
	return webrtc.RTPCodecCapability{
		MimeType:     webrtc.MimeTypeOpus,
		ClockRate:    48000,
		Channels:     2,
		SDPFmtpLine:  "minptime=10;useinbandfec=1",
		RTCPFeedback: []webrtc.RTCPFeedback{{Type: "transport-cc"}},
	}
}

// newLink prepares the transport. Audio heard from the voice chat goes to
// heard.
func newLink(heard sink) (*link, error) {
	media := &webrtc.MediaEngine{}
	if err := media.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: opusCodec(), PayloadType: 111}, webrtc.RTPCodecTypeAudio); err != nil {
		return nil, fmt.Errorf("register opus: %w", err)
	}
	// Registered in the order of the ids Telegram's clients use for them (1 and
	// 2; the default interceptors add transport-cc as 3). The server shows
	// who is speaking from the audio level.
	for _, uri := range []string{sdp.AudioLevelURI, sdp.ABSSendTimeURI} {
		if err := media.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: uri}, webrtc.RTPCodecTypeAudio); err != nil {
			return nil, fmt.Errorf("register header extension: %w", err)
		}
	}
	interceptors := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(media, interceptors); err != nil {
		return nil, fmt.Errorf("register interceptors: %w", err)
	}

	settings := webrtc.SettingEngine{}
	// Without this pion drops every stream the answer did not declare, which
	// here is all of them.
	settings.SetHandleUndeclaredSSRCWithoutAnswer(true)
	settings.SetICETimeouts(8*time.Second, 20*time.Second, 2*time.Second)
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4, webrtc.NetworkTypeUDP6})

	api := webrtc.NewAPI(webrtc.WithMediaEngine(media), webrtc.WithInterceptorRegistry(interceptors), webrtc.WithSettingEngine(settings))
	// No STUN: the media server has a public address and takes our checks
	// from wherever they come.
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		return nil, fmt.Errorf("create peer connection: %w", err)
	}

	l := &link{pc: pc, heard: heard, up: make(chan struct{}), down: make(chan struct{})}
	fail := func(err error) (*link, error) {
		_ = pc.Close()
		return nil, err
	}

	l.track, err = webrtc.NewTrackLocalStaticRTP(opusCodec(), "audio", "gocalis")
	if err != nil {
		return fail(fmt.Errorf("create audio track: %w", err))
	}
	transceiver, err := pc.AddTransceiverFromTrack(l.track, webrtc.RTPTransceiverInit{Direction: webrtc.RTPTransceiverDirectionSendrecv})
	if err != nil {
		return fail(fmt.Errorf("add audio track: %w", err))
	}
	l.sender = transceiver.Sender()
	if encodings := l.sender.GetParameters().Encodings; len(encodings) > 0 {
		l.ssrc = uint32(encodings[0].SSRC)
	}
	if l.ssrc == 0 {
		return fail(errors.New("audio track has no SSRC"))
	}

	l.enc, err = opus.NewEncoder(pcmRate, 1, opus.AppVoIP)
	if err != nil {
		return fail(fmt.Errorf("create opus encoder: %w", err))
	}
	if err := l.enc.SetBitrate(sendBitrate); err != nil {
		return fail(fmt.Errorf("set opus bitrate: %w", err))
	}

	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		switch state {
		case webrtc.PeerConnectionStateConnected:
			l.upOnce.Do(func() { close(l.up) })
		case webrtc.PeerConnectionStateFailed, webrtc.PeerConnectionStateClosed:
			l.downOnce.Do(func() { close(l.down) })
		}
	})
	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		if !strings.EqualFold(remote.Codec().MimeType, webrtc.MimeTypeOpus) {
			return
		}
		go l.receive(remote)
	})
	return l, nil
}

// joinPayload is the "params" of phone.joinGroupCall: our half of the
// transport and the SSRC our audio will have.
func (l *link) joinPayload() (string, error) {
	offer, err := l.pc.CreateOffer(nil)
	if err != nil {
		return "", fmt.Errorf("create offer: %w", err)
	}
	if err := l.pc.SetLocalDescription(offer); err != nil {
		return "", fmt.Errorf("set local description: %w", err)
	}
	var payload joinPayload
	for _, line := range strings.Split(l.pc.LocalDescription().SDP, "\n") {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "a=ice-ufrag:"):
			payload.Ufrag = strings.TrimPrefix(line, "a=ice-ufrag:")
		case strings.HasPrefix(line, "a=ice-pwd:"):
			payload.Pwd = strings.TrimPrefix(line, "a=ice-pwd:")
		case strings.HasPrefix(line, "a=fingerprint:"):
			if hash, value, ok := strings.Cut(strings.TrimPrefix(line, "a=fingerprint:"), " "); ok && len(payload.Fingerprints) == 0 {
				// "passive": the media server opens the DTLS handshake.
				payload.Fingerprints = []fingerprint{{Hash: hash, Setup: "passive", Fingerprint: value}}
			}
		}
	}
	if payload.Ufrag == "" || payload.Pwd == "" || len(payload.Fingerprints) == 0 {
		return "", errors.New("local description lacks ICE credentials or a fingerprint")
	}
	// The API carries the SSRC as a signed 32-bit number.
	payload.SSRC = int32(l.ssrc)
	data, err := json.Marshal(payload)
	return string(data), err
}

// connect applies the server's half of the transport (the params of
// updateGroupCallConnection). The connection then comes up on its own: up is
// closed when it has.
func (l *link) connect(params string) error {
	var response joinResponse
	if err := json.Unmarshal([]byte(params), &response); err != nil {
		return fmt.Errorf("parse connection params: %w", err)
	}
	answer, err := answerSDP(response)
	if err != nil {
		return err
	}
	if err := l.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		return fmt.Errorf("set remote description: %w", err)
	}
	for _, ext := range l.sender.GetParameters().HeaderExtensions {
		if ext.URI == sdp.AudioLevelURI && ext.ID > 0 && ext.ID < 15 {
			l.sendMu.Lock()
			l.levelID = uint8(ext.ID)
			l.sendMu.Unlock()
		}
	}
	return nil
}

// send puts one frame (frameSamples of 16 kHz mono) on the voice chat. Frames
// are expected at their own pace, 20 ms apart; a pause in them is sent as one.
func (l *link) send(pcm []int16) error {
	if len(pcm) != frameSamples {
		return fmt.Errorf("frame of %d samples, want %d", len(pcm), frameSamples)
	}
	l.sendMu.Lock()
	defer l.sendMu.Unlock()

	payload := make([]byte, 1275) // the largest Opus frame
	n, err := l.enc.Encode(pcm, payload)
	if err != nil {
		return fmt.Errorf("opus encode: %w", err)
	}

	now := time.Now()
	first := l.lastSent.IsZero()
	paused := !first && now.Sub(l.lastSent) > talkGap
	if paused {
		// Keep the RTP clock in step with the pause so the far side does not
		// play what follows on top of what came before.
		frames := uint32(now.Sub(l.lastSent) / (20 * time.Millisecond))
		l.ts += frames * rtpFrameTicks
	}
	l.lastSent = now
	l.seq++
	l.ts += rtpFrameTicks

	packet := &rtp.Packet{
		Header: rtp.Header{
			Version:        2,
			Marker:         first || paused,
			PayloadType:    111,
			SequenceNumber: l.seq,
			Timestamp:      l.ts,
			SSRC:           l.ssrc,
		},
		Payload: payload[:n],
	}
	if l.levelID != 0 {
		level, err := (&rtp.AudioLevelExtension{Level: audioLevel(pcm), Voice: true}).Marshal()
		if err == nil {
			_ = packet.Header.SetExtension(l.levelID, level)
		}
	}
	return l.track.WriteRTP(packet)
}

// receive decodes one speaker's stream until it ends.
func (l *link) receive(remote *webrtc.TrackRemote) {
	source := uint32(remote.SSRC())
	defer l.heard.drop(source)
	dec, err := opus.NewDecoder(pcmRate, 1)
	if err != nil {
		return
	}
	pcm := make([]int16, pcmRate*120/1000) // the longest Opus packet
	for {
		packet, _, err := remote.ReadRTP()
		if err != nil {
			return
		}
		if len(packet.Payload) == 0 {
			continue
		}
		n, err := dec.Decode(packet.Payload, pcm)
		if err != nil || n == 0 {
			continue
		}
		l.heard.push(source, append([]int16(nil), pcm[:n]...))
	}
}

// close tears the transport down.
func (l *link) close() {
	_ = l.pc.Close()
	l.downOnce.Do(func() { close(l.down) })
}

// audioLevel is the level of a frame the way the RTP header extension carries
// it: 0 for full scale down to 127 for silence, in dB below full scale.
func audioLevel(pcm []int16) uint8 {
	var sum float64
	for _, s := range pcm {
		sum += float64(s) * float64(s)
	}
	rms := math.Sqrt(sum / float64(len(pcm)))
	if rms < 1 {
		return 127
	}
	db := -20 * math.Log10(rms/32768)
	return uint8(min(max(db, 0), 127))
}

// joinPayload is what a client tells the media server when it joins.
type joinPayload struct {
	Ufrag        string        `json:"ufrag"`
	Pwd          string        `json:"pwd"`
	Fingerprints []fingerprint `json:"fingerprints"`
	SSRC         int32         `json:"ssrc"`
}

type fingerprint struct {
	Hash        string `json:"hash"`
	Setup       string `json:"setup"`
	Fingerprint string `json:"fingerprint"`
}

// joinResponse is what the media server answers: its transport and, in newer
// versions, how it wants the audio packed.
type joinResponse struct {
	Transport struct {
		Ufrag        string        `json:"ufrag"`
		Pwd          string        `json:"pwd"`
		Fingerprints []fingerprint `json:"fingerprints"`
		Candidates   []candidate   `json:"candidates"`
	} `json:"transport"`
	Audio *struct {
		PayloadTypes []payloadType `json:"payload-types"`
		Extensions   []extension   `json:"rtp-hdrexts"`
	} `json:"audio"`
}

type candidate struct {
	Foundation string `json:"foundation"`
	Component  string `json:"component"`
	Protocol   string `json:"protocol"`
	Priority   string `json:"priority"`
	IP         string `json:"ip"`
	Port       string `json:"port"`
	Type       string `json:"type"`
}

type payloadType struct {
	ID        int    `json:"id"`
	Name      string `json:"name"`
	Clockrate int    `json:"clockrate"`
	Channels  int    `json:"channels"`
}

type extension struct {
	ID  int    `json:"id"`
	URI string `json:"uri"`
}

// answerSDP writes the server's parameters as the SDP answer to our offer. The
// server is ICE-lite and opens the DTLS handshake.
func answerSDP(response joinResponse) (string, error) {
	transport := response.Transport
	if transport.Ufrag == "" || transport.Pwd == "" || len(transport.Fingerprints) == 0 {
		return "", errors.New("connection params lack ICE credentials or a fingerprint")
	}
	var candidates []candidate
	for _, c := range transport.Candidates {
		if net.ParseIP(c.IP) == nil || !strings.EqualFold(c.Protocol, "udp") {
			continue
		}
		if port, err := strconv.Atoi(c.Port); err != nil || port <= 0 || port > 65535 {
			continue
		}
		candidates = append(candidates, c)
	}
	if len(candidates) == 0 {
		return "", errors.New("connection params have no usable candidate")
	}

	opusID := 111
	var extensions []extension
	if response.Audio != nil {
		for _, pt := range response.Audio.PayloadTypes {
			if strings.EqualFold(pt.Name, "opus") {
				opusID = pt.ID
			}
		}
		extensions = response.Audio.Extensions
	}
	if len(extensions) == 0 {
		// Older answers leave the audio out; these are the ids Telegram's
		// clients have built in.
		extensions = []extension{
			{ID: 1, URI: sdp.AudioLevelURI},
			{ID: 2, URI: sdp.ABSSendTimeURI},
			{ID: 3, URI: sdp.TransportCCURI},
		}
	}

	family := "IP4"
	if net.ParseIP(candidates[0].IP).To4() == nil {
		family = "IP6"
	}
	id := strconv.Itoa(opusID)
	lines := []string{
		"v=0",
		"o=- 1 2 IN IP4 0.0.0.0",
		"s=-",
		"t=0 0",
		"a=group:BUNDLE 0",
		"a=ice-lite",
		"m=audio " + candidates[0].Port + " UDP/TLS/RTP/SAVPF " + id,
		"c=IN " + family + " " + candidates[0].IP,
		"a=mid:0",
		"a=ice-ufrag:" + transport.Ufrag,
		"a=ice-pwd:" + transport.Pwd,
		"a=fingerprint:" + transport.Fingerprints[0].Hash + " " + transport.Fingerprints[0].Fingerprint,
		"a=setup:active",
	}
	for i, c := range candidates {
		foundation, component, priority, kind := c.Foundation, c.Component, c.Priority, c.Type
		if foundation == "" {
			foundation = strconv.Itoa(i + 1)
		}
		if component == "" {
			component = "1"
		}
		if _, err := strconv.ParseUint(priority, 10, 32); err != nil {
			priority = strconv.Itoa(2130706431 - i)
		}
		if kind == "" {
			kind = "host"
		}
		lines = append(lines, fmt.Sprintf("a=candidate:%s %s udp %s %s %s typ %s", foundation, component, priority, c.IP, c.Port, kind))
	}
	lines = append(lines,
		"a=rtpmap:"+id+" opus/48000/2",
		"a=fmtp:"+id+" minptime=10;useinbandfec=1",
		"a=rtcp-fb:"+id+" transport-cc",
	)
	seen := map[int]bool{}
	for _, ext := range extensions {
		if ext.ID < 1 || ext.ID > 14 || ext.URI == "" || seen[ext.ID] {
			continue
		}
		seen[ext.ID] = true
		lines = append(lines, "a=extmap:"+strconv.Itoa(ext.ID)+" "+ext.URI)
	}
	lines = append(lines, "a=rtcp-mux", "a=sendrecv", "")
	return strings.Join(lines, "\r\n"), nil
}
