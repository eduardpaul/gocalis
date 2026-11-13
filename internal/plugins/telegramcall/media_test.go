package telegramcall

import (
	"encoding/json"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pion/interceptor"
	"github.com/pion/rtp"
	"github.com/pion/sdp/v3"
	"github.com/pion/webrtc/v4"
	opus "gopkg.in/hraban/opus.v2"
)

// fakeSFU stands in for Telegram's media server: it takes a join payload,
// answers with its transport as JSON, never declares the streams it sends, and
// sends several speakers down the one connection, each on its own SSRC.
type fakeSFU struct {
	t  *testing.T
	pc *webrtc.PeerConnection

	mu       sync.Mutex
	writer   webrtc.TrackLocalWriter
	heard    []int16 // what the client sent, decoded
	levelIDs map[uint8]int
}

// Bind and the rest make fakeSFU a local track that writes packets with
// whatever SSRC they carry.
func (s *fakeSFU) Bind(c webrtc.TrackLocalContext) (webrtc.RTPCodecParameters, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.writer = c.WriteStream()
	return c.CodecParameters()[0], nil
}
func (s *fakeSFU) Unbind(webrtc.TrackLocalContext) error { return nil }
func (s *fakeSFU) ID() string                            { return "audio" }
func (s *fakeSFU) RID() string                           { return "" }
func (s *fakeSFU) StreamID() string                      { return "sfu" }
func (s *fakeSFU) Kind() webrtc.RTPCodecType             { return webrtc.RTPCodecTypeAudio }

func newFakeSFU(t *testing.T) *fakeSFU {
	t.Helper()
	media := &webrtc.MediaEngine{}
	if err := media.RegisterCodec(webrtc.RTPCodecParameters{RTPCodecCapability: opusCodec(), PayloadType: 111}, webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatal(err)
	}
	if err := media.RegisterHeaderExtension(webrtc.RTPHeaderExtensionCapability{URI: sdp.AudioLevelURI}, webrtc.RTPCodecTypeAudio); err != nil {
		t.Fatal(err)
	}
	interceptors := &interceptor.Registry{}
	if err := webrtc.RegisterDefaultInterceptors(media, interceptors); err != nil {
		t.Fatal(err)
	}
	settings := webrtc.SettingEngine{}
	settings.SetLite(true)
	settings.SetNetworkTypes([]webrtc.NetworkType{webrtc.NetworkTypeUDP4})
	pc, err := webrtc.NewAPI(webrtc.WithMediaEngine(media), webrtc.WithInterceptorRegistry(interceptors), webrtc.WithSettingEngine(settings)).NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	s := &fakeSFU{t: t, pc: pc, levelIDs: map[uint8]int{}}
	t.Cleanup(func() { _ = pc.Close() })

	pc.OnTrack(func(remote *webrtc.TrackRemote, _ *webrtc.RTPReceiver) {
		dec, err := opus.NewDecoder(pcmRate, 1)
		if err != nil {
			return
		}
		pcm := make([]int16, pcmRate*120/1000)
		for {
			packet, _, err := remote.ReadRTP()
			if err != nil {
				return
			}
			n, err := dec.Decode(packet.Payload, pcm)
			if err != nil {
				continue
			}
			s.mu.Lock()
			s.heard = append(s.heard, pcm[:n]...)
			for _, id := range packet.Header.GetExtensionIDs() {
				s.levelIDs[id]++
			}
			s.mu.Unlock()
		}
	})
	return s
}

// join does what the server does with a join payload and returns the
// connection params for the client.
func (s *fakeSFU) join(payload string) string {
	s.t.Helper()
	var join joinPayload
	if err := json.Unmarshal([]byte(payload), &join); err != nil {
		s.t.Fatal(err)
	}
	if join.Ufrag == "" || join.Pwd == "" || len(join.Fingerprints) != 1 || join.SSRC == 0 {
		s.t.Fatalf("incomplete join payload: %s", payload)
	}
	if fp := join.Fingerprints[0]; fp.Hash != "sha-256" || fp.Setup != "passive" || fp.Fingerprint == "" {
		s.t.Fatalf("unexpected fingerprint in join payload: %+v", fp)
	}

	offer := strings.Join([]string{
		"v=0",
		"o=- 1 2 IN IP4 0.0.0.0",
		"s=-",
		"t=0 0",
		"a=group:BUNDLE 0",
		"m=audio 9 UDP/TLS/RTP/SAVPF 111",
		"c=IN IP4 0.0.0.0",
		"a=mid:0",
		"a=ice-ufrag:" + join.Ufrag,
		"a=ice-pwd:" + join.Pwd,
		"a=fingerprint:" + join.Fingerprints[0].Hash + " " + join.Fingerprints[0].Fingerprint,
		"a=setup:" + join.Fingerprints[0].Setup,
		"a=rtpmap:111 opus/48000/2",
		"a=extmap:1 " + sdp.AudioLevelURI,
		"a=rtcp-mux",
		"a=sendrecv",
		"",
	}, "\r\n")
	if err := s.pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeOffer, SDP: offer}); err != nil {
		s.t.Fatal(err)
	}
	if _, err := s.pc.AddTrack(s); err != nil {
		s.t.Fatal(err)
	}
	answer, err := s.pc.CreateAnswer(nil)
	if err != nil {
		s.t.Fatal(err)
	}
	gathered := webrtc.GatheringCompletePromise(s.pc)
	if err := s.pc.SetLocalDescription(answer); err != nil {
		s.t.Fatal(err)
	}
	<-gathered

	var response joinResponse
	for _, line := range strings.Split(s.pc.LocalDescription().SDP, "\r\n") {
		switch {
		case strings.HasPrefix(line, "a=ice-ufrag:"):
			response.Transport.Ufrag = strings.TrimPrefix(line, "a=ice-ufrag:")
		case strings.HasPrefix(line, "a=ice-pwd:"):
			response.Transport.Pwd = strings.TrimPrefix(line, "a=ice-pwd:")
		case strings.HasPrefix(line, "a=fingerprint:"):
			hash, value, _ := strings.Cut(strings.TrimPrefix(line, "a=fingerprint:"), " ")
			response.Transport.Fingerprints = []fingerprint{{Hash: hash, Setup: "active", Fingerprint: value}}
		case strings.HasPrefix(line, "a=candidate:"):
			f := strings.Fields(strings.TrimPrefix(line, "a=candidate:"))
			if len(f) >= 8 {
				response.Transport.Candidates = append(response.Transport.Candidates, candidate{Foundation: text(f[0]), Component: text(f[1]), Protocol: text(f[2]), Priority: text(f[3]), IP: text(f[4]), Port: text(f[5]), Type: text(f[7])})
			}
		}
	}
	// Something a client must skip over.
	response.Transport.Candidates = append(response.Transport.Candidates, candidate{Foundation: "9", Component: "1", Protocol: "ssltcp", Priority: "1", IP: "203.0.113.1", Port: "443", Type: "host"})
	data, err := json.Marshal(response)
	if err != nil {
		s.t.Fatal(err)
	}
	return string(data)
}

// speak sends a tone as one speaker for the given time.
func (s *fakeSFU) speak(ssrc uint32, hz float64, amplitude float64, length time.Duration) {
	enc, err := opus.NewEncoder(48000, 2, opus.AppVoIP) // as a phone sends it
	if err != nil {
		s.t.Error(err)
		return
	}
	frames := int(length / (20 * time.Millisecond))
	next := time.Now()
	for i := 0; i < frames; i++ {
		pcm := make([]int16, 960*2)
		for j := 0; j < 960; j++ {
			v := int16(amplitude * math.Sin(2*math.Pi*hz*float64(i*960+j)/48000))
			pcm[2*j], pcm[2*j+1] = v, v
		}
		payload := make([]byte, 1275)
		n, err := enc.Encode(pcm, payload)
		if err != nil {
			s.t.Error(err)
			return
		}
		s.mu.Lock()
		writer := s.writer
		s.mu.Unlock()
		if writer != nil {
			header := &rtp.Header{Version: 2, PayloadType: 111, SequenceNumber: uint16(i + 1), Timestamp: uint32(i+1) * 960, SSRC: ssrc}
			_, _ = writer.WriteRTP(header, payload[:n])
		}
		next = next.Add(20 * time.Millisecond)
		time.Sleep(time.Until(next))
	}
}

// collector is a sink that keeps what each speaker said.
type collector struct {
	mu      sync.Mutex
	sources map[uint32][]int16
	dropped map[uint32]bool
}

func (c *collector) push(source uint32, pcm []int16) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sources == nil {
		c.sources = map[uint32][]int16{}
	}
	c.sources[source] = append(c.sources[source], pcm...)
}

func (c *collector) drop(source uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.dropped == nil {
		c.dropped = map[uint32]bool{}
	}
	c.dropped[source] = true
}

func (c *collector) get(source uint32) []int16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int16(nil), c.sources[source]...)
}

// tonePower is how much of the signal is at the given frequency (Goertzel),
// as an amplitude.
func tonePower(pcm []int16, hz float64) float64 {
	if len(pcm) == 0 {
		return 0
	}
	w := 2 * math.Pi * hz / pcmRate
	coeff := 2 * math.Cos(w)
	var s1, s2 float64
	for _, v := range pcm {
		s0 := float64(v) + coeff*s1 - s2
		s2, s1 = s1, s0
	}
	power := s1*s1 + s2*s2 - coeff*s1*s2
	return 2 * math.Sqrt(math.Max(power, 0)) / float64(len(pcm))
}

func tone(hz, amplitude float64, frames int) [][]int16 {
	out := make([][]int16, frames)
	for i := range out {
		out[i] = make([]int16, frameSamples)
		for j := range out[i] {
			out[i][j] = int16(amplitude * math.Sin(2*math.Pi*hz*float64(i*frameSamples+j)/pcmRate))
		}
	}
	return out
}

// The link joins the way a Telegram client does, hears every speaker the
// server sends although none is declared, and what it sends arrives with the
// audio level the server reads.
func TestLinkCarriesAudioBothWays(t *testing.T) {
	sfu := newFakeSFU(t)
	heard := &collector{}
	l, err := newLink(heard)
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()

	payload, err := l.joinPayload()
	if err != nil {
		t.Fatal(err)
	}
	if err := l.connect(sfu.join(payload)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-l.up:
	case <-l.down:
		t.Fatal("the transport failed")
	case <-time.After(15 * time.Second):
		t.Fatal("the transport did not connect")
	}

	// Two people talk at once in the voice chat while we send a tone up.
	var wg sync.WaitGroup
	for ssrc, hz := range map[uint32]float64{1111: 440, 2222: 1000} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sfu.speak(ssrc, hz, 8000, 1500*time.Millisecond)
		}()
	}
	next := time.Now()
	for _, frame := range tone(700, 8000, 75) {
		if err := l.send(frame); err != nil {
			t.Fatal(err)
		}
		next = next.Add(20 * time.Millisecond)
		time.Sleep(time.Until(next))
	}
	wg.Wait()
	time.Sleep(200 * time.Millisecond)

	for ssrc, hz := range map[uint32]float64{1111: 440, 2222: 1000} {
		pcm := heard.get(ssrc)
		if len(pcm) < pcmRate {
			t.Fatalf("speaker %d: heard %d samples, want at least a second", ssrc, len(pcm))
		}
		other := 1440 - hz
		if own, cross := tonePower(pcm, hz), tonePower(pcm, other); own < 3000 || cross > own/10 {
			t.Fatalf("speaker %d: its %v Hz tone has amplitude %.0f and the other speaker's %.0f", ssrc, hz, own, cross)
		}
	}

	sfu.mu.Lock()
	sent, levels := append([]int16(nil), sfu.heard...), sfu.levelIDs[1]
	sfu.mu.Unlock()
	if len(sent) < pcmRate {
		t.Fatalf("the server got %d samples from us, want at least a second", len(sent))
	}
	if power := tonePower(sent, 700); power < 3000 {
		t.Fatalf("our 700 Hz tone reached the server with amplitude %.0f", power)
	}
	if levels < 50 {
		t.Fatalf("only %d of our packets carried the audio level (extension 1)", levels)
	}
}

func TestLinkSendWantsWholeFrames(t *testing.T) {
	l, err := newLink(&collector{})
	if err != nil {
		t.Fatal(err)
	}
	defer l.close()
	if err := l.send(make([]int16, frameSamples-1)); err == nil {
		t.Fatal("expected a short frame to be refused")
	}
}

func TestAnswerSDPNeedsATransport(t *testing.T) {
	var response joinResponse
	if _, err := answerSDP(response); err == nil {
		t.Fatal("expected empty connection params to be refused")
	}
	response.Transport.Ufrag, response.Transport.Pwd = "u", "p"
	response.Transport.Fingerprints = []fingerprint{{Hash: "sha-256", Fingerprint: "AA"}}
	response.Transport.Candidates = []candidate{{Protocol: "tcp", IP: "203.0.113.1", Port: "443"}, {Protocol: "udp", IP: "not-an-ip", Port: "1"}}
	if _, err := answerSDP(response); err == nil {
		t.Fatal("expected params without a usable candidate to be refused")
	}
	response.Transport.Candidates = append(response.Transport.Candidates, candidate{Protocol: "udp", IP: "2001:db8::1", Port: "32000"})
	answer, err := answerSDP(response)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"c=IN IP6 2001:db8::1", "a=setup:active", "a=ice-lite", "a=extmap:1 " + sdp.AudioLevelURI, "typ host"} {
		if !strings.Contains(answer, want) {
			t.Errorf("answer lacks %q:\n%s", want, answer)
		}
	}
	if strings.Contains(answer, "203.0.113.1") {
		t.Errorf("answer kept the TCP candidate:\n%s", answer)
	}
}

// The connection params are read whether the server writes numbers as strings
// (as it does) or as numbers, and whatever else it adds.
func TestConnectionParamsAreReadLeniently(t *testing.T) {
	const params = `{"transport":{"candidates":[
		{"generation":"0","component":"1","protocol":"udp","port":"32000","ip":"203.0.113.7","foundation":"1","id":"x","priority":"2130706431","type":"host","network":"0"},
		{"generation":0,"component":1,"protocol":"udp","port":32001,"ip":"203.0.113.8","foundation":2,"priority":2130706430,"type":"host"}],
		"xmlns":"urn:xmpp:jingle:transports:ice-udp:1","ufrag":"abc","rtcp-mux":true,"pwd":"def",
		"fingerprints":[{"fingerprint":"AA:BB","setup":"active","hash":"sha-256"}]},
		"audio":{"payload-types":[{"id":111,"name":"opus","clockrate":48000,"channels":2,"parameters":{"minptime":10,"useinbandfec":1},"rtcp-fbs":[{"type":"transport-cc"}]}],
		"rtp-hdrexts":[{"id":1,"uri":"urn:ietf:params:rtp-hdrext:ssrc-audio-level"}]},
		"video":{"endpoint":"x"}}`
	var response joinResponse
	if err := json.Unmarshal([]byte(params), &response); err != nil {
		t.Fatal(err)
	}
	answer, err := answerSDP(response)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"m=audio 32000 UDP/TLS/RTP/SAVPF 111",
		"a=candidate:1 1 udp 2130706431 203.0.113.7 32000 typ host",
		"a=candidate:2 1 udp 2130706430 203.0.113.8 32001 typ host",
		"a=ice-ufrag:abc",
		"a=fingerprint:sha-256 AA:BB",
	} {
		if !strings.Contains(answer, want) {
			t.Errorf("answer lacks %q:\n%s", want, answer)
		}
	}
}

func TestAudioLevel(t *testing.T) {
	if got := audioLevel(make([]int16, frameSamples)); got != 127 {
		t.Fatalf("silence has level %d, want 127", got)
	}
	loud := make([]int16, frameSamples)
	for i := range loud {
		loud[i] = 32767
	}
	if got := audioLevel(loud); got != 0 {
		t.Fatalf("full scale has level %d, want 0", got)
	}
	if got := audioLevel(tone(440, 3276, 1)[0]); got < 20 || got > 26 {
		t.Fatalf("a tone at a tenth of full scale has level %d, want about 23", got)
	}
}
