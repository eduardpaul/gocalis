package webrtc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/pion/webrtc/v4"
	"github.com/pion/webrtc/v4/pkg/media"
	opus "gopkg.in/hraban/opus.v2"

	"gocalis/internal/audio"
	"gocalis/internal/config"
)

// talkbackBitrate is the Opus bitrate for the WHIP producer feeding go2rtc. It is
// ample for speech; go2rtc re-encodes it to AAC-ELD downstream for the doorbell.
const talkbackBitrate = 16000

// talkbackSender delivers outbound TTS to a HomeKit doorbell backchannel using
// the proven pocwebrtc mechanism instead of sending Opus over the receive
// PeerConnection (which HomeKit doorbells silently drop):
//
//  1. A dedicated WHIP producer PeerConnection pushes the TTS as an Opus
//     sendonly track into a go2rtc stream (talkback_in).
//  2. The go2rtc streams API is asked to transcode that stream to AAC-ELD and
//     route it into the doorbell's raw HomeKit backchannel:
//     POST /api/streams?dst=<doorbell_raw_homekit>&src=ffmpeg:<talkback_in>#audio=eld
//
// A continuous 20ms Opus stream is paced onto the track (digital silence when
// idle, real speech during playback) so the ELD ffmpeg transcoder and the
// doorbell jitter buffer stay primed between utterances — matching the isochronous
// feed the POC validated end-to-end.
type talkbackSender struct {
	apiBaseURL string
	inStream   string // go2rtc stream fed by the WHIP producer (e.g. talkback_in)
	dstStream  string // HomeKit backchannel stream (e.g. doorbell_raw_homekit)

	pc    *webrtc.PeerConnection
	track *webrtc.TrackLocalStaticSample
	enc   *opus.Encoder

	silence []byte  // one pre-encoded 20ms Opus silence frame
	scratch []byte  // reusable opus encode buffer (play goroutine only)
	pcmIn   []int16 // leftover resampled-to-48k samples not yet framed (play goroutine only)

	resampler *audio.PCMResampler
	playMu    sync.Mutex // serializes whole utterances (push+flush+drain)

	mu              sync.Mutex
	cond            *sync.Cond
	queue           [][]byte // encoded frames awaiting isochronous pacing
	closed          bool
	failure         error
	writeFailures   uint64
	routeFailures   uint64
	maxLateness     int64
	queueHighWater  int
	underruns       uint64
	playing         bool
	playbackStarted time.Time
	firstAudioDelay int64
	firstAudioSent  bool
	inFlight        bool
	done            chan struct{}
	closeOnce       sync.Once
}

// newTalkbackSender establishes the WHIP producer connection to inStream and the
// AAC-ELD route into dstStream. setupCtx bounds the handshake; the caller starts
// run() with a long-lived context afterwards.
func newTalkbackSender(setupCtx context.Context, apiBaseURL, inStream, dstStream string, servers []config.ICEServer) (*talkbackSender, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("talkback register codecs: %w", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))
	var configuredICE []webrtc.ICEServer
	if servers != nil {
		configuredICE = iceServers(servers)
	}
	pc, err := api.NewPeerConnection(webrtc.Configuration{ICEServers: configuredICE})
	if err != nil {
		return nil, fmt.Errorf("talkback peer connection: %w", err)
	}

	track, err := webrtc.NewTrackLocalStaticSample(
		webrtc.RTPCodecCapability{
			MimeType:    webrtc.MimeTypeOpus,
			ClockRate:   48000,
			Channels:    2,
			SDPFmtpLine: "minptime=10;useinbandfec=1",
		},
		"audio", "gocalis-talkback",
	)
	if err != nil {
		pc.Close()
		return nil, fmt.Errorf("talkback track: %w", err)
	}
	if _, err = pc.AddTransceiverFromTrack(track, webrtc.RTPTransceiverInit{
		Direction: webrtc.RTPTransceiverDirectionSendonly,
	}); err != nil {
		pc.Close()
		return nil, fmt.Errorf("talkback transceiver: %w", err)
	}

	enc, err := opus.NewEncoder(opusRate, opusChannels, opus.AppVoIP)
	if err != nil {
		pc.Close()
		return nil, fmt.Errorf("talkback opus encoder: %w", err)
	}
	if err := enc.SetBitrate(talkbackBitrate); err != nil {
		pc.Close()
		return nil, fmt.Errorf("talkback opus bitrate: %w", err)
	}

	t := &talkbackSender{
		apiBaseURL: apiBaseURL,
		inStream:   inStream,
		dstStream:  dstStream,
		pc:         pc,
		track:      track,
		enc:        enc,
		scratch:    make([]byte, 4000),
		done:       make(chan struct{}),
	}
	t.cond = sync.NewCond(&t.mu)

	// Pre-encode a single 20ms Opus silence frame used to keep the feed isochronous.
	sil := make([]byte, 4000)
	n, err := enc.Encode(make([]int16, opusFrameSize), sil)
	if err != nil {
		pc.Close()
		return nil, fmt.Errorf("talkback silence encode: %w", err)
	}
	t.silence = append([]byte(nil), sil[:n]...)

	connected := make(chan struct{})
	pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
		log.Printf("[Talkback] peer connection state: %s", state)
		if state == webrtc.PeerConnectionStateFailed || state == webrtc.PeerConnectionStateDisconnected {
			t.mu.Lock()
			if !t.closed {
				t.failure = fmt.Errorf("talkback peer connection: %s", state)
				t.closed = true
				t.queue = nil
				t.cond.Broadcast()
			}
			t.mu.Unlock()
		}
		if state == webrtc.PeerConnectionStateConnected {
			select {
			case <-connected:
			default:
				close(connected)
			}
		}
	})

	// WHIP: push the Opus producer into the go2rtc inStream (?dst=inStream).
	endpoint, err := webrtcInputURL(apiBaseURL, inStream)
	if err != nil {
		pc.Close()
		return nil, err
	}
	answer, err := exchangeSDPHTTP(setupCtx, pc, endpoint)
	if err != nil {
		pc.Close()
		return nil, err
	}
	if err = pc.SetRemoteDescription(webrtc.SessionDescription{Type: webrtc.SDPTypeAnswer, SDP: answer}); err != nil {
		pc.Close()
		return nil, fmt.Errorf("talkback set remote answer: %w", err)
	}

	select {
	case <-connected:
		log.Printf("[Talkback] producer connected to go2rtc stream %q", inStream)
	case <-setupCtx.Done():
		pc.Close()
		return nil, errors.New("talkback: timed out before WebRTC connection reached connected state")
	}

	// Transcode inStream (Opus) -> AAC-ELD and route into the HomeKit backchannel.
	if err = t.assertRoute(setupCtx); err != nil {
		pc.Close()
		return nil, fmt.Errorf("talkback start AAC-ELD route: %w", err)
	}
	log.Printf("[Talkback] routing %q -> AAC-ELD -> %q", inStream, dstStream)

	return t, nil
}

// assertRoute (re)establishes the go2rtc AAC-ELD bridge that transcodes inStream
// (Opus) into the doorbell's HomeKit backchannel. It is NOT idempotent: go2rtc's
// Stream.Play (internal/streams/play.go) first stops every internal producer of
// the dst stream and then starts a fresh ffmpeg bridge, so each POST tears down a
// live bridge and pays a new ffmpeg start. Call it only when the bridge is known
// to be missing (see ensureRoute) or right after a new WHIP producer came up.
func (t *talkbackSender) assertRoute(ctx context.Context) error {
	source := "ffmpeg:" + t.inStream + "#audio=eld"
	routeURL, err := streamsURL(t.apiBaseURL, t.dstStream, source)
	if err != nil {
		return err
	}
	err = postStreams(ctx, routeURL)
	if err != nil {
		t.mu.Lock()
		t.routeFailures++
		t.mu.Unlock()
	}
	return err
}

// ensureRoute makes sure the AAC-ELD bridge is live before speech is paced. go2rtc
// tears down an idle ffmpeg bridge over time, but re-POSTing a live one would
// restart it (see assertRoute), so the route is re-asserted only when go2rtc no
// longer lists it, or when the listing cannot be read.
func (t *talkbackSender) ensureRoute(ctx context.Context) error {
	if active, err := t.routeActive(ctx); err == nil && active {
		return nil
	} else if err != nil {
		log.Printf("[Talkback] route check failed (%v); re-asserting the AAC-ELD route", err)
	}
	return t.assertRoute(ctx)
}

// run paces the encoded queue (or silence) onto the WebRTC track at 20ms until ctx
// is cancelled. It is the only goroutine that touches queue/silence/track.
func (t *talkbackSender) run(ctx context.Context) {
	defer close(t.done)
	ticker := time.NewTicker(frameMs * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.mu.Lock()
			t.closed = true
			t.queue = nil
			t.cond.Broadcast()
			t.mu.Unlock()
			return
		case scheduled := <-ticker.C:
			t.mu.Lock()
			if late := time.Since(scheduled).Nanoseconds(); late > t.maxLateness {
				t.maxLateness = late
			}
			t.mu.Unlock()
		}

		t.mu.Lock()
		if t.closed {
			t.mu.Unlock()
			return
		}
		frame := t.silence
		if t.playing && t.firstAudioSent && len(t.queue) == 0 {
			t.underruns++
		}
		if len(t.queue) > 0 {
			if t.playing && !t.firstAudioSent {
				t.firstAudioDelay = time.Since(t.playbackStarted).Nanoseconds()
				t.firstAudioSent = true
			}
			frame = t.queue[0]
			t.queue[0] = nil
			t.queue = t.queue[1:]
			t.inFlight = true
			t.cond.Broadcast()
		}
		t.mu.Unlock()

		if err := t.track.WriteSample(media.Sample{
			Data:     frame,
			Duration: frameMs * time.Millisecond,
		}); err != nil {
			t.mu.Lock()
			t.writeFailures++
			t.failure = fmt.Errorf("talkback write: %w", err)
			t.closed = true
			t.queue = nil
			t.inFlight = false
			t.cond.Broadcast()
			t.mu.Unlock()
			return
		}
		t.mu.Lock()
		t.inFlight = false
		t.cond.Broadcast()
		t.mu.Unlock()
	}
}

// pushPCM resamples pcm16 (at srcRate) to 48kHz, frames it into 20ms Opus packets
// and enqueues them for pacing. Leftover sub-frame samples carry over to the next
// call. Callers must hold playMu.
func (t *talkbackSender) pushPCM(ctx context.Context, pcm16 []int16, srcRate int) error {
	if srcRate <= 0 {
		return fmt.Errorf("invalid sample rate")
	}
	if t.resampler == nil {
		t.resampler = audio.NewPCMResampler(srcRate, opusRate)
	}
	t.pcmIn = append(t.pcmIn, t.resampler.Push(pcm16)...)
	for len(t.pcmIn) >= opusFrameSize {
		n, err := t.enc.Encode(t.pcmIn[:opusFrameSize], t.scratch)
		t.pcmIn = t.pcmIn[opusFrameSize:]
		if err != nil {
			return fmt.Errorf("talkback encode: %w", err)
		}
		if err := t.enqueue(ctx, append([]byte(nil), t.scratch[:n]...)); err != nil {
			return err
		}
	}
	return nil
}

func (t *talkbackSender) flushPCM(ctx context.Context) error {
	if t.resampler != nil {
		t.pcmIn = append(t.pcmIn, t.resampler.Flush()...)
		t.resampler = nil
	}
	if len(t.pcmIn) == 0 {
		return nil
	}
	for len(t.pcmIn) >= opusFrameSize {
		n, err := t.enc.Encode(t.pcmIn[:opusFrameSize], t.scratch)
		t.pcmIn = t.pcmIn[opusFrameSize:]
		if err != nil {
			return err
		}
		if err := t.enqueue(ctx, append([]byte(nil), t.scratch[:n]...)); err != nil {
			return err
		}
	}
	if len(t.pcmIn) == 0 {
		return nil
	}
	frame := make([]int16, opusFrameSize)
	copy(frame, t.pcmIn)
	t.pcmIn = nil
	n, err := t.enc.Encode(frame, t.scratch)
	if err != nil {
		return fmt.Errorf("talkback flush encode: %w", err)
	}
	return t.enqueue(ctx, append([]byte(nil), t.scratch[:n]...))
}

func (t *talkbackSender) enqueue(ctx context.Context, frame []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { t.mu.Lock(); t.cond.Broadcast(); t.mu.Unlock() })
	defer stop()
	for len(t.queue) >= 100 && !t.closed && ctx.Err() == nil {
		t.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.closed {
		if t.failure != nil {
			return t.failure
		}
		return io.ErrClosedPipe
	}
	t.queue = append(t.queue, frame)
	if len(t.queue) > t.queueHighWater {
		t.queueHighWater = len(t.queue)
	}
	return nil
}

func (t *talkbackSender) clearPending() {
	t.mu.Lock()
	t.queue = nil
	t.playing = false
	t.pcmIn = nil
	t.resampler = nil
	t.cond.Broadcast()
	t.mu.Unlock()
}

func (t *talkbackSender) waitDrained(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { t.mu.Lock(); t.cond.Broadcast(); t.mu.Unlock() })
	defer stop()
	for (len(t.queue) > 0 || t.inFlight) && !t.closed && ctx.Err() == nil {
		t.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if t.closed {
		if t.failure != nil {
			return t.failure
		}
		return io.ErrClosedPipe
	}
	return nil
}

// close clears the go2rtc route (releasing the doorbell backchannel) and tears
// down the producer PeerConnection.
func (t *talkbackSender) close() {
	t.closeOnce.Do(func() {
		t.mu.Lock()
		t.closed = true
		t.queue = nil
		t.cond.Broadcast()
		t.mu.Unlock()
		if t.pc != nil {
			_ = t.pc.Close()
		}
		<-t.done
		if clearURL, err := streamsURL(t.apiBaseURL, t.dstStream, ""); err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			_ = postStreams(ctx, clearURL)
			cancel()
		}
	})
}

// --- go2rtc HTTP control plane helpers (mirrors pocwebrtc) ---

// exchangeSDPHTTP gathers ICE, posts the SDP offer to a go2rtc endpoint and
// returns the SDP answer.
func exchangeSDPHTTP(ctx context.Context, pc *webrtc.PeerConnection, endpoint string) (string, error) {
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		return "", fmt.Errorf("talkback create offer: %w", err)
	}
	gatherDone := webrtc.GatheringCompletePromise(pc)
	if err = pc.SetLocalDescription(offer); err != nil {
		return "", fmt.Errorf("talkback set local offer: %w", err)
	}
	select {
	case <-gatherDone:
	case <-ctx.Done():
		return "", errors.New("talkback: timed out while gathering ICE candidates")
	}

	log.Printf("[Talkback] posting SDP offer to %s", endpoint)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewBufferString(pc.LocalDescription().SDP))
	if err != nil {
		return "", fmt.Errorf("talkback SDP request: %w", err)
	}
	req.Header.Set("Content-Type", "application/sdp")
	req.Header.Set("Accept", "application/sdp")
	req.Header.Set("User-Agent", "gocalis-talkback")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("talkback post SDP offer: %w", err)
	}
	defer res.Body.Close()

	body, err := io.ReadAll(res.Body)
	if err != nil {
		return "", fmt.Errorf("talkback read SDP answer: %w", err)
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return "", fmt.Errorf("go2rtc returned %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	return string(body), nil
}

// webrtcInputURL builds the WHIP producer endpoint (?dst=...): go2rtc adds the
// posted WebRTC connection as a producer feeding the named stream.
func webrtcInputURL(baseURL, stream string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse go2rtc URL: %w", err)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/api/webrtc"
	q := u.Query()
	q.Set("dst", stream)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// routeActive reports whether go2rtc currently lists the AAC-ELD bridge as a
// producer of the HomeKit backchannel stream. go2rtc accepts a route POST even
// when the bridge dies right after (e.g. while the HomeKit session is still
// coming up after a restart), so the POST result alone proves nothing.
func (t *talkbackSender) routeActive(ctx context.Context) (bool, error) {
	u, err := url.Parse(t.apiBaseURL)
	if err != nil {
		return false, err
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/api/streams"
	u.RawQuery = url.Values{"src": {t.dstStream}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return false, err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return false, err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return false, fmt.Errorf("go2rtc returned %s", res.Status)
	}
	var stream struct {
		Producers []struct {
			URL string `json:"url"`
		} `json:"producers"`
	}
	if err := json.NewDecoder(res.Body).Decode(&stream); err != nil {
		return false, err
	}
	// A bridge added via ?dst= is listed as an rtsp producer whose url is its
	// input, e.g. rtsp://127.0.0.1:8554/talkback_in?audio&source=ffmpeg:talkback_in%23audio%3Deld;
	// a bridge declared in go2rtc.yaml is listed with the ffmpeg: url itself.
	source := "ffmpeg:" + t.inStream + "#audio=eld"
	for _, p := range stream.Producers {
		if p.URL == source {
			return true, nil
		}
		if pu, err := url.Parse(p.URL); err == nil && pu.Query().Get("source") == source {
			return true, nil
		}
	}
	return false, nil
}

func streamsURL(baseURL, dst, src string) (string, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return "", fmt.Errorf("parse go2rtc URL: %w", err)
	}
	u.Path = strings.TrimRight(u.Path, "/") + "/api/streams"
	q := u.Query()
	q.Set("dst", dst)
	q.Set("src", src)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func postStreams(ctx context.Context, endpoint string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, nil)
	if err != nil {
		return err
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return fmt.Errorf("go2rtc returned %s: %s", res.Status, strings.TrimSpace(string(body)))
	}
	return nil
}
