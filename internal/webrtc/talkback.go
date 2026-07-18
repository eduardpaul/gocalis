package webrtc

import (
	"bytes"
	"context"
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

	playMu sync.Mutex // serializes whole utterances (push+flush+drain)

	mu     sync.Mutex
	cond   *sync.Cond
	queue  [][]byte // encoded frames awaiting isochronous pacing
	closed bool
}

// newTalkbackSender establishes the WHIP producer connection to inStream and the
// AAC-ELD route into dstStream. setupCtx bounds the handshake; the caller starts
// run() with a long-lived context afterwards.
func newTalkbackSender(setupCtx context.Context, apiBaseURL, inStream, dstStream string) (*talkbackSender, error) {
	m := &webrtc.MediaEngine{}
	if err := m.RegisterDefaultCodecs(); err != nil {
		return nil, fmt.Errorf("talkback register codecs: %w", err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))
	pc, err := api.NewPeerConnection(webrtc.Configuration{})
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
// (Opus) into the doorbell's HomeKit backchannel. go2rtc tears down the idle
// ffmpeg transcoder over time, so this must be called before each utterance to
// guarantee the bridge is live when speech is paced. Re-POSTing is idempotent:
// go2rtc dedups producers by source URL, so an already-live bridge is untouched.
func (t *talkbackSender) assertRoute(ctx context.Context) error {
	source := "ffmpeg:" + t.inStream + "#audio=eld"
	routeURL, err := streamsURL(t.apiBaseURL, t.dstStream, source)
	if err != nil {
		return err
	}
	return postStreams(ctx, routeURL)
}

// run paces the encoded queue (or silence) onto the WebRTC track at 20ms until ctx
// is cancelled. It is the only goroutine that touches queue/silence/track.
func (t *talkbackSender) run(ctx context.Context) {
	ticker := time.NewTicker(frameMs * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			t.mu.Lock()
			t.closed = true
			t.cond.Broadcast()
			t.mu.Unlock()
			return
		case <-ticker.C:
		}

		t.mu.Lock()
		frame := t.silence
		if len(t.queue) > 0 {
			frame = t.queue[0]
			t.queue = t.queue[1:]
			if len(t.queue) == 0 {
				t.cond.Broadcast()
			}
		}
		t.mu.Unlock()

		if err := t.track.WriteSample(media.Sample{
			Data:     frame,
			Duration: frameMs * time.Millisecond,
		}); err != nil {
			if !errors.Is(err, io.ErrClosedPipe) {
				log.Printf("[Talkback] write sample: %v", err)
			}
		}
	}
}

// pushPCM resamples pcm16 (at srcRate) to 48kHz, frames it into 20ms Opus packets
// and enqueues them for pacing. Leftover sub-frame samples carry over to the next
// call. Callers must hold playMu.
func (t *talkbackSender) pushPCM(pcm16 []int16, srcRate int) {
	t.pcmIn = append(t.pcmIn, audio.ResampleInt16(pcm16, srcRate, opusRate)...)
	var frames [][]byte
	for len(t.pcmIn) >= opusFrameSize {
		n, err := t.enc.Encode(t.pcmIn[:opusFrameSize], t.scratch)
		t.pcmIn = t.pcmIn[opusFrameSize:]
		if err != nil {
			log.Printf("[Talkback] opus encode: %v", err)
			continue
		}
		frames = append(frames, append([]byte(nil), t.scratch[:n]...))
	}
	t.enqueue(frames)
}

// flushPCM pads and encodes the final partial frame. Callers must hold playMu.
func (t *talkbackSender) flushPCM() {
	if len(t.pcmIn) == 0 {
		return
	}
	frame := make([]int16, opusFrameSize) // zero-padded PCM silence
	copy(frame, t.pcmIn)
	t.pcmIn = nil
	n, err := t.enc.Encode(frame, t.scratch)
	if err != nil {
		log.Printf("[Talkback] opus flush encode: %v", err)
		return
	}
	t.enqueue([][]byte{append([]byte(nil), t.scratch[:n]...)})
}

func (t *talkbackSender) enqueue(frames [][]byte) {
	if len(frames) == 0 {
		return
	}
	t.mu.Lock()
	t.queue = append(t.queue, frames...)
	t.mu.Unlock()
}

// waitDrained blocks until every queued frame has been paced onto the track, the
// sender is closed, or ctx is cancelled.
func (t *talkbackSender) waitDrained(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		t.mu.Lock()
		for len(t.queue) > 0 && !t.closed && ctx.Err() == nil {
			t.cond.Wait()
		}
		t.mu.Unlock()
		close(done)
	}()

	select {
	case <-done:
		return ctx.Err()
	case <-ctx.Done():
		t.mu.Lock()
		t.cond.Broadcast()
		t.mu.Unlock()
		<-done
		return ctx.Err()
	}
}

// close clears the go2rtc route (releasing the doorbell backchannel) and tears
// down the producer PeerConnection.
func (t *talkbackSender) close() {
	if clearURL, err := streamsURL(t.apiBaseURL, t.dstStream, ""); err == nil {
		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = postStreams(cctx, clearURL)
		cancel()
	}
	if t.pc != nil {
		_ = t.pc.Close()
	}
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
