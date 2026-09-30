package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"github.com/AlexxIT/go2rtc/pkg/aac"
	"github.com/AlexxIT/go2rtc/pkg/core"
	"github.com/AlexxIT/go2rtc/pkg/rtsp"
	"github.com/pion/rtp"
	opus "gopkg.in/hraban/opus.v2"
	"net"
	"strings"
	"sync"
	"time"
)

type Status struct {
	DeviceSpec
	Online          bool    `json:"online"`
	Connections     int     `json:"connections"`
	InputLevel      float64 `json:"input_level"`
	OutputLevel     float64 `json:"output_level"`
	MicPackets      uint64  `json:"mic_packets"`
	SpeakerPackets  uint64  `json:"speaker_packets"`
	Overruns        uint64  `json:"overruns"`
	DecodeErrors    uint64  `json:"decode_errors"`
	RecordedSeconds float64 `json:"recorded_seconds"`
	Input           string  `json:"input"`
	LastError       string  `json:"last_error,omitempty"`
}
type Event struct {
	Time   time.Time `json:"time"`
	Device string    `json:"device"`
	Type   string    `json:"type"`
	Detail any       `json:"detail,omitempty"`
}
type Device struct {
	mu          sync.Mutex
	status      Status
	fixture     []int16
	fixturePos  int
	inputEpoch  uint64
	live        []int16
	recording   []int16
	lastCapture time.Time
	sessions    map[net.Conn]bool
	sources     map[*core.Receiver]bool
	events      []Event
}

func newDevice(s DeviceSpec) *Device {
	return &Device{status: Status{DeviceSpec: s, Online: true, Input: "silence"}, sessions: map[net.Conn]bool{}, sources: map[*core.Receiver]bool{}}
}
func (d *Device) event(t string, detail any) {
	if len(d.events) < 10000 {
		d.events = append(d.events, Event{time.Now().UTC(), d.status.ID, t, detail})
	}
}
func (d *Device) snapshot() Status {
	d.mu.Lock()
	defer d.mu.Unlock()
	s := d.status
	if time.Since(d.lastCapture) > 100*time.Millisecond {
		s.OutputLevel = 0
	}
	s.Connections = len(d.sessions)
	s.RecordedSeconds = float64(len(d.recording)) / rate
	return s
}
func (d *Device) play(p []int16) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.status.Input == "live" {
		return fmt.Errorf("live microphone owns this device")
	}
	d.fixture = p
	d.fixturePos = 0
	d.status.Input = "fixture"
	d.event("play", len(p))
	return nil
}
func (d *Device) stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.inputEpoch++
	d.fixture = nil
	d.live = nil
	d.fixturePos = 0
	d.status.Input = "silence"
	d.event("stop", nil)
}
func (d *Device) online(on bool) {
	d.mu.Lock()
	d.status.Online = on
	d.event("online", on)
	if !on {
		for c := range d.sessions {
			_ = c.Close()
		}
	}
	d.mu.Unlock()
}
func (d *Device) next() []int16 {
	d.mu.Lock()
	defer d.mu.Unlock()
	p := make([]int16, 320)
	if d.status.Input == "fixture" {
		n := copy(p, d.fixture[d.fixturePos:])
		d.fixturePos += n
		if d.fixturePos == len(d.fixture) {
			d.fixture = nil
			d.fixturePos = 0
			d.status.Input = "silence"
			d.event("playback_complete", nil)
		}
	}
	if d.status.Input == "live" {
		n := copy(p, d.live)
		d.live = d.live[n:]
	}
	d.status.InputLevel = level(p)
	d.status.MicPackets++
	return p
}
func (d *Device) capture(p []int16, err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.status.SpeakerPackets++
	d.lastCapture = time.Now()
	if err != nil {
		d.status.DecodeErrors++
		d.status.LastError = err.Error()
		return
	}
	d.status.OutputLevel = level(p)
	if len(d.recording)+len(p) <= maxSamples {
		d.recording = append(d.recording, p...)
	} else {
		d.status.Overruns++
	}
}
func (d *Device) audio() []int16 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return append([]int16(nil), d.recording...)
}
func (d *Device) reset() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.recording = nil
	d.events = nil
	d.status.DecodeErrors = 0
	d.status.Overruns = 0
	d.status.OutputLevel = 0
	d.status.LastError = ""
}
func (d *Device) serve(ctx context.Context, c net.Conn) {
	defer c.Close()
	d.mu.Lock()
	if !d.status.Online {
		d.mu.Unlock()
		return
	}
	d.sessions[c] = true
	d.mu.Unlock()
	defer func() { d.mu.Lock(); delete(d.sessions, c); d.mu.Unlock() }()
	stop := context.AfterFunc(ctx, func() { c.Close() })
	defer stop()
	conn := rtsp.NewServer(c)
	var source *core.Receiver
	var eld *eldDecoder
	defer func() {
		if source != nil {
			source.Close()
		}
		if eld != nil {
			eld.close()
		}
	}()
	var setupErr error
	conn.Listen(func(msg any) {
		if msg != rtsp.MethodDescribe {
			return
		}
		if strings.Trim(conn.URL.Path, "/") != d.status.ID {
			setupErr = fmt.Errorf("wrong device path")
			return
		}
		codec := &core.Codec{Name: core.CodecOpus, ClockRate: 48000, Channels: 2, PayloadType: 96}
		media := &core.Media{Kind: core.KindAudio, Direction: core.DirectionSendonly, Codecs: []*core.Codec{codec}}
		source = core.NewReceiver(media, codec)
		setupErr = conn.AddTrack(media, codec, source)
		if setupErr != nil {
			return
		}
		back := &core.Codec{Name: core.CodecOpus, ClockRate: 48000, Channels: 2, PayloadType: 97}
		if d.status.Profile == "doorbell" {
			config := aac.EncodeConfig(aac.TypeAACELD, 16000, 1, false)
			back = &core.Codec{Name: core.CodecAAC, ClockRate: 16000, Channels: 1, PayloadType: 97, FmtpLine: aac.FMTP + hex.EncodeToString(config)}
			eld, setupErr = newELDDecoder(hex.EncodeToString(config))
			if setupErr != nil {
				return
			}
		}
		bm := &core.Media{Kind: core.KindAudio, Direction: core.DirectionRecvonly, Codecs: []*core.Codec{back}}
		receiver, e := conn.GetTrack(bm, back)
		if e != nil {
			setupErr = e
			return
		}

		if eld != nil {
			receiver.Input = func(p *rtp.Packet) {
				b := p.Payload
				if len(b) < 4 {
					d.capture(nil, fmt.Errorf("short AAC RTP"))
					return
				}
				n := int(binary.BigEndian.Uint16(b)) / 8
				if n < 2 || n%2 != 0 || 2+n > len(b) {
					d.capture(nil, fmt.Errorf("invalid AU header"))
					return
				}
				units := b[2+n:]
				for h := 2; h < 2+n; h += 2 {
					size := int(binary.BigEndian.Uint16(b[h:])) >> 3
					if size > len(units) {
						d.capture(nil, fmt.Errorf("truncated AU"))
						return
					}
					pcm, e := eld.decode(units[:size])
					d.capture(pcm, e)
					units = units[size:]
				}
			}
		} else {
			dec, e := opus.NewDecoder(rate, 1)
			if e != nil {
				setupErr = e
				return
			}
			receiver.Input = func(p *rtp.Packet) {
				pcm := make([]int16, 1920)
				n, e := dec.Decode(p.Payload, pcm)
				d.capture(pcm[:n], e)
			}
		}

	})
	_ = c.SetDeadline(time.Now().Add(15 * time.Second))
	if e := conn.Accept(); e != nil || setupErr != nil || source == nil {
		return
	}
	_ = c.SetDeadline(time.Time{})
	done := make(chan struct{})
	go func() { defer close(done); _ = conn.Handle(); c.Close() }()
	d.mu.Lock()
	d.sources[source] = true
	d.mu.Unlock()
	defer func() { d.mu.Lock(); delete(d.sources, source); d.mu.Unlock() }()
	select {
	case <-ctx.Done():
		c.Close()
		<-done
	case <-done:
	}
}
func (d *Device) run(ctx context.Context) {
	enc, err := opus.NewEncoder(rate, 1, opus.AppVoIP)
	if err != nil {
		d.mu.Lock()
		d.status.LastError = err.Error()
		d.mu.Unlock()
		return
	}
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	var seq uint16
	var ts uint32
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		p := d.next()
		b := make([]byte, 4000)
		n, err := enc.Encode(p, b)
		if err != nil {
			continue
		}
		packet := &rtp.Packet{Header: rtp.Header{Version: 2, Marker: true, PayloadType: 96, SequenceNumber: seq, Timestamp: ts, SSRC: 1}, Payload: b[:n]}
		d.mu.Lock()
		for source := range d.sources {
			source.WriteRTP(packet)
		}
		d.mu.Unlock()
		seq++
		ts += 960
	}
}

type Engine struct {
	devices  map[string]*Device
	order    []string
	listener net.Listener
	ctx      context.Context
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

func newEngine(m Manifest) *Engine {
	ctx, cancel := context.WithCancel(context.Background())
	e := &Engine{devices: map[string]*Device{}, ctx: ctx, cancel: cancel}
	for _, s := range m.Devices {
		e.devices[s.ID] = newDevice(s)
		e.order = append(e.order, s.ID)
	}
	return e
}
func (e *Engine) start(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	e.listener = l
	for _, d := range e.devices {
		e.wg.Add(1)
		go func(d *Device) { defer e.wg.Done(); d.run(e.ctx) }(d)
	}
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			e.wg.Add(1)
			go func() {
				defer e.wg.Done()
				stop := context.AfterFunc(e.ctx, func() { c.Close() })
				defer stop()
				// Dispatch without consuming the first RTSP request.
				bc := newPeekConn(c)
				id, err := bc.device()
				if err != nil {
					c.Close()
					return
				}
				d := e.devices[id]
				if d == nil {
					c.Close()
					return
				}
				d.serve(e.ctx, bc)
			}()
		}
	}()
	return nil
}
func (e *Engine) close() {
	e.cancel()
	if e.listener != nil {
		e.listener.Close()
	}
	e.wg.Wait()
}
