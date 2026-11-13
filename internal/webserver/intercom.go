package webserver

import (
	"context"
	_ "embed"
	"encoding/binary"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"gocalis/internal/httpsec"
	"gocalis/internal/intercom"
)

// The intercom page: a standalone page (no dashboard build needed) that joins
// the call of a node. It embeds the go2rtc viewer of that node, streams the
// browser microphone to gocalis and plays the rest of the call gocalis sends.
//
//go:embed intercom.html
var intercomPage []byte

const (
	// intercomPageCallSeconds is the call length used when the page does not
	// ask for one; intercomMaxCallSeconds caps what it may ask for.
	intercomPageCallSeconds = 300
	intercomMaxCallSeconds  = 1800

	// A page is known to be there by answering pings, which a browser does on
	// its own whether or not the page sends its microphone: intercomPingInterval
	// is how often it is pinged, and intercomIdleTimeout how long it may stay
	// silent (no pong, no message) before it is taken out of the call. That is
	// a browser that went away without closing the socket (network lost, device
	// asleep).
	intercomPingInterval = 2 * time.Second
	intercomIdleTimeout  = 6 * time.Second

	// intercomMaxFrameBytes bounds one microphone message (1 s of 16 kHz PCM16).
	intercomMaxFrameBytes = 2 * intercom.RemoteSampleRate
)

// SetIntercomViewers sets the go2rtc viewer page the intercom page embeds for
// each node (config intercom.viewers).
func (s *Server) SetIntercomViewers(viewers map[string]string) {
	s.intercomViewers = viewers
}

// handleIntercomPage serves the intercom page. It holds no secrets: the page
// asks for the API token like the dashboard does.
func (s *Server) handleIntercomPage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	_, _ = w.Write(intercomPage)
}

// handleIntercomConfig tells the page which nodes it can call and which go2rtc
// viewer to embed for each.
func (s *Server) handleIntercomConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var nodes []string
	if s.brain != nil {
		for _, n := range s.brain.ListNodes() {
			nodes = append(nodes, n.NodeID)
		}
	}
	viewers := s.intercomViewers
	if viewers == nil {
		viewers = map[string]string{}
	}
	s.writeJSON(w, map[string]any{
		"nodes":                    nodes,
		"viewers":                  viewers,
		"sample_rate":              intercom.RemoteSampleRate,
		"default_duration_seconds": intercomPageCallSeconds,
		"max_duration_seconds":     intercomMaxCallSeconds,
	})
}

// intercomMessage is a JSON message on the intercom socket.
type intercomMessage struct {
	Action string `json:"action,omitempty"` // page: "leave"

	// Server: "intercom_started" | "intercom_joined" | "intercom_left" |
	// "intercom_ended" (the events of the call the page is in) | "error".
	Event            string   `json:"event,omitempty"`
	RemoteID         string   `json:"remote_id,omitempty"`      // the page's own id in the call
	ParticipantID    string   `json:"participant_id,omitempty"` // who joined or left
	NodeIDs          []string `json:"node_ids,omitempty"`       // nodes in the call
	RemoteIDs        []string `json:"remote_ids,omitempty"`     // pages in the call
	Status           string   `json:"status,omitempty"`
	Reason           string   `json:"reason,omitempty"`
	Message          string   `json:"message,omitempty"`
	DurationSeconds  float64  `json:"duration_seconds,omitempty"`
	RemainingSeconds float64  `json:"remaining_seconds,omitempty"`
}

// handleIntercomWS puts the page in the call of a node:
//
//	GET /api/intercom/ws?node_id=front_door[&duration_seconds=300][&token=...]
//
// The page joins the call the node is in, or starts one with it, when the
// socket opens. Sending a microphone is up to the page: it may join to listen
// and open its microphone later, or never. When it does, it sends 16 kHz mono
// PCM16 little-endian as binary messages; gocalis mixes it into the call, which
// reaches the node through its normal output (for the doorbell, the one WHIP
// talkback track and AAC-ELD bridge that TTS uses). The page gets binary
// messages in the same format with the rest of the call (the other pages and
// nodes, the chimes), minus the node it shows when that one has a go2rtc
// viewer: the page plays it from there. It also gets the events of the call as
// JSON. The page leaves the call when the socket closes, when it sends
// {"action":"leave"} or when it stops answering pings; the call goes on for the
// others unless fewer than two remain.
func (s *Server) handleIntercomWS(w http.ResponseWriter, r *http.Request) {
	if !httpsec.TokenValid(r, s.authToken) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if s.executor == nil || s.executor.IntercomEngine == nil {
		http.Error(w, "intercom unavailable", http.StatusServiceUnavailable)
		return
	}
	q := r.URL.Query()
	nodeID := q.Get("node_id")
	if nodeID == "" || s.brain == nil || s.brain.GetNodeHandle(nodeID) == nil {
		http.Error(w, "unknown node_id", http.StatusNotFound)
		return
	}
	seconds := float64(intercomPageCallSeconds)
	if v := q.Get("duration_seconds"); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f <= 0 || f > intercomMaxCallSeconds {
			http.Error(w, "duration_seconds must be in (0, 1800]", http.StatusBadRequest)
			return
		}
		seconds = f
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Intercom] page WebSocket upgrade failed: %v\n", err)
		return
	}
	defer conn.Close()

	var writeMu sync.Mutex
	write := func(kind int, data []byte) error {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		return conn.WriteMessage(kind, data)
	}
	send := func(m intercomMessage) {
		if data, err := json.Marshal(m); err == nil {
			_ = write(websocket.TextMessage, data)
		}
	}

	// The engine must not be held up by a slow page: its events are queued
	// here (each one carries the whole list of participants, so a dropped one
	// is made up for by the next).
	events := make(chan intercom.Event, 32)
	engine := s.executor.IntercomEngine
	remote := engine.NewRemote(func(ev intercom.Event) {
		select {
		case events <- ev:
		default:
		}
	})
	_, playsNode := s.intercomViewers[nodeID]
	if err := engine.ConnectRemote(remote, nodeID, playsNode, time.Duration(seconds*float64(time.Second))); err != nil {
		send(intercomMessage{Event: "error", Status: "error", Message: err.Error()})
		return
	}
	log.Printf("[Intercom] page %s (%s) joins the call of '%s'\n", remote.ID, r.RemoteAddr, nodeID)
	sendEvent := func(ev intercom.Event) {
		send(intercomMessage{
			Event:            ev.Event,
			RemoteID:         remote.ID,
			ParticipantID:    ev.ParticipantID,
			NodeIDs:          ev.NodeIDs,
			RemoteIDs:        ev.RemoteIDs,
			Status:           ev.Status,
			Reason:           ev.Reason,
			Message:          ev.Message,
			DurationSeconds:  ev.DurationSeconds,
			RemainingSeconds: ev.RemainingSeconds,
		})
	}

	// Send the page the call's mix until the page is out of the call.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mixed := make(chan struct{})
	go func() {
		defer close(mixed)
		for {
			pcm, err := remote.Read(ctx)
			if len(pcm) > 0 {
				data := make([]byte, 2*len(pcm))
				for i, v := range pcm {
					binary.LittleEndian.PutUint16(data[2*i:], uint16(v))
				}
				if write(websocket.BinaryMessage, data) != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()

	// Send the page the events of its call. Once it is out of the call (it left
	// or the call ended) and it has been told, close the socket, which also
	// unblocks the read loop below.
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for out := false; !out; {
			select {
			case ev := <-events:
				sendEvent(ev)
			case <-remote.Left():
				out = true
			}
		}
		<-mixed
		for more := true; more; {
			select {
			case ev := <-events:
				sendEvent(ev)
			default:
				more = false
			}
		}
		writeMu.Lock()
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""), time.Now().Add(time.Second))
		writeMu.Unlock()
		_ = conn.Close()
	}()

	// Ping the page for as long as it is in the call. Its pongs, like anything
	// else it sends, push the read deadline back.
	go func() {
		ticker := time.NewTicker(s.intercomPing)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(time.Second))
			}
		}
	}()
	alive := func() { _ = conn.SetReadDeadline(time.Now().Add(s.intercomIdle)) }
	conn.SetPongHandler(func(string) error {
		alive()
		return nil
	})

	conn.SetReadLimit(intercomMaxFrameBytes)
	for {
		alive()
		kind, data, err := conn.ReadMessage()
		if err != nil {
			break
		}
		if kind == websocket.BinaryMessage {
			pcm := make([]int16, len(data)/2)
			for i := range pcm {
				pcm[i] = int16(binary.LittleEndian.Uint16(data[2*i:]))
			}
			remote.Push(pcm)
			continue
		}
		var m intercomMessage
		if json.Unmarshal(data, &m) == nil && m.Action == "leave" {
			break
		}
	}
	engine.Leave(remote.ID)
	cancel()
	<-closed
}
