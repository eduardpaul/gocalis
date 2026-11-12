package webserver

import (
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

// The intercom page: a standalone page (no dashboard build needed) that embeds
// the go2rtc viewer of a node and streams the browser microphone to gocalis.
//
//go:embed intercom.html
var intercomPage []byte

const (
	// intercomPageCallSeconds is the call length used when the page does not
	// ask for one; intercomMaxCallSeconds caps what it may ask for.
	intercomPageCallSeconds = 300
	intercomMaxCallSeconds  = 1800

	// intercomIdleTimeout ends a call whose page stopped sending: the page
	// streams microphone frames (silence included) every 20 ms, so a gap this
	// long means the browser went away (tab closed, phone locked, network lost).
	intercomIdleTimeout = 5 * time.Second

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

// handleIntercomConfig tells the page which nodes it can talk to and which
// go2rtc viewer to embed for each.
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
	Event           string  `json:"event,omitempty"` // server: intercom_started | intercom_ended | error
	Action          string  `json:"action,omitempty"`
	NodeID          string  `json:"node_id,omitempty"`
	RemoteID        string  `json:"remote_id,omitempty"`
	Status          string  `json:"status,omitempty"`
	Reason          string  `json:"reason,omitempty"`
	Message         string  `json:"message,omitempty"`
	DurationSeconds float64 `json:"duration_seconds,omitempty"`
}

// handleIntercomWS runs one intercom call from the page to a node:
//
//	GET /api/intercom/ws?node_id=front_door[&duration_seconds=300][&token=...]
//
// The call starts when the socket opens. The page sends its microphone as
// binary messages of 16 kHz mono PCM16 little-endian; gocalis mixes it into the
// node's output, which for the doorbell is the one WHIP talkback track and
// AAC-ELD bridge that TTS uses. The page never gets a route of its own to the
// doorbell, and it receives nothing but JSON status messages here: it plays the
// doorbell from go2rtc directly. The call ends when the socket closes, when the
// page sends {"action":"stop"} or stops sending, on its deadline, or when
// something else stops it; the page is then told why ("intercom_ended").
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
	send := func(m intercomMessage) {
		writeMu.Lock()
		defer writeMu.Unlock()
		_ = conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
		_ = conn.WriteJSON(m)
	}

	engine := s.executor.IntercomEngine
	remote := engine.NewRemote()
	call, err := engine.StartRemote(remote, []string{nodeID}, time.Duration(seconds*float64(time.Second)))
	if err != nil {
		send(intercomMessage{Event: "error", NodeID: nodeID, Status: "error", Message: err.Error()})
		return
	}
	log.Printf("[Intercom] page %s (%s) talks to '%s' for up to %.0fs\n", remote.ID, r.RemoteAddr, nodeID, seconds)
	send(intercomMessage{Event: "intercom_started", NodeID: nodeID, RemoteID: remote.ID, Status: "success", DurationSeconds: seconds})

	// When the call ends for any reason, tell the page and close the socket,
	// which also unblocks the read loop below.
	notified := make(chan struct{})
	go func() {
		defer close(notified)
		<-call.Done()
		ev := call.Ended()
		send(intercomMessage{Event: "intercom_ended", NodeID: nodeID, RemoteID: remote.ID, Status: ev.Status, Reason: ev.Reason, Message: ev.Message, DurationSeconds: ev.DurationSeconds})
		writeMu.Lock()
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ev.Reason), time.Now().Add(time.Second))
		writeMu.Unlock()
		_ = conn.Close()
	}()

	conn.SetReadLimit(intercomMaxFrameBytes)
	for {
		_ = conn.SetReadDeadline(time.Now().Add(intercomIdleTimeout))
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
		if json.Unmarshal(data, &m) == nil && m.Action == "stop" {
			break
		}
	}
	call.Stop()
	<-notified
}
