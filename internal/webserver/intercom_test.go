package webserver

import (
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/intercom"
	"gocalis/internal/node"
	"gocalis/internal/protocol"
)

func newIntercomTestServer(t *testing.T) (*httptest.Server, *audionode.Stub, *brain.Brain) {
	t.Helper()
	b := brain.New(nil)
	stub := &audionode.Stub{}
	p := node.NewPhysicalNode("door", "rtc_stream")
	t.Cleanup(p.Close)
	b.RegisterNode("door", &brain.NodeHandle{Node: p, Audio: stub, Config: config.NodeConfig{NodeID: "door", Type: "rtc_stream"}})
	eng := intercom.NewEngine(b, config.IntercomConfig{DefaultTimeoutSeconds: 30}, func(intercom.Event) {}, nil)
	t.Cleanup(eng.Shutdown)
	s := NewServer("", b, &protocol.Executor{IntercomEngine: eng}, nil, "secret", nil)
	t.Cleanup(s.hub.Close)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/intercom/ws", s.handleIntercomWS)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, stub, b
}

func dialIntercom(t *testing.T, srv *httptest.Server, query string) (*websocket.Conn, *http.Response, error) {
	t.Helper()
	u := "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/intercom/ws?" + query
	return websocket.DefaultDialer.Dial(u, nil)
}

func readIntercom(t *testing.T, c *websocket.Conn) intercomMessage {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	var m intercomMessage
	if err := c.ReadJSON(&m); err != nil {
		t.Fatalf("read: %v", err)
	}
	return m
}

func TestIntercomWSRejectsBadRequests(t *testing.T) {
	srv, _, _ := newIntercomTestServer(t)
	for _, tc := range []struct {
		query string
		code  int
	}{
		{"node_id=door", http.StatusUnauthorized},
		{"node_id=ghost&token=secret", http.StatusNotFound},
		{"node_id=door&token=secret&duration_seconds=99999", http.StatusBadRequest},
	} {
		_, res, err := dialIntercom(t, srv, tc.query)
		if err == nil || res == nil || res.StatusCode != tc.code {
			code := 0
			if res != nil {
				code = res.StatusCode
			}
			t.Fatalf("%s: dial err=%v code=%d, want %d", tc.query, err, code, tc.code)
		}
	}
}

// TestIntercomWSCarriesMicToNode verifies the page's microphone reaches the
// node's output, and the page learns when and why the call ended.
func TestIntercomWSCarriesMicToNode(t *testing.T) {
	srv, stub, _ := newIntercomTestServer(t)
	c, _, err := dialIntercom(t, srv, "node_id=door&token=secret&duration_seconds=0.5")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if m := readIntercom(t, c); m.Event != "intercom_started" || m.NodeID != "door" || m.RemoteID == "" {
		t.Fatalf("first message = %+v", m)
	}

	frame := make([]byte, 640*5) // 100 ms of 16 kHz PCM16
	for i := 0; i < len(frame)/2; i++ {
		binary.LittleEndian.PutUint16(frame[2*i:], uint16(int16(2000+i%5)))
	}
	if err := c.WriteMessage(websocket.BinaryMessage, frame); err != nil {
		t.Fatal(err)
	}
	m := readIntercom(t, c)
	if m.Event != "intercom_ended" || m.Reason != "timeout" {
		t.Fatalf("end message = %+v", m)
	}
	var played []int16
	for _, p := range stub.Played {
		played = append(played, p...)
	}
	found := false
	for _, v := range played {
		if v == 2000 {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("node did not play the page's microphone")
	}
}

// TestIntercomWSStopFreesNode verifies a page stop ends the call and gives the
// node back to TTS.
func TestIntercomWSStopFreesNode(t *testing.T) {
	srv, _, b := newIntercomTestServer(t)
	c, _, err := dialIntercom(t, srv, "node_id=door&token=secret")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	readIntercom(t, c)

	// A second page cannot open its own call to the same node.
	c2, _, err := dialIntercom(t, srv, "node_id=door&token=secret")
	if err != nil {
		t.Fatal(err)
	}
	if m := readIntercom(t, c2); m.Event != "error" {
		t.Fatalf("second page got %+v, want error", m)
	}
	c2.Close()

	if err := c.WriteJSON(map[string]string{"action": "stop"}); err != nil {
		t.Fatal(err)
	}
	if m := readIntercom(t, c); m.Event != "intercom_ended" || m.Reason != "stopped" {
		t.Fatalf("end message = %+v", m)
	}
	if err := b.PlaySamples(t.Context(), "door", []int16{1}, 16000, 0); err != nil {
		t.Fatalf("node not freed after the call: %v", err)
	}
}
