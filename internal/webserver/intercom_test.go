package webserver

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
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

// readIntercom returns the next JSON message on the socket, appending the audio
// received before it to heard (when not nil).
func readIntercom(t *testing.T, c *websocket.Conn, heard *[]int16) intercomMessage {
	t.Helper()
	_ = c.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		kind, data, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if kind == websocket.BinaryMessage {
			if heard != nil {
				for i := 0; i+1 < len(data); i += 2 {
					*heard = append(*heard, int16(binary.LittleEndian.Uint16(data[i:])))
				}
			}
			continue
		}
		var m intercomMessage
		if err := json.Unmarshal(data, &m); err != nil {
			t.Fatalf("bad message %q: %v", data, err)
		}
		return m
	}
}

// micFrame is 100 ms of 16 kHz PCM16 with a recognisable value.
func micFrame(value int16) []byte {
	frame := make([]byte, 2*1600)
	for i := 0; i < len(frame); i += 2 {
		binary.LittleEndian.PutUint16(frame[i:], uint16(value))
	}
	return frame
}

// runLength is the longest run of value in samples. A long run is audio that
// went through the mix verbatim, as opposed to a chime happening to pass by the
// value.
func runLength(samples []int16, value int16) int {
	best, run := 0, 0
	for _, v := range samples {
		if v != value {
			run = 0
			continue
		}
		run++
		best = max(best, run)
	}
	return best
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

// TestIntercomWSCarriesAudioBothWays verifies the page's microphone reaches the
// node's output, the node's microphone reaches the page, and the page learns
// when and why the call ended.
func TestIntercomWSCarriesAudioBothWays(t *testing.T) {
	srv, stub, b := newIntercomTestServer(t)
	c, _, err := dialIntercom(t, srv, "node_id=door&token=secret&duration_seconds=1.5")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	m := readIntercom(t, c, nil)
	if m.Event != "intercom_started" || m.RemoteID == "" || !slices.Equal(m.NodeIDs, []string{"door"}) || !slices.Equal(m.RemoteIDs, []string{m.RemoteID}) || m.RemainingSeconds <= 0 {
		t.Fatalf("first message = %+v", m)
	}

	// Let the chime of the call start play out, so the audio below is alone in
	// the mix and arrives verbatim.
	time.Sleep(700 * time.Millisecond)
	if err := c.WriteMessage(websocket.BinaryMessage, micFrame(2000)); err != nil {
		t.Fatal(err)
	}
	b.ForwardRawAudio("door", []float32{0.5, 0.5, 0.5})

	var heard []int16
	m = readIntercom(t, c, &heard)
	if m.Event != "intercom_ended" || m.Reason != "timeout" {
		t.Fatalf("end message = %+v", m)
	}
	if runLength(heard, 16383) < 3 {
		t.Fatal("the page did not hear the node's microphone")
	}
	if runLength(heard, 2000) > 2 {
		t.Fatal("the page heard itself")
	}
	var played []int16
	for _, p := range stub.Played {
		played = append(played, p...)
	}
	if runLength(played, 2000) < 100 {
		t.Fatal("node did not play the page's microphone")
	}
}

// TestIntercomWSPagesShareACall verifies a second page joins the call of the
// first one, that they hear each other, and that the call outlives the page
// that leaves first.
func TestIntercomWSPagesShareACall(t *testing.T) {
	srv, _, b := newIntercomTestServer(t)
	c1, _, err := dialIntercom(t, srv, "node_id=door&token=secret")
	if err != nil {
		t.Fatal(err)
	}
	defer c1.Close()
	first := readIntercom(t, c1, nil).RemoteID

	c2, _, err := dialIntercom(t, srv, "node_id=door&token=secret")
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	m := readIntercom(t, c2, nil)
	if m.Event != "intercom_joined" || m.RemoteID == first || m.ParticipantID != m.RemoteID || !slices.Equal(m.RemoteIDs, []string{first, m.RemoteID}) {
		t.Fatalf("second page got %+v", m)
	}
	second := m.RemoteID
	if m := readIntercom(t, c1, nil); m.Event != "intercom_joined" || m.ParticipantID != second || m.RemoteID != first {
		t.Fatalf("first page got %+v", m)
	}

	// The first page hears the second one (once the join chime is over).
	time.Sleep(700 * time.Millisecond)
	if err := c2.WriteMessage(websocket.BinaryMessage, micFrame(3000)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if err := c2.WriteJSON(map[string]string{"action": "leave"}); err != nil {
		t.Fatal(err)
	}
	if m := readIntercom(t, c2, nil); m.Event != "intercom_left" || m.ParticipantID != second {
		t.Fatalf("leaving page got %+v", m)
	}
	var heard []int16
	if m := readIntercom(t, c1, &heard); m.Event != "intercom_left" || m.ParticipantID != second || !slices.Equal(m.RemoteIDs, []string{first}) {
		t.Fatalf("remaining page got %+v", m)
	}
	if runLength(heard, 3000) < 100 {
		t.Fatal("the first page did not hear the second one")
	}

	// The call goes on for the first page and the node.
	if err := b.PlaySamples(t.Context(), "door", []int16{1}, 16000, 0, brain.CallReject); !errors.Is(err, brain.ErrIntercomActive) {
		t.Fatalf("PlaySamples during the call = %v, want ErrIntercomActive", err)
	}

	// The last page leaves: the call ends and the node is free again.
	if err := c1.WriteJSON(map[string]string{"action": "leave"}); err != nil {
		t.Fatal(err)
	}
	if m := readIntercom(t, c1, nil); m.Event != "intercom_left" || m.ParticipantID != first {
		t.Fatalf("last page got %+v", m)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := b.PlaySamples(t.Context(), "door", []int16{1}, 16000, 0, brain.CallReject)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("node not freed after the call: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
