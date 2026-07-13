// Command wsask is a small manual-testing WebSocket client for the gocalis
// Node-RED API (:9090/ws). It connects, sends one protocol.Request (e.g. an
// "ask" action), and prints every event the server broadcasts until it sees the
// terminal event for that action or the timeout elapses.
//
// Usage:
//
//	go run ./scripts/wsask -action ask -node front_door -tts "¿Qué desea?" -timeout 30s
package main

import (
	"encoding/json"
	"flag"
	"log"
	"time"

	"github.com/gorilla/websocket"
)

func main() {
	addr := flag.String("addr", "ws://localhost:9090/ws", "gocalis WebSocket URL")
	action := flag.String("action", "ask", "action: ask, tts, asr, speaker_id")
	node := flag.String("node", "front_door", "node_id")
	tts := flag.String("tts", "", "optional TTS prompt spoken before capture (ask) / text (tts)")
	vadTimeout := flag.Float64("vad-timeout", 6, "VAD silence timeout seconds (ask)")
	priority := flag.Int("priority", 10, "job priority")
	bargeIn := flag.Bool("barge-in", false, "allow barge-in during the prompt (ask)")
	timeout := flag.Duration("timeout", 30*time.Second, "overall wait for the result")
	flag.Parse()

	conn, _, err := websocket.DefaultDialer.Dial(*addr, nil)
	if err != nil {
		log.Fatalf("dial %s: %v", *addr, err)
	}
	defer conn.Close()

	req := map[string]any{
		"action":              *action,
		"node_id":             *node,
		"text":                *tts,
		"priority":            *priority,
		"barge_in":            *bargeIn,
		"vad_timeout_seconds": *vadTimeout,
	}
	payload, _ := json.Marshal(req)
	if err := conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		log.Fatalf("send: %v", err)
	}
	log.Printf("-> sent: %s", payload)

	terminal := *action + "_completed"
	_ = conn.SetReadDeadline(time.Now().Add(*timeout))
	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			log.Fatalf("read (or timeout): %v", err)
		}
		log.Printf("<- event: %s", msg)

		var ev struct {
			Event string `json:"event"`
		}
		if json.Unmarshal(msg, &ev) == nil && ev.Event == terminal {
			log.Printf("=== done (%s) ===", terminal)
			return
		}
	}
}
