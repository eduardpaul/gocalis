// Package protocol defines the shared request/response types and event publishing
// abstraction used by all transport adapters (WebSocket, MQTT, etc.).
package protocol

import (
	"encoding/json"
	"sync"
)

// Request defines the structure of incoming commands from any transport.
type Request struct {
	Action    string `json:"action"` // "tts", "asr", "speaker_id", "ask", "play", "intercom", "intercom_leave", "intercom_stop", "plugin"
	NodeID    string `json:"node_id"`
	Text      string `json:"text,omitempty"`       // For TTS
	AudioFile string `json:"audio_file,omitempty"` // For ASR and Speaker ID
	Priority  int    `json:"priority,omitempty"`   // For TTS/ASR priority sorting

	// AudioWavBase64 carries a base64-encoded PCM16 WAV recording to play back on
	// a node (action == "play"). It follows the same routing rules as TTS: a
	// node_id of "all" broadcasts to every node, and Priority orders the turn.
	AudioWavBase64 string `json:"audio_wav_base64,omitempty"`

	// CallMode says what "tts", "play" and "ask" do when the node is in an
	// intercom call, where the call owns the node. Omitted, the request fails
	// with "node is in an intercom call". "node" runs it inside the call for
	// that node alone: a tts/play is heard by the node on top of the call, an
	// ask sets the node aside from the call while it prompts and listens.
	// "all" (tts/play only) plays it for everyone in the call. It has no effect
	// on a node that is not in a call.
	CallMode string `json:"call_mode,omitempty"`

	// Ask-specific fields (used when action == "ask").
	ContextID         string  `json:"context_id,omitempty"`
	BargeIn           bool    `json:"barge_in,omitempty"`
	RequireSpeakerID  bool    `json:"require_speaker_id,omitempty"`
	OutputFormat      string  `json:"output_format,omitempty"`
	VADTimeoutSeconds float64 `json:"vad_timeout_seconds,omitempty"`
	// CaptureDelaySeconds is a pointer so an explicit 0 (no delay) can be told
	// apart from "omitted" (use the node's auto_ask_capture_delay_seconds).
	CaptureDelaySeconds      *float64 `json:"capture_delay_seconds,omitempty"`
	PostSpeechSilenceSeconds float64  `json:"post_speech_silence_seconds,omitempty"`

	// Intercom fields. action == "intercom" puts NodeIDs (two or more) in a call
	// together: a new call when none of them is in one, otherwise the others
	// join the call some already are in. DurationSeconds is how long the call
	// lasts from now at least (0 = the configured default). "intercom_leave"
	// takes NodeID out of its call; "intercom_stop" ends the whole call NodeID
	// is in.
	NodeIDs         []string `json:"node_ids,omitempty"`
	DurationSeconds float64  `json:"duration_seconds,omitempty"`

	// Plugin fields. action == "plugin" sends Command to the plugin whose id
	// (from the configuration) is Plugin. NodeID is the node the command is
	// about, when it is about one; Args is a JSON object with the command's
	// own parameters. The commands and their args belong to each plugin type.
	Plugin  string          `json:"plugin,omitempty"`
	Command string          `json:"command,omitempty"`
	Args    json.RawMessage `json:"args,omitempty"`
}

// Response defines the structure of outgoing events and command responses.
type Response struct {
	Event          string `json:"event"` // "tts_completed", "asr_completed", "speaker_id_completed", "wake", "speaker_identified", "state_changed", "error"
	NodeID         string `json:"node_id"`
	Status         string `json:"status,omitempty"`           // "success", "error"
	Text           string `json:"text,omitempty"`             // For ASR result
	AudioWavBase64 string `json:"audio_wav_base64,omitempty"` // base64 PCM16 WAV recording
	Speaker        string `json:"speaker,omitempty"`          // For Speaker ID result
	Keyword        string `json:"keyword,omitempty"`          // For Wake Word event
	Message        string `json:"message,omitempty"`          // For error messages
	AudioFile      string `json:"audio_file,omitempty"`       // For TTS output
	State          string `json:"state,omitempty"`            // For NodeState updates

	// Combined wake event fields. When a wake word triggers AutoAsk, a single
	// "wake" event carries the device (NodeID/Keyword) plus the captured ASR
	// result (Text/Speaker/Status) and, optionally, the recorded audio.
	AutoAsk    bool   `json:"auto_ask,omitempty"`    // true when the wake event includes an ASR turn
	Recording  string `json:"recording,omitempty"`   // base64-encoded WAV (PCM16) of the captured speech
	SampleRate int    `json:"sample_rate,omitempty"` // sample rate of the recording
	Timestamp  int64  `json:"timestamp,omitempty"`   // unix seconds when the event was raised

	// InCall marks a "wake" or "ask_completed" event that happened while the
	// node was in an intercom call: the wake word was said in the call, or the
	// ask ran inside it.
	InCall bool `json:"in_call,omitempty"`

	// Intercom event fields (event == "intercom_started" / "intercom_joined" /
	// "intercom_left" / "intercom_ended"). NodeIDs and RemoteIDs (intercom
	// pages) list who is in the call after the event; ParticipantID is who
	// joined or left. Reason is why the call ended ("timeout", "stopped",
	// "left", "shutdown", "error") and DurationSeconds how long it lasted;
	// the other events carry RemainingSeconds, the time left to its deadline.
	NodeIDs          []string `json:"node_ids,omitempty"`
	RemoteIDs        []string `json:"remote_ids,omitempty"`
	ParticipantID    string   `json:"participant_id,omitempty"`
	Reason           string   `json:"reason,omitempty"`
	DurationSeconds  float64  `json:"duration_seconds,omitempty"`
	RemainingSeconds float64  `json:"remaining_seconds,omitempty"`

	// Plugin is the id of the plugin an event comes from: one of its own
	// events, whose name and Data belong to the plugin type, or the "error"
	// that answers a command it refused.
	Plugin string         `json:"plugin,omitempty"`
	Data   map[string]any `json:"data,omitempty"`
}

// EventPublisher is the common interface for broadcasting events to transports.
type EventPublisher interface {
	Publish(event Response)
}

// MultiPublisher fans out events to every registered publisher.
type MultiPublisher struct {
	mu         sync.RWMutex
	publishers []EventPublisher
}

// NewMultiPublisher creates an empty multi-publisher.
func NewMultiPublisher() *MultiPublisher {
	return &MultiPublisher{}
}

// Add registers a new event publisher.
func (m *MultiPublisher) Add(p EventPublisher) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.publishers = append(m.publishers, p)
}

// Publish sends the event to all registered publishers.
func (m *MultiPublisher) Publish(event Response) {
	m.mu.RLock()
	publishers := append([]EventPublisher(nil), m.publishers...)
	m.mu.RUnlock()
	for _, p := range publishers {
		p.Publish(event)
	}
}
