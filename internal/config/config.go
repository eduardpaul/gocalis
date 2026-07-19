package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds the main application configuration.
type Config struct {
	GlobalNumThreads int            `yaml:"global_num_threads"`
	Models           ModelsConfig   `yaml:"models"`
	MQTT             MQTTConfig     `yaml:"mqtt"`
	Security         SecurityConfig `yaml:"security"`
	Intercom         IntercomConfig `yaml:"intercom"`
	Telegram         TelegramConfig `yaml:"telegram"`
	Nodes            []NodeConfig   `yaml:"nodes"`
}

// TelegramConfig holds the process-wide Telegram user account used by every
// telegram node. Signaling (login, contact/group resolution, call setup) runs
// over the MTProto client (gogram) while call media is carried by ntgcalls.
// A single logged-in user session backs all telegram nodes: group voice chats
// can be joined concurrently (one media source per chat), but Telegram permits
// only ONE active 1:1 private call at a time, so contact-target nodes are
// serialized by the telegram manager.
type TelegramConfig struct {
	// APIID / APIHash are the application credentials from https://my.telegram.org.
	APIID   int    `yaml:"api_id"`
	APIHash string `yaml:"api_hash"`
	// Phone is the user account's phone number (E.164). The first run performs an
	// interactive login (code / 2FA) and persists the session to SessionFile.
	Phone string `yaml:"phone"`
	// SessionFile is where the authenticated MTProto session is stored so later
	// runs start without re-authenticating. Defaults to "./telegram.session".
	SessionFile string `yaml:"session_file"`
}

// GetSessionFile returns the configured session path, defaulting to
// "./telegram.session" when unset.
func (t TelegramConfig) GetSessionFile() string {
	if t.SessionFile != "" {
		return t.SessionFile
	}
	return "./telegram.session"
}

// Enabled reports whether the global Telegram account is configured. When false,
// telegram nodes cannot be brought up.
func (t TelegramConfig) Enabled() bool {
	return t.APIID != 0 && t.APIHash != ""
}

// IntercomConfig holds settings for the two-node intercom bridge feature.
type IntercomConfig struct {
	// DefaultTimeoutSeconds is the default automatic end time for an intercom
	// call when the request does not override it. A call always ends by this
	// deadline unless stopped earlier. Defaults to 60s when unset (<=0).
	DefaultTimeoutSeconds float64 `yaml:"default_timeout_seconds"`

	// EchoCancellation controls the software acoustic echo canceller applied to
	// each node's mic before it is forwarded to the peer, so a device without
	// hardware echo cancellation does not feed its own speaker output (the peer's
	// voice) back into the call.
	EchoCancellation IntercomAECConfig `yaml:"echo_cancellation"`
}

// IntercomAECConfig configures the per-node software echo canceller.
type IntercomAECConfig struct {
	// Enabled turns the software echo canceller on. When false, mics are bridged
	// verbatim (safe only when the devices do hardware AEC or are acoustically
	// isolated).
	Enabled bool `yaml:"enabled"`
	// TailMs is the adaptive-filter length in milliseconds: the echo-path delay
	// budget (network + speaker + acoustic + mic). Defaults to 250ms when unset.
	TailMs int `yaml:"tail_ms"`
	// FrameMs is the processing frame size in milliseconds. Defaults to 20ms.
	FrameMs int `yaml:"frame_ms"`
}

// GetDefaultTimeoutSeconds returns the configured default intercom timeout,
// falling back to 60s when unset (<=0).
func (c IntercomConfig) GetDefaultTimeoutSeconds() float64 {
	if c.DefaultTimeoutSeconds > 0 {
		return c.DefaultTimeoutSeconds
	}
	return 60
}

// GetTailMs returns the configured AEC filter tail length, defaulting to 250ms.
func (a IntercomAECConfig) GetTailMs() int {
	if a.TailMs > 0 {
		return a.TailMs
	}
	return 250
}

// GetFrameMs returns the configured AEC frame size, defaulting to 20ms.
func (a IntercomAECConfig) GetFrameMs() int {
	if a.FrameMs > 0 {
		return a.FrameMs
	}
	return 20
}

// SecurityConfig holds transport hardening settings for the HTTP/WebSocket APIs.
type SecurityConfig struct {
	// AuthToken, when non-empty, is required on control endpoints (via
	// "Authorization: Bearer <token>", "X-Auth-Token: <token>" header, or a
	// "token" query parameter). When empty, control endpoints are unauthenticated.
	AuthToken string `yaml:"auth_token"`

	// AllowedOrigins is the list of Origin header values permitted to open a
	// WebSocket connection. When empty, only same-origin and localhost origins
	// are allowed. Use ["*"] to allow any origin (not recommended).
	AllowedOrigins []string `yaml:"allowed_origins"`
}

// ModelsConfig contains the global configurations for different models.
type ModelsConfig struct {
	VAD       VADConfig       `yaml:"vad"`
	ASR       ASRConfig       `yaml:"asr"`
	SpeakerID SpeakerIDConfig `yaml:"speaker_id"`
	TTS       TTSConfig       `yaml:"tts"`
}

// VADConfig contains Voice Activity Detection model configurations.
type VADConfig struct {
	SileroOnnxPath       string  `yaml:"silero_onnx_path"`
	Threshold            float32 `yaml:"threshold"`
	MinSilenceDurationMs int     `yaml:"min_silence_duration_ms"`
}

// ASRConfig contains global Speech-to-Text configurations.
type ASRConfig struct {
	Encoder string `yaml:"encoder"`
	Decoder string `yaml:"decoder"`
	Tokens  string `yaml:"tokens"`
}

// SpeakerIDConfig contains speaker identification and challenge configurations.
type SpeakerIDConfig struct {
	Model                   string   `yaml:"model"`
	EmbeddingsDir           string   `yaml:"embeddings_dir"`
	MinAudioDurationSeconds float32  `yaml:"min_audio_duration_seconds"`
	ConfidenceThreshold     float32  `yaml:"confidence_threshold"`
	ChallengeFailedPrompt   string   `yaml:"challenge_failed_prompt"`
	ChallengeInitPrompt     string   `yaml:"challenge_init_promt"`
	ChallengePrompts        []string `yaml:"challenge_prompts"`
}

// MQTTConfig contains MQTT broker and topic settings.
type MQTTConfig struct {
	Enabled       bool   `yaml:"enabled"`
	Broker        string `yaml:"broker"`
	ClientID      string `yaml:"client_id"`
	Username      string `yaml:"username"`
	Password      string `yaml:"password"`
	TopicPrefix   string `yaml:"topic_prefix"`
	QoS           int    `yaml:"qos"`
	AutoReconnect bool   `yaml:"auto_reconnect"`
}

// TTSConfig contains global Text-to-Speech configurations.
type TTSConfig struct {
	Engine   string `yaml:"engine"`
	ModelDir string `yaml:"model_dir"`
	// Generation parameters applied to every synthesized utterance.
	// Sid selects the voice/speaker id, NumSteps controls the diffusion steps
	// (Supertonic), Speed scales the utterance duration (1.0 = normal) and Lang
	// is forwarded to the engine as the {"lang": ...} extra hint.
	Sid         int         `yaml:"sid"`
	NumSteps    int         `yaml:"num_steps"`
	Speed       float32     `yaml:"speed"`
	Lang        string      `yaml:"lang"`
	CacheConfig CacheConfig `yaml:"cache"`
}

// CacheConfig holds cache configurations for TTS.
type CacheConfig struct {
	Enabled     bool     `yaml:"enabled"`
	Dir         string   `yaml:"dir"`
	PreGenerate []string `yaml:"pre_generate"`
}

// NodeConfig holds configuration for a specific audio node/channel.
type NodeConfig struct {
	NodeID    string             `yaml:"node_id"`
	Type      string             `yaml:"type"` // "local", "rtc_stream" or "telegram"
	Audio     AudioConfig        `yaml:"audio"`
	RTCStream RTCStreamConfig    `yaml:"rtc_stream"`
	Telegram  TelegramNodeConfig `yaml:"telegram"`
	KWS       KWSConfig          `yaml:"kws"`
}

// TelegramNodeConfig configures a single telegram node: which contact or group
// it talks to, when the call is placed, and how long to wait for a human peer.
type TelegramNodeConfig struct {
	// TargetType selects the call kind: "group" joins a group/channel voice chat
	// (many listeners, multiple such nodes can be active concurrently) or
	// "contact" places a 1:1 private call (one peer, at most one active at a time).
	TargetType string `yaml:"target_type"`

	// Target identifies the peer: a @username, phone number, or numeric chat/user
	// id, resolved against the logged-in account's dialogs/contacts.
	Target string `yaml:"target"`

	// AutoWake places/joins the call on startup and holds it, so wake-word and ask
	// run on the received call audio like an always-on physical node. It is only
	// valid for TargetType "group" (a standing 1:1 call would ring a contact
	// forever); ValidateTelegram rejects autowake on a contact node. When false,
	// the call is placed on demand — only when an ask/play/say/intercom targets
	// the node — and torn down after IdleTimeoutSeconds of no activity.
	AutoWake bool `yaml:"autowake"`

	// ReadyTimeoutSeconds bounds how long EnsureReady waits for a remote peer to
	// join (group) or accept (contact) before failing. Defaults to 60s when unset.
	ReadyTimeoutSeconds float64 `yaml:"ready_timeout_seconds"`

	// IdleTimeoutSeconds is how long an on-demand call lingers with no active
	// stream before the node leaves it. Ignored when AutoWake is true. Defaults to
	// 30s when unset.
	IdleTimeoutSeconds float64 `yaml:"idle_timeout_seconds"`

	// EchoCancellation declares the transport performs acoustic echo cancellation.
	// ntgcalls/WebRTC does its own AEC, so this defaults to true behavior via
	// GetEchoCancellation: the brain's half-duplex mic gate and the intercom's
	// software AEC are skipped for telegram nodes.
	EchoCancellation *bool `yaml:"echo_cancellation"`
}

// GetReadyTimeoutSeconds returns the peer-join wait bound, defaulting to 60s.
func (t TelegramNodeConfig) GetReadyTimeoutSeconds() float64 {
	if t.ReadyTimeoutSeconds > 0 {
		return t.ReadyTimeoutSeconds
	}
	return 60
}

// GetIdleTimeoutSeconds returns the on-demand idle-teardown delay, defaulting to 30s.
func (t TelegramNodeConfig) GetIdleTimeoutSeconds() float64 {
	if t.IdleTimeoutSeconds > 0 {
		return t.IdleTimeoutSeconds
	}
	return 30
}

// GetEchoCancellation reports whether the telegram transport does AEC. It
// defaults to true (ntgcalls/WebRTC cancels echo) unless explicitly disabled.
func (t TelegramNodeConfig) GetEchoCancellation() bool {
	if t.EchoCancellation == nil {
		return true
	}
	return *t.EchoCancellation
}

// EchoCancellationEnabled reports whether this node's transport performs acoustic
// echo cancellation, reading the correct per-type field. Nodes that do their own
// AEC run full-duplex (no half-duplex mic gate while speaking).
func (n *NodeConfig) EchoCancellationEnabled() bool {
	switch n.Type {
	case "telegram":
		return n.Telegram.GetEchoCancellation()
	default:
		return n.RTCStream.EchoCancellation
	}
}

// AudioConfig holds settings for local audio hardware.
type AudioConfig struct {
	InputDeviceIndex  string  `yaml:"input_device_index"`
	OutputDeviceIndex string  `yaml:"output_device_index"`
	SampleRate        int     `yaml:"sample_rate"`
	Channels          int     `yaml:"channels"`
	ChunkSize         int     `yaml:"chunk_size"`
	Gain              float32 `yaml:"gain"`
}

// RTCStreamConfig holds settings for WebRTC connections.
type RTCStreamConfig struct {
	RtspURL      string  `yaml:"rtsp_url"`
	ApiURL       string  `yaml:"api_url"`
	StreamName   string  `yaml:"stream_name"`
	Codec        string  `yaml:"codec"`
	OutputGainDb float32 `yaml:"output_gain_db"`

	// TalkbackStream, when set, routes outbound TTS to a HomeKit doorbell
	// backchannel via the go2rtc streams API (an ffmpeg "#audio=eld" producer
	// posted to this stream) instead of sending Opus over the WebRTC track.
	// The Aqara G4 (and other HomeKit doorbells) only accept AAC-ELD talkback on
	// their raw HomeKit stream, e.g. "doorbell_raw_homekit"; Opus over WebRTC is
	// silently dropped. The WebRTC connection is then used for receive only.
	TalkbackStream string `yaml:"talkback_stream"`

	// TalkbackInStream is the intermediate go2rtc stream that the outbound TTS is
	// pushed into over a WebRTC WHIP producer before go2rtc transcodes it to
	// AAC-ELD for TalkbackStream. Defaults to "talkback_in" when empty.
	TalkbackInStream string `yaml:"talkback_in_stream"`

	// EchoCancellation indicates the device (or transport) performs acoustic echo
	// cancellation. When false (default), the brain runs in half-duplex mode and
	// mutes the capture/VAD path while the node is SPEAKING so the microphone does
	// not pick up the node's own TTS output (preventing self-wake / self-barge-in).
	EchoCancellation bool `yaml:"echo_cancellation"`
}

// KWSConfig holds Wake Word / Keyword Spotting parameters for a node.
type KWSConfig struct {
	Enabled      bool    `yaml:"enabled"`
	Encoder      string  `yaml:"encoder"`
	Decoder      string  `yaml:"decoder"`
	Joiner       string  `yaml:"joiner"`
	Tokens       string  `yaml:"tokens"`
	KeywordsFile string  `yaml:"keywords_file"`
	Threshold    float32 `yaml:"threshold"`
	AutoAsk      bool    `yaml:"auto_ask"`

	// AutoAskBargeIn enables interruption of the wake reply: when true, the user
	// may start speaking over the prompt to interrupt it and go straight to
	// listening. Only meaningful when the transport does acoustic echo
	// cancellation (echo_cancellation: true); in half-duplex the mic is muted
	// during playback so the wake reply always plays out fully before listening.
	AutoAskBargeIn bool `yaml:"auto_ask_barge_in"`

	// AutoAskTimeoutSeconds is how long to wait for the user to start speaking
	// after the wake reply finishes (or is interrupted) before giving up.
	// Defaults to 10s when unset.
	AutoAskTimeoutSeconds float64 `yaml:"auto_ask_timeout_seconds"`

	// AutoAskCaptureDelaySeconds delays the start of user-speech capture after
	// the wake reply finishes playing. In half-duplex transports (no echo
	// cancellation) the doorbell keeps playing the tail of the prompt for a
	// short jitter-buffer window after gocalis has drained the audio; without a
	// delay the mic re-captures that TTS tail and ASR returns empty. Defaults to
	// 1.5s when unset.
	AutoAskCaptureDelaySeconds float64 `yaml:"auto_ask_capture_delay_seconds"`

	// PostSpeechSilenceSeconds is the trailing silence required to conclude an
	// ask turn after speech has started. Lower values reduce the gap between the
	// user stopping and the stop chime. Defaults to 1.5s when unset.
	PostSpeechSilenceSeconds float64 `yaml:"post_speech_silence_seconds"`

	// AutoAskRecord, when true, attaches a base64-encoded WAV recording of the
	// captured user speech to the combined wake event (published to MQTT/WS/etc).
	AutoAskRecord bool `yaml:"auto_ask_record"`

	Priority      int      `yaml:"priority"`
	WakeResponses []string `yaml:"wake_responses"`
	NumThreads    int      `yaml:"num_threads"`
}

// LoadConfig reads and parses a YAML configuration file.
func LoadConfig(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	if cfg.GlobalNumThreads <= 0 {
		cfg.GlobalNumThreads = 4
	}

	if err := cfg.validateTelegramNodes(); err != nil {
		return nil, err
	}

	return &cfg, nil
}

// validateTelegramNodes enforces the invariants for telegram nodes so a bad
// config fails fast at load instead of at call time: a target is required, the
// target type must be "group" or "contact", and autowake is restricted to group
// nodes (a standing 1:1 call would ring a contact indefinitely).
func (c *Config) validateTelegramNodes() error {
	for _, n := range c.Nodes {
		if n.Type != "telegram" {
			continue
		}
		if !c.Telegram.Enabled() {
			return fmt.Errorf("node %q is type telegram but the global 'telegram' account (api_id/api_hash) is not configured", n.NodeID)
		}
		if n.Telegram.Target == "" {
			return fmt.Errorf("telegram node %q requires 'telegram.target'", n.NodeID)
		}
		switch n.Telegram.TargetType {
		case "group", "contact":
		default:
			return fmt.Errorf("telegram node %q has invalid 'telegram.target_type' %q (want \"group\" or \"contact\")", n.NodeID, n.Telegram.TargetType)
		}
		if n.Telegram.AutoWake && n.Telegram.TargetType != "group" {
			return fmt.Errorf("telegram node %q: autowake is only allowed for target_type \"group\" (a standing 1:1 call would ring the contact indefinitely)", n.NodeID)
		}
	}
	return nil
}

// GetKWSNumThreads returns the node-specific KWS num threads, falling back to a default value if not specified (<=0).
func (n *NodeConfig) GetKWSNumThreads(defaultVal int) int {
	if n.KWS.NumThreads > 0 {
		return n.KWS.NumThreads
	}
	return defaultVal
}

// GetKWSThreshold returns the node-specific KWS threshold, falling back to a default value if not specified (0).
func (n *NodeConfig) GetKWSThreshold(defaultVal float32) float32 {
	if n.KWS.Threshold > 0 {
		return n.KWS.Threshold
	}
	return defaultVal
}

// GetAutoAskTimeoutSeconds returns the node-specific AutoAsk listening timeout,
// falling back to defaultVal when not specified (<=0).
func (n *NodeConfig) GetAutoAskTimeoutSeconds(defaultVal float64) float64 {
	if n.KWS.AutoAskTimeoutSeconds > 0 {
		return n.KWS.AutoAskTimeoutSeconds
	}
	return defaultVal
}

// GetAutoAskCaptureDelaySeconds returns the node-specific delay before capture
// starts after the wake reply, falling back to defaultVal when not specified
// (<0). A value of 0 is honored (no delay) so full-duplex nodes can opt out.
func (n *NodeConfig) GetAutoAskCaptureDelaySeconds(defaultVal float64) float64 {
	if n.KWS.AutoAskCaptureDelaySeconds < 0 {
		return defaultVal
	}
	if n.KWS.AutoAskCaptureDelaySeconds == 0 {
		return defaultVal
	}
	return n.KWS.AutoAskCaptureDelaySeconds
}

// GetPostSpeechSilenceSeconds returns the node-specific trailing silence needed
// to close an ask turn after speech has started, falling back to defaultVal
// when not specified (<=0).
func (n *NodeConfig) GetPostSpeechSilenceSeconds(defaultVal float64) float64 {
	if n.KWS.PostSpeechSilenceSeconds > 0 {
		return n.KWS.PostSpeechSilenceSeconds
	}
	return defaultVal
}
