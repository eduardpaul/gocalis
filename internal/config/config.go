package config

import (
	"os"

	"gopkg.in/yaml.v3"
)

// Config holds the main application configuration.
type Config struct {
	GlobalNumThreads int            `yaml:"global_num_threads"`
	Models           ModelsConfig   `yaml:"models"`
	MQTT             MQTTConfig     `yaml:"mqtt"`
	Security         SecurityConfig `yaml:"security"`
	Nodes            []NodeConfig   `yaml:"nodes"`
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
	Engine      string      `yaml:"engine"`
	ModelDir    string      `yaml:"model_dir"`
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
	NodeID    string          `yaml:"node_id"`
	Type      string          `yaml:"type"` // "local" or "rtc_stream"
	Audio     AudioConfig     `yaml:"audio"`
	RTCStream RTCStreamConfig `yaml:"rtc_stream"`
	KWS       KWSConfig       `yaml:"kws"`
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

	return &cfg, nil
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
