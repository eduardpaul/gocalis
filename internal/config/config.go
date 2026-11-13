package config

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// Config holds the main application configuration.
type Config struct {
	GlobalNumThreads int            `yaml:"global_num_threads"`
	Models           ModelsConfig   `yaml:"models"`
	MQTT             MQTTConfig     `yaml:"mqtt"`
	Security         SecurityConfig `yaml:"security"`
	Intercom         IntercomConfig `yaml:"intercom"`
	Plugins          []PluginConfig `yaml:"plugins"`
	Nodes            []NodeConfig   `yaml:"nodes"`
}

// PluginConfig switches one plugin on. ID is how API requests address it; Type
// selects the plugin code compiled into this build; Settings belong to that
// type, which reads them with DecodeSettings.
type PluginConfig struct {
	ID       string    `yaml:"id"`
	Type     string    `yaml:"type"`
	Settings yaml.Node `yaml:"settings"`
}

// DecodeSettings reads the plugin's settings into its own struct. Like the
// rest of the configuration, a key the struct does not have is an error.
// Without settings the struct is left as it is.
func (p PluginConfig) DecodeSettings(into any) error {
	if p.Settings.Kind == 0 {
		return nil
	}
	raw, err := yaml.Marshal(&p.Settings)
	if err != nil {
		return err
	}
	decoder := yaml.NewDecoder(bytes.NewReader(raw))
	decoder.KnownFields(true)
	if err := decoder.Decode(into); err != nil {
		return fmt.Errorf("settings: %w", err)
	}
	return nil
}

// IntercomConfig holds settings for intercom calls: conferences that nodes and
// intercom web pages (/intercom) join and leave.
type IntercomConfig struct {
	// DefaultTimeoutSeconds is how long a call lasts after the last join when
	// the request does not say. A call always ends by its deadline unless it
	// ends earlier. Defaults to 60s when unset (<=0).
	DefaultTimeoutSeconds float64 `yaml:"default_timeout_seconds"`

	// EchoCancellation controls the software acoustic echo canceller applied to
	// each node's mic before it is mixed into the call, so a device without
	// hardware echo cancellation does not feed its own speaker output (the
	// others' voices) back into the call.
	EchoCancellation IntercomAECConfig `yaml:"echo_cancellation"`

	// Viewers maps a node id to the go2rtc page the intercom web page embeds to
	// show and play that node's camera and microphone, e.g.
	// "https://go2rtc.example/stream.html?src=doorbell_tv&mode=webrtc". The page
	// is a plain go2rtc viewer (go2rtc fans the stream out); gocalis does not
	// relay that node's audio or video to the page, only the rest of the call.
	// It must be reachable from the browser (over HTTPS when the intercom page
	// is). Optional: a node without a viewer is heard through gocalis.
	Viewers map[string]string `yaml:"viewers"`
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
	// AuthToken, when non-empty, is required on controls and event sockets (via
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
	Engine   string `yaml:"engine"` // whisper or moonshine (merged decoder)
	Language string `yaml:"language"`
	Encoder  string `yaml:"encoder"`
	Decoder  string `yaml:"decoder"`
	Tokens   string `yaml:"tokens"`
}

// SpeakerIDConfig contains speaker identification and challenge configurations.
type SpeakerIDConfig struct {
	Model                   string   `yaml:"model"`
	EmbeddingsDir           string   `yaml:"embeddings_dir"`
	MinAudioDurationSeconds float32  `yaml:"min_audio_duration_seconds"`
	ConfidenceThreshold     float32  `yaml:"confidence_threshold"`
	ChallengeFailedPrompt   string   `yaml:"challenge_failed_prompt"`
	ChallengeInitPrompt     string   `yaml:"challenge_init_prompt"`
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
	Model    string `yaml:"model"`
	Tokens   string `yaml:"tokens"`
	DataDir  string `yaml:"data_dir"`
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
// DiagnosticsConfig is opt-in; recording_dir retains two bounded WAVs per node.
type DiagnosticsConfig struct {
	Enabled      bool   `yaml:"enabled"`
	RecordingDir string `yaml:"recording_dir"`
}

type NodeConfig struct {
	Diagnostics DiagnosticsConfig `yaml:"diagnostics"`
	NodeID      string            `yaml:"node_id"`
	Type        string            `yaml:"type"` // "local" or "rtc_stream"
	Audio       AudioConfig       `yaml:"audio"`
	RTCStream   RTCStreamConfig   `yaml:"rtc_stream"`
	KWS         KWSConfig         `yaml:"kws"`
}

// AudioConfig holds settings for local audio hardware.
type AudioConfig struct {
	InputDeviceIndex  string  `yaml:"input_device_index"`
	OutputDeviceIndex string  `yaml:"output_device_index"`
	SampleRate        int     `yaml:"sample_rate"`
	Gain              float32 `yaml:"gain"`
}

// RTCStreamConfig holds settings for WebRTC connections.
type ICEServer struct {
	URLs       []string `yaml:"urls"`
	Username   string   `yaml:"username"`
	Credential string   `yaml:"credential"`
}

type RTCStreamConfig struct {
	// Nil preserves the default STUN server; an explicit empty list disables it.
	ICEServers   []ICEServer `yaml:"ice_servers"`
	ApiURL       string      `yaml:"api_url"`
	StreamName   string      `yaml:"stream_name"`
	OutputGainDb float32     `yaml:"output_gain_db"`

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

	// WakeInCall keeps the wake word armed while the node is in an intercom
	// call. Saying it sets the node aside from the call (the others stop hearing
	// it, and it stops hearing them) while the assistant listens to the command,
	// as it would outside a call; the "wake" event then carries in_call. Off by
	// default: enable it only on nodes whose microphone does not pick up their
	// own speaker (hardware echo cancellation), or anyone in the call can wake
	// the assistant through that speaker.
	WakeInCall bool `yaml:"wake_in_call"`

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
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, err
	}

	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, fmt.Errorf("configuration must contain one YAML document")
	}
	if err := cfg.normalizeAndValidate(); err != nil {
		return nil, err
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

// GetAutoAskTimeoutSeconds returns the node-specific AutoAsk listening timeout,
// falling back to defaultVal when not specified (<=0).
func (n *NodeConfig) GetAutoAskTimeoutSeconds(defaultVal float64) float64 {
	if n.KWS.AutoAskTimeoutSeconds > 0 {
		return n.KWS.AutoAskTimeoutSeconds
	}
	return defaultVal
}

// GetAutoAskCaptureDelaySeconds returns the normalized capture delay.
// Zero disables the delay; negative values select the supplied default.
func (n *NodeConfig) GetAutoAskCaptureDelaySeconds(defaultVal float64) float64 {
	if n.KWS.AutoAskCaptureDelaySeconds < 0 {
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

func (c *Config) normalizeAndValidate() error {
	if c.GlobalNumThreads == 0 {
		c.GlobalNumThreads = 4
	}
	if c.GlobalNumThreads < 1 || c.GlobalNumThreads > 256 {
		return fmt.Errorf("global_num_threads must be in [1,256]")
	}
	if c.Models.ASR.Engine != "whisper" && c.Models.ASR.Engine != "moonshine" {
		return fmt.Errorf("models.asr.engine must be whisper or moonshine")
	}
	if c.Models.ASR.Language == "" {
		c.Models.ASR.Language = "es"
	}
	if c.Models.TTS.Engine != "vits" && c.Models.TTS.Engine != "supertonic" {
		return fmt.Errorf("models.tts.engine must be vits or supertonic")
	}
	paths := map[string]string{"models.asr.encoder": c.Models.ASR.Encoder, "models.asr.decoder": c.Models.ASR.Decoder, "models.asr.tokens": c.Models.ASR.Tokens, "models.vad.silero_onnx_path": c.Models.VAD.SileroOnnxPath, "models.speaker_id.model": c.Models.SpeakerID.Model}
	if c.Models.TTS.Engine == "supertonic" {
		paths["models.tts.model_dir"] = c.Models.TTS.ModelDir
	} else {
		paths["models.tts.model"] = c.Models.TTS.Model
		paths["models.tts.tokens"] = c.Models.TTS.Tokens
		paths["models.tts.data_dir"] = c.Models.TTS.DataDir
	}
	for name, path := range paths {
		if strings.TrimSpace(path) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if c.Models.VAD.Threshold == 0 {
		c.Models.VAD.Threshold = 0.5
	}
	if !validNumber(float64(c.Models.VAD.Threshold), 0.01, 1) {
		return fmt.Errorf("VAD threshold must be in (0,1]")
	}
	if c.Models.VAD.MinSilenceDurationMs == 0 {
		c.Models.VAD.MinSilenceDurationMs = 700
	}
	if c.Models.VAD.MinSilenceDurationMs < 1 || c.Models.VAD.MinSilenceDurationMs > 20000 {
		return fmt.Errorf("VAD silence duration must be in [1,20000] ms")
	}
	if !validNumber(float64(c.Models.SpeakerID.ConfidenceThreshold), 0, 1) || !validNumber(float64(c.Models.SpeakerID.MinAudioDurationSeconds), 0, 120) {
		return fmt.Errorf("invalid speaker confidence or duration")
	}
	if c.Models.TTS.Speed == 0 {
		c.Models.TTS.Speed = 1
	}
	if !validNumber(float64(c.Models.TTS.Speed), 0.1, 10) || c.Models.TTS.Sid < 0 || c.Models.TTS.NumSteps < 0 || c.Models.TTS.NumSteps > 100 {
		return fmt.Errorf("invalid TTS speed, speaker or step count")
	}
	if c.Models.TTS.CacheConfig.Enabled && c.Models.TTS.CacheConfig.Dir == "" {
		return fmt.Errorf("TTS cache directory is required when caching is enabled")
	}
	for _, text := range append(append([]string{}, c.Models.TTS.CacheConfig.PreGenerate...), c.Models.SpeakerID.ChallengePrompts...) {
		if len(text) > 4000 {
			return fmt.Errorf("configured phrase exceeds 4000 bytes")
		}
	}
	if c.MQTT.QoS < 0 || c.MQTT.QoS > 2 {
		return fmt.Errorf("mqtt.qos must be in [0,2]")
	}
	if c.MQTT.Enabled {
		u, err := url.Parse(c.MQTT.Broker)
		if err != nil || u.Host == "" || (u.Scheme != "tcp" && u.Scheme != "ssl" && u.Scheme != "ws" && u.Scheme != "wss") {
			return fmt.Errorf("invalid MQTT broker URL")
		}
	}
	ids := make(map[string]bool)
	for i := range c.Nodes {
		n := &c.Nodes[i]
		if strings.TrimSpace(n.NodeID) == "" || n.NodeID == "all" || ids[n.NodeID] {
			return fmt.Errorf("node IDs must be unique, nonempty and cannot be all: %q", n.NodeID)
		}
		ids[n.NodeID] = true
		if n.Type != "local" && n.Type != "rtc_stream" {
			return fmt.Errorf("unsupported node type %q", n.Type)
		}
		if n.Type == "local" {
			if n.Audio.SampleRate == 0 {
				n.Audio.SampleRate = 16000
			}
			if n.Audio.SampleRate != 16000 {
				return fmt.Errorf("node %s: capture sample_rate must be 16000", n.NodeID)
			}
			if n.Audio.Gain == 0 {
				n.Audio.Gain = 1
			}
			if !validNumber(float64(n.Audio.Gain), 0.01, 100) {
				return fmt.Errorf("node %s: invalid capture gain", n.NodeID)
			}
		} else {
			u, err := url.Parse(n.RTCStream.ApiURL)
			if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
				return fmt.Errorf("node %s: invalid go2rtc API URL", n.NodeID)
			}
			if n.RTCStream.StreamName == "" {
				return fmt.Errorf("node %s: stream_name is required", n.NodeID)
			}
		}
		if !validNumber(float64(n.RTCStream.OutputGainDb), -60, 30) {
			return fmt.Errorf("node %s: invalid output gain", n.NodeID)
		}
		if n.KWS.Enabled {
			if n.KWS.Encoder == "" || n.KWS.Decoder == "" || n.KWS.Joiner == "" || n.KWS.Tokens == "" || n.KWS.KeywordsFile == "" {
				return fmt.Errorf("node %s: all KWS model paths are required", n.NodeID)
			}
			if n.KWS.Threshold == 0 {
				n.KWS.Threshold = 0.25
			}
			if !validNumber(float64(n.KWS.Threshold), 0.01, 1) {
				return fmt.Errorf("node %s: invalid KWS threshold", n.NodeID)
			}
		}
		if n.KWS.NumThreads < 0 || n.KWS.NumThreads > c.GlobalNumThreads {
			return fmt.Errorf("node %s: KWS threads exceed global thread budget", n.NodeID)
		}
		if !validNumber(n.KWS.AutoAskTimeoutSeconds, 0, 120) || !validNumber(n.KWS.PostSpeechSilenceSeconds, 0, 20) || !validNumber(n.KWS.AutoAskCaptureDelaySeconds, -1, 20) {
			return fmt.Errorf("node %s: invalid ask duration", n.NodeID)
		}
		for _, text := range n.KWS.WakeResponses {
			if len(text) > 4000 {
				return fmt.Errorf("node %s: wake response exceeds 4000 bytes", n.NodeID)
			}
		}
	}
	pluginIDs := make(map[string]bool)
	for i := range c.Plugins {
		p := &c.Plugins[i]
		if p.ID == "" || len(p.ID) > 64 || strings.ContainsAny(p.ID, " \t/#+") {
			return fmt.Errorf("plugin %d: id is required, without spaces or any of / # +", i)
		}
		// Like a node's: "all" addresses a broadcast.
		if p.ID == "all" {
			return fmt.Errorf("plugin %d: id must not be \"all\"", i)
		}
		if pluginIDs[p.ID] {
			return fmt.Errorf("plugin %s: id is used twice", p.ID)
		}
		pluginIDs[p.ID] = true
		// A plugin can add a node, whose id is the plugin's.
		for _, n := range c.Nodes {
			if n.NodeID == p.ID {
				return fmt.Errorf("plugin %s: id is also the id of a node", p.ID)
			}
		}
		if p.Type == "" {
			return fmt.Errorf("plugin %s: type is required", p.ID)
		}
	}
	return nil
}

func validNumber(value, lo, hi float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0) && value >= lo && value <= hi
}

// ValidateModelFiles checks paths before entering native model constructors.
func (c *Config) ValidateModelFiles(mode, nodeID string) error {
	files := []string{}
	dirs := []string{}
	asr, tts, speaker, vad, kws := false, false, false, false, false
	switch mode {
	case "webrtc":
		asr, tts, speaker, vad, kws = true, true, true, true, true
	case "demo":
		asr, tts, speaker = true, true, true
	case "asr-file":
		asr = true
	case "wake-file":
		kws = true
	case "wake-file-vad":
		kws, vad = true, true
	case "rtc-say":
		tts = true
	case "rtc-record", "rtc-loopback":
	default:
		return fmt.Errorf("unsupported channel mode %q", mode)
	}
	if asr {
		files = append(files, c.Models.ASR.Encoder, c.Models.ASR.Decoder, c.Models.ASR.Tokens)
	}
	if speaker {
		files = append(files, c.Models.SpeakerID.Model)
	}
	if vad {
		files = append(files, c.Models.VAD.SileroOnnxPath)
	}
	if tts && c.Models.TTS.Engine == "supertonic" {
		for _, name := range []string{"duration_predictor.int8.onnx", "text_encoder.int8.onnx", "vector_estimator.int8.onnx", "vocoder.int8.onnx", "tts.json", "unicode_indexer.bin", "voice.bin"} {
			files = append(files, filepath.Join(c.Models.TTS.ModelDir, name))
		}
	} else if tts {
		files = append(files, c.Models.TTS.Model, c.Models.TTS.Tokens)
		dirs = append(dirs, c.Models.TTS.DataDir)
	}
	for _, n := range c.Nodes {
		if kws && n.KWS.Enabled && (mode == "webrtc" || n.NodeID == nodeID) {
			files = append(files, n.KWS.Encoder, n.KWS.Decoder, n.KWS.Joiner, n.KWS.Tokens, n.KWS.KeywordsFile)
		}
	}
	for _, path := range files {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("model file %s: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("model path %s must be a regular file", path)
		}
	}
	for _, path := range dirs {
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !info.IsDir() {
			return fmt.Errorf("model path %s must be a directory", path)
		}
	}
	return nil
}
