package config

import (
	"gopkg.in/yaml.v3"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const minimalConfig = `models:
  vad:
    silero_onnx_path: vad.onnx
  asr:
    engine: moonshine
    encoder: encoder.ort
    decoder: decoder.ort
    tokens: tokens.txt
  speaker_id:
    model: speaker.onnx
  tts:
    engine: supertonic
    model_dir: tts
nodes:
  - node_id: room
    type: local
`

func loadText(t *testing.T, text string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(text), 0600); err != nil {
		t.Fatal(err)
	}
	return LoadConfig(path)
}

func TestConfigurationDefaultsAndExplicitModelFamily(t *testing.T) {
	cfg, err := loadText(t, minimalConfig)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.GlobalNumThreads != 4 || cfg.Models.ASR.Language != "es" || cfg.Nodes[0].Audio.SampleRate != 16000 || cfg.Models.TTS.Speed != 1 {
		t.Fatalf("defaults = %+v", cfg)
	}
	cfg, err = LoadConfig("../../config.yaml")
	if err != nil {
		t.Fatalf("checked-in configuration: %v", err)
	}
	if cfg.Models.ASR.Engine != "moonshine" {
		t.Fatal("checked-in model family is not explicit")
	}
}

func TestRejectInvalidConfiguration(t *testing.T) {
	for name, text := range map[string]string{
		"unknown key":             minimalConfig + "misspelled_threads: 4\n",
		"unknown nested key":      strings.Replace(minimalConfig, "    model: speaker.onnx", "    model: speaker.onnx\n    challenge_init_promt: old", 1),
		"duplicate nodes":         minimalConfig + "  - node_id: room\n    type: local\n",
		"unknown transport":       strings.Replace(minimalConfig, "type: local", "type: unknown", 1),
		"unsupported sample rate": minimalConfig + "    audio:\n      sample_rate: 48000\n",
		"unknown model":           strings.Replace(minimalConfig, "engine: moonshine", "engine: guessed", 1),
		"nan duration":            minimalConfig + "    kws:\n      auto_ask_timeout_seconds: .nan\n",
		"thread count":            "global_num_threads: -1\n" + minimalConfig,
		"multiple documents":      minimalConfig + "---\n{}\n",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := loadText(t, text); err == nil {
				t.Fatal("invalid config accepted")
			}
		})
	}
}

func TestZeroCaptureDelayIsHonored(t *testing.T) {
	n := NodeConfig{}
	if got := n.GetAutoAskCaptureDelaySeconds(1.5); got != 0 {
		t.Fatalf("zero delay = %v", got)
	}
	n.KWS.AutoAskCaptureDelaySeconds = -1
	if got := n.GetAutoAskCaptureDelaySeconds(1.5); got != 1.5 {
		t.Fatalf("default delay = %v", got)
	}
}

func TestDiagnosticModeValidatesOnlyItsModelFiles(t *testing.T) {
	cfg, err := loadText(t, minimalConfig)
	if err != nil {
		t.Fatal(err)
	}
	// A recording diagnostic must work even before any model has been downloaded.
	if err := cfg.ValidateModelFiles("rtc-record", "room"); err != nil {
		t.Fatal(err)
	}
	if err := cfg.ValidateModelFiles("asr-file", "room"); err == nil {
		t.Fatal("missing ASR model was not reported")
	}
}

func TestICEConfigurationPresence(t *testing.T) {
	for _, tc := range []struct {
		text  string
		empty bool
	}{
		{"api_url: http://localhost:11984\nstream_name: kitchen\n", false},
		{"api_url: http://localhost:11984\nstream_name: kitchen\nice_servers: []\n", true},
	} {
		var cfg RTCStreamConfig
		if err := yaml.Unmarshal([]byte(tc.text), &cfg); err != nil {
			t.Fatal(err)
		}
		if (cfg.ICEServers != nil) != tc.empty {
			t.Fatalf("ICE presence lost: %+v", cfg)
		}
	}
}

func TestPluginsAreListedWithTheirOwnSettings(t *testing.T) {
	cfg, err := loadText(t, minimalConfig+`plugins:
  - id: door
    type: example
    settings:
      greeting: hola
      times: 2
  - id: bare
    type: example
`)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Plugins) != 2 || cfg.Plugins[0].ID != "door" || cfg.Plugins[0].Type != "example" {
		t.Fatalf("unexpected plugins: %+v", cfg.Plugins)
	}

	type settings struct {
		Greeting string `yaml:"greeting"`
		Times    int    `yaml:"times"`
	}
	var got settings
	if err := cfg.Plugins[0].DecodeSettings(&got); err != nil {
		t.Fatal(err)
	}
	if got.Greeting != "hola" || got.Times != 2 {
		t.Fatalf("unexpected settings: %+v", got)
	}

	// A key the plugin does not know is refused, like anywhere else in the file.
	var narrow struct {
		Greeting string `yaml:"greeting"`
	}
	if err := cfg.Plugins[0].DecodeSettings(&narrow); err == nil || !strings.Contains(err.Error(), "times") {
		t.Fatalf("expected the unknown key to be refused, got %v", err)
	}

	// No settings leaves the defaults alone.
	untouched := settings{Greeting: "default"}
	if err := cfg.Plugins[1].DecodeSettings(&untouched); err != nil || untouched.Greeting != "default" {
		t.Fatalf("expected defaults to stay, got %+v, %v", untouched, err)
	}
}

func TestRejectInvalidPlugins(t *testing.T) {
	for name, plugins := range map[string]string{
		"no id":         "plugins:\n  - type: example\n",
		"no type":       "plugins:\n  - id: door\n",
		"id twice":      "plugins:\n  - id: door\n    type: example\n  - id: door\n    type: example\n",
		"id with slash": "plugins:\n  - id: a/b\n    type: example\n",
		"unknown key":   "plugins:\n  - id: door\n    type: example\n    colour: red\n",
		"id of a node":  "plugins:\n  - id: room\n    type: example\n",
	} {
		if _, err := loadText(t, minimalConfig+plugins); err == nil {
			t.Errorf("%s: expected the configuration to be refused", name)
		}
	}
}
