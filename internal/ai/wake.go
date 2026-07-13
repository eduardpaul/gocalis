package ai

import (
	"errors"
	"sync"

	"gocalis/internal/config"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

// WakeDetector defines the interface for Wake Word / Keyword Spotting (KWS).
type WakeDetector interface {
	// DetectInFile checks if the wake word is present in a WAV file.
	DetectInFile(filePath string) (bool, string, error)

	// CreateStream initializes a live, chunk-based wake word detection stream.
	CreateStream(onDetected func(keyword string)) (WakeStream, error)

	// Close releases resources associated with the WakeDetector.
	Close()
}

// WakeStream defines the interface for streaming wake word detection.
type WakeStream interface {
	// AcceptAudio accepts raw float32 audio samples.
	AcceptAudio(samples []float32)

	// Reset clears the accumulated audio buffer.
	Reset()

	// Close releases resources associated with the stream.
	Close()
}

// --- No-op Wake Word Detector (used when KWS is disabled) ---

type noopWakeDetector struct{}

func (n *noopWakeDetector) DetectInFile(filePath string) (bool, string, error) {
	return false, "", nil
}

func (n *noopWakeDetector) CreateStream(onDetected func(keyword string)) (WakeStream, error) {
	return &noopWakeStream{}, nil
}

func (n *noopWakeDetector) Close() {}

type noopWakeStream struct{}

func (n *noopWakeStream) AcceptAudio(samples []float32) {}
func (n *noopWakeStream) Reset()                        {}
func (n *noopWakeStream) Close()                        {}

// --- Sherpa-ONNX Keyword Spotter Wake Word Detector ---

type sherpaONNXWakeDetector struct {
	spotter *sherpa.KeywordSpotter
	config  config.KWSConfig
}

// NewSherpaONNXWakeDetector initializes a WakeDetector using Sherpa-ONNX streaming KWS.
func NewSherpaONNXWakeDetector(cfg config.KWSConfig, numThreads int) (WakeDetector, error) {
	if !cfg.Enabled {
		return &noopWakeDetector{}, nil
	}

	spotterConfig := sherpa.KeywordSpotterConfig{
		FeatConfig: sherpa.FeatureConfig{
			SampleRate: 16000,
			FeatureDim: 80,
		},
		ModelConfig: sherpa.OnlineModelConfig{
			Transducer: sherpa.OnlineTransducerModelConfig{
				Encoder: cfg.Encoder,
				Decoder: cfg.Decoder,
				Joiner:  cfg.Joiner,
			},
			Tokens:     cfg.Tokens,
			NumThreads: numThreads,
			Provider:   "cpu",
			Debug:      0,
		},
		KeywordsFile:      cfg.KeywordsFile,
		KeywordsThreshold: cfg.Threshold,
	}

	spotter := sherpa.NewKeywordSpotter(&spotterConfig)
	if spotter == nil {
		return nil, errors.New("failed to initialize Sherpa-ONNX keyword spotter")
	}

	return &sherpaONNXWakeDetector{
		spotter: spotter,
		config:  cfg,
	}, nil
}

func (w *sherpaONNXWakeDetector) DetectInFile(filePath string) (bool, string, error) {
	wave := sherpa.ReadWave(filePath)
	if wave == nil {
		return false, "", errors.New("failed to read WAV file")
	}

	stream := sherpa.NewKeywordStream(w.spotter)
	defer sherpa.DeleteOnlineStream(stream)

	stream.AcceptWaveform(wave.SampleRate, wave.Samples)
	stream.InputFinished()

	// Check the result after EACH decode step, mirroring the live streaming path
	// (AcceptAudio). A single GetResult after the full decode loop only sees the
	// keyword still "active" at the very end of the stream, so a keyword spotted
	// mid-file (followed by more audio/silence) is missed — a false negative on
	// long recordings. Return as soon as any keyword is spotted.
	for w.spotter.IsReady(stream) {
		w.spotter.Decode(stream)
		if result := w.spotter.GetResult(stream); result != nil && result.Keyword != "" {
			return true, result.Keyword, nil
		}
	}

	return false, "", nil
}

func (w *sherpaONNXWakeDetector) CreateStream(onDetected func(keyword string)) (WakeStream, error) {
	stream := sherpa.NewKeywordStream(w.spotter)
	if stream == nil {
		return nil, errors.New("failed to create Sherpa-ONNX keyword stream")
	}

	return &sherpaONNXWakeStream{
		spotter:    w.spotter,
		stream:     stream,
		onDetected: onDetected,
	}, nil
}

func (w *sherpaONNXWakeDetector) Close() {
	if w.spotter != nil {
		sherpa.DeleteKeywordSpotter(w.spotter)
		w.spotter = nil
	}
}

// --- Sherpa-ONNX KWS Wake Stream ---

type sherpaONNXWakeStream struct {
	spotter    *sherpa.KeywordSpotter
	stream     *sherpa.OnlineStream
	onDetected func(keyword string)
	mutex      sync.Mutex
}

func (ws *sherpaONNXWakeStream) AcceptAudio(samples []float32) {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	if ws.stream == nil || ws.spotter == nil {
		return
	}

	ws.stream.AcceptWaveform(16000, samples)

	for ws.spotter.IsReady(ws.stream) {
		ws.spotter.Decode(ws.stream)
	}

	result := ws.spotter.GetResult(ws.stream)
	if result != nil && result.Keyword != "" {
		go ws.onDetected(result.Keyword)
		ws.spotter.Reset(ws.stream)
	}
}

func (ws *sherpaONNXWakeStream) Reset() {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	if ws.stream != nil && ws.spotter != nil {
		ws.spotter.Reset(ws.stream)
	}
}

func (ws *sherpaONNXWakeStream) Close() {
	ws.mutex.Lock()
	defer ws.mutex.Unlock()

	if ws.stream != nil {
		sherpa.DeleteOnlineStream(ws.stream)
		ws.stream = nil
	}
}
