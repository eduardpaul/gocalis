package ai

import (
	"errors"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"strings"
	"sync"

	"gocalis/internal/config"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

// SpeakerIdentifier defines the interface for Speaker Verification and Identification.
type SpeakerIdentifier interface {
	// IdentifySamples extracts a speaker embedding from mono float32 samples and
	// matches it against registered speakers. This is the primary, in-memory path.
	IdentifySamples(samples []float32, sampleRate int) (string, error)

	// IdentifyFile extracts speaker embedding from a WAV file and matches against
	// registered speakers. It is a thin convenience wrapper over IdentifySamples.
	IdentifyFile(filePath string) (string, error)

	// CreateStream initializes a live, chunk-based speaker identification stream.
	CreateStream(onSpeakerIdentified func(speakerName string)) (SpeakerStream, error)

	// ReloadSpeakers re-scans the voice profiles embeddings directory and hot-reloads them.
	ReloadSpeakers() error

	// Close releases resources associated with the SpeakerIdentifier.
	Close()
}

// SpeakerStream defines the interface for live streaming speaker identification.
type SpeakerStream interface {
	// AcceptAudio accepts raw float32 audio samples.
	AcceptAudio(samples []float32)

	// Reset clears accumulated stream buffer.
	Reset()

	// Close releases resources associated with the stream.
	Close()
}

// --- Speaker Identification Implementation ---

type speakerIdentifier struct {
	extractor *sherpa.SpeakerEmbeddingExtractor
	manager   *sherpa.SpeakerEmbeddingManager
	config    config.SpeakerIDConfig
	// mutex guards extractor and manager. Readers (IdentifyFile, stream
	// AcceptAudio) take RLock; mutators that free/swap them (ReloadSpeakers,
	// Close) take Lock. This prevents a hot-reload from freeing the manager
	// while a live stream is mid-Search across the cgo boundary.
	mutex sync.RWMutex
}

// NewSpeakerIdentifier creates and initializes a SpeakerIdentifier. It automatically
// registers speakers found in the configured EmbeddingsDir.
func NewSpeakerIdentifier(cfg config.SpeakerIDConfig, numThreads int) (SpeakerIdentifier, error) {
	extractorConfig := sherpa.SpeakerEmbeddingExtractorConfig{
		Model:      cfg.Model,
		NumThreads: numThreads,
		Debug:      0,
		Provider:   "cpu",
	}

	extractor := sherpa.NewSpeakerEmbeddingExtractor(&extractorConfig)
	if extractor == nil {
		return nil, errors.New("failed to initialize SpeakerEmbeddingExtractor")
	}

	dim := extractor.Dim()
	manager := sherpa.NewSpeakerEmbeddingManager(dim)
	if manager == nil {
		sherpa.DeleteSpeakerEmbeddingExtractor(extractor)
		return nil, errors.New("failed to initialize SpeakerEmbeddingManager")
	}

	sID := &speakerIdentifier{
		extractor: extractor,
		manager:   manager,
		config:    cfg,
	}

	// Scan the embeddings directory and register WAV voice profiles
	if cfg.EmbeddingsDir != "" {
		err := filepath.WalkDir(cfg.EmbeddingsDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.ToLower(filepath.Ext(path)) == ".wav" {
				name := strings.TrimSuffix(d.Name(), filepath.Ext(d.Name()))
				log.Printf("[SpeakerID] Loading voice profile for speaker: '%s'...\n", name)

				wave := sherpa.ReadWave(path)
				if wave == nil {
					log.Printf("[SpeakerID] Warning: failed to read WAV file for speaker: '%s'\n", name)
					return nil
				}

				embedding, err := sID.extractEmbedding(wave.SampleRate, wave.Samples)
				if err != nil {
					log.Printf("[SpeakerID] Warning: failed to extract embedding for speaker: '%s': %v\n", name, err)
					return nil
				}

				if ok := manager.Register(name, embedding); !ok {
					log.Printf("[SpeakerID] Warning: failed to register embedding for speaker: '%s'\n", name)
				} else {
					log.Printf("[SpeakerID] Speaker '%s' registered successfully (Dim: %d)!\n", name, len(embedding))
				}
			}
			return nil
		})
		if err != nil {
			log.Printf("[SpeakerID] Warning: error reading embeddings directory: %v\n", err)
		}
	}

	return sID, nil
}

func (s *speakerIdentifier) extractEmbedding(sampleRate int, samples []float32) ([]float32, error) {
	stream := s.extractor.CreateStream()
	defer sherpa.DeleteOnlineStream(stream)

	stream.AcceptWaveform(sampleRate, samples)
	stream.InputFinished()

	if !s.extractor.IsReady(stream) {
		return nil, fmt.Errorf("audio too short to extract speaker fingerprint (samples: %d)", len(samples))
	}

	embedding := s.extractor.Compute(stream)
	return embedding, nil
}

func (s *speakerIdentifier) IdentifySamples(samples []float32, sampleRate int) (string, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.extractor == nil || s.manager == nil {
		return "", errors.New("speaker identifier is closed")
	}

	embedding, err := s.extractEmbedding(sampleRate, samples)
	if err != nil {
		return "", err
	}

	speaker := s.manager.Search(embedding, s.config.ConfidenceThreshold)
	return speaker, nil
}

func (s *speakerIdentifier) IdentifyFile(filePath string) (string, error) {
	wave := sherpa.ReadWave(filePath)
	if wave == nil {
		return "", errors.New("failed to read WAV file")
	}
	return s.IdentifySamples(wave.Samples, wave.SampleRate)
}

func (s *speakerIdentifier) CreateStream(onSpeakerIdentified func(speakerName string)) (SpeakerStream, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if s.extractor == nil {
		return nil, errors.New("speaker identifier is closed")
	}

	return &speakerStream{
		sID:                s,
		stream:             s.extractor.CreateStream(),
		onSpeakerIdentified: onSpeakerIdentified,
		samplesCount:       0,
	}, nil
}

func (s *speakerIdentifier) ReloadSpeakers() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	log.Println("[SpeakerID] Reloading speaker embeddings from directory...")
	dim := s.extractor.Dim()
	newManager := sherpa.NewSpeakerEmbeddingManager(dim)
	if newManager == nil {
		return errors.New("failed to initialize new SpeakerEmbeddingManager during reload")
	}

	if s.config.EmbeddingsDir != "" {
		err := filepath.WalkDir(s.config.EmbeddingsDir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.ToLower(filepath.Ext(path)) == ".wav" {
				name := strings.TrimSuffix(d.Name(), filepath.Ext(d.Name()))
				log.Printf("[SpeakerID] Loading voice profile for speaker: '%s'...\n", name)

				wave := sherpa.ReadWave(path)
				if wave == nil {
					log.Printf("[SpeakerID] Warning: failed to read WAV file for speaker: '%s'\n", name)
					return nil
				}

				embedding, err := s.extractEmbedding(wave.SampleRate, wave.Samples)
				if err != nil {
					log.Printf("[SpeakerID] Warning: failed to extract embedding for speaker: '%s': %v\n", name, err)
					return nil
				}

				if ok := newManager.Register(name, embedding); !ok {
					log.Printf("[SpeakerID] Warning: failed to register embedding for speaker: '%s'\n", name)
				} else {
					log.Printf("[SpeakerID] Speaker '%s' registered successfully (Dim: %d)!\n", name, len(embedding))
				}
			}
			return nil
		})
		if err != nil {
			sherpa.DeleteSpeakerEmbeddingManager(newManager)
			return err
		}
	}

	// Swap managers and free the old one
	if s.manager != nil {
		sherpa.DeleteSpeakerEmbeddingManager(s.manager)
	}
	s.manager = newManager
	return nil
}

func (s *speakerIdentifier) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.extractor != nil {
		sherpa.DeleteSpeakerEmbeddingExtractor(s.extractor)
		s.extractor = nil
	}
	if s.manager != nil {
		sherpa.DeleteSpeakerEmbeddingManager(s.manager)
		s.manager = nil
	}
}

// --- Live Speaker Stream ---

type speakerStream struct {
	sID                *speakerIdentifier
	stream             *sherpa.OnlineStream
	onSpeakerIdentified func(speakerName string)
	samplesCount       int
	mutex              sync.Mutex
}

func (ss *speakerStream) AcceptAudio(samples []float32) {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	if ss.stream == nil {
		return
	}

	// Hold the identifier's read lock so a concurrent ReloadSpeakers/Close
	// cannot free the extractor or manager while we use them.
	ss.sID.mutex.RLock()
	defer ss.sID.mutex.RUnlock()

	if ss.sID.extractor == nil || ss.sID.manager == nil {
		return
	}

	ss.stream.AcceptWaveform(16000, samples) // Speaker models typically expect 16kHz
	ss.samplesCount += len(samples)

	// Check if we have gathered enough samples (min_audio_duration_seconds * 16000)
	minSamples := int(ss.sID.config.MinAudioDurationSeconds * 16000)
	if ss.samplesCount >= minSamples && ss.sID.extractor.IsReady(ss.stream) {
		embedding := ss.sID.extractor.Compute(ss.stream)

		speaker := ss.sID.manager.Search(embedding, ss.sID.config.ConfidenceThreshold)
		if speaker != "" {
			go ss.onSpeakerIdentified(speaker)
		}

		// Reset counters and stream for the next identification interval
		sherpa.DeleteOnlineStream(ss.stream)
		ss.stream = ss.sID.extractor.CreateStream()
		ss.samplesCount = 0
	}
}

func (ss *speakerStream) Reset() {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	if ss.stream != nil {
		sherpa.DeleteOnlineStream(ss.stream)
		ss.stream = ss.sID.extractor.CreateStream()
	}
	ss.samplesCount = 0
}

func (ss *speakerStream) Close() {
	ss.mutex.Lock()
	defer ss.mutex.Unlock()

	if ss.stream != nil {
		sherpa.DeleteOnlineStream(ss.stream)
		ss.stream = nil
	}
}
