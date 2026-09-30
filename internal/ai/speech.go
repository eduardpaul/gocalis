package ai

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gocalis/internal/audio"
	"gocalis/internal/audionode"
	"gocalis/internal/config"
	"gocalis/internal/workqueue"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

// Transcriber defines the interface for Automatic Speech Recognition (ASR).
type Transcriber interface {
	// TranscribeSamples transcribes mono float32 samples (range [-1,1]). Samples
	// not already at 16000Hz are resampled. This is the primary, in-memory path.
	TranscribeSamples(ctx context.Context, samples []float32, sampleRate int, opts JobOptions) (string, error)

	// TranscribeFile transcribes an audio file on disk (must be WAV/PCM). It is a
	// thin convenience wrapper over TranscribeSamples.
	TranscribeFile(ctx context.Context, filePath string, opts JobOptions) (string, error)

	// CreateStream initializes a live, chunk-based audio transcription stream.
	CreateStream() (TranscriptionStream, error)

	// Close releases resources associated with the Transcriber.
	Close()
}

// TranscriptionStream defines the interface for live, chunk-based audio transcription.
type TranscriptionStream interface {
	// AcceptAudio accepts raw float32 audio samples (16000Hz, single-channel).
	AcceptAudio(samples []float32)

	// Result returns the current transcribed text from the accumulated audio.
	Result(ctx context.Context, opts JobOptions) (string, error)

	// Reset clears the accumulated audio buffer.
	Reset()

	// Close releases resources associated with the stream.
	Close()
}

// Synthesizer defines the interface for Text-to-Speech (TTS).
type Synthesizer interface {
	// SynthesizeToFile converts text to speech and saves it as a WAV file on disk.
	SynthesizeToFile(ctx context.Context, text string, outputPath string, opts JobOptions) error

	// SynthesizeToStream converts text to speech and returns a stream reader.
	SynthesizeToStream(ctx context.Context, text string, opts JobOptions) (AudioStream, error)

	// Close releases resources associated with the Synthesizer.
	Close()
}

// AudioStream is the shared cancellation-aware PCM source used by audio nodes.
type AudioStream = audionode.PCM16Source

const maxQueuedJobs = 32
const MaxTextBytes = 4000
const MaxAudioSeconds = 120

// --- Whisper/Moonshine ASR Implementation with Priority Queue ---

type asrJob struct {
	queued     time.Time
	timing     func(time.Duration, time.Duration)
	ctx        context.Context
	samples    []float32
	resultChan chan asrResult
}

type asrResult struct {
	text string
	err  error
}

type whisperTranscriber struct {
	recognizer *sherpa.OfflineRecognizer
	pq         *workqueue.Queue[*asrJob]
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	closeOnce  sync.Once
}

// NewTranscriber initializes a new Transcriber using configuration.
func NewTranscriber(cfg config.ASRConfig, numThreads int) (Transcriber, error) {
	if cfg.Engine != "whisper" && cfg.Engine != "moonshine" {
		return nil, fmt.Errorf("unsupported ASR engine %q", cfg.Engine)
	}
	config := sherpa.OfflineRecognizerConfig{}
	config.FeatConfig.SampleRate = 16000
	config.FeatConfig.FeatureDim = 80
	config.ModelConfig.Tokens = cfg.Tokens

	config.ModelConfig.NumThreads = numThreads
	config.ModelConfig.Debug = 0
	config.ModelConfig.Provider = "cpu"
	config.DecodingMethod = "greedy_search"

	if cfg.Engine == "moonshine" {
		config.ModelConfig.Moonshine.Encoder = cfg.Encoder
		config.ModelConfig.Moonshine.MergedDecoder = cfg.Decoder
	} else {
		config.ModelConfig.Whisper.Encoder = cfg.Encoder
		config.ModelConfig.Whisper.Decoder = cfg.Decoder
		config.ModelConfig.Whisper.Language = cfg.Language
		config.ModelConfig.Whisper.Task = "transcribe"
	}

	recognizer := sherpa.NewOfflineRecognizer(&config)
	if recognizer == nil {
		return nil, errors.New("failed to initialize offline recognizer")
	}

	ctx, cancel := context.WithCancel(context.Background())
	syn := &whisperTranscriber{
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		recognizer: recognizer,
		pq:         workqueue.New[*asrJob](maxQueuedJobs),
	}

	// Start background worker loop to serialize CPU-heavy ASR operations
	go syn.workerLoop()

	return syn, nil
}

func (t *whisperTranscriber) workerLoop() {
	defer close(t.done)
	for {
		job, ok := t.pq.Pop()
		if !ok {
			return
		}
		if err := job.ctx.Err(); err != nil {
			job.resultChan <- asrResult{err: err}
			continue
		}
		if t.ctx.Err() != nil {
			return
		}
		started := time.Now()
		text, err := t.decodeSync(job.samples)
		if job.timing != nil {
			job.timing(started.Sub(job.queued), time.Since(started))
		}
		job.resultChan <- asrResult{text: text, err: err}
	}
}

func (t *whisperTranscriber) decodeSync(samples []float32) (string, error) {
	recognizer := t.recognizer

	if recognizer == nil {
		return "", errors.New("ASR recognizer unavailable")
	}

	stream := sherpa.NewOfflineStream(recognizer)
	if stream == nil {
		return "", errors.New("failed to create ASR stream")
	}
	defer sherpa.DeleteOfflineStream(stream)

	// Whisper expects 16000Hz.
	stream.AcceptWaveform(16000, samples)
	recognizer.Decode(stream)
	res := stream.GetResult()
	if res == nil {
		return "", errors.New("ASR recognizer unavailable")
	}
	return res.Text, nil
}

func (t *whisperTranscriber) TranscribeSamples(ctx context.Context, samples []float32, sampleRate int, opts JobOptions) (string, error) {
	if sampleRate <= 0 || sampleRate > 192000 {
		return "", errors.New("invalid sample rate")
	}
	if len(samples) > sampleRate*MaxAudioSeconds {
		return "", errors.New("audio exceeds maximum duration")
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if sampleRate != 16000 {
		samples = audio.ResampleFloat32(samples, sampleRate, 16000)
	}
	result := make(chan asrResult, 1)
	if err := t.pq.Push(ctx, &asrJob{queued: time.Now(), timing: opts.OnTiming, ctx: ctx, samples: append([]float32(nil), samples...), resultChan: result}, opts.Priority); err != nil {
		return "", err
	}
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-t.ctx.Done():
		return "", workqueue.ErrClosed
	case res := <-result:
		return res.text, res.err
	}
}

func (t *whisperTranscriber) TranscribeFile(ctx context.Context, filePath string, opts JobOptions) (string, error) {
	if err := validateAudioFile(ctx, filePath); err != nil {
		return "", err
	}
	wave := sherpa.ReadWave(filePath)
	if wave == nil {
		return "", errors.New("failed to read WAV file")
	}
	return t.TranscribeSamples(ctx, wave.Samples, wave.SampleRate, opts)
}

func (t *whisperTranscriber) CreateStream() (TranscriptionStream, error) {
	return &whisperStream{
		transcriber: t,
		samples:     make([]float32, 0, 16000*10), // preallocate 10s of audio
	}, nil
}

func (t *whisperTranscriber) Close() {
	t.closeOnce.Do(func() {
		t.cancel()
		t.pq.Close()
		<-t.done
		if t.recognizer != nil {
			sherpa.DeleteOfflineRecognizer(t.recognizer)
		}
		t.recognizer = nil
	})
}

// --- Live Transcription Stream Implementation ---

type whisperStream struct {
	transcriber *whisperTranscriber
	samples     []float32
	mutex       sync.Mutex
}

func (s *whisperStream) AcceptAudio(samples []float32) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	remaining := 16000*MaxAudioSeconds - len(s.samples)
	if len(samples) > remaining {
		samples = samples[:remaining]
	}
	s.samples = append(s.samples, samples...)
}

func (s *whisperStream) Result(ctx context.Context, opts JobOptions) (string, error) {
	s.mutex.Lock()
	if len(s.samples) == 0 {
		s.mutex.Unlock()
		return "", nil
	}
	// Copy samples buffer to avoid modification during queued execution
	samplesCopy := make([]float32, len(s.samples))
	copy(samplesCopy, s.samples)
	s.mutex.Unlock()

	return s.transcriber.TranscribeSamples(ctx, samplesCopy, 16000, opts)
}

func (s *whisperStream) Reset() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.samples = s.samples[:0]
}

func (s *whisperStream) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.samples = nil
}

// --- VITS/Supertonic TTS Implementation with Priority Queue ---

type ttsJob struct {
	ctx        context.Context
	text       string
	outputPath string
	isStream   bool
	resultChan chan ttsResult
}

type ttsResult struct {
	audioStream AudioStream
	err         error
}

type vitsSynthesizer struct {
	tts        *sherpa.OfflineTts
	configCopy config.TTSConfig
	pq         *workqueue.Queue[*ttsJob]
	ctx        context.Context
	cancel     context.CancelFunc
	done       chan struct{}
	closeOnce  sync.Once
}

func (s *vitsSynthesizer) cacheFilename(text string) string {
	voiceConfig := s.configCopy
	voiceConfig.CacheConfig = config.CacheConfig{}
	voice, _ := json.Marshal(voiceConfig)
	hash := sha256.Sum256(append(voice, []byte(text)...))
	return hex.EncodeToString(hash[:]) + ".wav"
}

// NewSynthesizer initializes a new Synthesizer using configuration.
func NewSynthesizer(cfg config.TTSConfig, numThreads int) (Synthesizer, error) {
	if cfg.Engine != "vits" && cfg.Engine != "supertonic" {
		return nil, fmt.Errorf("unsupported TTS engine %q", cfg.Engine)
	}
	config := sherpa.OfflineTtsConfig{}

	// Raspberry Pi 5 optimization settings:
	config.Model.NumThreads = numThreads
	config.Model.Debug = 0
	config.Model.Provider = "cpu"
	config.MaxNumSentences = 1

	if cfg.Engine == "supertonic" {
		config.Model.Supertonic.DurationPredictor = cfg.ModelDir + "/duration_predictor.int8.onnx"
		config.Model.Supertonic.TextEncoder = cfg.ModelDir + "/text_encoder.int8.onnx"
		config.Model.Supertonic.VectorEstimator = cfg.ModelDir + "/vector_estimator.int8.onnx"
		config.Model.Supertonic.Vocoder = cfg.ModelDir + "/vocoder.int8.onnx"
		config.Model.Supertonic.TtsJson = cfg.ModelDir + "/tts.json"
		config.Model.Supertonic.UnicodeIndexer = cfg.ModelDir + "/unicode_indexer.bin"
		config.Model.Supertonic.VoiceStyle = cfg.ModelDir + "/voice.bin"
	} else {
		// Default VITS/Piper configuration
		config.Model.Vits.Model = cfg.Model
		config.Model.Vits.Tokens = cfg.Tokens
		config.Model.Vits.DataDir = cfg.DataDir
		config.Model.Vits.NoiseScale = 0.667
		config.Model.Vits.NoiseScaleW = 0.8
		config.Model.Vits.LengthScale = 1.0
	}

	tts := sherpa.NewOfflineTts(&config)
	if tts == nil {
		return nil, errors.New("failed to initialize offline TTS engine")
	}

	ctx, cancel := context.WithCancel(context.Background())
	syn := &vitsSynthesizer{
		ctx: ctx, cancel: cancel, done: make(chan struct{}),
		tts:        tts,
		configCopy: cfg,
		pq:         workqueue.New[*ttsJob](maxQueuedJobs),
	}

	// Pre-generate cached phrases if cache is enabled
	if cfg.CacheConfig.Enabled {
		if err := os.MkdirAll(cfg.CacheConfig.Dir, 0755); err != nil {
			log.Printf("[TTS Cache] Warning: failed to create cache directory: %v\n", err)
		} else {
			log.Printf("[TTS Cache] Pre-generating %d cached phrases...\n", len(cfg.CacheConfig.PreGenerate))
			for _, phrase := range cfg.CacheConfig.PreGenerate {
				cacheFile := filepath.Join(cfg.CacheConfig.Dir, syn.cacheFilename(phrase))
				if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
					log.Printf("[TTS Cache] Pre-synthesizing: \"%s\" -> %s\n", phrase, cacheFile)
					err := syn.synthesizeToFileSync(ctx, phrase, cacheFile)
					if err != nil {
						log.Printf("[TTS Cache] Warning: failed to pre-synthesize phrase \"%s\": %v\n", phrase, err)
					}
				}
			}
			log.Println("[TTS Cache] Pre-generation completed successfully!")
		}
	}

	// Start background worker loop to serialize CPU-heavy TTS operations
	go syn.workerLoop()

	return syn, nil
}

func (s *vitsSynthesizer) workerLoop() {
	defer close(s.done)
	for {
		job, ok := s.pq.Pop()
		if !ok {
			return
		}
		if err := job.ctx.Err(); err != nil {
			job.resultChan <- ttsResult{err: err}
			continue
		}
		if s.ctx.Err() != nil {
			return
		}
		ctx, cancel := context.WithCancel(job.ctx)
		stop := context.AfterFunc(s.ctx, cancel)
		if job.isStream {
			stream, gen, err := s.prepareStream(ctx, job.text)
			job.resultChan <- ttsResult{audioStream: stream, err: err}
			if err == nil {
				gen()
			}
		} else {
			err := ctx.Err()
			if err == nil {
				err = s.synthesizeToFileSync(ctx, job.text, job.outputPath)
			}
			job.resultChan <- ttsResult{err: err}
		}
		stop()
		cancel()
	}
}

// genConfig builds the sherpa generation config from the TTS configuration so
// the same voice/speed/steps/language settings apply to every synthesized
// utterance (both the file/cache path and the streaming path).
func (s *vitsSynthesizer) genConfig() sherpa.GenerationConfig {
	cfg := s.configCopy
	speed := cfg.Speed
	if speed <= 0 {
		speed = 1.0
	}
	gc := sherpa.GenerationConfig{
		SilenceScale: 0.2,
		Speed:        speed,
		Sid:          cfg.Sid,
		NumSteps:     cfg.NumSteps,
	}
	if cfg.Lang != "" {
		if extra, err := json.Marshal(map[string]string{"lang": cfg.Lang}); err == nil {
			gc.Extra = extra
		}
	}
	return gc
}

func (s *vitsSynthesizer) synthesizeToFileSync(ctx context.Context, text string, outputPath string) error {
	genConfig := s.genConfig()

	ttsEngine := s.tts

	if ttsEngine == nil {
		return errors.New("TTS engine is closed")
	}

	if err := ctx.Err(); err != nil {
		return err
	}
	total := 0
	limit := ttsEngine.SampleRate() * MaxAudioSeconds
	audio := ttsEngine.GenerateWithConfig(text, &genConfig, func(samples []float32, _ float32) bool {
		total += len(samples)
		return ctx.Err() == nil && total <= limit
	})
	if err := ctx.Err(); err != nil {
		return err
	}
	if total > limit {
		return errors.New("generated audio exceeds maximum duration")
	}
	if audio == nil || audio.Samples == nil {
		return errors.New("failed to generate speech audio")
	}

	if len(audio.Samples) > audio.SampleRate*MaxAudioSeconds {
		return errors.New("generated audio exceeds maximum duration")
	}
	if ok := audio.Save(outputPath); !ok {
		return errors.New("failed to save WAV file")
	}
	return nil
}

// prepareStream builds a streamingAudioStream and returns a generation closure
// that must be run (synchronously, on the serialized worker) to fill it. The
// closure drives sherpa's generation callback so audio chunks are emitted as
// they are produced rather than after the whole utterance is synthesized.
func (s *vitsSynthesizer) prepareStream(ctx context.Context, text string) (*streamingAudioStream, func(), error) {
	ttsEngine := s.tts

	if ttsEngine == nil {
		return nil, nil, errors.New("TTS engine is closed")
	}

	stream := newStreamingAudioStream(ttsEngine.SampleRate())

	gen := func() {
		genConfig := s.genConfig()

		// The callback runs on the worker goroutine as sherpa produces audio.
		// It appends to the stream's internal buffer at synthesis (CPU) speed,
		// decoupled from the slower real-time playback consumer, so the worker
		// is only busy for the synthesis duration, not the whole playback.
		result := ttsEngine.GenerateWithConfig(text, &genConfig, func(samples []float32, _ float32) bool {
			return stream.push(ctx, samples) == nil
		})

		if err := ctx.Err(); err != nil {
			stream.finish(err)
			return
		}
		if result == nil || result.Samples == nil {
			stream.finish(errors.New("failed to generate speech audio"))
			return
		}
		stream.finish(nil)
	}

	return stream, gen, nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	return err
}

func readWavToStream(ctx context.Context, filePath string) (AudioStream, error) {
	if err := validateAudioFile(ctx, filePath); err != nil {
		return nil, err
	}
	wave := sherpa.ReadWave(filePath)
	if wave == nil {
		return nil, fmt.Errorf("failed to read cached WAV file: %s", filePath)
	}
	return &vitsAudioStream{
		sampleRate: wave.SampleRate,
		samples:    wave.Samples,
		offset:     0,
	}, nil
}

func (s *vitsSynthesizer) SynthesizeToFile(ctx context.Context, text string, outputPath string, opts JobOptions) error {
	if err := validateText(ctx, text); err != nil {
		return err
	}
	if s.ctx.Err() != nil {
		return workqueue.ErrClosed
	}
	enabled := s.configCopy.CacheConfig.Enabled
	cacheDir := s.configCopy.CacheConfig.Dir
	queue := s.pq

	if enabled && cacheDir != "" {
		cacheFile := filepath.Join(cacheDir, s.cacheFilename(text))
		if _, err := os.Stat(cacheFile); err == nil {
			log.Printf("[TTS Cache] Hit! Copying pre-generated file for text: \"%s\"\n", text)
			return copyFile(cacheFile, outputPath)
		}
	}

	resultChan := make(chan ttsResult, 1)
	if err := queue.Push(ctx, &ttsJob{
		ctx:        ctx,
		text:       text,
		outputPath: outputPath,
		isStream:   false,
		resultChan: resultChan,
	}, opts.Priority); err != nil {
		return err
	}
	var res ttsResult
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.ctx.Done():
		return workqueue.ErrClosed
	case res = <-resultChan:
	}
	return res.err
}

func (s *vitsSynthesizer) SynthesizeToStream(ctx context.Context, text string, opts JobOptions) (AudioStream, error) {
	if err := validateText(ctx, text); err != nil {
		return nil, err
	}
	if s.ctx.Err() != nil {
		return nil, workqueue.ErrClosed
	}
	enabled := s.configCopy.CacheConfig.Enabled
	cacheDir := s.configCopy.CacheConfig.Dir
	queue := s.pq

	if enabled && cacheDir != "" {
		cacheFile := filepath.Join(cacheDir, s.cacheFilename(text))
		if _, err := os.Stat(cacheFile); err == nil {
			log.Printf("[TTS Cache] Hit! Streaming pre-generated file for text: \"%s\"\n", text)
			return readWavToStream(ctx, cacheFile)
		}
	}

	resultChan := make(chan ttsResult, 1)
	if err := queue.Push(ctx, &ttsJob{
		ctx:        ctx,
		text:       text,
		isStream:   true,
		resultChan: resultChan,
	}, opts.Priority); err != nil {
		return nil, err
	}
	var res ttsResult
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-s.ctx.Done():
		return nil, workqueue.ErrClosed
	case res = <-resultChan:
	}
	return res.audioStream, res.err
}

func (s *vitsSynthesizer) Close() {
	s.closeOnce.Do(func() {
		s.cancel()
		s.pq.Close()
		<-s.done
		if s.tts != nil {
			sherpa.DeleteOfflineTts(s.tts)
		}
		s.tts = nil
	})
}

func validateAudioFile(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Size() > 24<<20 {
		return errors.New("audio file must be regular and at most 24 MiB")
	}
	return nil
}

func validateText(ctx context.Context, text string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(text) > MaxTextBytes {
		return errors.New("text exceeds maximum length")
	}
	return nil
}

// --- Audio Stream Reader Implementation ---

type vitsAudioStream struct {
	sampleRate int
	samples    []float32
	offset     int
}

func (as *vitsAudioStream) SampleRate() int {
	return as.sampleRate
}

func (as *vitsAudioStream) ReadPCM16(ctx context.Context, chunkSize int) ([]int16, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if chunkSize <= 0 {
		return nil, errors.New("chunk size must be positive")
	}
	if as.offset >= len(as.samples) {
		return nil, io.EOF
	}

	end := as.offset + chunkSize
	if end > len(as.samples) {
		end = len(as.samples)
	}

	chunk := as.samples[as.offset:end]
	as.offset = end

	return audio.FloatToPCM16(chunk), nil
}

// --- Streaming Audio Stream Implementation ---

// streamingAudioStream is an AudioStream whose PCM16 samples are appended by a
// producer (sherpa's generation callback) while a consumer (playback) drains
// them via ReadPCM16. An internal buffer decouples the two: synthesis fills the
// buffer at CPU speed and finishes quickly, while playback reads at real-time
// pace. The producer buffers at most two seconds and waits for the consumer.
// Reads and writes unblock when their context is cancelled.
type streamingAudioStream struct {
	sampleRate int

	mu    sync.Mutex
	cond  *sync.Cond
	buf   []int16
	done  bool
	err   error
	total int
}

func newStreamingAudioStream(sampleRate int) *streamingAudioStream {
	s := &streamingAudioStream{sampleRate: sampleRate}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// push converts borrowed native callback samples directly to owned PCM, limiting
// buffered audio to two seconds. Backpressure ends promptly on cancellation.
func (as *streamingAudioStream) push(ctx context.Context, samples []float32) error {
	as.mu.Lock()
	defer as.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { as.mu.Lock(); as.cond.Broadcast(); as.mu.Unlock() })
	defer stop()
	if as.total+len(samples) > as.sampleRate*MaxAudioSeconds {
		as.done = true
		as.err = errors.New("generated audio exceeds maximum duration")
		as.cond.Broadcast()
		return as.err
	}
	as.total += len(samples)
	capacity := as.sampleRate * 2
	for len(samples) > 0 {
		for len(as.buf) >= capacity && ctx.Err() == nil && !as.done {
			as.cond.Wait()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if as.done {
			return io.ErrClosedPipe
		}
		n := min(len(samples), capacity-len(as.buf))
		as.buf = append(as.buf, audio.FloatToPCM16(samples[:n])...)
		samples = samples[n:]
		as.cond.Broadcast()
	}
	return nil
}

func (as *streamingAudioStream) finish(err error) {
	as.mu.Lock()
	if !as.done {
		as.done = true
		as.err = err
	}
	as.cond.Broadcast()
	as.mu.Unlock()
}

func (as *streamingAudioStream) SampleRate() int { return as.sampleRate }

func (as *streamingAudioStream) ReadPCM16(ctx context.Context, chunkSize int) ([]int16, error) {
	if chunkSize <= 0 {
		return nil, errors.New("chunk size must be positive")
	}
	as.mu.Lock()
	defer as.mu.Unlock()
	stop := context.AfterFunc(ctx, func() { as.mu.Lock(); as.cond.Broadcast(); as.mu.Unlock() })
	defer stop()
	for len(as.buf) == 0 && !as.done && ctx.Err() == nil {
		as.cond.Wait()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(as.buf) == 0 {
		if as.err != nil {
			return nil, as.err
		}
		return nil, io.EOF
	}
	n := min(chunkSize, len(as.buf))
	out := append([]int16(nil), as.buf[:n]...)
	as.buf = as.buf[n:]
	as.cond.Broadcast()
	return out, nil
}
