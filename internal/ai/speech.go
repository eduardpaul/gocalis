package ai

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"math"
	"path/filepath"
	"strings"
	"sync"

	"gocalis/internal/audio"
	"gocalis/internal/config"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

// Transcriber defines the interface for Automatic Speech Recognition (ASR).
type Transcriber interface {
	// TranscribeSamples transcribes mono float32 samples (range [-1,1]). Samples
	// not already at 16000Hz are resampled. This is the primary, in-memory path.
	TranscribeSamples(samples []float32, sampleRate int, opts JobOptions) (string, error)

	// TranscribeFile transcribes an audio file on disk (must be WAV/PCM). It is a
	// thin convenience wrapper over TranscribeSamples.
	TranscribeFile(filePath string, opts JobOptions) (string, error)

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
	Result(opts JobOptions) string

	// Reset clears the accumulated audio buffer.
	Reset()

	// Close releases resources associated with the stream.
	Close()
}

// Synthesizer defines the interface for Text-to-Speech (TTS).
type Synthesizer interface {
	// SynthesizeToFile converts text to speech and saves it as a WAV file on disk.
	SynthesizeToFile(text string, outputPath string, opts JobOptions) error

	// SynthesizeToStream converts text to speech and returns a stream reader.
	SynthesizeToStream(text string, opts JobOptions) (AudioStream, error)

	// Close releases resources associated with the Synthesizer.
	Close()
}

// AudioStream defines the interface for reading synthesized audio in chunks.
type AudioStream interface {
	// SampleRate returns the sample rate of the generated audio (e.g. 22050Hz).
	SampleRate() int

	// ReadPCM16 reads the next chunk of PCM16 samples. Returns io.EOF when done.
	ReadPCM16(chunkSize int) ([]int16, error)
}

// --- Whisper/Moonshine ASR Implementation with Priority Queue ---

type asrJob struct {
	samples    []float32
	priority   int
	resultChan chan string
}

type asrPriorityQueue struct {
	jobs   []*asrJob
	cond   *sync.Cond
	mutex  sync.Mutex
	closed bool
}

func newASRPriorityQueue() *asrPriorityQueue {
	pq := &asrPriorityQueue{
		jobs: make([]*asrJob, 0),
	}
	pq.cond = sync.NewCond(&pq.mutex)
	return pq
}

func (pq *asrPriorityQueue) Push(job *asrJob) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	if pq.closed {
		return
	}

	pq.jobs = append(pq.jobs, job)
	pq.cond.Signal()
}

func (pq *asrPriorityQueue) Pop() *asrJob {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	for len(pq.jobs) == 0 && !pq.closed {
		pq.cond.Wait()
	}

	if pq.closed || len(pq.jobs) == 0 {
		return nil
	}

	// Stable priority queue pop: highest priority value runs first
	bestIdx := 0
	for i := 1; i < len(pq.jobs); i++ {
		if pq.jobs[i].priority > pq.jobs[bestIdx].priority {
			bestIdx = i
		}
	}

	job := pq.jobs[bestIdx]
	pq.jobs = append(pq.jobs[:bestIdx], pq.jobs[bestIdx+1:]...)

	return job
}

func (pq *asrPriorityQueue) Close() {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	pq.closed = true
	pq.cond.Broadcast()
}

type whisperTranscriber struct {
	recognizer *sherpa.OfflineRecognizer
	config     *sherpa.OfflineRecognizerConfig
	pq         *asrPriorityQueue
	mutex      sync.Mutex
}

// NewTranscriber initializes a new Transcriber using configuration.
func NewTranscriber(cfg config.ASRConfig, numThreads int) (Transcriber, error) {
	config := sherpa.OfflineRecognizerConfig{}
	config.FeatConfig.SampleRate = 16000
	config.FeatConfig.FeatureDim = 80
	config.ModelConfig.Tokens = cfg.Tokens

	config.ModelConfig.NumThreads = numThreads
	config.ModelConfig.Debug = 0
	config.ModelConfig.Provider = "cpu"
	config.DecodingMethod = "greedy_search"

	// Dynamically configure Whisper or Moonshine based on model path
	if strings.Contains(strings.ToLower(cfg.Encoder), "moonshine") {
		config.ModelConfig.Moonshine.Encoder = cfg.Encoder
		if strings.Contains(strings.ToLower(cfg.Decoder), "merged") {
			config.ModelConfig.Moonshine.MergedDecoder = cfg.Decoder
		} else {
			config.ModelConfig.Moonshine.UncachedDecoder = cfg.Decoder
			config.ModelConfig.Moonshine.CachedDecoder = cfg.Decoder
		}
	} else {
		config.ModelConfig.Whisper.Encoder = cfg.Encoder
		config.ModelConfig.Whisper.Decoder = cfg.Decoder
		config.ModelConfig.Whisper.Language = "es" // default to Spanish
		config.ModelConfig.Whisper.Task = "transcribe"
	}

	recognizer := sherpa.NewOfflineRecognizer(&config)
	if recognizer == nil {
		return nil, errors.New("failed to initialize offline recognizer")
	}

	syn := &whisperTranscriber{
		recognizer: recognizer,
		config:     &config,
		pq:         newASRPriorityQueue(),
	}

	// Start background worker loop to serialize CPU-heavy ASR operations
	go syn.workerLoop()

	return syn, nil
}

func (t *whisperTranscriber) workerLoop() {
	for {
		t.mutex.Lock()
		queue := t.pq
		t.mutex.Unlock()

		if queue == nil {
			break
		}

		job := queue.Pop()
		if job == nil {
			break
		}

		text := t.decodeSync(job.samples)
		job.resultChan <- text
	}
}

func (t *whisperTranscriber) decodeSync(samples []float32) string {
	t.mutex.Lock()
	recognizer := t.recognizer
	t.mutex.Unlock()

	if recognizer == nil {
		return ""
	}

	stream := sherpa.NewOfflineStream(recognizer)
	defer sherpa.DeleteOfflineStream(stream)

	// Whisper expects 16000Hz.
	stream.AcceptWaveform(16000, samples)
	recognizer.Decode(stream)
	res := stream.GetResult()
	if res == nil {
		return ""
	}
	return res.Text
}

func (t *whisperTranscriber) TranscribeSamples(samples []float32, sampleRate int, opts JobOptions) (string, error) {
	if sampleRate != 16000 {
		samples = audio.ResampleFloat32(samples, sampleRate, 16000)
	}

	t.mutex.Lock()
	queue := t.pq
	t.mutex.Unlock()

	if queue == nil {
		return "", errors.New("ASR transcriber is closed")
	}

	resultChan := make(chan string, 1)
	queue.Push(&asrJob{
		samples:    samples,
		priority:   opts.Priority,
		resultChan: resultChan,
	})

	text := <-resultChan
	return text, nil
}

func (t *whisperTranscriber) TranscribeFile(filePath string, opts JobOptions) (string, error) {
	wave := sherpa.ReadWave(filePath)
	if wave == nil {
		return "", errors.New("failed to read WAV file")
	}
	return t.TranscribeSamples(wave.Samples, wave.SampleRate, opts)
}

func (t *whisperTranscriber) CreateStream() (TranscriptionStream, error) {
	return &whisperStream{
		transcriber: t,
		samples:     make([]float32, 0, 16000*10), // preallocate 10s of audio
	}, nil
}

func (t *whisperTranscriber) Close() {
	t.mutex.Lock()
	defer t.mutex.Unlock()

	if t.pq != nil {
		t.pq.Close()
		t.pq = nil
	}

	if t.recognizer != nil {
		sherpa.DeleteOfflineRecognizer(t.recognizer)
		t.recognizer = nil
	}
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
	s.samples = append(s.samples, samples...)
}

func (s *whisperStream) Result(opts JobOptions) string {
	s.mutex.Lock()
	if len(s.samples) == 0 {
		s.mutex.Unlock()
		return ""
	}
	// Copy samples buffer to avoid modification during queued execution
	samplesCopy := make([]float32, len(s.samples))
	copy(samplesCopy, s.samples)
	s.mutex.Unlock()

	t := s.transcriber
	t.mutex.Lock()
	queue := t.pq
	t.mutex.Unlock()

	if queue == nil {
		return ""
	}

	resultChan := make(chan string, 1)
	queue.Push(&asrJob{
		samples:    samplesCopy,
		priority:   opts.Priority,
		resultChan: resultChan,
	})

	text := <-resultChan
	return text
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
	text       string
	outputPath string
	isStream   bool
	priority   int
	resultChan chan ttsResult
}

type ttsResult struct {
	audioStream AudioStream
	err         error
}

type priorityQueue struct {
	jobs   []*ttsJob
	cond   *sync.Cond
	mutex  sync.Mutex
	closed bool
}

func newPriorityQueue() *priorityQueue {
	pq := &priorityQueue{
		jobs: make([]*ttsJob, 0),
	}
	pq.cond = sync.NewCond(&pq.mutex)
	return pq
}

func (pq *priorityQueue) Push(job *ttsJob) {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	if pq.closed {
		return
	}

	pq.jobs = append(pq.jobs, job)
	pq.cond.Signal()
}

func (pq *priorityQueue) Pop() *ttsJob {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	for len(pq.jobs) == 0 && !pq.closed {
		pq.cond.Wait()
	}

	if pq.closed || len(pq.jobs) == 0 {
		return nil
	}

	// Stable priority queue pop: find the item with the highest priority score.
	// If scores are equal, FIFO is preserved.
	bestIdx := 0
	for i := 1; i < len(pq.jobs); i++ {
		if pq.jobs[i].priority > pq.jobs[bestIdx].priority {
			bestIdx = i
		}
	}

	job := pq.jobs[bestIdx]
	pq.jobs = append(pq.jobs[:bestIdx], pq.jobs[bestIdx+1:]...)

	return job
}

func (pq *priorityQueue) Close() {
	pq.mutex.Lock()
	defer pq.mutex.Unlock()

	pq.closed = true
	pq.cond.Broadcast()
}

type vitsSynthesizer struct {
	tts        *sherpa.OfflineTts
	config     *sherpa.OfflineTtsConfig
	configCopy config.TTSConfig
	pq         *priorityQueue
	mutex      sync.Mutex
}

func getCacheFilename(text string) string {
	hash := sha256.Sum256([]byte(text))
	return hex.EncodeToString(hash[:]) + ".wav"
}

// NewSynthesizer initializes a new Synthesizer using configuration.
func NewSynthesizer(cfg config.TTSConfig, numThreads int) (Synthesizer, error) {
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
		config.Model.Vits.Model = "./models/vits-es/es_ES-sharvard-medium.onnx"
		config.Model.Vits.Tokens = "./models/vits-es/tokens.txt"
		config.Model.Vits.DataDir = "./models/vits-es/espeak-ng-data"
		config.Model.Vits.NoiseScale = 0.667
		config.Model.Vits.NoiseScaleW = 0.8
		config.Model.Vits.LengthScale = 1.0
	}

	tts := sherpa.NewOfflineTts(&config)
	if tts == nil {
		return nil, errors.New("failed to initialize offline TTS engine")
	}

	syn := &vitsSynthesizer{
		tts:        tts,
		config:     &config,
		configCopy: cfg,
		pq:         newPriorityQueue(),
	}

	// Pre-generate cached phrases if cache is enabled
	if cfg.CacheConfig.Enabled {
		if err := os.MkdirAll(cfg.CacheConfig.Dir, 0755); err != nil {
			log.Printf("[TTS Cache] Warning: failed to create cache directory: %v\n", err)
		} else {
			log.Printf("[TTS Cache] Pre-generating %d cached phrases...\n", len(cfg.CacheConfig.PreGenerate))
			for _, phrase := range cfg.CacheConfig.PreGenerate {
				cacheFile := filepath.Join(cfg.CacheConfig.Dir, getCacheFilename(phrase))
				if _, err := os.Stat(cacheFile); os.IsNotExist(err) {
					log.Printf("[TTS Cache] Pre-synthesizing: \"%s\" -> %s\n", phrase, cacheFile)
					err := syn.synthesizeToFileSync(phrase, cacheFile)
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
	for {
		s.mutex.Lock()
		queue := s.pq
		s.mutex.Unlock()

		if queue == nil {
			break
		}

		job := queue.Pop()
		if job == nil {
			break
		}

		if job.isStream {
			stream, gen, err := s.prepareStream(job.text)
			if err != nil {
				job.resultChan <- ttsResult{err: err}
				continue
			}
			// Hand the stream back immediately so the caller can start playing
			// the first chunk while synthesis continues to fill the buffer.
			job.resultChan <- ttsResult{audioStream: stream}
			gen()
		} else {
			err := s.synthesizeToFileSync(job.text, job.outputPath)
			job.resultChan <- ttsResult{err: err}
		}
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

func (s *vitsSynthesizer) synthesizeToFileSync(text string, outputPath string) error {
	genConfig := s.genConfig()

	s.mutex.Lock()
	ttsEngine := s.tts
	s.mutex.Unlock()

	if ttsEngine == nil {
		return errors.New("TTS engine is closed")
	}

	audio := ttsEngine.GenerateWithConfig(text, &genConfig, nil)
	if audio.Samples == nil {
		return errors.New("failed to generate speech audio")
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
func (s *vitsSynthesizer) prepareStream(text string) (*streamingAudioStream, func(), error) {
	s.mutex.Lock()
	ttsEngine := s.tts
	s.mutex.Unlock()

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
			chunk := make([]float32, len(samples))
			copy(chunk, samples)
			stream.push(chunk)
			return true
		})

		if result == nil || result.Samples == nil {
			stream.finish(errors.New("failed to generate speech audio"))
			return
		}

		stream.mu.Lock()
		bufEmpty := len(stream.buf) == 0
		stream.mu.Unlock()
		if bufEmpty && len(result.Samples) > 0 {
			stream.push(result.Samples)
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

func readWavToStream(filePath string) (AudioStream, error) {
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

func (s *vitsSynthesizer) SynthesizeToFile(text string, outputPath string, opts JobOptions) error {
	s.mutex.Lock()
	enabled := s.configCopy.CacheConfig.Enabled
	cacheDir := s.configCopy.CacheConfig.Dir
	queue := s.pq
	s.mutex.Unlock()

	if enabled && cacheDir != "" {
		cacheFile := filepath.Join(cacheDir, getCacheFilename(text))
		if _, err := os.Stat(cacheFile); err == nil {
			log.Printf("[TTS Cache] Hit! Copying pre-generated file for text: \"%s\"\n", text)
			return copyFile(cacheFile, outputPath)
		}
	}

	if queue == nil {
		return errors.New("TTS synthesizer is closed")
	}

	resultChan := make(chan ttsResult, 1)
	queue.Push(&ttsJob{
		text:       text,
		outputPath: outputPath,
		isStream:   false,
		priority:   opts.Priority,
		resultChan: resultChan,
	})
	res := <-resultChan
	return res.err
}

func (s *vitsSynthesizer) SynthesizeToStream(text string, opts JobOptions) (AudioStream, error) {
	if text == "test-beep" {
		sr := 16000
		duration := 5
		samples := make([]float32, sr*duration)
		for i := range samples {
			t := float64(i) / float64(sr)
			samples[i] = float32(math.Sin(2 * math.Pi * 440.0 * t))
		}
		stream := newStreamingAudioStream(sr)
		go func() {
			stream.push(samples)
			stream.finish(nil)
		}()
		return stream, nil
	}

	s.mutex.Lock()
	enabled := s.configCopy.CacheConfig.Enabled
	cacheDir := s.configCopy.CacheConfig.Dir
	queue := s.pq
	s.mutex.Unlock()

	if enabled && cacheDir != "" {
		cacheFile := filepath.Join(cacheDir, getCacheFilename(text))
		if _, err := os.Stat(cacheFile); err == nil {
			log.Printf("[TTS Cache] Hit! Streaming pre-generated file for text: \"%s\"\n", text)
			return readWavToStream(cacheFile)
		}
	}

	if queue == nil {
		return nil, errors.New("TTS synthesizer is closed")
	}

	resultChan := make(chan ttsResult, 1)
	queue.Push(&ttsJob{
		text:       text,
		isStream:   true,
		priority:   opts.Priority,
		resultChan: resultChan,
	})
	res := <-resultChan
	return res.audioStream, res.err
}

func (s *vitsSynthesizer) Close() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.pq != nil {
		s.pq.Close()
		s.pq = nil
	}

	if s.tts != nil {
		sherpa.DeleteOfflineTts(s.tts)
		s.tts = nil
	}
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

func (as *vitsAudioStream) ReadPCM16(chunkSize int) ([]int16, error) {
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
// pace. ReadPCM16 blocks only when the buffer is empty and generation is still
// in progress.
type streamingAudioStream struct {
	sampleRate int

	mu   sync.Mutex
	cond *sync.Cond
	buf  []int16
	done bool
	err  error
}

func newStreamingAudioStream(sampleRate int) *streamingAudioStream {
	s := &streamingAudioStream{sampleRate: sampleRate}
	s.cond = sync.NewCond(&s.mu)
	return s
}

// push appends newly generated float32 samples to the buffer.
func (as *streamingAudioStream) push(samples []float32) {
	pcm := audio.FloatToPCM16(samples)
	as.mu.Lock()
	as.buf = append(as.buf, pcm...)
	as.cond.Broadcast()
	as.mu.Unlock()
}

// finish marks generation complete, optionally with a terminal error.
func (as *streamingAudioStream) finish(err error) {
	as.mu.Lock()
	as.done = true
	as.err = err
	as.cond.Broadcast()
	as.mu.Unlock()
}

func (as *streamingAudioStream) SampleRate() int {
	return as.sampleRate
}

func (as *streamingAudioStream) ReadPCM16(chunkSize int) ([]int16, error) {
	as.mu.Lock()
	for len(as.buf) == 0 && !as.done {
		as.cond.Wait()
	}

	if len(as.buf) == 0 {
		err := as.err
		as.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	n := chunkSize
	if n > len(as.buf) {
		n = len(as.buf)
	}
	out := make([]int16, n)
	copy(out, as.buf[:n])
	as.buf = as.buf[n:]
	as.mu.Unlock()

	return out, nil
}
