// Package audionode defines the AudioNode port: the minimal contract the brain
// needs to drive a physical audio endpoint (speaker + microphone). It decouples
// orchestration from any concrete transport, so WebRTC/go2rtc, a local
// soundcard, or a test stub can all be plugged in behind the same interface.
package audionode

import "context"

// PCM16Source yields PCM16 audio in chunks until io.EOF. It is the pull-based
// contract a node uses to play audio incrementally (e.g. streamed straight from
// the TTS engine as it is synthesized). The ai.AudioStream satisfies it.
type PCM16Source interface {
	// SampleRate returns the sample rate of the produced PCM16 samples.
	SampleRate() int

	// ReadPCM16 returns the next chunk of up to chunkSize samples, or io.EOF
	// once the source is exhausted.
	ReadPCM16(chunkSize int) ([]int16, error)
}

// AudioNode is a bidirectional audio endpoint: it plays PCM16 audio to a device
// and delivers captured microphone audio (mono float32 in [-1, 1]) via a callback.
type AudioNode interface {
	// Connect establishes the transport and begins streaming. It returns once the
	// endpoint is ready or the context is cancelled.
	Connect(ctx context.Context) error

	// Play sends PCM16 samples (at sampleRate) to the device output, blocking
	// until the audio has been streamed or ctx is cancelled.
	Play(ctx context.Context, pcm16 []int16, sampleRate int) error

	// PlayStream plays PCM16 audio pulled incrementally from src until io.EOF,
	// letting playback begin before synthesis completes. It blocks until all
	// audio has been streamed or ctx is cancelled.
	PlayStream(ctx context.Context, src PCM16Source) error

	// OnAudio registers the callback invoked for each chunk of captured
	// microphone audio.
	OnAudio(callback func(samples []float32))

	// Close tears down the transport and releases resources.
	Close() error
}
