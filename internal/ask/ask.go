// Package ask implements the ask orchestration flow: play a TTS prompt with
// optional barge-in, capture user speech, run ASR, and optionally verify the
// speaker. It is used by the HTTP /ask endpoint and by the MQTT/WebSocket
// "ask" action.
package ask

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"gocalis/internal/ai"
	"gocalis/internal/brain"
	"gocalis/internal/node"
	"gocalis/internal/session"
)

// Config describes a single ask session.
type Config struct {
	ContextID         string
	NodeID            string
	TTSText           string
	BargeIn           bool
	RequireSpeakerID  bool
	VADTimeoutSeconds float64
	Priority          int

	// CaptureDelaySeconds delays the start of capture after the prompt finishes
	// playing (non-barge-in path only). In half-duplex transports this lets the
	// remote device flush its jitter-buffered TTS tail so the mic does not
	// re-capture the prompt itself. Ignored when barge-in interrupted the prompt.
	CaptureDelaySeconds float64

	// PostSpeechSilenceSeconds is the trailing silence required to conclude the
	// user's turn after speech has started. Lower values reduce the gap between
	// user stop-speaking and the end chime, at the cost of more aggressive turn
	// cutting. Defaults to 1.5s when unset.
	PostSpeechSilenceSeconds float64

	// PromptSamples and PromptSampleRate allow the caller to provide
	// pre-synthesized prompt audio. When empty, the engine synthesizes
	// TTSText itself.
	PromptSamples    []int16
	PromptSampleRate int

	// NodeAlreadyAcquired signals that the caller has already reserved the node's
	// turn (via Brain.AcquireNode) and will release it after Run returns. Run
	// then skips its own acquisition. Callers that pre-synthesize the prompt use
	// this to reserve the node BEFORE synthesis so a lower-priority speak cannot
	// grab the free node during the synthesis window.
	NodeAlreadyAcquired bool
}

// Result is the outcome of an ask session.
type Result struct {
	ContextID     string
	NodeID        string
	Status        string // "success", "silence_timeout", "verification_failed", "error"
	Transcription string
	Speaker       string
	ErrorMessage  string

	// Audio holds the captured user speech (float32, -1..1) when Status ==
	// "success". SampleRate is its rate in Hz. Callers may encode this into a
	// recording (e.g. a PCM16 WAV).
	Audio      []float32
	SampleRate int
}

// Engine runs ask sessions against the central brain.
type Engine struct {
	Brain     *brain.Brain
	ASR       ai.Transcriber
	SpeakerID ai.SpeakerIdentifier
}

// NewEngine creates an ask engine backed by the given brain and AI engines.
func NewEngine(b *brain.Brain, asr ai.Transcriber, speakerID ai.SpeakerIdentifier) *Engine {
	return &Engine{
		Brain:     b,
		ASR:       asr,
		SpeakerID: speakerID,
	}
}

// Run executes the ask flow for the given configuration.
func (e *Engine) Run(ctx context.Context, cfg Config) Result {
	handle := e.Brain.GetNodeHandle(cfg.NodeID)
	if handle == nil {
		return Result{
			ContextID:    cfg.ContextID,
			NodeID:       cfg.NodeID,
			Status:       "error",
			ErrorMessage: "node not registered",
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Hold the node for the entire ask turn (prompt -> listen -> ASR) so a
	// lower-priority speak queues behind it instead of cutting into it. Wake
	// driven AutoAsk passes a high priority; explicit API asks pass the request
	// priority. Blocks until the node is free (or ctx is cancelled). When the
	// caller already reserved the node (and pre-synthesized the prompt under that
	// reservation), Run does not re-acquire.
	if !cfg.NodeAlreadyAcquired {
		release, err := e.Brain.AcquireNode(ctx, cfg.NodeID, cfg.Priority)
		if err != nil {
			return Result{
				ContextID:    cfg.ContextID,
				NodeID:       cfg.NodeID,
				Status:       "error",
				ErrorMessage: fmt.Sprintf("could not acquire node: %v", err),
			}
		}
		defer release()
	}

	// Each turn owns an isolated Session registered on the brain so the audio
	// ingestion path fans captured speech (and barge-in) into it. A unique ID lets
	// concurrent turns coexist on the same node even if they share a context ID.
	sessID := cfg.ContextID
	if sessID == "" {
		sessID = "ask"
	}
	sess := session.New(fmt.Sprintf("%s-%d", sessID, time.Now().UnixNano()), cfg.NodeID)
	e.Brain.Sessions().Add(sess)
	defer func() {
		sess.ToDone()
		e.Brain.Sessions().Remove(sess)
	}()

	// Phase 1: Speak the prompt, allowing barge-in to cancel playback.
	barged := false
	startChimeSpoken := false
	if strings.TrimSpace(cfg.TTSText) != "" || len(cfg.PromptSamples) > 0 {
		samples := cfg.PromptSamples
		sampleRate := cfg.PromptSampleRate

		if len(samples) == 0 {
			var err error
			samples, sampleRate, err = e.Brain.Synthesize(cfg.TTSText, cfg.Priority)
			if err != nil {
				return Result{
					ContextID:    cfg.ContextID,
					NodeID:       cfg.NodeID,
					Status:       "error",
					ErrorMessage: err.Error(),
				}
			}
		}

		// Only speak when synthesis actually produced audio. Blank/empty text
		// yields no prompt, in which case just the listening chime is played
		// below (the TTS pipeline is never run for empty text).
		if len(samples) > 0 && sampleRate > 0 {
			padForRTC := handle.Config.Type == "rtc_stream"
			// Concatenate the "start listening" chime onto the prompt so both play
			// in a single, gapless transmission. Playing the chime as a separate
			// call would trigger another route-assert + buffer/drain cycle, leaving
			// an audible silence between the end of the prompt and the chime.
			if padForRTC {
				// Doorbell backchannels can clip packet edges; add a tiny guard silence
				// before the chime and at utterance tail so the prompt ending and chime
				// are not cut off by transcode/jitter-buffer timing.
				samples = append(samples, silencePCMAt(sampleRate, 80)...)
			}
			samples = append(samples, renderChimeAt(chimeStart, sampleRate)...)
			if padForRTC {
				samples = append(samples, silencePCMAt(sampleRate, 180)...)
			}
			startChimeSpoken = true

			bargeDetected := make(chan struct{}, 1)
			if cfg.BargeIn {
				bargeCh := sess.ArmBargeIn()
				go func() {
					select {
					case <-bargeCh:
						log.Printf("[Ask:%s] Barge-in detected, interrupting prompt and switching to LISTENING\n", cfg.NodeID)
						bargeDetected <- struct{}{}
						cancel()
					case <-ctx.Done():
					}
				}()
			}

			// Drive the state machine explicitly: idle -> speaking. The prompt
			// (with the appended start chime) is played state-neutrally so it
			// does not emit its own PROCESSING/SPEAKING/IDLE churn — the flow
			// transitions straight to LISTENING next.
			handle.Node.SetState(node.StateSpeaking)
			err := e.Brain.PlayAudio(ctx, cfg.NodeID, samples, sampleRate)
			sess.DisarmBargeIn()

			if err != nil && err != context.Canceled {
				return Result{
					ContextID:    cfg.ContextID,
					NodeID:       cfg.NodeID,
					Status:       "error",
					ErrorMessage: err.Error(),
				}
			}

			// If barge-in cancelled the prompt, ensure the node is in LISTENING
			// before capture starts. This also wins the race with SpeakSamples
			// resetting the state to IDLE on return.
			select {
			case <-bargeDetected:
				barged = true
				handle.Node.SetState(node.StateListening)
			default:
			}
		}
	}

	// Announce the start of listening with a chime on every device. Skip it when
	// the user barged in — they are already speaking. When a prompt played, the
	// chime was already appended to it (gapless), so only play it standalone here
	// when there was no prompt. After the chime, let its tail (and any lingering
	// prompt tail) clear in half-duplex before capture so the mic does not
	// re-capture our own audio as speech.
	if !barged {
		if !startChimeSpoken {
			e.playChime(cfg.NodeID, chimeStart)
		}
		if cfg.CaptureDelaySeconds > 0 {
			select {
			case <-time.After(time.Duration(cfg.CaptureDelaySeconds * float64(time.Second))):
			case <-ctx.Done():
			}
		}
	}

	// Phase 2: Capture user response until VAD timeout or post-speech silence.
	sess.ToListening()
	handle.Node.SetState(node.StateListening)
	sess.StartCapture()

	timeout := time.Duration(cfg.VADTimeoutSeconds * float64(time.Second))
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	deadline := time.After(timeout)

	hadSpeech := false
	lastCount := 0
	lastSpeechTime := time.Now()

	pollTicker := time.NewTicker(50 * time.Millisecond)
	defer pollTicker.Stop()

	postSpeechSilence := cfg.PostSpeechSilenceSeconds
	if postSpeechSilence <= 0 {
		postSpeechSilence = handle.Config.GetPostSpeechSilenceSeconds(1.5)
	}

listenLoop:
	for {
		select {
		case <-ctx.Done():
			break listenLoop
		case <-deadline:
			break listenLoop
		case <-pollTicker.C:
			count := sess.CapturedCount()
			if count > lastCount {
				hadSpeech = true
				lastCount = count
				lastSpeechTime = time.Now()
			} else if hadSpeech && time.Since(lastSpeechTime) > time.Duration(postSpeechSilence*float64(time.Second)) {
				break listenLoop
			}
		}
	}

	captured := sess.StopCapture()
	// Announce the end of listening with a chime on every device.
	e.playChime(cfg.NodeID, chimeStop)
	sess.ToProcessing()
	handle.Node.SetState(node.StateProcessing)

	if len(captured) == 0 {
		handle.Node.SetState(node.StateIdle)
		return Result{
			ContextID: cfg.ContextID,
			NodeID:    cfg.NodeID,
			Status:    "silence_timeout",
		}
	}

	transcription, err := e.ASR.TranscribeSamples(captured, 16000, ai.JobOptions{Priority: cfg.Priority})
	if err != nil {
		handle.Node.SetState(node.StateIdle)
		return Result{
			ContextID:    cfg.ContextID,
			NodeID:       cfg.NodeID,
			Status:       "error",
			ErrorMessage: "ASR failed: " + err.Error(),
		}
	}

	if cfg.RequireSpeakerID {
		speaker, err := e.SpeakerID.IdentifySamples(captured, 16000)
		if err != nil {
			handle.Node.SetState(node.StateIdle)
			return Result{
				ContextID:    cfg.ContextID,
				NodeID:       cfg.NodeID,
				Status:       "error",
				ErrorMessage: "speaker identification failed: " + err.Error(),
			}
		}
		if speaker == "" {
			handle.Node.SetState(node.StateIdle)
			return Result{
				ContextID: cfg.ContextID,
				NodeID:    cfg.NodeID,
				Status:    "verification_failed",
			}
		}
		handle.Node.SetState(node.StateIdle)
		return Result{
			ContextID:     cfg.ContextID,
			NodeID:        cfg.NodeID,
			Status:        "success",
			Transcription: transcription,
			Speaker:       speaker,
			Audio:         captured,
			SampleRate:    16000,
		}
	}

	handle.Node.SetState(node.StateIdle)
	return Result{
		ContextID:     cfg.ContextID,
		NodeID:        cfg.NodeID,
		Status:        "success",
		Transcription: transcription,
		Audio:         captured,
		SampleRate:    16000,
	}
}

// silencePCMAt returns mono PCM16 silence for the given sample rate and
// duration in milliseconds.
func silencePCMAt(sampleRate int, ms int) []int16 {
	if sampleRate <= 0 || ms <= 0 {
		return nil
	}
	n := sampleRate * ms / 1000
	if n <= 0 {
		return nil
	}
	return make([]int16, n)
}

// playChime plays a short UI chime on the node. It uses an independent context
// so the chime always plays even if the caller's context was cancelled (e.g.
// after the listen loop). The chime is played state-neutrally so it never emits
// SPEAKING/IDLE transitions — it is cosmetic and must not pollute the node's
// state stream (the ask flow owns the state machine). Errors are logged and
// otherwise ignored.
func (e *Engine) playChime(nodeID string, kind chimeKind) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := e.Brain.PlayAudio(ctx, nodeID, chimePCM(kind), chimeSampleRate); err != nil && err != context.Canceled {
		log.Printf("[Ask:%s] chime playback error: %v\n", nodeID, err)
	}
}
