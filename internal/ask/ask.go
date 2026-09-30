// Package ask implements the ask orchestration flow: play a TTS prompt with
// optional barge-in, capture user speech, run ASR, and optionally verify the
// speaker. It is used by the HTTP /ask endpoint and by the MQTT/WebSocket
// "ask" action.
package ask

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strings"
	"time"

	"gocalis/internal/ai"
	"gocalis/internal/brain"
	"gocalis/internal/config"
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
	Brain      *brain.Brain
	ASR        ai.Transcriber
	SpeakerID  ai.SpeakerIdentifier
	SpeakerCfg config.SpeakerIDConfig
}

// NewEngine creates an ask engine backed by the given brain and AI engines.
func NewEngine(b *brain.Brain, asr ai.Transcriber, speakerID ai.SpeakerIdentifier, speakerCfg config.SpeakerIDConfig) *Engine {
	return &Engine{
		Brain:      b,
		ASR:        asr,
		SpeakerID:  speakerID,
		SpeakerCfg: speakerCfg,
	}
}

// Run executes the ask flow for the given configuration.
func (e *Engine) Run(ctx context.Context, cfg Config) Result {
	if err := cfg.Validate(); err != nil {
		return Result{ContextID: cfg.ContextID, NodeID: cfg.NodeID, Status: "error", ErrorMessage: err.Error()}
	}
	turn, err := e.Brain.AcquireNode(ctx, cfg.NodeID, cfg.Priority)
	if err != nil {
		return Result{ContextID: cfg.ContextID, NodeID: cfg.NodeID, Status: "error", ErrorMessage: err.Error()}
	}
	defer turn.Release()
	handle := turn.Handle
	defer handle.Node.SetState(node.StateIdle)
	handle.Node.SetState(node.StateProcessing)

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
	if strings.TrimSpace(cfg.TTSText) != "" {
		samples, sampleRate, err := e.Brain.Synthesize(ctx, cfg.TTSText, cfg.Priority)
		if err != nil {
			return Result{
				ContextID:    cfg.ContextID,
				NodeID:       cfg.NodeID,
				Status:       "error",
				ErrorMessage: err.Error(),
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

			promptCtx, cancelPrompt := context.WithCancel(ctx)
			bargeDone := make(chan struct{})
			bargeCh := (<-chan struct{})(nil)
			if cfg.BargeIn {
				bargeCh = sess.ArmBargeIn()
				go func() {
					defer close(bargeDone)
					select {
					case <-bargeCh:
						handle.Node.SetState(node.StateListening)
						cancelPrompt()
					case <-promptCtx.Done():
					}
				}()
			} else {
				close(bargeDone)
			}

			// Drive the state machine explicitly: idle -> speaking. The prompt
			// (with the appended start chime) is played state-neutrally so it
			// does not emit its own PROCESSING/SPEAKING/IDLE churn — the flow
			// transitions straight to LISTENING next.
			handle.Node.SetState(node.StateSpeaking)
			err := turn.Play(promptCtx, samples, sampleRate)
			cancelPrompt()
			<-bargeDone
			barged = sess.DisarmBargeIn()
			if ctx.Err() != nil {
				return Result{ContextID: cfg.ContextID, NodeID: cfg.NodeID, Status: "error", ErrorMessage: ctx.Err().Error()}
			}
			if err != nil && !(barged && errors.Is(err, context.Canceled)) {
				return Result{ContextID: cfg.ContextID, NodeID: cfg.NodeID, Status: "error", ErrorMessage: err.Error()}
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
			handle.Node.SetState(node.StateSpeaking)
			e.playChime(ctx, turn, chimeStart)
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
	if !barged {
		sess.StartCapture()
	}

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
	if ctx.Err() != nil {
		return Result{ContextID: cfg.ContextID, NodeID: cfg.NodeID, Status: "error", ErrorMessage: ctx.Err().Error()}
	}
	// Announce the end of listening with a chime on every device.
	e.playChime(ctx, turn, chimeStop)
	sess.ToProcessing()
	handle.Node.SetState(node.StateProcessing)

	if len(captured) == 0 {
		return Result{
			ContextID: cfg.ContextID,
			NodeID:    cfg.NodeID,
			Status:    "silence_timeout",
		}
	}

	transcription, err := e.ASR.TranscribeSamples(ctx, captured, 16000, ai.JobOptions{Priority: cfg.Priority})
	if err != nil {
		return Result{
			ContextID:    cfg.ContextID,
			NodeID:       cfg.NodeID,
			Status:       "error",
			ErrorMessage: "ASR failed: " + err.Error(),
		}
	}

	var verifiedSpeaker string
	if cfg.RequireSpeakerID {
		audioDuration := float64(len(captured)) / 16000.0
		minDur := float64(e.SpeakerCfg.MinAudioDurationSeconds)

		scoreOk := false
		var matchedSpk string
		if audioDuration >= minDur {
			var err error
			matchedSpk, err = e.SpeakerID.IdentifySamples(captured, 16000)
			if err == nil && matchedSpk != "" {
				scoreOk = true
			}
		}

		if !scoreOk {
			log.Printf("[Ask:%s] Low confidence or short audio (duration: %.2fs, min: %.2fs). Initiating voice challenge.\n", cfg.NodeID, audioDuration, minDur)

			handle.Node.SetState(node.StateChallenging)

			if len(e.SpeakerCfg.ChallengePrompts) == 0 {
				return Result{
					ContextID:    cfg.ContextID,
					NodeID:       cfg.NodeID,
					Status:       "error",
					ErrorMessage: "no challenge prompts configured",
				}
			}

			challengePrompt := e.SpeakerCfg.ChallengePrompts[rand.Intn(len(e.SpeakerCfg.ChallengePrompts))]
			challengeText := challengePrompt
			if e.SpeakerCfg.ChallengeInitPrompt != "" {
				challengeText = fmt.Sprintf("%s: %s", e.SpeakerCfg.ChallengeInitPrompt, challengePrompt)
			}

			log.Printf("[Ask:%s] Challenge prompt: %q\n", cfg.NodeID, challengeText)

			challengeSamples, challengeSampleRate, err := e.Brain.Synthesize(ctx, challengeText, cfg.Priority)
			if err != nil {
				return Result{
					ContextID:    cfg.ContextID,
					NodeID:       cfg.NodeID,
					Status:       "error",
					ErrorMessage: "challenge synthesis failed: " + err.Error(),
				}
			}

			handle.Node.SetState(node.StateSpeaking)
			err = turn.Play(ctx, challengeSamples, challengeSampleRate)
			if err != nil && err != context.Canceled {
				return Result{
					ContextID:    cfg.ContextID,
					NodeID:       cfg.NodeID,
					Status:       "error",
					ErrorMessage: "challenge playback failed: " + err.Error(),
				}
			}

			select {
			case <-time.After(150 * time.Millisecond):
			case <-ctx.Done():
				return Result{
					ContextID:    cfg.ContextID,
					NodeID:       cfg.NodeID,
					Status:       "error",
					ErrorMessage: "context cancelled during challenge",
				}
			}

			sess.ToListening()
			handle.Node.SetState(node.StateListening)
			sess.StartCapture()

			deadlineSec := time.After(timeout)
			hadSpeechSec := false
			lastCountSec := 0
			lastSpeechTimeSec := time.Now()

			pollTickerSec := time.NewTicker(50 * time.Millisecond)
			defer pollTickerSec.Stop()

		listenLoopSec:
			for {
				select {
				case <-ctx.Done():
					break listenLoopSec
				case <-deadlineSec:
					break listenLoopSec
				case <-pollTickerSec.C:
					count := sess.CapturedCount()
					if count > lastCountSec {
						hadSpeechSec = true
						lastCountSec = count
						lastSpeechTimeSec = time.Now()
					} else if hadSpeechSec && time.Since(lastSpeechTimeSec) > time.Duration(postSpeechSilence*float64(time.Second)) {
						break listenLoopSec
					}
				}
			}

			secondaryCaptured := sess.StopCapture()
			if ctx.Err() != nil {
				return Result{ContextID: cfg.ContextID, NodeID: cfg.NodeID, Status: "error", ErrorMessage: ctx.Err().Error()}
			}
			e.playChime(ctx, turn, chimeStop)
			sess.ToProcessing()
			handle.Node.SetState(node.StateProcessing)

			if len(secondaryCaptured) == 0 {
				log.Printf("[Ask:%s] No speech captured during challenge loop.\n", cfg.NodeID)
				e.playFailPrompt(ctx, turn, cfg.Priority)
				return Result{
					ContextID: cfg.ContextID,
					NodeID:    cfg.NodeID,
					Status:    "verification_failed",
				}
			}

			matchedSpkSec, err := e.SpeakerID.IdentifySamples(secondaryCaptured, 16000)
			if err != nil || matchedSpkSec == "" {
				log.Printf("[Ask:%s] Challenge verification failed. Matched: %q, Err: %v\n", cfg.NodeID, matchedSpkSec, err)
				e.playFailPrompt(ctx, turn, cfg.Priority)
				return Result{
					ContextID: cfg.ContextID,
					NodeID:    cfg.NodeID,
					Status:    "verification_failed",
				}
			}

			log.Printf("[Ask:%s] Challenge verification successful: speaker=%s\n", cfg.NodeID, matchedSpkSec)
			verifiedSpeaker = matchedSpkSec
		} else {
			log.Printf("[Ask:%s] Speaker verification successful on primary audio: %s\n", cfg.NodeID, matchedSpk)
			verifiedSpeaker = matchedSpk
		}

		return Result{
			ContextID:     cfg.ContextID,
			NodeID:        cfg.NodeID,
			Status:        "success",
			Transcription: transcription,
			Speaker:       verifiedSpeaker,
			Audio:         captured,
			SampleRate:    16000,
		}
	}

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

// playChime uses the turn's lifetime, so shutdown never starts independent work.
func (e *Engine) playChime(parent context.Context, turn *brain.Turn, kind chimeKind) {
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	if err := turn.Play(ctx, chimePCM(kind), chimeSampleRate); err != nil && !errors.Is(err, context.Canceled) {
		log.Printf("[Ask:%s] chime playback error: %v", turn.Handle.Node.NodeID, err)
	}
}

// playFailPrompt plays the challenge failed TTS prompt on the node.
func (e *Engine) playFailPrompt(ctx context.Context, turn *brain.Turn, priority int) {
	prompt := e.SpeakerCfg.ChallengeFailedPrompt
	if prompt == "" {
		prompt = "Acceso denegado."
	}
	samples, sampleRate, err := e.Brain.Synthesize(ctx, prompt, priority)
	if err != nil || len(samples) == 0 {
		return
	}
	handle := turn.Handle
	if handle != nil {
		handle.Node.SetState(node.StateSpeaking)
	}
	_ = turn.Play(ctx, samples, sampleRate)
}

// Validate limits memory and capture time before acquiring a node.
func (c Config) Validate() error {
	if c.NodeID == "" || c.NodeID == "all" {
		return fmt.Errorf("ask requires one node_id")
	}
	if len(c.TTSText) > ai.MaxTextBytes {
		return fmt.Errorf("prompt exceeds %d bytes", ai.MaxTextBytes)
	}
	for _, d := range []struct{ value, max float64 }{{c.VADTimeoutSeconds, ai.MaxAudioSeconds}, {c.CaptureDelaySeconds, 20}, {c.PostSpeechSilenceSeconds, 20}} {
		if math.IsNaN(d.value) || math.IsInf(d.value, 0) || d.value < 0 || d.value > d.max {
			return fmt.Errorf("invalid ask duration")
		}
	}
	return nil
}
