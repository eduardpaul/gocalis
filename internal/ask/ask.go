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
	"gocalis/internal/audionode"
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

	// InCall lets the ask run while the node is in an intercom call, inside
	// the call: the node is set aside from the conversation for as long as the
	// ask plays its prompt and listens. Without it an ask for a node in a call
	// fails, as the call owns the node.
	InCall bool
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

	// InCall tells that the ask ran inside the intercom call the node is in.
	InCall bool
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
func (e *Engine) Run(ctx context.Context, cfg Config) (result Result) {
	startedAt := time.Now()
	timings := make(map[string]time.Duration)
	if err := cfg.Validate(); err != nil {
		return Result{ContextID: cfg.ContextID, NodeID: cfg.NodeID, Status: "error", ErrorMessage: err.Error()}
	}
	if cfg.InCall {
		if line := e.Brain.CallLine(cfg.NodeID); line != nil {
			return e.runInCall(ctx, cfg, line)
		}
	}
	turn, err := e.Brain.AcquireNode(ctx, cfg.NodeID, cfg.Priority)
	if err != nil {
		return Result{ContextID: cfg.ContextID, NodeID: cfg.NodeID, Status: "error", ErrorMessage: err.Error()}
	}
	defer turn.Release()
	// Run the whole flow on the turn's context: an intercom call that takes the
	// node over cancels it, ending the ask cleanly with a clear reason instead of
	// letting it listen to (or answer) the people talking on the intercom.
	ctx = turn.Context()
	defer func() {
		if turn.Preempted() && result.Status != "success" {
			result.Status = "error"
			result.ErrorMessage = brain.ErrPreempted.Error()
			result.Transcription = ""
			result.Audio = nil
		}
	}()
	handle := turn.Handle
	if handle.Config.Diagnostics.Enabled {
		defer func() {
			log.Printf("[Ask:%s] diagnostics status=%s total=%s stages=%v", cfg.NodeID, result.Status, time.Since(startedAt), timings)
		}()
	}
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
		promptCtx, cancelPrompt := context.WithCancel(ctx)
		defer cancelPrompt()
		synthesisAt := time.Now()
		prompt, err := e.Brain.SynthesisStream(promptCtx, cfg.TTSText, cfg.Priority)
		timings["prompt_synthesis"] = time.Since(synthesisAt)
		if err != nil {
			return Result{
				ContextID:    cfg.ContextID,
				NodeID:       cfg.NodeID,
				Status:       "error",
				ErrorMessage: err.Error(),
			}
		}

		// Stream the prompt, then append the listening chime in the same route.
		if prompt.SampleRate() > 0 {
			sampleRate := prompt.SampleRate()
			var samples []int16
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

			source := &promptSource{prompt: prompt, tail: audionode.NewSliceSource(samples, sampleRate)}
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
			playbackAt := time.Now()
			err := turn.PlayStream(promptCtx, source)
			timings["prompt_playback"] = time.Since(playbackAt)
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
	if !barged {
		if reset, ok := handle.Audio.(interface{ ResetCapture() }); ok {
			reset.ResetCapture()
		}
		sess.StartCapture()
	}
	handle.Node.SetState(node.StateListening)

	timings["capture_open_offset"] = time.Since(startedAt)
	captureAt := time.Now()
	timeout := time.Duration(cfg.VADTimeoutSeconds * float64(time.Second))
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	postSpeechSilence := cfg.PostSpeechSilenceSeconds
	if postSpeechSilence <= 0 {
		postSpeechSilence = handle.Config.GetPostSpeechSilenceSeconds(1.5)
	}
	waitForEndpoint(ctx, sess, timeout, time.Duration(postSpeechSilence*float64(time.Second)))

	captured := sess.StopCapture()
	timings["capture"] = time.Since(captureAt)
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

	asrAt := time.Now()
	transcription, err := e.transcribe(ctx, cfg, handle, captured)
	timings["asr_total"] = time.Since(asrAt)
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
			if reset, ok := handle.Audio.(interface{ ResetCapture() }); ok {
				reset.ResetCapture()
			}
			sess.StartCapture()
			handle.Node.SetState(node.StateListening)

			waitForEndpoint(ctx, sess, timeout, time.Duration(postSpeechSilence*float64(time.Second)))

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

// transcribe keeps the captured speech for diagnostics and runs ASR on it.
func (e *Engine) transcribe(ctx context.Context, cfg Config, handle *brain.NodeHandle, captured []float32) (string, error) {
	if err := handle.Recorder.Save(captured); err != nil {
		log.Printf("[Ask:%s] recording: %v", cfg.NodeID, err)
	}
	return e.ASR.TranscribeSamples(ctx, captured, 16000, ai.JobOptions{Priority: cfg.Priority, OnTiming: func(wait, inference time.Duration) {
		if handle.Config.Diagnostics.Enabled {
			log.Printf("[Ask:%s] ASR queue=%s inference=%s", cfg.NodeID, wait, inference)
		}
	}})
}

// runInCall is the ask flow for a node that is in an intercom call. The call
// keeps the node (its state stays INTERCOM and its speaker keeps playing the
// call's mix), so the ask goes through the node's line into the call: the node
// is set aside from the conversation, hears the prompt and the chimes alone, and
// is put back as soon as it has been listened to. The others never hear the
// question, and whatever they say meanwhile reaches neither the node's speaker
// nor, through it, the captured answer.
//
// There is no barge-in and no voice challenge in a call: a speaker that cannot
// be verified on what it said fails the ask.
func (e *Engine) runInCall(ctx context.Context, cfg Config, line brain.CallLine) Result {
	result := func(status, message string) Result {
		return Result{ContextID: cfg.ContextID, NodeID: cfg.NodeID, Status: status, ErrorMessage: message, InCall: true}
	}
	handle := e.Brain.GetNodeHandle(cfg.NodeID)
	if handle == nil {
		return result("error", fmt.Sprintf("node '%s' not registered", cfg.NodeID))
	}
	hold, err := line.Hold(ctx)
	if err != nil {
		return result("error", err.Error())
	}
	defer hold.Release()
	// While the node is held the ask runs on the hold's context, which also
	// ends when the node goes out of the call.
	request := ctx
	ctx = hold.Context()
	// failed reports why the ask stopped short: the node left the call, or the
	// request was cancelled.
	failed := func(err error) Result {
		if cause := context.Cause(hold.Context()); cause != nil {
			err = cause
		}
		return result("error", err.Error())
	}

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

	// The prompt, then the "listening" chime, as one clip.
	cue, rate := chimePCM(chimeStart), chimeSampleRate
	if strings.TrimSpace(cfg.TTSText) != "" {
		prompt, promptRate, err := e.Brain.Synthesize(ctx, cfg.TTSText, cfg.Priority)
		if err != nil {
			return failed(err)
		}
		if len(prompt) > 0 {
			cue, rate = append(prompt, renderChimeAt(chimeStart, promptRate)...), promptRate
		}
	}
	if err := hold.Play(cue, rate); err != nil {
		return failed(err)
	}
	// Let the node's speaker play the chime out before listening, so a
	// half-duplex device does not capture it as speech.
	if cfg.CaptureDelaySeconds > 0 {
		select {
		case <-time.After(time.Duration(cfg.CaptureDelaySeconds * float64(time.Second))):
		case <-ctx.Done():
			return failed(ctx.Err())
		}
	}

	sess.ToListening()
	sess.StartCapture()
	timeout := time.Duration(cfg.VADTimeoutSeconds * float64(time.Second))
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	postSpeechSilence := cfg.PostSpeechSilenceSeconds
	if postSpeechSilence <= 0 {
		postSpeechSilence = handle.Config.GetPostSpeechSilenceSeconds(1.5)
	}
	waitForEndpoint(ctx, sess, timeout, time.Duration(postSpeechSilence*float64(time.Second)))
	captured := sess.StopCapture()
	if ctx.Err() != nil {
		return failed(ctx.Err())
	}
	if err := hold.Play(chimePCM(chimeStop), chimeSampleRate); err != nil {
		return failed(err)
	}
	// The node goes back to its call while what it said is worked out.
	hold.Release()
	ctx = request
	sess.ToProcessing()

	if len(captured) == 0 {
		return result("silence_timeout", "")
	}
	transcription, err := e.transcribe(ctx, cfg, handle, captured)
	if err != nil {
		return result("error", "ASR failed: "+err.Error())
	}
	res := result("success", "")
	res.Transcription = transcription
	res.Audio = captured
	res.SampleRate = 16000
	if cfg.RequireSpeakerID {
		if float64(len(captured))/16000 >= float64(e.SpeakerCfg.MinAudioDurationSeconds) {
			res.Speaker, _ = e.SpeakerID.IdentifySamples(captured, 16000)
		}
		if res.Speaker == "" {
			return result("verification_failed", "")
		}
	}
	return res
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

// Wait for onset separately from the bounded spoken turn. PCM arrival is not
// speech activity: natural pauses remain in the waveform without stacking waits.
func waitForEndpoint(ctx context.Context, sess *session.Session, onsetTimeout, silence time.Duration) {
	opened := time.Now()
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		started, last := sess.SpeechActivity()
		now := time.Now()
		if started.IsZero() {
			if now.Sub(opened) >= onsetTimeout {
				return
			}
		} else if now.Sub(last) >= silence || now.Sub(started) >= ai.MaxAudioSeconds*time.Second {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
