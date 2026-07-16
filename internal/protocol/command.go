package protocol

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"path/filepath"
	"strings"

	"gocalis/internal/ai"
	"gocalis/internal/ask"
	"gocalis/internal/audio"
	"gocalis/internal/brain"
	"gocalis/internal/config"
)

// Executor runs actions requested by any transport adapter using the central brain
// and the AI engines, then publishes the results back through EventPublisher.
type Executor struct {
	Brain         *brain.Brain
	ASREngine     ai.Transcriber
	SpeakerEngine ai.SpeakerIdentifier
	Publisher     EventPublisher
	AskEngine     *ask.Engine

	// AudioBaseDir constrains where transport-supplied audio_file paths may point.
	// It defaults to the process working directory. Requests referencing paths
	// outside this directory are rejected to prevent arbitrary file reads.
	AudioBaseDir string
}

// NewExecutor creates a command executor backed by the given engines and publisher.
func NewExecutor(brain *brain.Brain, asr ai.Transcriber, speaker ai.SpeakerIdentifier, publisher EventPublisher, speakerIDCfg config.SpeakerIDConfig) *Executor {
	return &Executor{
		Brain:         brain,
		ASREngine:     asr,
		SpeakerEngine: speaker,
		Publisher:     publisher,
		AskEngine:     ask.NewEngine(brain, asr, speaker, speakerIDCfg),
		AudioBaseDir:  ".",
	}
}

// resolveAudioFile validates a transport-supplied audio file path and returns a
// cleaned absolute path confined to AudioBaseDir. It rejects empty paths and any
// path that escapes the base directory (e.g. via "..").
func (e *Executor) resolveAudioFile(audioFile string) (string, error) {
	if audioFile == "" {
		return "", fmt.Errorf("missing 'audio_file' parameter")
	}

	base := e.AudioBaseDir
	if base == "" {
		base = "."
	}
	baseAbs, err := filepath.Abs(base)
	if err != nil {
		return "", fmt.Errorf("invalid audio base directory: %w", err)
	}

	resolved := audioFile
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(baseAbs, resolved)
	}
	resolved = filepath.Clean(resolved)

	rel, err := filepath.Rel(baseAbs, resolved)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("audio_file path is outside the allowed directory")
	}

	return resolved, nil
}

// Execute dispatches a request to the appropriate handler based on Action.
func (e *Executor) Execute(ctx context.Context, req Request) {
	switch req.Action {
	case "tts":
		e.executeTTS(ctx, req)
	case "asr":
		e.executeASR(ctx, req)
	case "speaker_id":
		e.executeSpeakerID(ctx, req)
	case "ask":
		e.executeAsk(ctx, req)
	case "play":
		e.executePlay(ctx, req)
	default:
		e.publishError(req.NodeID, fmt.Sprintf("unknown action: %s", req.Action))
	}
}

func (e *Executor) executeTTS(ctx context.Context, req Request) {
	if req.Text == "" {
		e.publishError(req.NodeID, "missing 'text' parameter")
		return
	}
	if req.NodeID == "" {
		e.publishError("", "missing 'node_id' parameter")
		return
	}

	var err error
	if req.NodeID == "all" {
		log.Printf("[Executor] TTS to all nodes: \"%s\"\n", req.Text)
		err = e.Brain.SpeakAll(ctx, req.Text, req.Priority)
	} else {
		log.Printf("[Executor] TTS to node '%s': \"%s\"\n", req.NodeID, req.Text)
		err = e.Brain.Speak(ctx, req.NodeID, req.Text, req.Priority)
	}

	if err != nil {
		e.publishError(req.NodeID, err.Error())
		return
	}

	e.Publisher.Publish(Response{
		Event:  "tts_completed",
		NodeID: req.NodeID,
		Status: "success",
	})
}

// executePlay plays back a base64-encoded PCM16 WAV recording on a node,
// following the same routing rules as TTS: node_id "all" broadcasts to every
// node and Priority orders the node's turn.
func (e *Executor) executePlay(ctx context.Context, req Request) {
	if req.NodeID == "" {
		e.publishError("", "missing 'node_id' parameter")
		return
	}
	if req.AudioWavBase64 == "" {
		e.publishError(req.NodeID, "missing 'audio_wav_base64' parameter")
		return
	}

	raw, err := base64.StdEncoding.DecodeString(req.AudioWavBase64)
	if err != nil {
		e.publishError(req.NodeID, "invalid base64 audio: "+err.Error())
		return
	}

	samples, sampleRate, err := audio.DecodeWAVPCM16(raw)
	if err != nil {
		e.publishError(req.NodeID, "invalid WAV audio: "+err.Error())
		return
	}

	if req.NodeID == "all" {
		log.Printf("[Executor] Play recording on all nodes (%d samples @ %dHz)\n", len(samples), sampleRate)
		err = e.Brain.PlaySamplesAll(ctx, samples, sampleRate, req.Priority)
	} else {
		log.Printf("[Executor] Play recording on node '%s' (%d samples @ %dHz)\n", req.NodeID, len(samples), sampleRate)
		err = e.Brain.PlaySamples(ctx, req.NodeID, samples, sampleRate, req.Priority)
	}

	if err != nil {
		e.publishError(req.NodeID, err.Error())
		return
	}

	e.Publisher.Publish(Response{
		Event:  "play_completed",
		NodeID: req.NodeID,
		Status: "success",
	})
}

func (e *Executor) executeASR(ctx context.Context, req Request) {
	audioFile, err := e.resolveAudioFile(req.AudioFile)
	if err != nil {
		e.publishError(req.NodeID, err.Error())
		return
	}

	log.Printf("[Executor] ASR for file '%s' (node: %s)\n", audioFile, req.NodeID)
	text, err := e.ASREngine.TranscribeFile(audioFile, ai.JobOptions{Priority: req.Priority})
	if err != nil {
		e.publishError(req.NodeID, "ASR transcription failed: "+err.Error())
		return
	}

	e.Publisher.Publish(Response{
		Event:  "asr_completed",
		NodeID: req.NodeID,
		Status: "success",
		Text:   text,
	})
}

func (e *Executor) executeSpeakerID(ctx context.Context, req Request) {
	audioFile, err := e.resolveAudioFile(req.AudioFile)
	if err != nil {
		e.publishError(req.NodeID, err.Error())
		return
	}

	log.Printf("[Executor] SpeakerID for file '%s' (node: %s)\n", audioFile, req.NodeID)
	speaker, err := e.SpeakerEngine.IdentifyFile(audioFile)
	if err != nil {
		e.publishError(req.NodeID, "Speaker ID failed: "+err.Error())
		return
	}

	e.Publisher.Publish(Response{
		Event:   "speaker_id_completed",
		NodeID:  req.NodeID,
		Status:  "success",
		Speaker: speaker,
	})
}

func (e *Executor) executeAsk(ctx context.Context, req Request) {
	if req.NodeID == "" {
		e.publishError("", "missing 'node_id' parameter")
		return
	}

	log.Printf("[Executor] Ask on node '%s' (barge_in=%v)\n", req.NodeID, req.BargeIn)

	result := e.AskEngine.Run(ctx, ask.Config{
		ContextID:                req.ContextID,
		NodeID:                   req.NodeID,
		TTSText:                  req.Text,
		BargeIn:                  req.BargeIn,
		RequireSpeakerID:         req.RequireSpeakerID,
		VADTimeoutSeconds:        req.VADTimeoutSeconds,
		PostSpeechSilenceSeconds: req.PostSpeechSilenceSeconds,
		Priority:                 req.Priority,
	})

	resp := Response{
		Event:   "ask_completed",
		NodeID:  req.NodeID,
		Status:  result.Status,
		Speaker: result.Speaker,
		Message: result.ErrorMessage,
	}

	switch req.OutputFormat {
	case "audio":
		if len(result.Audio) > 0 {
			resp.AudioWavBase64 = base64.StdEncoding.EncodeToString(audio.EncodeWAVFloat32(result.Audio, result.SampleRate))
		}
	case "text":
		resp.Text = result.Transcription
	default:
		resp.Text = result.Transcription
		if len(result.Audio) > 0 {
			resp.AudioWavBase64 = base64.StdEncoding.EncodeToString(audio.EncodeWAVFloat32(result.Audio, result.SampleRate))
		}
	}

	e.Publisher.Publish(resp)
}

func (e *Executor) publishError(nodeID string, message string) {
	log.Printf("[Executor] Error (node: %s): %s\n", nodeID, message)
	e.Publisher.Publish(Response{
		Event:   "error",
		NodeID:  nodeID,
		Status:  "error",
		Message: message,
	})
}
