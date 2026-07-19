// Package runtime encapsulates the per-node orchestration that was previously
// inlined in main: it wires the capture pipeline (VAD -> wake word -> speaker-ID
// -> session capture), routes wake/speaker events and TTS replies through the
// brain, and owns the node's lifecycle. Moving this out of main keeps the entry
// point to flag parsing + dependency injection and makes the wiring testable.
package runtime

import (
	"context"
	"encoding/base64"
	"fmt"
	"log"
	"net/url"
	"time"

	"gocalis/internal/ai"
	"gocalis/internal/ask"
	"gocalis/internal/audio"
	"gocalis/internal/audionode"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/localaudio"
	"gocalis/internal/node"
	"gocalis/internal/protocol"
	"gocalis/internal/webrtc"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

// AudioNodeFactory builds an AudioNode from a WebRTC client config. It is
// injectable so tests can substitute a stub for the concrete WebRTC transport.
type AudioNodeFactory func(cfg webrtc.Config) (audionode.AudioNode, error)

// NodeRuntime drives a single physical audio node end to end.
type NodeRuntime struct {
	node    config.NodeConfig
	global  *config.Config
	asr     ai.Transcriber
	speaker ai.SpeakerIdentifier
	events  protocol.EventPublisher
	brain   *brain.Brain
	ask     *ask.Engine
	threads int

	newAudioNode AudioNodeFactory
}

// New creates a NodeRuntime with the default WebRTC audio-node factory.
func New(
	nodeCfg config.NodeConfig,
	global *config.Config,
	asr ai.Transcriber,
	speaker ai.SpeakerIdentifier,
	events protocol.EventPublisher,
	b *brain.Brain,
	askEngine *ask.Engine,
	threads int,
) *NodeRuntime {
	return &NodeRuntime{
		node:    nodeCfg,
		global:  global,
		asr:     asr,
		speaker: speaker,
		events:  events,
		brain:   b,
		ask:     askEngine,
		threads: threads,
		newAudioNode: func(cfg webrtc.Config) (audionode.AudioNode, error) {
			return webrtc.NewClientWithConfig(cfg)
		},
	}
}

// signalingURL converts a go2rtc HTTP api URL + stream name into the WebSocket
// signaling URL used to negotiate the WebRTC connection.
func signalingURL(apiURL, streamName string) (string, error) {
	u, err := url.Parse(apiURL)
	if err != nil {
		return "", err
	}
	scheme := "ws"
	if u.Scheme == "https" {
		scheme = "wss"
	}
	return scheme + "://" + u.Host + "/api/ws?src=" + streamName, nil
}

// Run manages the life-cycle, signaling, ASR, TTS, and state transitions for the
// node. It blocks until ctx is cancelled, then tears everything down via defers.
func (r *NodeRuntime) Run(ctx context.Context) {
	nodeCfg := r.node
	pNode := node.NewPhysicalNode(nodeCfg.NodeID, nodeCfg.Type)

	// Broadcast PhysicalNode state changes to all transports.
	pNode.OnStateChanged(func(oldState, newState node.NodeState) {
		log.Printf("[Node:%s] State changed: %s -> %s\n", pNode.NodeID, oldState, newState)
		r.events.Publish(protocol.Response{
			Event:  "state_changed",
			NodeID: pNode.NodeID,
			State:  string(newState),
		})
	})

	var rtcCfg webrtc.Config
	if nodeCfg.Type == "rtc_stream" {
		sigURL, err := signalingURL(nodeCfg.RTCStream.ApiURL, nodeCfg.RTCStream.StreamName)
		if err != nil {
			log.Printf("[Node:%s] Error parsing apiURL: %v\n", nodeCfg.NodeID, err)
			return
		}
		rtcCfg = webrtc.Config{
			SignalingURL:   sigURL,
			SendCodec:      "opus",
			APIBaseURL:     nodeCfg.RTCStream.ApiURL,
			TalkbackStream: nodeCfg.RTCStream.TalkbackStream,
			TalkbackIn:     nodeCfg.RTCStream.TalkbackInStream,
		}
	} else if nodeCfg.Type != "local" {
		log.Printf("[Node:%s] Skipping handler: node type '%s' is not supported in this version\n", nodeCfg.NodeID, nodeCfg.Type)
		return
	}

	// Initialize VoiceActivityDetector (VAD Gate)
	log.Printf("[Node:%s] Initializing VAD Gate (model: %s)...\n", nodeCfg.NodeID, r.global.Models.VAD.SileroOnnxPath)
	vadConfig := sherpa.VadModelConfig{
		SileroVad: sherpa.SileroVadModelConfig{
			Model:              r.global.Models.VAD.SileroOnnxPath,
			Threshold:          r.global.Models.VAD.Threshold,
			MinSilenceDuration: float32(r.global.Models.VAD.MinSilenceDurationMs) / 1000.0,
			MinSpeechDuration:  0.25,
			WindowSize:         512,
			MaxSpeechDuration:  20.0,
		},
		SampleRate: 16000,
		NumThreads: r.threads,
		Provider:   "cpu",
		Debug:      0,
	}
	vadGate := sherpa.NewVoiceActivityDetector(&vadConfig, 10.0)
	if vadGate != nil {
		defer sherpa.DeleteVoiceActivityDetector(vadGate)
	}

	// Initialize Wake Word Detector (Sherpa-ONNX streaming KWS from config)
	log.Printf("[Node:%s] Loading Wake Word Detector (model: %s, keywords: %s)...\n", nodeCfg.NodeID, nodeCfg.KWS.Encoder, nodeCfg.KWS.KeywordsFile)
	wakeDetector, err := ai.NewSherpaONNXWakeDetector(nodeCfg.KWS, nodeCfg.GetKWSNumThreads(r.threads))
	if err != nil {
		log.Printf("[Node:%s] Failed to initialize Wake Word Detector: %v\n", nodeCfg.NodeID, err)
		return
	}
	defer wakeDetector.Close()

	// Initialize the audio transport (WebRTC/go2rtc or local audio device).
	var audioNode audionode.AudioNode
	if nodeCfg.Type == "local" {
		log.Printf("[Node:%s] Initializing local audio hardware transport...\n", nodeCfg.NodeID)
		audioNode = localaudio.New(nodeCfg)
	} else {
		log.Printf("[Node:%s] Initializing WebRTC audio transport...\n", nodeCfg.NodeID)
		var err error
		audioNode, err = r.newAudioNode(rtcCfg)
		if err != nil {
			log.Printf("[Node:%s] Failed to create WebRTC audio transport: %v\n", nodeCfg.NodeID, err)
			return
		}
	}
	defer audioNode.Close()

	// Register this active connection with the central brain orchestrator.
	handle := &brain.NodeHandle{
		Node:   pNode,
		Audio:  audioNode,
		Config: nodeCfg,
	}
	r.brain.RegisterNode(nodeCfg.NodeID, handle)
	defer r.brain.UnregisterNode(nodeCfg.NodeID)

	// Create wake word audio stream
	wakeStream, err := wakeDetector.CreateStream(func(keyword string) {
		log.Printf("[Node:%s] [WAKE WORD DETECTED] Match found: '%s'!\n", nodeCfg.NodeID, keyword)

		// Trigger Spanish TTS Reply over the WebRTC talkback backchannel. When no
		// wake_responses are configured, there is no spoken reply: the ask flow below
		// still plays the listening chime, and the empty TTS pipeline is never run.
		replyText := ""
		if len(nodeCfg.KWS.WakeResponses) > 0 {
			replyText = nodeCfg.KWS.WakeResponses[0]
		}

		// When AutoAsk is configured, drive the wake reply THROUGH the ask flow so
		// that capture only begins once the prompt has finished playing (or, when
		// barge-in is enabled, once the user interrupts it). This makes the next
		// VAD-detected speech transition the node into listening, instead of racing
		// a fixed window against playback+jitter latency.
		//
		// A SINGLE combined "wake" event is raised per trigger: without AutoAsk it
		// carries only the device; with AutoAsk it is deferred until the ASR turn
		// completes and additionally carries the transcription, speaker, and an
		// optional base64 WAV recording.
		if nodeCfg.KWS.AutoAsk {
			// Leave IDLE synchronously so wake detection pauses immediately (no
			// re-fire on the follow-up speech) and, in half-duplex, the mic is
			// gated while the prompt plays. Start in the SAME state the ask flow
			// will use so no spurious transition is emitted — SetState dedups the
			// repeat. This yields a clean stream:
			//   with a reply: idle -> speaking -> listening -> processing -> idle
			//   no reply:     idle -> listening -> processing -> idle
			if replyText != "" {
				pNode.SetState(node.StateSpeaking)
			} else {
				pNode.SetState(node.StateListening)
			}
			go func() {
				log.Printf("[Node:%s] AutoAsk: playing wake reply then listening for the next speech...\n", nodeCfg.NodeID)
				result := r.ask.Run(ctx, ask.Config{
					ContextID:           "autoask-" + nodeCfg.NodeID,
					NodeID:              nodeCfg.NodeID,
					TTSText:             replyText,
					BargeIn:             nodeCfg.KWS.AutoAskBargeIn,
					VADTimeoutSeconds:   nodeCfg.GetAutoAskTimeoutSeconds(10),
					CaptureDelaySeconds: nodeCfg.GetAutoAskCaptureDelaySeconds(1.5),
					Priority:            10,
				})

				event := protocol.Response{
					Event:     "wake",
					NodeID:    nodeCfg.NodeID,
					Keyword:   keyword,
					AutoAsk:   true,
					Status:    result.Status,
					Text:      result.Transcription,
					Speaker:   result.Speaker,
					Timestamp: time.Now().Unix(),
				}

				switch result.Status {
				case "success":
					log.Printf("[Node:%s] AutoAsk Result: \"%s\"\n", nodeCfg.NodeID, result.Transcription)
					if nodeCfg.KWS.AutoAskRecord && len(result.Audio) > 0 {
						sr := result.SampleRate
						if sr <= 0 {
							sr = 16000
						}
						event.Recording = base64.StdEncoding.EncodeToString(audio.EncodeWAVFloat32(result.Audio, sr))
						event.SampleRate = sr
					}
				case "silence_timeout":
					log.Printf("[Node:%s] AutoAsk: No speech captured (silence).\n", nodeCfg.NodeID)
				default:
					log.Printf("[Node:%s] AutoAsk: session ended with status '%s' (%s)\n", nodeCfg.NodeID, result.Status, result.ErrorMessage)
				}

				r.events.Publish(event)
			}()
			return
		}

		// No AutoAsk: raise the device-only wake event, then optionally play the reply.
		pNode.SetState(node.StateListening)
		r.events.Publish(protocol.Response{
			Event:     "wake",
			NodeID:    pNode.NodeID,
			Keyword:   keyword,
			Timestamp: time.Now().Unix(),
		})

		if replyText == "" {
			return
		}
		log.Printf("[Node:%s] TTS: Routing wake reply through brain: \"%s\"\n", nodeCfg.NodeID, replyText)
		if err := r.brain.Speak(ctx, nodeCfg.NodeID, replyText, 10); err != nil {
			log.Printf("[Node:%s] TTS: Failed to play wake reply: %v\n", nodeCfg.NodeID, err)
		}
	})
	if err != nil {
		log.Printf("[Node:%s] Failed to create Wake Word Stream: %v\n", nodeCfg.NodeID, err)
		return
	}
	defer wakeStream.Close()



	// Subscribe incoming audio: wake word runs on the continuous stream while
	// speaker ID and ask/capture are routed through the VAD Gate.
	halfDuplex := !nodeCfg.RTCStream.EchoCancellation
	var dbgCalls, dbgDrops, dbgSegs, dbgSamples int64
	wakeActive := false // whether the wake stream is currently being fed (node IDLE)
	audioNode.OnAudio(func(samples []float32) {
		dbgCalls++
		dbgSamples += int64(len(samples))
		state := pNode.GetState()
		if dbgCalls == 1 || dbgCalls%200 == 0 {
			log.Printf("[Node:%s] DBG audio call#%d state=%s totalSamples=%d drops=%d segs=%d",
				nodeCfg.NodeID, dbgCalls, state, dbgSamples, dbgDrops, dbgSegs)
		}
		// Half-duplex gate: while the node is playing its own TTS, the microphone
		// picks up that audio. Without acoustic echo cancellation, feeding it to the
		// wake/speaker/barge-in/capture paths causes self-wake and self-barge-in.
		// Drop incoming audio while SPEAKING unless the device declares echo
		// cancellation (rtc_stream.echo_cancellation: true).
		if halfDuplex && state == node.StateSpeaking {
			dbgDrops++
			return
		}

		// Intercom tap: while this node is bridged to a peer, forward the raw
		// continuous mic to the intercom engine (which streams it to the peer's
		// speaker). No tap installed = a cheap map lookup. The node is in the
		// INTERCOM state during a call, so the half-duplex gate above does not
		// drop this audio and full-duplex conversation is preserved.
		r.brain.ForwardRawAudio(nodeCfg.NodeID, samples)

		// Wake word detection runs on the CONTINUOUS audio stream, but only while the
		// node is IDLE (waiting for a fresh activation). The streaming zipformer KWS
		// needs future (right-context) frames to decode the final phonemes of a
		// keyword, and its featurizer holds back the last frames of every
		// AcceptWaveform call until more audio arrives. Feeding it isolated VAD
		// segments truncates the keyword tail — a short keyword like "Alfred" that
		// ends a segment is never decoded — so it must see the continuous stream.
		//
		// While a turn is already in progress (LISTENING/PROCESSING/SPEAKING/
		// CHALLENGING) the detector is paused: otherwise it re-fires on the user's
		// follow-up speech or the echo of its own backchannel TTS reply, spawning
		// overlapping AutoAsk sessions that abort each other (empty ASR results). On
		// the transition back to IDLE the stream is reset so stale audio buffered
		// during the previous turn cannot immediately re-trigger the wake word.
		if state == node.StateIdle {
			if !wakeActive {
				wakeStream.Reset()
				wakeActive = true
			}
			wakeStream.AcceptAudio(samples)
		} else {
			wakeActive = false
		}

		if vadGate != nil {
			vadGate.AcceptWaveform(samples)
			for !vadGate.IsEmpty() {
				segment := vadGate.Front()
				dbgSegs++
				log.Printf("[Node:%s] DBG VAD segment #%d -> speaker/ask (%d samples)", nodeCfg.NodeID, dbgSegs, len(segment.Samples))

				// Fan the segment out to any active /ask or AutoAsk sessions
				// (barge-in signaling + capture) on this node.
				r.brain.FeedAudio(nodeCfg.NodeID, segment.Samples)
				vadGate.Pop()
			}
		} else {
			// Fan raw audio out to active sessions when VAD is unavailable.
			r.brain.FeedAudio(nodeCfg.NodeID, samples)
		}
	})

	// Connect to the transport.
	connectTarget := "local audio devices"
	if nodeCfg.Type == "rtc_stream" {
		connectTarget = fmt.Sprintf("WebRTC at %s", rtcCfg.SignalingURL)
	}
	log.Printf("[Node:%s] Connecting to audio transport (%s)...\n", nodeCfg.NodeID, connectTarget)
	if err := audioNode.Connect(ctx); err != nil {
		log.Printf("[Node:%s] Failed to connect audio transport: %v\n", nodeCfg.NodeID, err)
		return
	}
	log.Printf("[Node:%s] Connection handshake complete. State machine is active!\n", nodeCfg.NodeID)

	// Block until context is cancelled
	<-ctx.Done()
	log.Printf("[Node:%s] Closing node handler...\n", nodeCfg.NodeID)
}
