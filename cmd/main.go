package main

import (
	"context"
	"errors"
	"flag"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gocalis/internal/ai"
	"gocalis/internal/audio"
	"gocalis/internal/brain"
	"gocalis/internal/config"
	"gocalis/internal/mqtt"
	"gocalis/internal/protocol"
	"gocalis/internal/runtime"
	"gocalis/internal/server"
	"gocalis/internal/webrtc"
	"gocalis/internal/webserver"

	sherpa "github.com/k2-fsa/sherpa-onnx-go/sherpa_onnx"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	// Command-line configurations
	channel := flag.String("channel", "webrtc", "Audio channel/mode to run: 'webrtc' or 'demo'")
	configPath := flag.String("config", "config.yaml", "Path to config.yaml file")
	audioFile := flag.String("audio-file", "", "Path to a WAV file for diagnostic modes")
	nodeID := flag.String("node-id", "front_door", "Node ID to use for diagnostic modes")
	streamName := flag.String("stream-name", "", "Override go2rtc stream name for RTC diagnostic modes")
	sendCodec := flag.String("send-codec", "pcmu", "WebRTC send codec for RTC diagnostic modes: pcmu, opus, or opus-sendonly")
	duration := flag.Duration("duration", 10*time.Second, "Duration for recording diagnostic modes")
	text := flag.String("text", "Prueba de audio del timbre.", "Text for speech playback diagnostic modes")
	wsAddr := flag.String("ws-addr", ":9090", "WebSocket server listen address")
	httpAddr := flag.String("http-addr", ":8080", "Dashboard HTTP server listen address")
	flag.Parse()

	log.Printf("=== Starting Gocalis Speech Agent Proxy (Mode: %s, Config: %s) ===\n", *channel, *configPath)

	// Load configuration file
	cfg, err := config.LoadConfig(*configPath)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	// Calculate dynamic division of thread allocation to match Pi 5 core limits.
	// Only active (rtc_stream) nodes actually load models; local/unsupported nodes
	// are skipped by the node runtime, so they must not dilute the thread budget.
	numNodes := 0
	for _, n := range cfg.Nodes {
		if n.Type == "rtc_stream" {
			numNodes++
		}
	}
	if numNodes <= 0 {
		numNodes = 1
	}
	threadsPerModel := cfg.GlobalNumThreads / numNodes
	if threadsPerModel < 1 {
		threadsPerModel = 1
	}
	log.Printf("Thread limits config: global_num_threads=%d, active_nodes=%d. Allocation per model: %d threads\n", cfg.GlobalNumThreads, numNodes, threadsPerModel)

	if *channel == "wake-file" {
		runWakeFileMode(cfg, *nodeID, *audioFile, threadsPerModel)
		return
	}
	if *channel == "wake-file-vad" {
		runWakeFileVADMode(cfg, *nodeID, *audioFile, threadsPerModel)
		return
	}
	if *channel == "asr-file" {
		runASRFileMode(cfg, *audioFile, threadsPerModel)
		return
	}
	if *channel == "rtc-record" {
		runRTCRecordMode(cfg, *nodeID, *streamName, *audioFile, *duration)
		return
	}
	if *channel == "rtc-say" {
		runRTCSayMode(cfg, *nodeID, *streamName, *sendCodec, *text, threadsPerModel)
		return
	}
	if *channel == "rtc-loopback" {
		runRTCLoopbackMode(cfg, *nodeID, *streamName, *sendCodec, *duration)
		return
	}

	// 1. Initialize TTS Synthesizer (VITS or Supertonic from config)
	log.Printf("Loading TTS Engine (Engine: %s)...\n", cfg.Models.TTS.Engine)
	ttsEngine, err := ai.NewSynthesizer(cfg.Models.TTS, threadsPerModel)
	if err != nil {
		log.Fatalf("Failed to initialize TTS Engine: %v", err)
	}
	defer ttsEngine.Close()

	// 2. Initialize ASR Transcriber (Whisper or Moonshine from config)
	log.Println("Loading ASR Engine...")
	asrEngine, err := ai.NewTranscriber(cfg.Models.ASR, threadsPerModel)
	if err != nil {
		log.Fatalf("Failed to initialize ASR Engine: %v", err)
	}
	defer asrEngine.Close()

	// 3. Initialize Speaker Identification Engine
	log.Println("Loading Speaker ID Engine...")
	speakerEngine, err := ai.NewSpeakerIdentifier(cfg.Models.SpeakerID, threadsPerModel)
	if err != nil {
		log.Fatalf("Failed to initialize Speaker ID Engine: %v", err)
	}
	defer speakerEngine.Close()

	switch *channel {
	case "webrtc":
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Create the central brain orchestrator.
		centralBrain := brain.New(ttsEngine)

		// Shared event bus used by WebSocket, MQTT, and node state callbacks.
		eventBus := protocol.NewMultiPublisher()

		// Shared command executor used by all transports.
		executor := protocol.NewExecutor(centralBrain, asrEngine, speakerEngine, eventBus, cfg.Models.SpeakerID, cfg.Intercom)
		defer executor.IntercomEngine.Shutdown()

		// Initialize global WebSocket API server for Node-RED integration.
		log.Printf("Starting Global Node-RED WebSocket API Server on %s...\n", *wsAddr)
		wsServer := server.NewServer(*wsAddr, executor, cfg.Security.AuthToken, cfg.Security.AllowedOrigins)
		eventBus.Add(wsServer)

		go func() {
			if err := wsServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("[Server] WebSocket Server failed: %v\n", err)
			}
		}()

		// Initialize dashboard web server.
		log.Printf("Starting Dashboard Web Server on %s...\n", *httpAddr)
		webServer := webserver.NewServer(*httpAddr, centralBrain, executor, speakerEngine, cfg.Security.AuthToken, cfg.Security.AllowedOrigins)
		eventBus.Add(webServer)

		go func() {
			if err := webServer.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Printf("[WebServer] HTTP Server failed: %v\n", err)
			}
		}()

		// Initialize MQTT transport if enabled.
		var mqttClient *mqtt.Client
		if cfg.MQTT.Enabled {
			log.Printf("Starting MQTT client (broker: %s)...\n", cfg.MQTT.Broker)
			mqttCfg := mqtt.Config{
				Broker:        cfg.MQTT.Broker,
				ClientID:      cfg.MQTT.ClientID,
				Username:      cfg.MQTT.Username,
				Password:      cfg.MQTT.Password,
				TopicPrefix:   cfg.MQTT.TopicPrefix,
				QoS:           byte(cfg.MQTT.QoS),
				AutoReconnect: cfg.MQTT.AutoReconnect,
			}
			var err error
			mqttClient, err = mqtt.NewClient(mqttCfg, executor)
			if err != nil {
				log.Fatalf("Failed to create MQTT client: %v", err)
			}
			if err := mqttClient.Connect(ctx); err != nil {
				log.Fatalf("Failed to connect MQTT client: %v", err)
			}
			eventBus.Add(mqttClient)
			defer mqttClient.Close()
		}

		// Start configuration hot-reloading file watcher (speaker profiles only).
		config.WatchSpeakers(*configPath, func() {
			log.Println("[Main] Config changed; hot-reloading speaker profiles...")
			if err := speakerEngine.ReloadSpeakers(); err != nil {
				log.Printf("[Main] Failed to hot-reload speaker profiles: %v\n", err)
			} else {
				log.Println("[Main] Speaker profiles hot-reloaded successfully!")
			}
		})

		// Spawn a runtime goroutine for each configured physical node. The
		// WaitGroup lets shutdown wait for every runtime to release its models and
		// transport before the deferred engine Close() calls free model memory.
		var nodeWG sync.WaitGroup
		for i := range cfg.Nodes {
			nodeCfg := cfg.Nodes[i]
			rt := runtime.New(nodeCfg, cfg, asrEngine, speakerEngine, eventBus, centralBrain, executor.AskEngine, threadsPerModel)
			nodeWG.Add(1)
			go func() {
				defer nodeWG.Done()
				rt.Run(ctx)
			}()
		}

		log.Println("All node handlers spawned successfully. Running loop... Press Ctrl+C to exit.")

		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-sigChan:
			log.Println("Shutdown signal received. Draining...")
		case <-ctx.Done():
			log.Println("Context closed. Draining...")
		}

		// Graceful shutdown, ordered so nothing uses an engine after it is closed:
		//   1. Cancel the root context so node runtimes begin tearing down.
		//   2. Stop the HTTP servers (drains in-flight control requests).
		//   3. Wait for node runtimes to finish.
		//   4. Deferred engine Close() calls then run as main returns.
		cancel()

		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer shutdownCancel()

		if err := wsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("[Server] WebSocket server shutdown error: %v\n", err)
		}
		if err := webServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("[WebServer] Dashboard server shutdown error: %v\n", err)
		}

		drained := make(chan struct{})
		go func() {
			nodeWG.Wait()
			close(drained)
		}()
		select {
		case <-drained:
			log.Println("All node runtimes drained. Shutting down engines...")
		case <-shutdownCtx.Done():
			log.Println("Timed out waiting for node runtimes to drain; shutting down anyway...")
		}

	case "demo":
		runDemoMode(asrEngine, ttsEngine, speakerEngine)

	default:
		log.Fatalf("Unknown channel mode: %s. Supported modes: webrtc, demo, wake-file, asr-file, rtc-record, rtc-say, rtc-loopback", *channel)
	}
}

func findNodeConfig(cfg *config.Config, nodeID string) *config.NodeConfig {
	for i := range cfg.Nodes {
		if cfg.Nodes[i].NodeID == nodeID {
			return &cfg.Nodes[i]
		}
	}
	return nil
}

func diagnosticSignalingURL(apiURL, streamName string) (string, error) {
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

func runASRFileMode(cfg *config.Config, audioFile string, threadsPerModel int) {
	if audioFile == "" {
		log.Fatal("asr-file mode requires -audio-file")
	}

	log.Printf("ASRFile: loading ASR engine for %s", audioFile)
	asrEngine, err := ai.NewTranscriber(cfg.Models.ASR, threadsPerModel)
	if err != nil {
		log.Fatalf("ASRFile: failed to initialize ASR engine: %v", err)
	}
	defer asrEngine.Close()

	transcription, err := asrEngine.TranscribeFile(audioFile, ai.JobOptions{})
	if err != nil {
		log.Fatalf("ASRFile: transcription failed: %v", err)
	}
	log.Printf("ASRFile: transcription=%q", transcription)
}

func runWakeFileMode(cfg *config.Config, nodeID, audioFile string, threadsPerModel int) {
	if audioFile == "" {
		log.Fatal("wake-file mode requires -audio-file")
	}

	nodeCfg := findNodeConfig(cfg, nodeID)
	if nodeCfg == nil {
		log.Fatalf("node %q not found in config", nodeID)
	}

	log.Printf("WakeFile: loading detector for node %s (keywords: %s)", nodeCfg.NodeID, nodeCfg.KWS.KeywordsFile)
	wakeDetector, err := ai.NewSherpaONNXWakeDetector(nodeCfg.KWS, nodeCfg.GetKWSNumThreads(threadsPerModel))
	if err != nil {
		log.Fatalf("WakeFile: failed to initialize wake detector: %v", err)
	}
	defer wakeDetector.Close()

	detected, keyword, err := wakeDetector.DetectInFile(audioFile)
	if err != nil {
		log.Fatalf("WakeFile: detection failed: %v", err)
	}
	if detected {
		log.Printf("WakeFile: DETECTED keyword=%q", keyword)
		return
	}
	log.Println("WakeFile: no wake keyword detected")
}

// runWakeFileVADMode replays a WAV through the EXACT live path (VAD gate ->
// wake stream, fed in 320-sample chunks like the WebRTC opus decoder) so we can
// tell whether the VAD gate is what stops the live front_door node from
// detecting audio that the raw detector (wake-file) accepts.
func runWakeFileVADMode(cfg *config.Config, nodeID, audioFile string, threadsPerModel int) {
	if audioFile == "" {
		log.Fatal("wake-file-vad mode requires -audio-file")
	}
	nodeCfg := findNodeConfig(cfg, nodeID)
	if nodeCfg == nil {
		log.Fatalf("node %q not found in config", nodeID)
	}

	vadConfig := sherpa.VadModelConfig{
		SileroVad: sherpa.SileroVadModelConfig{
			Model:              cfg.Models.VAD.SileroOnnxPath,
			Threshold:          cfg.Models.VAD.Threshold,
			MinSilenceDuration: float32(cfg.Models.VAD.MinSilenceDurationMs) / 1000.0,
			MinSpeechDuration:  0.25,
			WindowSize:         512,
			MaxSpeechDuration:  20.0,
		},
		SampleRate: 16000,
		NumThreads: threadsPerModel,
		Provider:   "cpu",
		Debug:      0,
	}
	vadGate := sherpa.NewVoiceActivityDetector(&vadConfig, 10.0)
	if vadGate == nil {
		log.Fatal("WakeFileVAD: failed to initialize VAD")
	}
	defer sherpa.DeleteVoiceActivityDetector(vadGate)

	wakeDetector, err := ai.NewSherpaONNXWakeDetector(nodeCfg.KWS, nodeCfg.GetKWSNumThreads(threadsPerModel))
	if err != nil {
		log.Fatalf("WakeFileVAD: failed to initialize wake detector: %v", err)
	}
	defer wakeDetector.Close()

	var detections int64
	wakeStream, err := wakeDetector.CreateStream(func(keyword string) {
		atomic.AddInt64(&detections, 1)
		log.Printf("WakeFileVAD: [DETECTED] keyword=%q", keyword)
	})
	if err != nil {
		log.Fatalf("WakeFileVAD: failed to create wake stream: %v", err)
	}
	defer wakeStream.Close()

	wave := sherpa.ReadWave(audioFile)
	if wave == nil {
		log.Fatal("WakeFileVAD: failed to read WAV file")
	}
	log.Printf("WakeFileVAD: read %d samples @ %dHz; feeding VAD->wake in 320-sample chunks", len(wave.Samples), wave.SampleRate)

	segments := 0
	feed := func(samples []float32) {
		vadGate.AcceptWaveform(samples)
		for !vadGate.IsEmpty() {
			seg := vadGate.Front()
			segments++
			wakeStream.AcceptAudio(seg.Samples)
			vadGate.Pop()
		}
	}
	const chunk = 320 // 20ms @ 16kHz, matching the opus decode chunk size
	for i := 0; i < len(wave.Samples); i += chunk {
		end := i + chunk
		if end > len(wave.Samples) {
			end = len(wave.Samples)
		}
		feed(wave.Samples[i:end])
	}
	vadGate.Flush()
	for !vadGate.IsEmpty() {
		seg := vadGate.Front()
		segments++
		wakeStream.AcceptAudio(seg.Samples)
		vadGate.Pop()
	}

	time.Sleep(300 * time.Millisecond) // let async onDetected callbacks land
	log.Printf("WakeFileVAD: VAD produced %d speech segment(s); wake detections=%d", segments, atomic.LoadInt64(&detections))
}

func diagnosticStreamName(nodeCfg *config.NodeConfig, override string) string {
	if override != "" {
		return override
	}
	return nodeCfg.RTCStream.StreamName
}

func runRTCRecordMode(cfg *config.Config, nodeID, streamName, outputFile string, duration time.Duration) {
	if outputFile == "" {
		log.Fatal("rtc-record mode requires -audio-file as the output WAV path")
	}
	nodeCfg := findNodeConfig(cfg, nodeID)
	if nodeCfg == nil {
		log.Fatalf("node %q not found in config", nodeID)
	}
	if nodeCfg.Type != "rtc_stream" {
		log.Fatalf("node %q is type %q, not rtc_stream", nodeID, nodeCfg.Type)
	}

	signaling, err := diagnosticSignalingURL(nodeCfg.RTCStream.ApiURL, diagnosticStreamName(nodeCfg, streamName))
	if err != nil {
		log.Fatalf("RTCRecord: failed to build signaling URL: %v", err)
	}
	client, err := webrtc.NewClient(signaling)
	if err != nil {
		log.Fatalf("RTCRecord: failed to create WebRTC client: %v", err)
	}
	defer client.Close()

	var samples []float32
	var samplesMu sync.Mutex
	client.OnAudio(func(chunk []float32) {
		samplesMu.Lock()
		samples = append(samples, chunk...)
		samplesMu.Unlock()
	})

	ctx, cancel := context.WithTimeout(context.Background(), duration+15*time.Second)
	defer cancel()
	log.Printf("RTCRecord: connecting to %s", signaling)
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("RTCRecord: connect failed: %v", err)
	}
	log.Printf("RTCRecord: recording %s of incoming WebRTC audio", duration)
	timer := time.NewTimer(duration)
	select {
	case <-ctx.Done():
		log.Fatalf("RTCRecord: timed out: %v", ctx.Err())
	case <-timer.C:
	}

	samplesMu.Lock()
	recorded := append([]float32(nil), samples...)
	samplesMu.Unlock()
	if err := audio.WriteWAVFloat32(outputFile, recorded, 16000); err != nil {
		log.Fatalf("RTCRecord: failed to write WAV: %v", err)
	}
	log.Printf("RTCRecord: wrote %d samples (%.2fs) to %s", len(recorded), float64(len(recorded))/16000.0, outputFile)
}

func runRTCSayMode(cfg *config.Config, nodeID, streamName, sendCodec, text string, threadsPerModel int) {
	nodeCfg := findNodeConfig(cfg, nodeID)
	if nodeCfg == nil {
		log.Fatalf("node %q not found in config", nodeID)
	}
	if nodeCfg.Type != "rtc_stream" {
		log.Fatalf("node %q is type %q, not rtc_stream", nodeID, nodeCfg.Type)
	}

	signaling, err := diagnosticSignalingURL(nodeCfg.RTCStream.ApiURL, diagnosticStreamName(nodeCfg, streamName))
	if err != nil {
		log.Fatalf("RTCSay: failed to build signaling URL: %v", err)
	}
	client, err := webrtc.NewClientWithSendCodec(signaling, sendCodec)
	if err != nil {
		log.Fatalf("RTCSay: failed to create WebRTC client: %v", err)
	}
	defer client.Close()

	log.Printf("RTCSay: loading TTS engine")
	ttsEngine, err := ai.NewSynthesizer(cfg.Models.TTS, threadsPerModel)
	if err != nil {
		log.Fatalf("RTCSay: failed to initialize TTS engine: %v", err)
	}
	defer ttsEngine.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	log.Printf("RTCSay: connecting to %s", signaling)
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("RTCSay: connect failed: %v", err)
	}
	audioStream, err := ttsEngine.SynthesizeToStream(text, ai.JobOptions{Priority: 10})
	if err != nil {
		log.Fatalf("RTCSay: synthesis failed: %v", err)
	}
	log.Printf("RTCSay: playing %q", text)
	if err := client.PlayStream(ctx, audioStream); err != nil {
		log.Fatalf("RTCSay: playback failed: %v", err)
	}
	log.Println("RTCSay: playback completed")
}

func runRTCLoopbackMode(cfg *config.Config, nodeID, streamName, sendCodec string, duration time.Duration) {
	nodeCfg := findNodeConfig(cfg, nodeID)
	if nodeCfg == nil {
		log.Fatalf("node %q not found in config", nodeID)
	}
	if nodeCfg.Type != "rtc_stream" {
		log.Fatalf("node %q is type %q, not rtc_stream", nodeID, nodeCfg.Type)
	}

	signaling, err := diagnosticSignalingURL(nodeCfg.RTCStream.ApiURL, diagnosticStreamName(nodeCfg, streamName))
	if err != nil {
		log.Fatalf("RTCLoopback: failed to build signaling URL: %v", err)
	}
	client, err := webrtc.NewClientWithSendCodec(signaling, sendCodec)
	if err != nil {
		log.Fatalf("RTCLoopback: failed to create WebRTC client: %v", err)
	}
	defer client.Close()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var samples []float32
	var samplesMu sync.Mutex
	recording := false
	client.OnAudio(func(chunk []float32) {
		samplesMu.Lock()
		defer samplesMu.Unlock()
		if recording {
			samples = append(samples, chunk...)
		}
	})

	log.Printf("RTCLoopback: connecting to %s", signaling)
	if err := client.Connect(ctx); err != nil {
		log.Fatalf("RTCLoopback: connect failed: %v", err)
	}

	log.Printf("RTCLoopback: looping %s capture -> playback. Press Ctrl+C to stop.", duration)
	for cycle := 1; ; cycle++ {
		samplesMu.Lock()
		samples = nil
		recording = true
		samplesMu.Unlock()

		log.Printf("RTCLoopback: cycle %d recording for %s", cycle, duration)
		timer := time.NewTimer(duration)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			log.Println("RTCLoopback: stopped")
			return
		case <-timer.C:
		}

		samplesMu.Lock()
		recording = false
		recorded := append([]float32(nil), samples...)
		samplesMu.Unlock()

		seconds := float64(len(recorded)) / 16000.0
		log.Printf("RTCLoopback: cycle %d captured %d samples (%.2fs)", cycle, len(recorded), seconds)
		if len(recorded) == 0 {
			log.Println("RTCLoopback: no incoming samples captured; retrying after 2s")
		} else {
			log.Printf("RTCLoopback: cycle %d playing captured audio", cycle)
			if err := client.Play(ctx, audio.FloatToPCM16(recorded), 16000); err != nil {
				log.Printf("RTCLoopback: playback failed: %v", err)
				return
			}
		}

		select {
		case <-ctx.Done():
			log.Println("RTCLoopback: stopped")
			return
		case <-time.After(2 * time.Second):
		}
	}
}

// runDemoMode runs validation on Spanish speech ASR/TTS files and streams.
func runDemoMode(asrEngine ai.Transcriber, ttsEngine ai.Synthesizer, speakerID ai.SpeakerIdentifier) {
	// Test Phrase in Spanish
	spanishPhrase := "Hola, esta es su cámara de timbre inteligente llamando. ¿Cómo puedo ayudarle hoy?"
	outputWav := "generated_es.wav"

	// 1. File Mode: TTS Synthesize
	log.Printf("TTS: Synthesizing to file '%s': \"%s\"\n", outputWav, spanishPhrase)
	err := ttsEngine.SynthesizeToFile(spanishPhrase, outputWav, ai.JobOptions{})
	if err != nil {
		log.Fatalf("Failed to synthesize to file: %v", err)
	}
	log.Println("TTS: Speech synthesis saved successfully!")

	// 2. File Mode: ASR Transcribe
	log.Printf("ASR: Transcribing WAV file '%s'...\n", outputWav)
	transcription, err := asrEngine.TranscribeFile(outputWav, ai.JobOptions{})
	if err != nil {
		log.Fatalf("Failed to transcribe file: %v", err)
	}
	log.Println("ASR File Mode Results:")
	log.Println("==================================================")
	log.Printf("Spanish Text: %s\n", transcription)
	log.Println("==================================================")

	// 3. File Mode: Speaker ID Verification
	log.Printf("SpeakerID: Identifying speaker profile in file '%s'...\n", outputWav)
	matchedSpeaker, err := speakerID.IdentifyFile(outputWav)
	if err != nil {
		log.Fatalf("Failed to run Speaker ID: %v", err)
	}
	log.Println("SpeakerID Results:")
	log.Println("==================================================")
	log.Printf("Matched Speaker: '%s'\n", matchedSpeaker)
	log.Println("==================================================")

	// 4. Streaming Mode: TTS & ASR Stream loop
	log.Println("TTS/ASR: Testing Streaming Interface (Synthesizing -> Streaming -> Transcribing live)...")
	audioStream, err := ttsEngine.SynthesizeToStream(spanishPhrase, ai.JobOptions{})
	if err != nil {
		log.Fatalf("Failed to synthesize to stream: %v", err)
	}

	transcriptionStream, err := asrEngine.CreateStream()
	if err != nil {
		log.Fatalf("Failed to create transcription stream: %v", err)
	}
	defer transcriptionStream.Close()

	chunkSize := 1024
	totalSamples := 0

	for {
		pcmChunk, err := audioStream.ReadPCM16(chunkSize)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			log.Fatalf("Failed to read PCM chunk: %v", err)
		}

		floatChunk := make([]float32, len(pcmChunk))
		for i, val := range pcmChunk {
			floatChunk[i] = float32(val) / 32767.0
		}

		transcriptionStream.AcceptAudio(floatChunk)
		totalSamples += len(floatChunk)
	}

	log.Printf("TTS/ASR: Streamed %d samples successfully!\n", totalSamples)
	log.Println("ASR Streaming Results:")
	log.Println("==================================================")
	log.Printf("Spanish Text: %s\n", transcriptionStream.Result(ai.JobOptions{}))
	log.Println("==================================================")

	// 5. Local Playback
	log.Println("Playing back the generated Spanish phrase using aplay...")
	cmd := exec.Command("aplay", outputWav)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	err = cmd.Run()
	if err != nil {
		log.Printf("Warning: Failed to play audio using aplay: %v", err)
	} else {
		log.Println("Playback completed successfully!")
	}

	log.Println("=== Demo Completed Successfully ===")
}
