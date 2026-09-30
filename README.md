# Modern Go Speech Agent Proxy (`gocalis`)

This project implements a Go-based local Speech/AI Service proxy utilizing the **Pion WebRTC** library and the **Sherpa-ONNX** local inference engine.

It exposes four main speech modules decoupled behind clear interfaces:
- **TTS (Text-to-Speech)**: High-quality **VITS** or **Supertonic 3** voice models, backed by an **internal sequential worker queue** to prevent CPU thrashing.
- **ASR (Automatic Speech Recognition)**: Quantized **Whisper Tiny** or **Moonshine** speech-to-text models.
- **Wake Word (Keyword Spotting)**: Streaming **Sherpa-ONNX KWS** keyword spotter, configured per node via `config.yaml`.
- **Speaker ID (Speaker Identification/Verification)**: Local speaker biometric fingerprinting using **WeSpeaker CAM++** embedding models.

Additionally, it runs an embedded **Node-RED WebSocket Server API**, allowing automation platforms to trigger speech commands (TTS, ASR, Speaker ID) and subscribe to live voice events (Wake Word triggers, Speaker matches).

---

## 🏗️ Go Architecture Layout

The codebase separates concerns into decoupled, testable packages with clear interface boundaries:

```
gocalis/
├── cmd/
│   └── main.go           # Unified Speech Proxy CLI (supports 'webrtc' and 'demo' channels)
├── internal/
│   ├── ai/
│   │   ├── speech.go     # Speech TTS & ASR package (sequential TTS worker queue, interfaces)
│   │   ├── wake.go       # Wake Word detector package (WakeDetector & WakeStream interfaces)
│   │   └── speaker.go    # Speaker Identification package (SpeakerIdentifier & SpeakerStream interfaces)
│   ├── config/
│   │   └── config.go     # YAML Configuration module (parsing config.yaml and overrides)
│   ├── server/
│   │   └── websocket.go  # Node-RED WebSocket API server (handles request routing & events)
│   └── webrtc/
│       └── client.go     # WebRTC Client package (Pion client, websocket signaling, PCMU encoding)
├── config.yaml           # Global configurations and device (node) overrides
├── docker-compose.yaml   # Docker environment configuration (maps /dev/snd, runs host network)
├── Dockerfile            # Container image builder (installs ALSA, bzip2, Go 1.24)
├── go.mod                # Go module descriptor
├── go.sum                # Go dependency lockfile
├── README.md             # Project documentation (this file)
└── models/               # Subdirectory containing ASR, TTS, Wake and Speaker ONNX model files
```

---

## 🧠 Architectural Interfaces

All AI modules are decoupled using clean interfaces that natively support both **file** (disk) and **stream** (live buffer) modes:

### 1. TTS (Text-to-Speech with Queue Serialization)
*   **File Output**: `SynthesizeToFile(ctx context.Context, text string, outputPath string, opts JobOptions) error` — submits task to queue, waits, and saves WAV to disk.
*   **Stream Output**: `SynthesizeToStream(ctx context.Context, text string, opts JobOptions) (AudioStream, error)` — submits the task to the queue and returns a stream reader (`ReadPCM16(ctx, chunkSize)`) **before** synthesis finishes. Audio chunks are emitted as they are produced by the Sherpa generation callback, so the first audio can play while later audio is still being synthesized (lower first-audio latency). The brain's single-node `Speak` path uses `AudioNode.PlayStream` to play chunks as they arrive.
*   *Scheduling*: Priority is carried by `JobOptions` (a submission/scheduler concern) rather than leaking into the domain method signatures.
*   *Optimization*: The synthesizer implements an internal **priority worker queue** with bounded, stable priority admission. Concurrent requests (e.g. from multiple audio nodes or Node-RED automation) are serialized automatically so the heavy ONNX synthesis runs one request at a time (preventing CPU thread thrashing and latency spikes). Generation buffers at most two seconds of PCM and applies cancellation-aware backpressure. Broadcast commands synthesize once and share immutable PCM across nodes.

### 2. ASR (Speech-to-Text)
*   **Samples Input**: `TranscribeSamples(ctx context.Context, samples []float32, sampleRate int, opts JobOptions) (string, error)` — transcribes an in-memory PCM buffer (resampling to 16 kHz when needed). This is the primary path used by the live `/ask` capture flow (no temp-WAV round-trip).
*   **File Input**: `TranscribeFile(ctx context.Context, filePath string, opts JobOptions) (string, error)` — thin wrapper that reads a WAV file and calls `TranscribeSamples`.
*   **Stream Input**: `CreateStream() (TranscriptionStream, error)` — initializes a live, chunk-based PCM receiver (`AcceptAudio`) transcribing on-the-fly.

### 3. Wake Word Detection (KWS)
*   **File Input**: `DetectInFile(filePath string) (bool, string, error)` — inspects a WAV file to check if keywords were spoken.
*   **Stream Input**: `CreateStream(onDetected func(string)) (WakeStream, error)` — creates a live PCM receiver (`AcceptAudio`) triggering a callback when a wake keyword matches the stream.

### 4. Speaker Identification (Speaker ID)
*   **Samples Input**: `IdentifySamples(samples []float32, sampleRate int) (string, error)` — matches an in-memory PCM buffer against registered speaker profiles (used by the live capture flow, no temp-WAV round-trip).
*   **File Input**: `IdentifyFile(filePath string) (string, error)` — thin wrapper that reads a WAV file and calls `IdentifySamples`.
*   **Stream Input**: `CreateStream(onSpeakerIdentified func(string)) (SpeakerStream, error)` — creates a live PCM receiver (`AcceptAudio`) triggering a callback when a known speaker profile is recognized in real-time.

---

## 🔌 Node-RED WebSocket API Protocol

The embedded WebSocket server listens at `/ws` (default port: `:9090`) and complies with the expected `node-red-contrib-gocalis` node properties.

### 1. Commands (Node-RED -> Go Proxy)
*   **TTS command** (single node):
    ```json
    {
      "action": "tts",
      "node_id": "living_room",
      "text": "Hola, bienvenido a casa."
    }
    ```
    Returns:
    ```json
    {
      "event": "tts_completed",
      "node_id": "living_room",
      "status": "success"
    }
    ```
*   **TTS command** (all registered nodes simultaneously):
    ```json
    {
      "action": "tts",
      "node_id": "all",
      "text": "Hola, bienvenido a casa."
    }
    ```
    Returns:
    ```json
    {
      "event": "tts_completed",
      "node_id": "all",
      "status": "success"
    }
    ```
*   **ASR command**:
    ```json
    {
      "action": "asr",
      "node_id": "living_room",
      "audio_file": "received_audio.wav"
    }
    ```
    Returns:
    ```json
    {
      "event": "asr_completed",
      "node_id": "living_room",
      "status": "success",
      "text": "hola bienvenido a casa"
    }
    ```
*   **Speaker ID command**:
    ```json
    {
      "action": "speaker_id",
      "node_id": "living_room",
      "audio_file": "received_audio.wav"
    }
    ```
    Returns:
    ```json
    {
      "event": "speaker_id_completed",
      "node_id": "living_room",
      "status": "success",
      "speaker": "eduardo"
    }
    ```

### 2. Events Broadcast (Go Proxy -> Node-RED)
*   **Wake word trigger**:
    ```json
    {
      "event": "wake",
      "node_id": "front_door",
      "keyword": "hola"
    }
    ```
*   **Speaker identified**:
    ```json
    {
      "event": "speaker_identified",
      "node_id": "front_door",
      "speaker": "eduardo"
    }
    ```

---

## 📡 MQTT Transport

The same command executor and event bus used by the WebSocket server are exposed over MQTT. Enable it in `config.yaml` under the `mqtt` section.

### Command Topics (Node-RED/HA -> Gocalis)

Publish JSON payloads to:

*   `gocalis/cmd/tts`
    ```json
    {
      "node_id": "all",
      "text": "Hola, bienvenido a casa."
    }
    ```
*   `gocalis/cmd/asr`
    ```json
    {
      "node_id": "living_room",
      "audio_file": "received_audio.wav"
    }
    ```
*   `gocalis/cmd/speaker_id`
    ```json
    {
      "node_id": "living_room",
      "audio_file": "received_audio.wav"
    }
    ```

### Event Topics (Gocalis -> Node-RED/HA)

Events are published to `gocalis/event/<event_type>`, for example:

*   `gocalis/event/state_changed`
    ```json
    {
      "event": "state_changed",
      "node_id": "front_door",
      "state": "SPEAKING"
    }
    ```
*   `gocalis/event/wake`
    ```json
    {
      "event": "wake",
      "node_id": "front_door",
      "keyword": "hola"
    }
    ```
*   `gocalis/event/asr_completed`
    ```json
    {
      "event": "asr_completed",
      "node_id": "living_room",
      "status": "success",
      "text": "hola bienvenido a casa"
    }
    ```

---

## Build and run

The runtime image contains a compiled Go executable, the embedded dashboard and the required native inference/audio libraries. It does not contain a Go toolchain. Configure model paths and nodes in `config.yaml`, and place the matching artifacts under `models/` before starting the service. Missing files are reported before native model initialization.

```bash
docker compose up -d --build app
docker compose logs -f app
```

Compose mounts only configuration and models, so repository mounts cannot hide the embedded dashboard. It uses host networking for WebRTC and maps `/dev/snd` for local ALSA nodes. For a deployment using only WebRTC, remove the `devices` entry if the host has no sound devices. `privileged` mode is unnecessary.

The default dashboard is at `http://localhost:8080`; the automation WebSocket is `ws://localhost:9090/ws`. Configure `security.auth_token` to require a bearer token for controls and both event sockets. Enter it in the dashboard's access-token field; it is stored for the current browser tab. The status endpoints remain public.

## Container-based testing

The separate `test` image includes Go, native development libraries, tests and dashboard assets. No host Go installation or audio hardware is required for unit tests:

```bash
docker compose --profile test run --rm --build test
docker compose --profile test run --rm test go vet ./...
docker compose --profile test run --rm test go test -race -count=1 ./internal/ask ./internal/brain ./internal/webrtc
```

The default test command runs the whole suite with the race detector. The tests use in-memory audio and model stand-ins; real recognition accuracy, echo cancellation and go2rtc delivery still require the configured models and representative hardware.

For local frontend work:

```bash
npm --prefix web ci
npm --prefix web run lint
npm --prefix web run build
mkdir -p internal/webserver/dist
cp -r web/dist/. internal/webserver/dist/
```

## Configuration and command contracts

Configuration rejects unknown YAML fields, duplicate node IDs and invalid durations, rates and URLs. ASR explicitly selects `models.asr.engine: whisper` or `moonshine`; Moonshine uses a merged decoder. Whisper uses `models.asr.language` (default `es`). TTS selects `supertonic` with `model_dir`, or `vits` with `model`, `tokens` and `data_dir`. There is no filename-based engine detection.

The speaker challenge key is `challenge_init_prompt`. Local capture is mono 16 kHz. Unused `channels`, `chunk_size`, `rtsp_url` and `codec` configuration keys have been removed. `auto_ask_capture_delay_seconds: 0` disables the delay; use `1.5` for the supplied half-duplex doorbell configuration, or `-1` to select the runtime default.

Speaker profile WAVs under `models.speaker_id.embeddings_dir` reload after a 500 ms trailing debounce, including atomic file replacement. Application configuration changes require a restart. `POST /api/reload-speakers` explicitly rereads the same profile directory.

HTTP `/api/ask` and `/ask` use the same fields as an `action: ask` command: `text`, `node_id`, `context_id`, `barge_in`, `require_speaker_id`, `vad_timeout_seconds`, `capture_delay_seconds`, `post_speech_silence_seconds`, `priority`, and `output_format`. The response's audio is the captured response, not the spoken prompt. Detached `/api/execute` commands return HTTP 202 after admission; invalid requests return 400 and overload/shutdown returns 503.

Each service admits at most 32 concurrent commands. Model and per-node queues hold at most 32 pending jobs/turns. Text is limited to 4,000 bytes; audio and capture to 120 seconds; HTTP and WebSocket command messages to 8 MiB. Each WebSocket server admits at most 64 clients with 32 queued outbound events per client and a five-second write deadline. Slow consumers are disconnected. Talkback buffers at most 100 encoded frames and discards pending playback on cancellation.

Shutdown cancels commands, closes event connections, joins command/wake/capture/reload work, and only then closes inference engines. An engine waits for any active native call before deleting its model. The review and rationale are in [docs/architecture-performance-review.md](docs/architecture-performance-review.md).

### Local multi-device audio simulator

[tools/simulator](tools/simulator/README.md) provides virtual RTSP microphones and
speaker capture beside a pinned go2rtc instance, a localhost browser UI, YAML
scenarios, and container transport checks using Gocalis's real WebRTC client.
It supports bidirectional Opus and the existing WHIP → AAC-ELD doorbell route.
