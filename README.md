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
*   **File Output**: `SynthesizeToFile(text string, outputPath string, opts JobOptions) error` — submits task to queue, waits, and saves WAV to disk.
*   **Stream Output**: `SynthesizeToStream(text string, opts JobOptions) (AudioStream, error)` — submits the task to the queue and returns a stream reader (`ReadPCM16(chunkSize)`) **before** synthesis finishes. Audio chunks are emitted as they are produced by the Sherpa generation callback, so the first audio can play while later audio is still being synthesized (lower first-audio latency). The brain's single-node `Speak` path uses `AudioNode.PlayStream` to play chunks as they arrive.
*   *Scheduling*: Priority is carried by `JobOptions` (a submission/scheduler concern) rather than leaking into the domain method signatures.
*   *Optimization*: The synthesizer implements an internal **priority worker queue** via Go channels. Concurrent requests (e.g. from multiple audio nodes or Node-RED automation) are serialized automatically so the heavy ONNX synthesis runs one request at a time (preventing CPU thread thrashing and latency spikes). Generation fills an internal buffer at CPU speed and the worker frees up as soon as synthesis completes, decoupled from the slower real-time playback consumer.

### 2. ASR (Speech-to-Text)
*   **Samples Input**: `TranscribeSamples(samples []float32, sampleRate int, opts JobOptions) (string, error)` — transcribes an in-memory PCM buffer (resampling to 16 kHz when needed). This is the primary path used by the live `/ask` capture flow (no temp-WAV round-trip).
*   **File Input**: `TranscribeFile(filePath string, opts JobOptions) (string, error)` — thin wrapper that reads a WAV file and calls `TranscribeSamples`.
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

## 🛠️ How to Build & Run the Project

This section details how to get the `gocalis` proxy and the React dashboard up and running.

### 1. Build the Web Dashboard (Required for Go Embedding)
Because `gocalis` embeds the React dashboard assets at compile time using Go `embed`, you must build the frontend before running or testing the Go project:

```bash
# 1. Install dependencies and build the React application
cd web
npm install
npm run build
cd ..

# 2. Copy the dist folder to the webserver package (so go:embed can find it)
cp -r web/dist internal/webserver/dist
```

> [!IMPORTANT]
> If you are using Docker Compose with local volume mounts (e.g., `.:/app`), the host's directory overrides the container's `/app` folder. Thus, the compiled `internal/webserver/dist` folder must exist on the **host** for it to be visible inside the running container.

### 2. Setup the Docker Development Environment
Bring up the development container (which installs the required audio libraries and configures network access):
```bash
docker compose up -d --build
```

### 3. Download Model Files
Initialize the ONNX models for automatic speech recognition (ASR), text-to-speech (TTS), and speaker identification (Speaker ID):

*   **Spanish Models** (Whisper Tiny & VITS es):
    ```bash
    docker compose exec app bash scripts/download_spanish_models.sh
    ```
*   **English Models** (Moonshine Tiny & Supertonic 3):
    ```bash
    docker compose exec app bash scripts/download_models.sh
    ```
*   **Speaker Verification Model** (Wespeaker CAM++):
    ```bash
    docker compose exec app bash scripts/download_speaker_id_model.sh
    ```

### 4. Running Gocalis
You can run `gocalis` in two primary modes using command-line flags:

#### A. WebRTC Proxy & Server Mode (Default)
Starts the unified speech agent, opens the Node-RED WebSocket API server, and launches the dashboard HTTP server:
```bash
docker compose exec app go run cmd/main.go -channel webrtc -config config.yaml -ws-addr :9090 -http-addr :8080
```
Parameters:
- `-channel`: Either `webrtc` (production/server) or `demo` (local pipeline validation).
- `-config`: Path to the YAML configuration file (default: `config.yaml`).
- `-ws-addr`: WebSocket port for automation platform integration (default: `:9090`).
- `-http-addr`: Web dashboard listen address (default: `:8080`).

#### B. Validation Demo Mode
Runs a local execution loop of the Spanish ASR, TTS, and Speaker ID pipeline using mock input files and streams, then plays the output using `aplay` (requires host audio configuration):
```bash
docker compose exec app go run cmd/main.go -channel demo -config config.yaml
```

---

## 🧪 How to Test

Gocalis uses standard Go unit testing. All tests are located in `*_test.go` files inside their respective packages.

### 1. Preparation
Ensure the Web Dashboard is built and copied to `internal/webserver/dist` (see [Build the Web Dashboard](#1-build-the-web-dashboard-required-for-go-embedding) above). If this folder is missing, Go compiler/test setup will fail with: `pattern all:dist: no matching files found`.

### 2. Run All Tests
Execute all package tests inside the container environment:
```bash
docker compose exec app go test ./...
```

### 3. Running Specific Subsets of Tests
*   **Run without caching**: Force tests to execute again rather than using cached results:
    ```bash
    docker compose exec app go test -count=1 ./...
    ```
*   **Run a specific package**:
    ```bash
    docker compose exec app go test ./internal/audio/...
    ```
*   **Run a single test by name pattern**:
    ```bash
    docker compose exec app go test -v ./internal/audio/... -run TestFloatPCM16RoundTrip
    ```
*   **Run with the Data Race Detector**: Check for concurrent write conflicts (e.g., hot-reloading configurations or streams):
    ```bash
    docker compose exec app go test -race ./...
    ```

---

## 🔍 How to Debug

### 1. Viewing Logs
The Go app logs directly to `stdout`/`stderr` with microsecond precision timestamp formatting:
```bash
# View active container output
docker compose logs -f app
```

### 2. Interactive Debugging with Delve (`dlv`)
To debug the running Go binary line-by-line, inspect variables, or set breakpoints:

#### A. Running with Delve inside the Container
1. Execute Delve inside the app container, starting a headless debug server:
   ```bash
   docker compose exec app dlv debug cmd/main.go --headless --listen=:2345 --api-version=2 --accept-multiclient -- -channel webrtc -config config.yaml
   ```
   *(Note: Make sure to expose port `2345` in `docker-compose.yaml` if you want to connect a debugger client from the host).*

#### B. VS Code Launch Configuration (`.vscode/launch.json`)
You can attach your IDE debugger to the headless Delve server. Add the following to your `.vscode/launch.json`:
```json
{
  "version": "0.2.0",
  "configurations": [
    {
      "name": "Attach to Delve (Docker)",
      "type": "go",
      "request": "attach",
      "mode": "remote",
      "port": 2345,
      "host": "127.0.0.1",
      "showLog": true
    }
  ]
}
```

### 3. WebRTC & Stream Debugging
When dealing with `rtc_stream` nodes:
*   **Status Page**: Check `http://localhost:8080/api/status` or `http://localhost:8080/api/nodes` to see if active nodes are running or in an error state.
*   **Pion Log Output**: Increase logging by modifying the config or runtime to monitor WebRTC signaling/ICE state transitions.
*   **Self-Barge-in Gate**: If the microphone captures its own speaker output (causing audio loops), verify that the node is running in half-duplex. The mic stream automatically mutes while the node status is `SPEAKING` unless `echo_cancellation` is enabled in `config.yaml`.

### 4. Configuration & Speaker Hot-Reloading Troubleshooting
*   **Modifying configurations**: Update values directly in `config.yaml`.
*   **Hot-reloading speakers**: Speaker embedding files in `./models/known_speakers/` are monitored. Modifying the folder or calling `POST /api/reload-speakers` triggers a hot-reload of speaker biometrics.
*   **Check for lock contentions**: Gocalis prevents data races during reloading using a read-write lock (`RWMutex`). Check log outputs if speaker identification requests block during reloading.

