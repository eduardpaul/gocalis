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
├── Dockerfile            # Container image builder (installs ALSA, bzip2, Go 1.26)
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
*   `gocalis/cmd/ask` and `gocalis/cmd/play` take the same payloads as over WebSocket.
*   `gocalis/cmd/intercom`
    ```json
    {
      "node_ids": ["front_door", "telegram"],
      "duration_seconds": 120
    }
    ```
*   `gocalis/cmd/intercom_leave` (one node leaves its call) and `gocalis/cmd/intercom_stop` (the call of that node ends)
    ```json
    {
      "node_id": "front_door"
    }
    ```
*   `gocalis/cmd/plugin`
    ```json
    {
      "plugin": "telegram",
      "command": "hangup"
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

### Intercom

An intercom call is a conference carried through Gocalis's nodes: participants
join and leave it while it runs, and each one hears all the others. A
participant is a node, or an intercom page (a browser). A bell chime tells the
call that someone joined (rising) or left (falling), and that the call is over.

| Request | Effect |
| --- | --- |
| `{"action": "intercom", "node_ids": ["living_room", "front_door"], "duration_seconds": 120}` | Puts the nodes in a call together. When none is in a call, a new one starts; when some already are in one, the others join it. |
| `{"action": "intercom_leave", "node_id": "living_room"}` | Takes one node out of its call. The call goes on for the others. |
| `{"action": "intercom_stop", "node_id": "front_door"}` | Ends the whole call the node is in. |

A call ends when fewer than two participants remain (`reason: "left"`), on its
deadline (`"timeout"`), or when it is stopped (`"stopped"`). The deadline is
`duration_seconds` (default `intercom.default_timeout_seconds`) after the last
join: every join pushes it back.

Events: `intercom_started`, `intercom_joined`, `intercom_left` and
`intercom_ended`. All carry `node_ids` and `remote_ids` (intercom pages, e.g.
`web-1`): who is in the call after the event. Joined and left add
`participant_id` (who), ended adds `reason` and `duration_seconds`, the others
`remaining_seconds`. A node whose playback fails is dropped from the call
(`intercom_left` with `status: "error"`); the call ends in error only if that
leaves a single participant. A node somebody has to answer (see
[Nodes somebody has to answer](#nodes-somebody-has-to-answer)) is rung first:
`intercom_ringing` comes before `intercom_started`, and nobody answering ends
the call with `reason: "unanswered"`.

**Intercom page.** `GET /intercom` serves a standalone page meant to sit behind
a signing-in reverse proxy (it asks for the API token like the dashboard, or
takes `?token=`; `?node=front_door` preselects the node). The page joins the
call the node is in, or starts one with it, so several browsers can be on the
doorbell, and talk to each other, at once. "Hablar" joins with the microphone
open and "Escuchar" joins to listen only; once in the call the microphone can be
opened and closed at any time, and the browser only asks for it when it is first
opened. "Colgar" leaves, and the call goes on for the others. The page shows and
plays the node through a plain go2rtc viewer in an iframe (`intercom.viewers`,
e.g. `stream.html?src=doorbell_tv&mode=webrtc`): go2rtc fans that stream out and
Gocalis does not relay it. Everything else travels over
`GET /api/intercom/ws?node_id=...`: the browser microphone, while open, goes up
as 16 kHz mono PCM16 binary frames, and the rest of the call (the other pages
and nodes, the chimes) comes down in the same format, next to the call's events
as JSON. A node without a viewer is heard through that same downlink. Gocalis
pings each page and takes one that stops answering out of the call, whether or
not it was sending a microphone.

**One source of doorbell audio.** Every call plays through the node's normal
output: for the doorbell, the single WHIP producer on `talkback_in` and the one
`ffmpeg:talkback_in#audio=eld` bridge into `doorbell_raw_homekit` that TTS uses.
Nothing in the intercom opens another `?dst=` route or a browser backchannel;
go2rtc's `Stream.Play` stops every other bridge on that stream, so a second
sender would cut Gocalis off. Gocalis re-posts its bridge only when go2rtc no
longer lists it.

**Priority.** A call takes its nodes over: the TTS, play or ask in progress is
cut off (an ask ends with status `error`, `"interrupted by an intercom call"`,
without running ASR). Until the node leaves the call, every TTS/play/ask for it,
including `gocalis-say` and the HTTP API, fails at once with
`"node is in an intercom call"` rather than being queued (a stale reply played
after the call would be worse). A broadcast to `all` skips it. A node is in one
call at a time.

**No self-hearing.** While a node is in a call its microphone reaches only the
call's mix, never wake word detection, VAD, ASR or the assistant, and the mix a
participant gets leaves its own microphone out. That holds for the node's
`auto_ask_capture_delay_seconds` after the call too, while the device plays out
its buffered tail.

**The assistant in a call.** Both rules above have an opt-in exception, so that
a call does not put a node out of the assistant's reach.

- `call_mode` on a `tts`, `play` or `ask` request says what to do when the node
  is in a call instead of failing. `"node"` runs it inside the call for that
  node alone: a TTS or recording is mixed into what the node hears, and an ask
  sets the node aside from the conversation (the others do not hear its
  microphone and it does not hear them) while it plays its prompt and listens,
  then puts it back. `"all"` (TTS and play) plays it for everyone in the call,
  pages included. The option does nothing for a node that is not in a call, and
  a broadcast to `all` nodes with it reaches the ones in a call individually.
  An ask in a call has no barge-in and no voice challenge; its result carries
  `in_call: true`.
- `kws.wake_in_call: true` on a node keeps its wake word armed during a call.
  Saying it starts the node's usual wake turn inside the call, as an ask with
  `call_mode: "node"`: the node is set aside, hears the listening chime, says
  its command, and is back in the call while the command is transcribed. The
  `wake` event then carries `in_call: true`, so a flow can tell "Alfred, cuelga"
  said in a call (and answer it with `intercom_stop` or `intercom_leave`) from
  an ordinary command. The others still hear the wake word itself: the node is
  set aside once it has been recognised.

  Enable it only on a node whose microphone does not pick up its own speaker
  (a speakerphone with hardware echo cancellation). On any other node, whoever
  is in the call can say the wake word through that node's speaker. Setting the
  node aside keeps them from dictating the command that follows, but a flow
  should still check `in_call` (and `speaker`, with speaker verification)
  before it does anything a visitor at the door must not trigger.

### Plugins

A plugin is an optional part of gocalis, compiled in and switched on in
`config.yaml`:

```yaml
plugins:
  - id: telegram          # how requests address it
    type: telegram_call   # the plugin code
    settings: {...}       # belong to the type
```

A plugin can add a **node**, whose id is the plugin's. `tts`, `play`, `ask`
and `intercom` reach it like a configured node, so flows use it with the
nodes they already have. It can also take commands of its own, and report
with events that carry its id:

```json
{"action": "plugin", "plugin": "telegram", "command": "hangup"}
```

```json
{"event": "call_ringing", "plugin": "telegram", "node_id": "telegram", "data": {"chat": "Casa", "ring_seconds": 60, "started_voice_chat": true}}
```

`args` is an object with the command's own parameters, and an arg the plugin
does not know is refused. A command the plugin refuses is answered with an
`error` event that has `plugin` set. `GET /api/plugins` lists the plugins and
whether each is usable right now (`ready`, `detail`); the dashboard shows the
same. A plugin that needs a login has a setup step: `gocalis -config
config.yaml -plugin-setup <id>`.

A plugin type is a package under `internal/plugins` that registers itself with
`plugin.Register` and is linked in from `cmd/main.go`. It gets a way to add its
node and to emit events; see `internal/plugin`.

#### Nodes somebody has to answer

A speaker or a doorbell is always there: what is said to it plays at once. A
call is not: somebody has to pick up. `GET /api/nodes` tells the two apart
(`"answer": "auto"` or `"call"`), and every action does the right thing on
both, so a flow does not need to know which kind it talks to:

| Action on a node that has to be answered | What happens |
|---|---|
| `tts`, `play` | The node rings (state `RINGING`). Once somebody answers it plays; `tts_completed` / `play_completed` as usual. Nobody answering is an `error` with `reason: "unanswered"`; somebody leaving midway, `reason: "hung_up"`. |
| `ask` | Rings, asks whoever answers and listens to them. `ask_completed` has `status: "unanswered"` when nobody did and `"hung_up"` when they left without an answer; an answer given before hanging up counts. |
| `intercom` | Rings that node first and leaves the others alone: `intercom_ringing`, then `intercom_started` when somebody answers, with the call's time running from there. Nobody answering takes the node out (`intercom_left`, `reason: "unanswered"`), which ends a call of two (`intercom_ended`, `reason: "unanswered"`). Hanging up takes the node out too (`intercom_left`, `reason: "hung_up"`). Joining a running call, the node is announced with `intercom_ringing` and `intercom_joined` once answered. |
| `tts` / `play` to `"all"` | Never rings anybody: a broadcast is for the nodes that are always there. |

Once somebody answered, what follows does not ring again while they are on the
line: an `ask`, then a `tts` with the reply, is one call.

#### `telegram_call`: a Telegram group as a node

The plugin makes the voice chat of a Telegram group a node that has to be
answered. Using the node opens the group's voice chat and gocalis waits in it;
the moment somebody of the group comes in, they are who the node reaches:

- **Answer the door from Telegram:** when the doorbell rings, the flow starts
  an intercom call between the doorbell and this node
  (`{"action": "intercom", "node_ids": ["front_door", "telegram"]}`). The
  doorbell is left alone until somebody answers; then the chime plays and the
  two sides talk. Intercom pages and other nodes can be in the same call.
- **Say or ask something:** `{"action": "tts", "node_id": "telegram", "text":
  "..."}` and `{"action": "ask", "node_id": "telegram", "text": "..."}` ring
  the group and speak to, or ask, whoever comes in.

```yaml
plugins:
  - id: telegram          # the node's id
    type: telegram_call
    settings:
      api_id: 123456
      api_hash: "0123456789abcdef0123456789abcdef"
      phone: "+34600000000"
      session_file: "data/telegram.session"
      chat: "Casa"
      title: "Timbre"
      ring_seconds: 60
      idle_seconds: 15
```

- **The account.** Only a Telegram *user* can be in a voice chat; a bot
  cannot. Give gocalis an account of its own: an account is in a voice chat
  once, so the one you answer with cannot also be gocalis. `api_id` and
  `api_hash` come from <https://my.telegram.org> ("API development tools").
- **The login** is done once: `gocalis -config config.yaml -plugin-setup
  telegram` asks for the code Telegram sends (and the two-step password, if
  the account has one) and writes `session_file`. In Docker, run it in the
  running container (`docker exec -it gocalis gocalis -config /app/config.yaml
  -plugin-setup telegram`); gocalis picks the login up without a restart.
  Whoever has that file has the account: keep it on a volume that outlives the
  container, and out of repositories and shared backups. Until there is a
  login the node exists and tells whoever uses it why it cannot be reached.
- **The group** (`chat`) is its `@username`, its numeric id, or its exact
  title: a group, not a person, because only groups have voice chats. The
  account must be a member, and to start the voice chat an admin that can
  manage video chats. Use a private group: everybody who can come into its
  voice chat can talk at your door. `-plugin-setup` checks `chat` and, when it
  is empty or does not name a group, lists the account's groups with their ids.
- **`ring_seconds`** (default 60, at most 240) is how long gocalis waits in
  the voice chat before what rang fails as unanswered.
- **`idle_seconds`** (default 15) is how long gocalis stays after a `tts`,
  `play` or `ask`, so that a reply does not ring again. An intercom call hangs
  up as soon as it is over.

| Command | Meaning |
|---|---|
| `hangup` | Takes gocalis out of the voice chat at whatever point it is: ringing, or with somebody on the line. What was waiting or running on the node ends. |

| Event | Meaning |
|---|---|
| `call_ringing` | The voice chat is open and nobody is there yet. `data`: `chat`, `ring_seconds`, `started_voice_chat`. |
| `call_answered` | Somebody came in. `data`: `chat`, `answered` (their names). |
| `call_ended` | gocalis is out of the voice chat. `reason`: `unanswered` (nobody came in time), `hung_up` (everybody on Telegram left), `completed` (nothing more to do on the node: the intercom call is over, or `idle_seconds` passed), `closed` (the voice chat was closed from Telegram), `cancelled` (what rang was given up before anybody answered), `stopped` (`hangup`), `shutdown`, or `error` with `message`. `data`: `chat`, `answered` (whether anybody came in), `duration_seconds`. |

These come on top of the node's own events (`state_changed` with `RINGING`,
and the ones of whatever action used the node), and all carry `node_id` and
`plugin`, both the plugin's id.

At the end gocalis leaves the voice chat, and closes it if it was the one that
started it. A voice chat that was already going is used as it is: with people
in it the node is answered at once, and afterwards it is left open.

Telegram does not ring anybody for a voice chat: members see it at the top of
the group, with a notification according to their settings for that group. To
make it ring, have the flow also send a message to the group when it starts
(`call_ringing` is the moment).

### Local multi-device audio simulator

[tools/simulator](tools/simulator/README.md) provides virtual RTSP microphones and
speaker capture beside a pinned go2rtc instance, a localhost browser UI, YAML
scenarios, and container transport checks using Gocalis's real WebRTC client.
It supports bidirectional Opus and the existing WHIP → AAC-ELD doorbell route.

Doorbell capture/transport behavior, opt-in diagnostics, simulator faults, and
physical measurement steps are documented in
[Doorbell audio reliability](docs/doorbell-audio-reliability.md).
