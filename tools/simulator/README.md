# Local audio simulator

Run virtual audio devices beside **go2rtc v1.9.14**. Gocalis connects using its
production WebRTC client. Two devices negotiate Opus in both directions; a third
receives microphone Opus and captures the existing WHIP → FFmpeg → AAC-ELD
speaker route. The RTSP destination replaces the accessory. This does not test
HomeKit pairing, encryption, acoustic echo cancellation, or WAN behavior.

The module and codec dependencies are separate from the application module.
The shared image builds **FFmpeg 7.1.1**, **FDK-AAC 2.0.3**, and libopus. Startup
encodes a tone to mono 16 kHz ELD, checks the actual profile, decodes it with FDK,
and checks its content and duration. Failure prevents startup; AAC-LC is never
used as a fallback. The build takes several minutes on its first run.

## Start

From the repository root, on Linux with Docker Compose:

```sh
docker compose -f tools/simulator/compose.yaml build generate
docker compose -f tools/simulator/compose.yaml run --rm generate
docker compose -f tools/simulator/compose.yaml up -d simulator go2rtc
```

Open **http://localhost:18080**. The manifest is `devices.yaml`; generated files
are written to `generated/`. Production configuration is read as a model-settings
base and is never modified. Generated configuration disables MQTT, sets local ICE
servers to `[]`, and uses distinct stream names and intermediate doorbell inputs.
Use `docker compose -f tools/simulator/compose.yaml down` to stop the project.

The UI uploads/replays **mono PCM16 16 kHz WAV**, stops input, disconnects and
reconnects the virtual device, shows levels/counters/errors, and downloads or
plays captured speaker recordings. “Use browser mic” requests permission with a
user gesture and sends mono PCM16 from an AudioWorklet into one selected device.
Live input and fixture playback are exclusive. Switching devices stops the old
browser microphone. The browser and server bound queued input to two seconds and
report dropped audio. Devices keep sending paced silence between inputs.

**Rootless Docker:** host networking shares Docker's RootlessKit namespace, which
may differ from your desktop's namespace. Containers still reach each other, and
browser tests run inside that namespace. To expose the UI to your host browser,
add this localhost-only RootlessKit port mapping (adjust the socket path if your
Docker installation uses a different state directory):

```sh
curl --unix-socket "$XDG_RUNTIME_DIR/dockerd-rootless/api.sock" \
  -H 'Content-Type: application/json' \
  -d '{"proto":"tcp4","parentIP":"127.0.0.1","parentPort":18080,"childIP":"127.0.0.1","childPort":18080}' \
  http://localhost/v1/ports
```

The response contains an `id`. Remove your mapping with `curl -X DELETE
--unix-socket "$XDG_RUNTIME_DIR/dockerd-rootless/api.sock"
http://localhost/v1/ports/ID`. Do not remove unrelated mappings.

## Checks and scenarios

All Go builds/checks run in containers:

```sh
./tools/simulator/check.sh
```

This builds the images, runs simulator race tests and frontend checks, runs an
engine scenario, and runs the real Gocalis client against all devices concurrently.
The transport harness checks decoded microphone/speaker markers, active duration,
isolation, repeated disconnect/reconnect and route recreation, and joined client
shutdown. Its final export writes a JSON report, per-device WAVs, chronological
events, and codec/packet diagnostics to `runs/<UTC timestamp>/`. Go test output is
also saved in `runs/transport.log`. The application integration test is opt-in via
`GOCALIS_SIMULATOR`; ordinary application tests skip external services.

Run browser smoke checks, including a fake browser microphone and AudioWorklet:

```sh
docker compose -f tools/simulator/compose.yaml run --rm browser-checks
```

Run another YAML scenario through the same HTTP endpoint used by the UI:

```sh
docker compose -f tools/simulator/compose.yaml run --rm scenario \
  -mode scenario -scenario /scenarios/prompt-reply.yaml -out /runs/cli
```

A failed assertion or missing prerequisite exits nonzero and writes `report.json`.
Scenarios have a five-minute maximum lifetime; each wait and command requires a
positive `timeout` of at most five minutes. One sequence runs per device, and
sequences run concurrently. Commands within a sequence must be followed by their
completion event before another command is issued. Replay/connection operations
can occur while a command is pending, allowing asks and barge-in. Run one scenario
at a time and stop browser microphones first. Other application clients should
avoid issuing commands during an automated run because Gocalis completion events
have no command correlation ID.

Supported steps:

| Action | Parameters |
| --- | --- |
| `replay` | `replay`: WAV path; or `audio`: base64 WAV |
| `tone` | `hz` (100–7000), `seconds` (up to 300) |
| `stop`, `reset` | Stop microphone input / clear recording and run diagnostics |
| `disconnect`, `reconnect` | Close RTSP connections / allow new connections |
| `wait` | `seconds`, `timeout` |
| `wait_input_idle` | `timeout` |
| `wait_activity` | `timeout`, `direction`: `input` or `output`, optional `min_rms` |
| `command` | `timeout`, `command`: existing Gocalis `/api/execute` request |
| `wait_event` | `timeout`, `event`, optional `field` (defaults to `text`) and case-insensitive `contains` |
| `assert_output` | Required `min_active_seconds`; optional `max_seconds`, `min_rms`, `hz`, `min_marker` |

`assert_output` examines decoded PCM, fails codec/buffer errors, counts active
20 ms windows, and optionally checks the spectral energy of a known tone with
codec tolerance. `max_seconds` includes recorded silence. Speech scenario
keywords are asserted on app transcript events; output assertions measure decoded
speech activity and duration, **not the semantic content of synthesized speech**.
The model-free tier uses known tones to verify audio content directly.

CLI replay paths and `requires` paths are resolved relative to the YAML file;
CLI embeds replay WAVs before posting the scenario. Browser/HTTP paths resolve
inside the simulator's mounted `fixtures/` directory and cannot escape it. Engine
buffers are independent per device; fixture files and recordings are capped at
five minutes. Events and app-event queues are bounded; event queue overrun fails
the run. A slow UI poll never schedules concurrent polling requests.

## Full application tier

Full-app templates are included for wake/AutoAsk, prompt/reply, simultaneous asks,
broadcast, silence timeout recovery, and barge-in. **They require supplied models,
speech fixtures, and expected keywords.** No speech models or recordings are
downloaded or fabricated by these checks. Place `question.wav`,
`office-question.wav`, and `wake.wav` in `fixtures/`, and edit the scenario's
`contains` values for the spoken language and recordings.

Set `GOCALIS_MODELS` to the directory matching the model paths in your base
`config.yaml`, then start the app:

```sh
GOCALIS_MODELS=/absolute/path/to/models \
  docker compose -f tools/simulator/compose.yaml --profile full-app up -d gocalis
```

Missing model files fail Gocalis's existing startup validation. Missing speech
fixtures or an unavailable app fail the scenario with an explicit prerequisite
error. The default manifest disables KWS. For wake/AutoAsk, add a `kws` mapping to
the device entry with your existing KWS model paths, keyword file, `enabled: true`,
`auto_ask: true`, and wake responses, then regenerate configuration and restart the
app. Each device also accepts `echo_cancellation`; the default is `true` because
this simulator does not feed speaker audio back into the microphone. Barge-in
requires that setting. Timing in speech templates should be adjusted to your
model's prompt duration and VAD configuration.

## HTTP controls

The simulator binds to localhost and rejects cross-origin browser controls.

- `GET /api/devices`: status of all devices.
- `POST /api/devices/{id}/play`: WAV body.
- `POST /api/devices/{id}/tone`: JSON `{ "hz": 600, "seconds": 1 }`.
- `POST /api/devices/{id}/{stop|reset|disconnect|reconnect}`.
- `GET /api/devices/{id}/recording`: PCM16 WAV.
- `GET /api/devices/{id}/live`: binary WebSocket PCM16 mono 16 kHz; maximum message 6400 bytes.
- `POST /api/scenarios`: YAML body; JSON pass/fail report.
- `POST /api/export`: JSON report metadata; writes current recordings and diagnostics.

Set `token` in the manifest for Gocalis command/event authentication; it is copied
into generated Gocalis security settings. The simulator endpoints are local test
controls. No production application API was added.

Gocalis RTC configuration now accepts `ice_servers`, including TURN `urls`,
`username`, and string `credential`. Omission preserves the receive connection's
existing public STUN default and talkback's existing empty ICE configuration;
explicit `ice_servers: []` disables public STUN for both connections.

Deterministic microphone packet loss, reordering, and burst delays can be set in
per-device `faults` fields. See
[the doorbell measurement record](../../docs/doorbell-audio-reliability.md) for
configuration, limits, and the distinction between simulator and physical results.
