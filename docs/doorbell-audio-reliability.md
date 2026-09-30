# Doorbell audio reliability: implementation and measurement record

The WHIP producer → go2rtc FFmpeg → AAC-ELD destination is preserved. No
production stream names, model selection, thread allocation, output gain, ICE
servers, or capture guard delays were changed.

## Implemented behavior

Listening captures continuous mono 16 kHz PCM. Live Silero activity determines
speech onset and endpoint; completed segments no longer drive capture. A bounded
250 ms pre-roll retains onset context, and pauses remain in the waveform. VAD is
reset on node-state transitions. Its silence hangover is one 32 ms window;
`post_speech_silence_seconds` owns the conversational endpoint. The old global
`min_silence_duration_ms` no longer adds another silence wait to asks. The
existing 250 ms minimum VAD speech duration remains.

The listening timeout applies until speech starts. Speech then has an independent
120-second maximum, matching the ASR input limit. The capture buffer is bounded
at 120 seconds including pre-roll. Listening state is published after capture is
armed. Half-duplex gating and the configured playback-tail guard remain; an
answer spoken entirely during that guard still cannot be recovered. Barge-in
uses live onset and remains gated by the transport's echo-cancellation setting.
The setting is a deployment assertion, not evidence of acoustic cancellation.

RTP ingestion is separate from model callbacks. A 32-packet queue drops its oldest
packet on overflow; sequence gaps wait for up to four pending packets or 40 ms.
Contiguous packets pass immediately. Duplicates and late packets are rejected,
including across sequence wraparound. Opus PLC fills timestamp gaps up to 120 ms
in 2.5 ms units; PCMU uses silence. Larger discontinuities recover at the current
packet. These are conservative engineering bounds, **not hardware-tuned values**.
FEC is not assumed. Capture reset rejects PCM queued before a new turn.

Talkback queues remain bounded at 100 frames. Route and track-write failures
reach callers, and failed cached senders are recreated on subsequent playback.
Resampling retains interpolation phase across TTS chunks. Ask prompts stream
before synthesis completes, with guard/chime/tail ordered in the same playback
route and synthesis cancelled on barge-in or turn cancellation.

An HTTP route acknowledgement verifies control-plane success only. It cannot
prove that an accessory is audibly playing. Downstream acoustic tail timing and
full route-readiness verification remain physical acceptance work.

## Opt-in diagnostics

Add this to the desired node in `config.yaml`:

```yaml
diagnostics:
  enabled: true
  recording_dir: /data/doorbell-diagnostics # omit for counters/timing only
```

`GET /api/status` and `/api/nodes` include a fixed-size `diagnostics` object on
that node. It reports packet counts, loss, reordering, rejected packets, queue
overflows, decode errors, concealed samples, maximum packet arrival gap and
receive processing time, talkback queue depth/high-water mark, pacing lateness,
route/write failures, and silence ticks during active playback. Nanosecond timing
fields use the `_ns` suffix. Receive counters span the client lifetime; talkback
counters reset with sender recreation. Silence ticks can include source starvation
and the final drain race, so interpret them alongside recordings.

Ask logs report prompt stream acquisition, prompt playback, capture opening and
duration, ASR total time, and native ASR queue wait/inference separately. Prompt
stream acquisition is not complete synthesis time. `last_first_audio_delay_ns`
measures playback entry to the first paced talkback speech frame; it includes
route assertion but excludes synthesis and node queue wait. It is not an acoustic
first-audio measurement. Wake timestamp remains available in the existing event.

Recordings are disabled unless both `enabled` and `recording_dir` are set. Each
node uses a hash-derived filename and overwrites two WAVs: `*-microphone.wav`
contains at most the latest 120 seconds before gating; `*-asr.wav` contains the
primary ASR submission. Files use mode 0600. Memory and disk usage are bounded
per configured node. Recordings are saved before primary transcription; the
speaker challenge is not submitted to ASR. Disable diagnostics and remove the
recording directory to stop and remove recordings. Disk errors are logged without
failing the conversation. The WAV pair is written atomically per file, not as a
transactional pair.

## Verification on 2026-09-30

Tests ran in the repository's existing Docker images because the host has no Go
toolchain. The application internal packages passed `go test -race -timeout=2m
./internal/...`. Tests cover continuous capture/pre-roll/pause preservation,
onset timeout versus active speech, streamed prompt/chime order, chunked
resampling at five rates, sequence wraparound/reorder/loss, queue bounds,
propagated playback failures, and stale-capture rejection. Simulator module race
tests also passed.

The existing live simulator passed `TestSimulatorTransport` under the race
detector for kitchen, office, and doorbell, including two connect/play/close cycles
per device and the WHIP → AAC-ELD route. The run took approximately 29 seconds;
this is test runtime, not conversation latency. It does not establish microphone
accuracy, Wi-Fi resilience, acoustic output quality, or physical doorbell timing.

The simulator now accepts deterministic faults per device:

```yaml
- id: doorbell
  profile: doorbell
  faults:
    drop_every: 25
    reorder_every: 7
    burst_every: 100
    burst_frames: 3 # 20 ms source frames; max 50
```

Zero values disable each fault. Sequence numbers/timestamps advance through loss.
Burst frames are held and released together; reordered packets are held for two
source ticks. The deterministic injector has unit coverage, but the live
transport proof above used the existing fault-free simulator. Faulted end-to-end
duration tolerances and deliberately stalled-model scenarios remain to validate.

## Physical baseline and model comparison still required

The repository includes a front-door go2rtc address, but its read-only API could
not be reached from this workspace (connection error). No physical recording
corpus, locally usable model files, or Wi-Fi statistics were available. No
before/after latency or transcript accuracy is claimed.

For each run, record hardware/CPU, Wi-Fi signal/retries/loss, go2rtc and FFmpeg
versions, sanitized stream definitions, Opus and AAC-ELD profiles/packet duration,
and the selected ICE candidate pair. Collect 20 or more repetitions per scenario
with other camera consumers stopped and running: immediate answer, short command,
pause then a longer sentence, multiple sentences, noise, repeated turns,
idle-to-speech, and concurrent inference/transcoding.

Compare the pre-gate waveform with ASR input and reference text. Classify damage
as transport (loss/arrival gaps), capture (missing onset/pause/tail), model
(transcription errors despite complete PCM), or deliberate waiting (guard,
endpoint, route, queue). Report median/p95 acoustic first-audio and
end-of-speech-to-transcript latency, plus clipped words, false wakes, echo capture,
and playback failures. Align timestamps to one clock or measure acoustic timing
from a reference recording.

Compare current Spanish Moonshine, multilingual Whisper base and small using
identical recordings and thread settings. Evaluate wake-word accuracy separately.
Tune one setting at a time: local ICE, output gain, prompt length, capture guard,
endpoint, packet durations, and CPU threads. Keep AAC-ELD at the destination.
Choose the model/defaults only after comparing accuracy, audio quality, median
and p95 latency. Record baseline/after values, configuration, and run identifiers.

Rollback uses the previous application revision and saved configuration. New
optional diagnostics fields can be removed for that revision. Keep model/thread,
ICE/gain/guard settings unchanged until physical measurements justify a change.
