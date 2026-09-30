# Architecture and performance review

Reviewed 2026-09-30 against the original implementation. The findings below explain the triggers that motivated the fixes; their source line numbers refer to that original tree. The implementation status describes the final changes. This is a source review, not a measured hardware benchmark.

## Implementation status

All eight actionable findings have been addressed:

- Shared bounded priority queues, cancellable submissions and PCM reads, bounded streaming generation, and joined native model shutdown.
- A bounded application task group for detached and synchronous controls; joined wake/reload/capture work and WebRTC reconnects; explicit shutdown of WebSockets and physical-node dispatchers.
- One shared WebSocket hub with ordered per-client writers, bounded queues/client counts and write deadlines.
- Prompt-only barge-in cancellation with capture of the triggering samples and deferred node state reset.
- Strict request decoding and validation, text/audio/capture limits, overload responses, authenticated transcript/recording events, and dashboard token support.
- One synthesis per broadcast, immutable shared PCM, independent gain and aggregated device errors.
- Strict normalized YAML, explicit ASR family/language and VITS paths, corrected challenge key, early model-file validation, and profile-directory watching with trailing debounce.
- A compiled runtime image containing native libraries and embedded frontend; separate container test target; lockfile installs and config/models-only runtime mounts. Removed unused runtime dependencies and duplicate playback/ask paths.

Regression tests cover worker shutdown, bounded admission, streaming backpressure/cancellation, barge-in, broadcast synthesis/gain/failure, configuration and atomic profile replacement, event ordering/slow consumers, transport callback shutdown and HTTP authentication/overload. Validation uses the Go container as requested. Physical audio, model accuracy and go2rtc integration remain hardware-dependent; no throughput or latency improvement is claimed without measurement.

## Overall assessment

Keep the single-process Go service and small React dashboard. `audionode.AudioNode`, the shared command executor, centralized audio helpers, and per-node turn ordering are useful boundaries. Microservices, another message broker, a dependency injection framework, or compatibility adapters would increase complexity without solving the current problems.

The main risk is resource ownership: contexts reach orchestration but stop at inference queues and audio readers. Several background tasks have no joined shutdown. Fix those boundaries before tuning inference threads or replacing DSP algorithms.

## Prioritized findings

### 1. Critical: inference shutdown can free models still in use

Sources: `internal/ai/speech.go:196`, `:216`, `:277`, `:505`, `:586`, `:711`.

ASR decode and TTS generation copy a native model pointer under a mutex, then use it after releasing the mutex. `Close` deletes that pointer without waiting for the worker. A concurrent shutdown can therefore delete native resources during inference. Both queues silently reject pushes after close, and pop stops immediately on close even when jobs remain. Callers waiting on those jobs' result channels can wait forever.

Make each engine own its worker, queue, admission state and completion channel. Closing should reject new submissions with an error, resolve every pending submission, join active inference, then delete the native model. Do not hold the engine mutex while joining a worker that needs it. Add concurrency tests for submit versus close, queued shutdown, active generation and repeated close.

### 2. High: service shutdown does not own all work

Sources: `cmd/main.go:225`, `internal/server/websocket.go:73`, `:152`, `internal/webserver/webserver.go:210`, `internal/runtime/runtime.go:207`, `internal/localaudio/localaudio.go:290`.

HTTP execute, WebSocket and MQTT commands create detached background contexts. Node runtime completion does not join AutoAsk work or guarantee that audio callbacks have finished. HTTP shutdown does not close upgraded WebSocket connections. The ten-second node timeout proceeds to model deletion even if a runtime is still active.

Use one application lifetime context and a tracked command dispatcher. Stop admissions, cancel commands and reconnect loops, close WebSockets, join command workers and capture callbacks, then close engines. Define `AudioNode.Close` to wait until callbacks have stopped. A shutdown timeout should not lead to freeing resources that live workers still use.

### 3. High: slow event consumers block publishers

Sources: `internal/server/websocket.go:88`, `internal/webserver/webserver.go:134`, `internal/protocol/protocol.go:70`, `internal/node/node.go:78`.

Both WebSocket servers synchronously write to each client without write deadlines. One stalled client can block publication to every later client and transport. State callbacks have an asynchronous dispatcher, but its queue is unbounded; direct wake publication can also stall wake handling.

Extract one small WebSocket connection hub shared by both servers: one writer per connection, bounded outbound queue, write deadline, explicit disconnect of slow consumers, and joined shutdown. Keep transport-specific request handling outside it. Preserve event order; do not spawn an unbounded goroutine per broadcast. `PhysicalNode` also needs a stop/join operation: its current dispatcher waits forever after the runtime exits.

### 4. High: barge-in cancels the entire ask turn

Source: `internal/ask/ask.go:103`, `:184`, `:259`.

Barge-in calls the cancellation function for the context later used by the listening loop. That loop immediately exits on cancellation. Capture starts after interruption, so the triggering speech segment is not retained either. Several prompt error returns also leave the physical node in its previous speaking/listening state.

Use a separate child context for prompt playback. Barge-in cancels only that playback, starts capture at the interruption boundary, and preserves the triggering samples. Install one deferred state reset after acquiring the turn. Test prompt cancellation separately from parent cancellation and verify that a barge-in produces a transcription rather than a silence result.

### 5. High: request admission and memory are unbounded

Sources: `internal/server/websocket.go:142`, `:157`, `internal/webserver/webserver.go:200`, `internal/mqtt/client.go:133`, `internal/ai/speech.go:98`, `:379`, `:779`, `internal/webrtc/talkback.go:258`.

Requests can create unlimited goroutines and queued inference jobs. HTTP JSON bodies and WebSocket messages have no application size limits. TTS and talkback buffers can grow faster than real-time playback drains them; request timeouts do not cancel inference queue waits.

Use bounded admission in the shared dispatcher, explicit HTTP body and WebSocket read limits, and maximum text/audio/turn durations. Carry `context.Context` through inference submission and stream reads. Reject overload predictably instead of retaining arbitrarily many jobs. Bound streaming buffers with cancellation-aware backpressure. Authenticate the dashboard events connection when a token is configured: it currently broadcasts transcripts and recordings without checking that token.

### 6. Medium: broadcast TTS repeats inference for each node

Source: `internal/brain/brain.go:256`.

`SpeakAll` calls `Speak` independently for every node; on an uncached utterance each node submits identical synthesis to the single TTS worker. Compute grows with node count and later devices wait for repeated generation. Both audio broadcast methods log per-node errors but return nil, allowing the executor to report success even when playback fails.

For broadcast, synthesize once and share immutable PCM with each node's turn queue; apply gain to a separate per-device buffer. This trades initial streaming latency for less inference work and simpler ownership. Return aggregated per-node failures using `errors.Join`. Measure before adding a concurrent streaming multicast mechanism.

### 7. Medium: configuration and model selection are implicit

Sources: `internal/config/config.go:191`, `internal/ai/speech.go:152`, `internal/config/watcher.go:14`.

Configuration parsing accepts unknown YAML keys and mainly defaults the thread count. ASR chooses the model family from a filename substring and hardcodes Spanish. Speaker watching validates a new config but reloads the engine's original configuration; it watches the file itself, so atomic file replacement can invalidate the watch. Its throttle can discard a final change rather than debounce it, and it has no shutdown context.

Normalize and validate configuration once: explicit model family/language, unique node IDs, supported transports, valid URLs/rates/durations, model paths and thread budgets. Reject unknown keys. Decide whether speaker reload means rereading existing profile files or applying changed config; expose exactly that behavior. Watch the parent directory with a trailing debounce and application context if automatic reload remains necessary.

### 8. Medium: build and API surfaces can be simpler

Sources: `Dockerfile`, `docker-compose.yaml`, `internal/webserver/webserver.go:82`, `internal/brain/brain.go:320`.

The container uses `go run`; Compose mounts the repository over `/app`, hiding the embedded dashboard copied into the image, and overrides startup with `tail -f /dev/null`. The file is effectively a development container, not an application deployment. Frontend dependencies are installed with `npm install` despite a lockfile.

Build an executable in a builder stage using `npm ci`, copy it and required native runtime libraries into a runtime image, and mount only config/models. Keep a development container explicitly separate. Consolidate duplicate ask request mapping and playback paths after defining node turn ownership. Low-level state-neutral playback should only be callable by an owner of the turn; avoid two public ways to bypass ordering. No compatibility wrappers are needed in this greenfield codebase.

## Initial focused changes

- Dashboard socket, reconnect timer and status fetch now share one effect lifetime. Cleanup stops reconnects and aborts fetches, including React StrictMode remounts. Status polling cannot overlap itself and rejects HTTP error responses.
- Node queue checks cancellation before admitting a waiter and uses `context.AfterFunc` instead of one parked watcher goroutine per request. Cancellation removal wakes remaining waiters. Added cancellation regression tests.
- Removed the unused session `lastFeed` timestamp and associated clock reads. The ask engine currently tracks speech timing itself.

## Performance work after lifecycle fixes

Measure wake-to-listening, prompt time-to-first-audio, speech-end-to-transcript, ASR/TTS queue wait and execution time, audio callback duration, heap growth and goroutine count. Run idle, one active node, simultaneous asks, broadcast, stalled clients and interrupted playback on representative hardware. Report p50/p95 and steady-state memory; compare the same models and audio.

Likely optimization candidates are repeated broadcast inference, per-chunk allocation/copying, and CPU oversubscription from per-node VAD/KWS plus shared inference. `prepareStream` copies float samples before `push` immediately converts them to owned PCM; that temporary copy appears removable after checking callback lifetime. Linear resampling lacks anti-alias filtering, so replacing it is an audio quality decision requiring listening/recognition tests, not an assumed throughput improvement. Keep the small bounded event log and existing DSP helpers; avoid speculative memoization or buffer pools without profiles.

## Original recommended implementation order

1. Engine worker ownership, cancellation and shutdown; application command lifecycle.
2. Barge-in capture correctness and consistent node state reset.
3. Shared bounded WebSocket hub and request admission limits.
4. Single-synthesis broadcast and accurate error propagation.
5. Strict configuration, reload semantics and reproducible container build.
6. Profile representative workloads and optimize measured bottlenecks (pending representative hardware and models).

## Validation

Passed Go container checks: `go test -race -timeout=2m ./...`, `go vet ./...`, and executable build. Frontend lint/build passed. Both Compose image targets build successfully, and the runtime image's `-h` smoke test confirms the executable loads its native libraries. Real microphones, model accuracy, MQTT broker behavior and go2rtc media integration still require deployment testing.
