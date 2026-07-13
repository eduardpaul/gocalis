# node-red-contrib-gocalis

Custom Node-RED nodes to integrate seamlessly with the **Gocalis Voice Assistant I/O Engine**.

This package provides a drag-and-drop suite of nodes to speak, ask questions, identify speakers, and trigger automations on wake word detection. By routing all hardware audio streams through a unified daemon, this package prevents audio driver conflicts and allows multiple flows to interact with the same microphone/speaker safely.

---

## Installation

To install this package locally in your Node-RED instance:

1. Locate your Node-RED user directory (typically `~/.node-red` or `C:\Users\<username>\.node-red`).
2. Run npm install pointing to the directory containing this package:
   ```bash
   cd ~/.node-red
   npm install /path/to/voice_clean/node-red-contrib-gocalis
   ```
3. Restart Node-RED. The **Gocalis Voice** section will appear in your palette.

---

## Available Nodes

### 1. `gocalis config` (Configuration Node)
Stores connection settings for the Gocalis daemon.
* **Host**: IP address or hostname of the daemon (e.g. `localhost`).
* **Port**: Port of the WebSocket API server (default: `9090`).
* **SSL/TLS**: Enable if the Gocalis server is hosted behind a secure proxy.
* **WebSocket**: Automatically connects to `ws://<host>:<port>/ws` and manages automatic reconnection.

### 2. `gocalis say`
Plays synthesized Text-To-Speech (TTS) audio out loud through the system speaker.
* **Node ID**: The physical satellite node to play the audio on (defaults to `"default"`). Can be overridden dynamically via `msg.node_id` or `msg.room`.
* **TTS Text**: The message to synthesize (can also be passed dynamically in `msg.payload`).
* **Output**: Sends a message when playback finishes, allowing you to sequence actions.

### 3. `gocalis ask`
An interactive prompt-and-capture node. Plays a TTS prompt, then records and transcribes the user's voice command.
* **Node ID**: The physical satellite node to play and capture audio on (defaults to `"default"`). Can be overridden dynamically via `msg.node_id` or `msg.room`.
* **Prompt Text**: Greeting spoken before listening (can also be passed dynamically in `msg.payload`).
* **Barge-In**: If enabled, the user can interrupt the prompt by speaking immediately.
* **Verify Speaker**: If enabled, runs biometric voice verification to check the speaker's identity.
* **Outputs**:
  * **Output 1 (Success)**: Emits if speech is successfully captured and verified.
    * `msg.transcription`: The transcribed text.
    * `msg.speaker`: The verified speaker's name.
    * `msg.node_id`: The originating physical node ID.
    * `msg.audio_wav_base64`: The captured audio as a WAV base64 string.
  * **Output 2 (Timeout / Failed)**: Emits if VAD times out (silence) or speaker verification fails.

### 4. `gocalis wake` (Wake Word Trigger)
A real-time listener node that triggers when the background wakeword detection loop hears the wake word (e.g., *"Alfred"* or *"Hey Bro"*).
* **Node ID**: Filter events by physical node ID (e.g., `"default"`, `"kitchen"`) or set to `"all"` to trigger on any satellite's events.
* **Outputs**:
  * **Output 1 (Wake Event)**: Emits immediately when the wake word is heard. Perfect for turning down TV volume or flashing visual indicators.
    * `msg.payload`: `{ event: "wake_detected", node_id: "default", model: "alfred", score: 0.89 }`
  * **Output 2 (Command Captured)**: Emits once the user finishes speaking their command (if `auto_ask` is enabled on the server).
    * `msg.transcription`: Decoded voice command.
    * `msg.node_id`: Originating physical node ID.
    * `msg.speaker`: Verified speaker name.

### 5. `gocalis stt` (Speech-to-Text on Files)
A stateless file transcription node.
* **Audio Source**: Can be a file path string or a raw Buffer passed in `msg.payload` containing the audio file.
* **Verify Speaker**: If enabled, runs biometric voice verification to check the speaker's identity on the file.
* **Outputs**:
  * `msg.payload`: JSON containing `transcription` and optional `speaker`.
  * `msg.transcription`: The transcribed text.
  * `msg.speaker`: The identified speaker (if verification was enabled).

### 6. `gocalis tts` (Text-to-Speech to Files)
A stateless speech synthesis node.
* **TTS Text**: The text to synthesize (can also be passed dynamically in `msg.payload`).
* **Outputs**:
  * `msg.payload`: A Node.js `Buffer` containing the generated WAV file.
  * `msg.contentType`: `"audio/wav"`
