import { useState } from 'react'

// Tab definitions. `id` maps to a different backend call:
//  - say        -> POST /api/execute {action:'tts'}  (plays TTS on a node)
//  - tts        -> POST /api/synthesize               (renders TTS to a WAV file only)
//  - ask        -> POST /api/ask                      (prompt -> listen -> ASR, returns text)
//  - asr        -> POST /api/execute {action:'asr'}
//  - speaker_id -> POST /api/execute {action:'speaker_id'}
const TABS = [
  { id: 'say', label: 'Say', hint: 'Speak text on a node' },
  { id: 'tts', label: 'TTS', hint: 'Render text to a WAV file (no playback)' },
  { id: 'ask', label: 'Ask', hint: 'Speak a prompt, listen, and transcribe the reply' },
  { id: 'intercom', label: 'Intercom', hint: 'Bridge two nodes into a live two-way call' },
  { id: 'asr', label: 'ASR', hint: 'Transcribe an audio file' },
  { id: 'speaker_id', label: 'Speaker ID', hint: 'Identify the speaker in an audio file' },
]

function CommandCenter({ nodes, onExecute, onSynthesize, onAsk, onReloadSpeakers }) {
  const [activeTab, setActiveTab] = useState('say')
  const [nodeId, setNodeId] = useState('all')
  const [intercomNodes, setIntercomNodes] = useState([])
  const [intercomDuration, setIntercomDuration] = useState(60)
  const [text, setText] = useState('')
  const [filename, setFilename] = useState('')
  const [audioFile, setAudioFile] = useState('')
  const [priority, setPriority] = useState(10)
  const [requireSpeakerId, setRequireSpeakerId] = useState(false)
  const [vadTimeout, setVadTimeout] = useState(10)
  const [loading, setLoading] = useState(false)
  const [lastResult, setLastResult] = useState(null)

  const nodeOptions = [
    { value: 'all', label: 'All Nodes' },
    ...(nodes || []).map((n) => ({ value: n.node_id, label: n.node_id })),
  ]
  const singleNodeOptions = (nodes || []).map((n) => ({ value: n.node_id, label: n.node_id }))

  const activeMeta = TABS.find((t) => t.id === activeTab)

  // selectTab switches tabs, seeding the intercom participant set with the first
  // two available nodes (a call needs at least two).
  const selectTab = (id) => {
    if (id === 'intercom' && intercomNodes.length < 2) {
      setIntercomNodes(singleNodeOptions.slice(0, 2).map((o) => o.value))
    }
    setActiveTab(id)
    setLastResult(null)
  }

  // toggleIntercomNode adds or removes a node from the participant set.
  const toggleIntercomNode = (value) => {
    setIntercomNodes((prev) =>
      prev.includes(value) ? prev.filter((v) => v !== value) : [...prev, value]
    )
  }

  const handleSubmit = async (e) => {
    e.preventDefault()
    setLoading(true)
    setLastResult(null)

    try {
      let result
      if (activeTab === 'say') {
        result = await onExecute('tts', { node_id: nodeId, text, priority: Number(priority) })
      } else if (activeTab === 'tts') {
        result = await onSynthesize({ text, filename, priority: Number(priority) })
      } else if (activeTab === 'ask') {
        result = await onAsk({
          node_id: nodeId === 'all' ? '' : nodeId,
          tts_text: text,
          require_speaker_id: requireSpeakerId,
          vad_timeout_seconds: Number(vadTimeout),
          priority: Number(priority),
        })
      } else if (activeTab === 'intercom') {
        if (intercomNodes.length < 2) {
          throw new Error('Select at least two nodes to bridge')
        }
        result = await onExecute('intercom', {
          node_ids: intercomNodes,
          duration_seconds: Number(intercomDuration),
        })
      } else {
        // asr / speaker_id
        result = await onExecute(activeTab, {
          node_id: nodeId === 'all' ? '' : nodeId,
          audio_file: audioFile,
          priority: Number(priority),
        })
      }
      setLastResult(result)
    } catch (err) {
      setLastResult({ status: 'error', error_message: err.message })
    } finally {
      setLoading(false)
    }
  }

  const handleReload = async () => {
    setLoading(true)
    try {
      const result = await onReloadSpeakers()
      setLastResult(result)
    } catch (err) {
      setLastResult({ status: 'error', error_message: err.message })
    } finally {
      setLoading(false)
    }
  }

  // handleIntercomStop ends the active call that the selected nodes participate
  // in (stopping any one participant ends the whole call).
  const handleIntercomStop = async () => {
    const a = intercomNodes[0]
    if (!a) {
      setLastResult({ status: 'error', error_message: 'Select a participating node to stop' })
      return
    }
    setLoading(true)
    setLastResult(null)
    try {
      const result = await onExecute('intercom_stop', { node_id: a })
      setLastResult(result)
    } catch (err) {
      setLastResult({ status: 'error', error_message: err.message })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="card command-center">
      <h2>Command Center</h2>

      <div className="command-tabs">
        {TABS.map((tab) => (
          <button
            key={tab.id}
            className={activeTab === tab.id ? 'active' : ''}
            onClick={() => selectTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {activeMeta?.hint && <p className="hint">{activeMeta.hint}</p>}

      <form className="command-form" onSubmit={handleSubmit}>
        {activeTab === 'say' && (
          <>
            <label>
              Target Node
              <select value={nodeId} onChange={(e) => setNodeId(e.target.value)}>
                {nodeOptions.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Text to speak
              <textarea
                rows={3}
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder="Enter text to speak on the node..."
                required
              />
            </label>
          </>
        )}

        {activeTab === 'tts' && (
          <>
            <label>
              Text to synthesize
              <textarea
                rows={3}
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder="Enter text to render to a WAV file..."
                required
              />
            </label>
            <label>
              Output filename <span className="hint">(optional, saved under models/tts_cache)</span>
              <input
                type="text"
                value={filename}
                onChange={(e) => setFilename(e.target.value)}
                placeholder="auto-generated if empty (e.g. greeting.wav)"
              />
            </label>
          </>
        )}

        {activeTab === 'ask' && (
          <>
            <label>
              Target Node
              <select value={nodeId} onChange={(e) => setNodeId(e.target.value)}>
                {singleNodeOptions.length === 0 && <option value="">No nodes available</option>}
                {singleNodeOptions.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </label>
            <label>
              Prompt to speak <span className="hint">(optional)</span>
              <textarea
                rows={2}
                value={text}
                onChange={(e) => setText(e.target.value)}
                placeholder="Spoken before listening, e.g. 'How can I help you?'"
              />
            </label>
            <div className="form-row">
              <label>
                VAD timeout (s)
                <input
                  type="number"
                  value={vadTimeout}
                  onChange={(e) => setVadTimeout(e.target.value)}
                  min={1}
                  max={120}
                />
              </label>
              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={requireSpeakerId}
                  onChange={(e) => setRequireSpeakerId(e.target.checked)}
                />
                Require Speaker ID
              </label>
            </div>
          </>
        )}

        {activeTab === 'intercom' && (
          <>
            <label>
              Participants <span className="hint">(select two or more nodes to bridge)</span>
              <div className="intercom-participants">
                {singleNodeOptions.length === 0 && <span className="hint">No nodes available</span>}
                {singleNodeOptions.map((opt) => (
                  <label key={opt.value} className="checkbox-label">
                    <input
                      type="checkbox"
                      checked={intercomNodes.includes(opt.value)}
                      onChange={() => toggleIntercomNode(opt.value)}
                    />
                    {opt.label}
                  </label>
                ))}
              </div>
            </label>
            <label>
              Auto-end after (seconds) <span className="hint">(0 = server default)</span>
              <input
                type="number"
                value={intercomDuration}
                onChange={(e) => setIntercomDuration(e.target.value)}
                min={0}
                max={3600}
              />
            </label>
            <p className="hint">
              While a call is live, every participant is held: any Say/Ask targeting them is queued
              until it ends. Each node hears all the others (mix-minus). The call auto-ends after the
              timeout, or use Stop below.
            </p>
          </>
        )}

        {(activeTab === 'asr' || activeTab === 'speaker_id') && (
          <>
            <label>
              Target Node
              <select value={nodeId} onChange={(e) => setNodeId(e.target.value)}>
                {nodeOptions.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
              <span className="hint">Optional metadata for ASR/SpeakerID results.</span>
            </label>
            <label>
              Audio File Path
              <input
                type="text"
                value={audioFile}
                onChange={(e) => setAudioFile(e.target.value)}
                placeholder="/path/to/audio.wav"
                required
              />
            </label>
          </>
        )}

        {activeTab !== 'intercom' && (
          <div className="form-row">
            <label>
              Priority
              <input
                type="number"
                value={priority}
                onChange={(e) => setPriority(e.target.value)}
                min={0}
                max={100}
              />
            </label>
          </div>
        )}

        <div className="command-actions">
          <button type="submit" className="btn" disabled={loading}>
            {loading ? 'Sending...' : activeTab === 'intercom' ? 'Start Intercom' : `Run ${activeMeta?.label ?? activeTab}`}
          </button>
          {activeTab === 'intercom' ? (
            <button type="button" className="btn btn-secondary" onClick={handleIntercomStop} disabled={loading}>
              Stop Intercom
            </button>
          ) : (
            <button type="button" className="btn btn-secondary" onClick={handleReload} disabled={loading}>
              Reload Speakers
            </button>
          )}
        </div>
      </form>

      {lastResult && <ResultView tab={activeTab} result={lastResult} />}
    </div>
  )
}

// ResultView renders each tab's differently-shaped response.
function ResultView({ tab, result }) {
  const isError = result.status === 'error' || result.event === 'error'
  const rows = []
  let audioSrc = null
  let downloadName = null

  if (isError) {
    rows.push(['Error', result.error_message || result.message || 'unknown error'])
  } else if (tab === 'tts') {
    rows.push(['Status', result.status])
    if (result.file) rows.push(['File', result.file])
    if (result.duration_seconds != null) rows.push(['Duration', `${result.duration_seconds.toFixed(2)}s`])
    if (result.sample_rate) rows.push(['Sample rate', `${result.sample_rate} Hz`])
    if (result.audio_wav_base64) {
      audioSrc = `data:audio/wav;base64,${result.audio_wav_base64}`
      downloadName = result.filename || 'tts.wav'
    }
  } else if (tab === 'ask') {
    rows.push(['Status', result.status])
    rows.push(['Heard', result.transcription || '(nothing)'])
    if (result.speaker) rows.push(['Speaker', result.speaker])
    if (result.error_message) rows.push(['Message', result.error_message])
  } else {
    // say / asr / speaker_id (async accepted ack; full result appears in Event Log)
    rows.push(['Status', result.status || result.event])
    if (result.message) rows.push(['Message', result.message])
  }

  return (
    <div className="card result-view" style={{ background: 'var(--accent-bg)', marginTop: 8 }}>
      <strong>Result</strong>
      <dl className="result-list">
        {rows.map(([k, v]) => (
          <div key={k} className="result-row">
            <dt>{k}</dt>
            <dd className={isError && k === 'Error' ? 'result-error' : ''}>{String(v)}</dd>
          </div>
        ))}
      </dl>
      {audioSrc && (
        <div className="result-audio">
          <audio controls src={audioSrc} />
          <a className="btn btn-secondary" href={audioSrc} download={downloadName}>
            Download {downloadName}
          </a>
        </div>
      )}
    </div>
  )
}

export default CommandCenter
