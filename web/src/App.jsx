import { useCallback, useEffect, useState } from 'react'
import './App.css'
import StatusHeader from './components/StatusHeader'
import NodeGrid from './components/NodeGrid'
import EventLog from './components/EventLog'
import CommandCenter from './components/CommandCenter'

const WS_URL = `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/api/events`
const API_URL = `${window.location.protocol}//${window.location.host}/api`

function App() {
  const [connected, setConnected] = useState(false)
  const [token, setToken] = useState(() => sessionStorage.getItem('gocalis-token') || '')
  const [tokenDraft, setTokenDraft] = useState(token)
  const [status, setStatus] = useState(null)
  const [nodes, setNodes] = useState([])
  const [events, setEvents] = useState([])

  const addEvent = useCallback((event) => {
    setEvents((prev) => {
      const { audio_wav_base64: _audio, recording: _recording, ...details } = event
      const next = [{ ...details, time: new Date().toLocaleTimeString() }, ...prev]
      return next.slice(0, 200)
    })
  }, [])

  useEffect(() => {
    let disposed = false
    let socket
    let reconnectTimer
    let fetching = false
    const controller = new AbortController()

    const refreshStatus = async () => {
      if (fetching) return
      fetching = true
      try {
        const res = await fetch(`${API_URL}/status`, { signal: controller.signal })
        if (!res.ok) throw new Error(`Status request failed: ${res.status}`)
        const data = await res.json()
        if (!disposed) {
          setStatus(data)
          if (data.nodes) setNodes(data.nodes)
        }
      } catch (err) {
        if (err.name !== 'AbortError') console.error('Failed to fetch status:', err)
      } finally {
        fetching = false
      }
    }

    const connect = () => {
      socket = new WebSocket(token ? `${WS_URL}?token=${encodeURIComponent(token)}` : WS_URL)
      socket.onopen = () => {
        setConnected(true)
        refreshStatus()
      }
      socket.onclose = () => {
        if (disposed) return
        setConnected(false)
        reconnectTimer = setTimeout(connect, 3000)
      }
      socket.onerror = (err) => console.error('WebSocket error:', err)
      socket.onmessage = (msg) => {
        try {
          const event = JSON.parse(msg.data)
          addEvent(event)
          if (event.event === 'state_changed') {
            setNodes((prev) => prev.map((n) =>
              n.node_id === event.node_id ? { ...n, state: event.state } : n
            ))
          }
        } catch (err) {
          console.error('Invalid WS message:', err)
        }
      }
    }

    refreshStatus()
    connect()
    const interval = setInterval(refreshStatus, 5000)
    return () => {
      disposed = true
      clearInterval(interval)
      clearTimeout(reconnectTimer)
      controller.abort()
      socket.onopen = null
      socket.onmessage = null
      socket.onerror = null
      socket.onclose = null
      socket.close()
    }
  }, [addEvent, token])

  const post = async (endpoint, payload) => {
    const res = await fetch(`${API_URL}/${endpoint}`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify(payload),
    })
    if (!res.ok) throw new Error(await res.text())
    return res.json()
  }

  const execute = (action, payload) => post('execute', { action, ...payload })
  const synthesize = (payload) => post('synthesize', payload)
  const ask = (payload) => post('ask', payload)
  const reloadSpeakers = () => post('reload-speakers')


  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>Gocalis Command Center</h1>
        <div className={`connection-badge ${connected ? 'connected' : 'disconnected'}`}>
          {connected ? '● Live' : '○ Offline'}
        </div>
      </header>

      <form className="auth-token" onSubmit={(event) => {
        event.preventDefault()
        const value = tokenDraft.trim()
        sessionStorage.setItem('gocalis-token', value)
        setToken(value)
      }}>
        <label htmlFor="access-token">Access token</label>
        <input
          id="access-token"
          type="password"
          value={tokenDraft}
          placeholder="Optional when authentication is disabled"
          autoComplete="off"
          onChange={(event) => setTokenDraft(event.target.value)}
        />
        <button type="submit">Apply</button>
      </form>

      <StatusHeader status={status} />

      <main className="dashboard-grid">
        <section className="dashboard-section">
          <NodeGrid nodes={nodes} />
        </section>

        <section className="dashboard-section">
          <CommandCenter nodes={nodes} onExecute={execute} onSynthesize={synthesize} onAsk={ask} onReloadSpeakers={reloadSpeakers} />
        </section>
      </main>

      <section className="dashboard-section full-width">
        <EventLog events={events} />
      </section>
    </div>
  )
}

export default App
