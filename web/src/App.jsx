import { useCallback, useEffect, useRef, useState } from 'react'
import './App.css'
import StatusHeader from './components/StatusHeader'
import NodeGrid from './components/NodeGrid'
import EventLog from './components/EventLog'
import CommandCenter from './components/CommandCenter'

const WS_URL = `${window.location.protocol === 'https:' ? 'wss' : 'ws'}://${window.location.host}/api/events`
const API_URL = `${window.location.protocol}//${window.location.host}/api`

// Extract token from query parameters if present
const urlParams = new URLSearchParams(window.location.search)
const token = urlParams.get('token') || ''

function App() {
  const [connected, setConnected] = useState(false)
  const [status, setStatus] = useState(null)
  const [nodes, setNodes] = useState([])
  const [events, setEvents] = useState([])
  const wsRef = useRef(null)

  const getHeaders = () => {
    const headers = { 'Content-Type': 'application/json' }
    if (token) {
      headers['X-Auth-Token'] = token
    }
    return headers
  }

  const fetchStatus = useCallback(async () => {
    try {
      const res = await fetch(`${API_URL}/status`)
      const data = await res.json()
      setStatus(data)
      if (data.nodes) setNodes(data.nodes)
    } catch (err) {
      console.error('Failed to fetch status:', err)
    }
  }, [])

  const addEvent = useCallback((event) => {
    setEvents((prev) => {
      const next = [{ ...event, time: new Date().toLocaleTimeString() }, ...prev]
      return next.slice(0, 200)
    })
  }, [])

  const connectWebSocket = useCallback(() => {
    const wsUrl = token ? `${WS_URL}?token=${encodeURIComponent(token)}` : WS_URL
    const ws = new WebSocket(wsUrl)
    wsRef.current = ws

    ws.onopen = () => setConnected(true)
    ws.onclose = () => {
      setConnected(false)
      setTimeout(connectWebSocket, 3000)
    }
    ws.onerror = (err) => console.error('WebSocket error:', err)
    ws.onmessage = (msg) => {
      try {
        const event = JSON.parse(msg.data)
        addEvent(event)
        if (event.event === 'status' && event.nodes) {
          setNodes(event.nodes)
        }
        if (event.event === 'state_changed') {
          setNodes((prev) =>
            prev.map((n) =>
              n.node_id === event.node_id ? { ...n, state: event.state } : n
            )
          )
        }
      } catch (err) {
        console.error('Invalid WS message:', err)
      }
    }
  }, [addEvent])

  useEffect(() => {
    fetchStatus()
    connectWebSocket()
    const interval = setInterval(fetchStatus, 5000)
    return () => {
      clearInterval(interval)
      if (wsRef.current) wsRef.current.close()
    }
  }, [fetchStatus, connectWebSocket])

  const execute = async (action, payload) => {
    const res = await fetch(`${API_URL}/execute`, {
      method: 'POST',
      headers: getHeaders(),
      body: JSON.stringify({ action, ...payload }),
    })
    return res.json()
  }

  const synthesize = async (payload) => {
    const res = await fetch(`${API_URL}/synthesize`, {
      method: 'POST',
      headers: getHeaders(),
      body: JSON.stringify(payload),
    })
    return res.json()
  }

  const ask = async (payload) => {
    const res = await fetch(`${API_URL}/ask`, {
      method: 'POST',
      headers: getHeaders(),
      body: JSON.stringify(payload),
    })
    return res.json()
  }

  const reloadSpeakers = async () => {
    const res = await fetch(`${API_URL}/reload-speakers`, {
      method: 'POST',
      headers: getHeaders(),
    })
    return res.json()
  }

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h1>Gocalis Command Center</h1>
        <div className={`connection-badge ${connected ? 'connected' : 'disconnected'}`}>
          {connected ? '● Live' : '○ Offline'}
        </div>
      </header>

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
