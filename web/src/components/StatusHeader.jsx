function formatDuration(seconds) {
  if (!seconds || seconds < 0) return '—'
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  const s = Math.floor(seconds % 60)
  return `${h}h ${m}m ${s}s`
}

function StatusHeader({ status }) {
  return (
    <div className="card status-header">
      <div className="status-item">
        <span className="label">System</span>
        <span className="value">{status?.status ?? '—'}</span>
      </div>
      <div className="status-item">
        <span className="label">Uptime</span>
        <span className="value">{formatDuration(status?.uptime)}</span>
      </div>
      <div className="status-item">
        <span className="label">Active Nodes</span>
        <span className="value">{status?.node_count ?? 0}</span>
      </div>
      <div className="status-item">
        <span className="label">Mode</span>
        <span className="value">webrtc</span>
      </div>
    </div>
  )
}

export default StatusHeader
