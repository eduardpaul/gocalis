function NodeGrid({ nodes }) {
  if (!nodes || nodes.length === 0) {
    return (
      <div className="card">
        <h2>Nodes</h2>
        <p>No nodes registered yet.</p>
      </div>
    )
  }

  return (
    <div className="card">
      <h2>Nodes</h2>
      <div className="node-grid">
        {nodes.map((node) => (
          <div key={node.node_id} className="card node-card">
            <h3>{node.node_id}</h3>
            <span className={`badge ${node.state?.toLowerCase() || 'idle'}`}>
              {node.state || 'IDLE'}
            </span>
            <span className="node-meta">Type: {node.type || 'unknown'}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

export default NodeGrid
