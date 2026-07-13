function EventLog({ events }) {
  return (
    <div className="card">
      <h2>Event Log</h2>
      <div className="event-log">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Event</th>
              <th>Node</th>
              <th>Details</th>
            </tr>
          </thead>
          <tbody>
            {events.length === 0 && (
              <tr>
                <td colSpan={4}>Waiting for events...</td>
              </tr>
            )}
            {events.map((event, index) => (
              <tr key={index}>
                <td>{event.time}</td>
                <td className="event-cell">{event.event}</td>
                <td>{event.node_id || '—'}</td>
                <td>
                  {event.text && <div>Text: {event.text}</div>}
                  {event.speaker && <div>Speaker: {event.speaker}</div>}
                  {event.keyword && <div>Keyword: {event.keyword}</div>}
                  {event.state && <div>State: {event.state}</div>}
                  {event.status && <div>Status: {event.status}</div>}
                  {event.message && <div>Message: {event.message}</div>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default EventLog
