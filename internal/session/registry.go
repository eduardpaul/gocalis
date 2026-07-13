package session

import "sync"

// Registry tracks the active sessions on each node and fans captured audio out
// to all of them. It is the single point through which the audio ingestion path
// delivers speech segments, decoupling capture from any individual turn.
type Registry struct {
	mu     sync.RWMutex
	byNode map[string]map[string]*Session // nodeID -> sessionID -> session
}

// NewRegistry creates an empty Registry.
func NewRegistry() *Registry {
	return &Registry{byNode: make(map[string]map[string]*Session)}
}

// Add registers an active session.
func (r *Registry) Add(s *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.byNode[s.NodeID]
	if m == nil {
		m = make(map[string]*Session)
		r.byNode[s.NodeID] = m
	}
	m[s.ID] = s
}

// Remove deregisters a session.
func (r *Registry) Remove(s *Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if m := r.byNode[s.NodeID]; m != nil {
		delete(m, s.ID)
		if len(m) == 0 {
			delete(r.byNode, s.NodeID)
		}
	}
}

// Feed fans a detected-speech segment out to every active session on the node.
func (r *Registry) Feed(nodeID string, samples []float32) {
	r.mu.RLock()
	m := r.byNode[nodeID]
	sessions := make([]*Session, 0, len(m))
	for _, s := range m {
		sessions = append(sessions, s)
	}
	r.mu.RUnlock()

	for _, s := range sessions {
		s.Feed(samples)
	}
}

// ActiveCount returns the number of active sessions on a node.
func (r *Registry) ActiveCount(nodeID string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.byNode[nodeID])
}
