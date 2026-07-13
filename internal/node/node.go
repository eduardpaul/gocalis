package node

import (
	"sync"
)

// NodeState defines the valid states of a PhysicalNode.
type NodeState string

const (
	StateIdle        NodeState = "IDLE"
	StateSpeaking    NodeState = "SPEAKING"
	StateListening   NodeState = "LISTENING"
	StateProcessing  NodeState = "PROCESSING"
	StateChallenging NodeState = "CHALLENGING"
)

type stateTransition struct {
	oldState NodeState
	newState NodeState
}

// PhysicalNode represents a hardware audio channel (WebRTC stream, Local soundcard, etc.) with state tracking.
type PhysicalNode struct {
	NodeID          string
	Type            string // "local" or "rtc_stream"
	state           NodeState
	stateMutex      sync.RWMutex
	changeCallbacks []func(oldState, newState NodeState)
	callbacksMutex  sync.Mutex

	// dispatch queue: transitions are delivered to callbacks in FIFO order by a
	// single background goroutine so listeners never observe reordered events.
	queueMutex sync.Mutex
	queueCond  *sync.Cond
	queue      []stateTransition
}

// NewPhysicalNode creates a new PhysicalNode in the default IDLE state.
func NewPhysicalNode(nodeID string, nodeType string) *PhysicalNode {
	n := &PhysicalNode{
		NodeID: nodeID,
		Type:   nodeType,
		state:  StateIdle,
	}
	n.queueCond = sync.NewCond(&n.queueMutex)
	go n.dispatchLoop()
	return n
}

// GetState returns the current state of the node.
func (n *PhysicalNode) GetState() NodeState {
	n.stateMutex.RLock()
	defer n.stateMutex.RUnlock()
	return n.state
}

// SetState updates the node state and enqueues a transition for ordered, async
// delivery to all registered change handlers.
func (n *PhysicalNode) SetState(newState NodeState) {
	n.stateMutex.Lock()
	oldState := n.state
	if oldState == newState {
		n.stateMutex.Unlock()
		return
	}
	n.state = newState
	// Enqueue the transition while holding stateMutex so the queue order matches
	// the exact order of state changes.
	n.queueMutex.Lock()
	n.queue = append(n.queue, stateTransition{oldState: oldState, newState: newState})
	n.queueCond.Signal()
	n.queueMutex.Unlock()
	n.stateMutex.Unlock()
}

// dispatchLoop delivers queued transitions to callbacks sequentially, in order.
func (n *PhysicalNode) dispatchLoop() {
	for {
		n.queueMutex.Lock()
		for len(n.queue) == 0 {
			n.queueCond.Wait()
		}
		transition := n.queue[0]
		n.queue = n.queue[1:]
		n.queueMutex.Unlock()

		n.callbacksMutex.Lock()
		callbacks := make([]func(oldState, newState NodeState), len(n.changeCallbacks))
		copy(callbacks, n.changeCallbacks)
		n.callbacksMutex.Unlock()

		for _, callback := range callbacks {
			callback(transition.oldState, transition.newState)
		}
	}
}

// OnStateChanged registers a callback that will be triggered when the node changes its state.
func (n *PhysicalNode) OnStateChanged(callback func(oldState, newState NodeState)) {
	n.callbacksMutex.Lock()
	defer n.callbacksMutex.Unlock()
	n.changeCallbacks = append(n.changeCallbacks, callback)
}
