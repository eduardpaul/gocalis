package brain

import (
	"container/heap"
	"context"
	"errors"
	"math"
	"sync"

	"gocalis/internal/audionode"
)

// ErrIntercomActive is returned to a turn request (speak, play, ask, wake reply)
// for a node that is in an intercom call. Such requests are rejected instead of
// queued: a human is talking through the node, and a reply that only plays once
// the call is over would be stale.
var ErrIntercomActive = errors.New("node is in an intercom call")

// ErrPreempted is the cancellation cause of a turn cut off by an intercom call:
// the human voice takes the node's speaker over TTS and assistant turns.
var ErrPreempted = errors.New("interrupted by an intercom call")

// exclusivePriority orders an exclusive (intercom) waiter ahead of every
// ordinary one.
const exclusivePriority = math.MaxInt

// nodeQueue serializes access to a single node's audio output so that at most
// one "turn" owns the device at a time. A turn is either a speak utterance
// (synthesize + play) or a full ask flow (prompt -> listen -> ASR). Everything
// that wants to drive a node's audio acquires the queue first, runs its turn,
// then releases it.
//
// Waiters are admitted highest-Priority-first; ties break FIFO by arrival
// order. This is what lets a wake-triggered ask (high priority) jump ahead of a
// queued broadcast (default priority) while a plain speak simply waits its turn
// instead of being dropped or cutting in.
//
// An exclusive acquire (an intercom call) is different: it cancels the current
// holder's turn context with ErrPreempted, and from the moment it is requested
// until it is released every ordinary turn request, queued or new, fails with
// ErrIntercomActive.
type nodeQueue struct {
	mu        sync.Mutex
	cond      *sync.Cond
	pq        waiterPQ
	seq       uint64
	busy      bool
	closed    bool
	exclusive bool                    // an exclusive turn holds or is taking over the node
	preempt   context.CancelCauseFunc // cancels the current holder's turn context
}

func newNodeQueue() *nodeQueue {
	q := &nodeQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// acquire blocks until the caller owns the node or ctx is cancelled. On success
// it returns the turn's context (ctx, cancelled with cause ErrPreempted if an
// exclusive turn takes the node over) and a release func that MUST be called
// exactly once to hand the node to the next waiter; release also cancels the
// turn context. On failure it returns a nil context, a nil release and the
// error (ctx.Err(), ErrIntercomActive, or a closed/full queue error).
func (q *nodeQueue) acquire(ctx context.Context, priority int) (context.Context, func(), error) {
	return q.acquireMode(ctx, priority, false)
}

// acquireExclusive takes the node over for an exclusive turn: the current
// holder (if any) is preempted, ordinary waiters are rejected, and the call
// blocks only until the preempted holder has released the node.
func (q *nodeQueue) acquireExclusive(ctx context.Context) (context.Context, func(), error) {
	return q.acquireMode(ctx, exclusivePriority, true)
}

func (q *nodeQueue) acquireMode(ctx context.Context, priority int, exclusive bool) (context.Context, func(), error) {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil, nil, errors.New("node is closed")
	}
	if q.exclusive {
		q.mu.Unlock()
		return nil, nil, ErrIntercomActive
	}
	if !exclusive && len(q.pq) >= 32 {
		q.mu.Unlock()
		return nil, nil, errors.New("node turn queue is full")
	}
	if exclusive {
		q.exclusive = true
		if q.busy && q.preempt != nil {
			q.preempt(ErrPreempted)
		}
		// Ordinary waiters must notice the takeover and leave.
		q.cond.Broadcast()
	}

	w := &waiter{priority: priority, seq: q.seq}
	q.seq++
	heap.Push(&q.pq, w)

	// Wake parked waiters without a watcher goroutine per queued turn.
	stop := context.AfterFunc(ctx, func() {
		q.mu.Lock()
		q.cond.Broadcast()
		q.mu.Unlock()
	})
	defer stop()

	for {
		err := ctx.Err()
		if err == nil && q.closed {
			err = errors.New("node is closed")
		}
		if err == nil && !exclusive && q.exclusive {
			err = ErrIntercomActive
		}
		if err != nil {
			if w.index >= 0 {
				heap.Remove(&q.pq, w.index)
			}
			if exclusive {
				q.exclusive = false // the takeover was abandoned
			}
			q.cond.Broadcast()
			q.mu.Unlock()
			return nil, nil, err
		}
		if !q.busy && q.pq.Len() > 0 && q.pq[0] == w {
			heap.Pop(&q.pq)
			q.busy = true
			turnCtx, cancel := context.WithCancelCause(ctx)
			q.preempt = cancel
			q.mu.Unlock()

			var once sync.Once
			return turnCtx, func() {
				once.Do(func() {
					q.mu.Lock()
					q.busy = false
					q.preempt = nil
					if exclusive {
						q.exclusive = false
					}
					q.cond.Broadcast()
					q.mu.Unlock()
					cancel(nil)
				})
			}, nil
		}
		q.cond.Wait()
	}
}

// turnErr reports the error of a turn that was cut short as what cut it: an
// intercom call taking the node over (ErrPreempted) or the far side of a node
// hanging up (audionode.ErrHungUp), so callers see why instead of a bare
// "context canceled".
func turnErr(turnCtx context.Context, err error) error {
	if err == nil || turnCtx == nil {
		return err
	}
	if cause := context.Cause(turnCtx); errors.Is(cause, ErrPreempted) || errors.Is(cause, audionode.ErrHungUp) {
		return cause
	}
	return err
}

type waiter struct {
	priority int
	seq      uint64
	index    int
}

// waiterPQ is a max-priority heap (highest priority first, FIFO on ties).
type waiterPQ []*waiter

func (pq waiterPQ) Len() int { return len(pq) }

func (pq waiterPQ) Less(i, j int) bool {
	if pq[i].priority != pq[j].priority {
		return pq[i].priority > pq[j].priority
	}
	return pq[i].seq < pq[j].seq
}

func (pq waiterPQ) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *waiterPQ) Push(x any) {
	w := x.(*waiter)
	w.index = len(*pq)
	*pq = append(*pq, w)
}

func (pq *waiterPQ) Pop() any {
	old := *pq
	n := len(old)
	w := old[n-1]
	old[n-1] = nil
	w.index = -1
	*pq = old[:n-1]
	return w
}

func (q *nodeQueue) close() {
	q.mu.Lock()
	q.closed = true
	q.cond.Broadcast()
	q.mu.Unlock()
}
