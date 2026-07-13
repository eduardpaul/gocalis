package brain

import (
	"container/heap"
	"context"
	"sync"
)

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
type nodeQueue struct {
	mu   sync.Mutex
	cond *sync.Cond
	pq   waiterPQ
	seq  uint64
	busy bool
}

func newNodeQueue() *nodeQueue {
	q := &nodeQueue{}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// acquire blocks until the caller owns the node or ctx is cancelled. On success
// it returns a release func that MUST be called exactly once to hand the node
// to the next waiter. On cancellation it returns ctx.Err() and a nil release.
func (q *nodeQueue) acquire(ctx context.Context, priority int) (func(), error) {
	q.mu.Lock()

	w := &waiter{priority: priority, seq: q.seq}
	q.seq++
	heap.Push(&q.pq, w)

	// cond.Wait cannot select on ctx, so a tiny watcher wakes the loop if ctx is
	// cancelled while this waiter is still parked. stop tears it down on return.
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			w.canceled = true
			q.cond.Broadcast()
			q.mu.Unlock()
		case <-stop:
		}
	}()

	for {
		if w.canceled {
			if w.index >= 0 {
				heap.Remove(&q.pq, w.index)
			}
			q.mu.Unlock()
			return nil, ctx.Err()
		}
		if !q.busy && q.pq.Len() > 0 && q.pq[0] == w {
			heap.Pop(&q.pq)
			q.busy = true
			q.mu.Unlock()

			var once sync.Once
			return func() {
				once.Do(func() {
					q.mu.Lock()
					q.busy = false
					q.cond.Broadcast()
					q.mu.Unlock()
				})
			}, nil
		}
		q.cond.Wait()
	}
}

// waiter is a single parked caller on a nodeQueue.
type waiter struct {
	priority int
	seq      uint64
	index    int
	canceled bool
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
