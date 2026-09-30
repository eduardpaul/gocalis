// Package workqueue provides bounded, stable priority admission for model workers.
package workqueue

import (
	"context"
	"errors"
	"sync"
)

var ErrClosed = errors.New("queue is closed")
var ErrFull = errors.New("queue is full")

type entry[T any] struct {
	ctx      context.Context
	value    T
	priority int
}

type Queue[T any] struct {
	mu       sync.Mutex
	cond     *sync.Cond
	entries  []entry[T]
	capacity int
	closed   bool
}

func New[T any](capacity int) *Queue[T] {
	q := &Queue[T]{capacity: capacity}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (q *Queue[T]) Push(ctx context.Context, value T, priority int) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if q.closed {
		return ErrClosed
	}
	// Cancelled submissions must not prevent fresh work from being admitted.
	live := q.entries[:0]
	for _, e := range q.entries {
		if e.ctx.Err() == nil {
			live = append(live, e)
		}
	}
	clear(q.entries[len(live):])
	q.entries = live
	if len(q.entries) >= q.capacity {
		return ErrFull
	}
	q.entries = append(q.entries, entry[T]{ctx: ctx, value: value, priority: priority})
	q.cond.Signal()
	return nil
}

func (q *Queue[T]) Pop() (T, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.entries) == 0 && !q.closed {
		q.cond.Wait()
	}
	if q.closed {
		var zero T
		return zero, false
	}
	best := 0
	for i := 1; i < len(q.entries); i++ {
		if q.entries[i].priority > q.entries[best].priority {
			best = i
		}
	}
	value := q.entries[best].value
	copy(q.entries[best:], q.entries[best+1:])
	q.entries[len(q.entries)-1] = entry[T]{}
	q.entries = q.entries[:len(q.entries)-1]
	return value, true
}

// Close stops admission and releases pending entries. Callers waiting for results
// must also select on the owning worker's lifetime context.
func (q *Queue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	q.entries = nil
	q.cond.Broadcast()
}
