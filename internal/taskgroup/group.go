// Package taskgroup owns bounded work and joins it before resources are freed.
package taskgroup

import (
	"context"
	"errors"
	"sync"
)

var ErrBusy = errors.New("service is busy; retry later")
var ErrClosed = errors.New("service is shutting down")

type Group struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	active int
	limit  int
	wg     sync.WaitGroup
}

func New(ctx context.Context, limit int) *Group {
	ctx, cancel := context.WithCancel(ctx)
	return &Group{ctx: ctx, cancel: cancel, limit: limit}
}

// Start reserves a slot and links the caller's context to the group lifetime.
// finish must be called exactly once after all work using the slot ends.
func (g *Group) Start(parent context.Context) (context.Context, func(), error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed || g.ctx.Err() != nil {
		return nil, nil, ErrClosed
	}
	if err := parent.Err(); err != nil {
		return nil, nil, err
	}
	if g.active >= g.limit {
		return nil, nil, ErrBusy
	}
	g.active++
	g.wg.Add(1)
	ctx, cancel := context.WithCancel(parent)
	stop := context.AfterFunc(g.ctx, cancel)
	var once sync.Once
	return ctx, func() {
		once.Do(func() {
			stop()
			cancel()
			g.mu.Lock()
			g.active--
			g.mu.Unlock()
			g.wg.Done()
		})
	}, nil
}

func (g *Group) Go(run func(context.Context)) error {
	ctx, finish, err := g.Start(g.ctx)
	if err != nil {
		return err
	}
	go func() { defer finish(); run(ctx) }()
	return nil
}

// Close cancels every task and waits for it. No resource may be freed until it returns.
func (g *Group) Close() {
	g.mu.Lock()
	g.closed = true
	g.cancel()
	g.mu.Unlock()
	g.wg.Wait()
}
