package ai

import (
	"context"
	"errors"
	"testing"
	"time"

	"gocalis/internal/workqueue"
)

// Exercise worker ownership without loading native model files: one active job
// holds the worker's completion signal while Close cancels a waiting submitter.
func TestASRCloseCancelsSubmittersAndJoinsWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	engine := &whisperTranscriber{ctx: ctx, cancel: cancel, done: make(chan struct{}), pq: workqueue.New[*asrJob](2)}
	submitted := make(chan error, 1)
	go func() {
		_, err := engine.TranscribeSamples(context.Background(), []float32{0.1}, 16000, JobOptions{})
		submitted <- err
	}()
	job, ok := engine.pq.Pop()
	if !ok || len(job.samples) != 1 {
		t.Fatal("job was not admitted")
	}
	closed := make(chan struct{})
	go func() { engine.Close(); close(closed) }()
	select {
	case err := <-submitted:
		if !errors.Is(err, workqueue.ErrClosed) {
			t.Fatalf("submission = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("submitter hung during Close")
	}
	select {
	case <-closed:
		t.Fatal("model closed before worker finished")
	default:
	}
	close(engine.done)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not join worker")
	}
	engine.Close()
	if _, err := engine.TranscribeSamples(context.Background(), nil, 16000, JobOptions{}); !errors.Is(err, workqueue.ErrClosed) {
		t.Fatalf("post-close submit = %v", err)
	}
}

func TestTTSCloseCancelsSubmittersAndJoinsWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	engine := &vitsSynthesizer{ctx: ctx, cancel: cancel, done: make(chan struct{}), pq: workqueue.New[*ttsJob](2)}
	submitted := make(chan error, 1)
	go func() {
		_, err := engine.SynthesizeToStream(context.Background(), "hello", JobOptions{})
		submitted <- err
	}()
	if _, ok := engine.pq.Pop(); !ok {
		t.Fatal("job was not admitted")
	}
	closed := make(chan struct{})
	go func() { engine.Close(); close(closed) }()
	select {
	case err := <-submitted:
		if !errors.Is(err, workqueue.ErrClosed) {
			t.Fatalf("submission = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("submitter hung during Close")
	}
	select {
	case <-closed:
		t.Fatal("model closed before worker finished")
	default:
	}
	close(engine.done)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close did not join worker")
	}
	engine.Close()
}
