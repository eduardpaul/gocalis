package intercom

import (
	"context"
	"io"
	"testing"
	"time"
)

func TestBridgePushRead(t *testing.T) {
	b := newBridgeBuffer(16000, 1000)
	b.push([]int16{1, 2, 3, 4, 5})

	got, err := b.ReadPCM16(context.Background(), 3)
	if err != nil {
		t.Fatalf("ReadPCM16 error: %v", err)
	}
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("unexpected first read: %v", got)
	}

	got, err = b.ReadPCM16(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadPCM16 error: %v", err)
	}
	if len(got) != 2 || got[0] != 4 || got[1] != 5 {
		t.Fatalf("unexpected second read: %v", got)
	}
}

func TestBridgeDropsOldestOnOverflow(t *testing.T) {
	b := newBridgeBuffer(16000, 4)
	b.push([]int16{1, 2, 3, 4, 5, 6}) // exceeds cap of 4 -> newest 4 kept

	got, err := b.ReadPCM16(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadPCM16 error: %v", err)
	}
	want := []int16{3, 4, 5, 6}
	if len(got) != len(want) {
		t.Fatalf("expected %v, got %v", want, got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("expected %v, got %v", want, got)
		}
	}
}

func TestBridgeEOFAfterClose(t *testing.T) {
	b := newBridgeBuffer(16000, 1000)
	b.push([]int16{7, 8})
	b.close()

	// Buffered audio is still drained before EOF.
	got, err := b.ReadPCM16(context.Background(), 10)
	if err != nil || len(got) != 2 {
		t.Fatalf("expected drained samples before EOF, got %v err %v", got, err)
	}
	if _, err := b.ReadPCM16(context.Background(), 10); err != io.EOF {
		t.Fatalf("expected io.EOF after drain, got %v", err)
	}
}

func TestBridgeReadBlocksUntilPush(t *testing.T) {
	b := newBridgeBuffer(16000, 1000)
	done := make(chan []int16, 1)
	go func() {
		got, _ := b.ReadPCM16(context.Background(), 10)
		done <- got
	}()

	select {
	case <-done:
		t.Fatal("ReadPCM16 returned before any data was available")
	case <-time.After(50 * time.Millisecond):
	}

	b.push([]int16{9})
	select {
	case got := <-done:
		if len(got) != 1 || got[0] != 9 {
			t.Fatalf("unexpected data after push: %v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("ReadPCM16 did not wake after push")
	}
}

// TestBridgeTakeFramePrimes checks the jitter cushion of a network talker: no
// audio is released until prime samples are buffered, and the cushion is rebuilt
// after an underrun.
func TestBridgeTakeFramePrimes(t *testing.T) {
	b := newBridgeBuffer(16000, 1000)
	b.prime = 6
	b.push([]int16{1, 2, 3, 4})
	if got := b.takeFrame(2); got[0] != 0 || got[1] != 0 {
		t.Fatalf("released audio before priming: %v", got)
	}
	b.push([]int16{5, 6})
	if got := b.takeFrame(4); got[0] != 1 || got[3] != 4 {
		t.Fatalf("primed frame = %v, want 1..4", got)
	}
	// Two samples left: an underrun frame plays them, then re-primes.
	if got := b.takeFrame(4); got[0] != 5 || got[1] != 6 || got[2] != 0 {
		t.Fatalf("underrun frame = %v", got)
	}
	b.push([]int16{7})
	if got := b.takeFrame(1); got[0] != 0 {
		t.Fatalf("released audio while re-priming: %v", got)
	}
}
