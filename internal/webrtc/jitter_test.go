package webrtc

import (
	"github.com/pion/rtp"
	"testing"
	"time"
)

func TestPacketWindowReorderLossAndWrap(t *testing.T) {
	var w packetWindow
	now := time.Unix(1, 0)
	packet := func(seq uint16) *rtp.Packet { return &rtp.Packet{Header: rtp.Header{SequenceNumber: seq}} }
	if len(w.push(packet(65534), now)) != 1 {
		t.Fatal("initial packet delayed")
	}
	if len(w.push(packet(0), now)) != 0 {
		t.Fatal("gap did not wait")
	}
	batch := w.push(packet(65535), now)
	if len(batch) != 2 || batch[1].SequenceNumber != 0 {
		t.Fatal("wraparound reordering failed")
	}
	if len(w.push(packet(0), now)) != 0 || w.rejected != 1 {
		t.Fatal("duplicate accepted")
	}
	w.push(packet(2), now)
	if len(w.drain(now.Add(39*time.Millisecond), false)) != 0 {
		t.Fatal("gap expired early")
	}
	if len(w.drain(now.Add(40*time.Millisecond), false)) != 1 || w.lost != 1 {
		t.Fatal("loss not recovered")
	}
}
func TestPacketWindowIsBounded(t *testing.T) {
	var w packetWindow
	now := time.Unix(1, 0)
	for i := 0; i < 100; i++ {
		w.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(i * 2)}}, now)
		if len(w.pending) > 3 {
			t.Fatal("unbounded jitter window")
		}
	}
}
