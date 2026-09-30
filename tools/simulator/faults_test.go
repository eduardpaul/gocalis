package main

import (
	"github.com/pion/rtp"
	"testing"
)

func TestDeterministicFaults(t *testing.T) {
	f := faultInjector{spec: FaultSpec{DropEvery: 5, ReorderEvery: 3, BurstEvery: 10, BurstFrames: 3}}
	var seq []uint16
	for i := 1; i <= 100; i++ {
		for _, p := range f.push(&rtp.Packet{Header: rtp.Header{SequenceNumber: uint16(i)}}) {
			seq = append(seq, p.SequenceNumber)
		}
		if len(f.pending) > 50 {
			t.Fatal("unbounded faults")
		}
	}
	reordered := false
	for i, s := range seq {
		if s%5 == 0 {
			t.Fatal("loss injection failed")
		}
		if i > 0 && s < seq[i-1] {
			reordered = true
		}
	}
	if !reordered {
		t.Fatal("no reordered packets")
	}
}
