package main

import "github.com/pion/rtp"

// FaultSpec applies deterministic disturbances after encoding; RTP sequence and
// timestamps still advance through dropped/delayed packets.
type FaultSpec struct {
	DropEvery    int `yaml:"drop_every" json:"drop_every,omitempty"`
	ReorderEvery int `yaml:"reorder_every" json:"reorder_every,omitempty"`
	BurstEvery   int `yaml:"burst_every" json:"burst_every,omitempty"`
	BurstFrames  int `yaml:"burst_frames" json:"burst_frames,omitempty"`
}
type delayedPacket struct {
	due    int
	packet *rtp.Packet
}
type faultInjector struct {
	spec           FaultSpec
	tick, burstEnd int
	pending        []delayedPacket
}

func (f *faultInjector) push(packet *rtp.Packet) []*rtp.Packet {
	f.tick++
	var out []*rtp.Packet
	kept := f.pending[:0]
	for _, pending := range f.pending {
		if pending.due <= f.tick {
			out = append(out, pending.packet)
		} else {
			kept = append(kept, pending)
		}
	}
	f.pending = kept
	if f.spec.DropEvery > 0 && f.tick%f.spec.DropEvery == 0 {
		return out
	}
	due := f.tick
	if f.spec.BurstEvery > 0 && f.tick%f.spec.BurstEvery == 0 {
		f.burstEnd = f.tick + f.spec.BurstFrames
	}
	if f.tick < f.burstEnd {
		due = f.burstEnd
	}
	if f.spec.ReorderEvery > 0 && f.tick%f.spec.ReorderEvery == 0 {
		due = max(due, f.tick+2)
	}
	if due > f.tick {
		f.pending = append(f.pending, delayedPacket{due, packet})
	} else {
		out = append(out, packet)
	}
	return out
}
