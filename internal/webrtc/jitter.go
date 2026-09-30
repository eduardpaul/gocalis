package webrtc

import (
	"github.com/pion/rtp"
	"time"
)

// A short bounded window only waits when a sequence gap is observed. Contiguous
// traffic passes through immediately. Serial arithmetic handles RTP wraparound.
type packetWindow struct {
	initialized               bool
	next                      uint16
	pending                   map[uint16]*rtp.Packet
	waiting                   time.Time
	lost, reordered, rejected uint64
}

func (w *packetWindow) push(p *rtp.Packet, now time.Time) []*rtp.Packet {
	if !w.initialized {
		w.initialized = true
		w.next = p.SequenceNumber
		w.pending = make(map[uint16]*rtp.Packet)
	}
	delta := int16(p.SequenceNumber - w.next)
	if delta < 0 || w.pending[p.SequenceNumber] != nil {
		w.rejected++
		return nil
	}
	if delta > 0 {
		w.reordered++
	}
	w.pending[p.SequenceNumber] = p
	return w.drain(now, len(w.pending) >= 4)
}
func (w *packetWindow) drain(now time.Time, force bool) []*rtp.Packet {
	var out []*rtp.Packet
	for len(w.pending) > 0 {
		if p := w.pending[w.next]; p != nil {
			out = append(out, p)
			delete(w.pending, w.next)
			w.next++
			w.waiting = time.Time{}
			continue
		}
		if w.waiting.IsZero() {
			w.waiting = now
		}
		if !force && now.Sub(w.waiting) < 40*time.Millisecond {
			break
		}
		distance := uint16(65535)
		for sequence := range w.pending {
			if d := sequence - w.next; d < distance {
				distance = d
			}
		}
		w.lost += uint64(distance)
		w.next += distance
		w.waiting = time.Time{}
		force = false
	}
	return out
}
