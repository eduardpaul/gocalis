package webrtc

import "sync/atomic"

type transportCounters struct {
	packets, overflow, lost, reordered, rejected, decodeErrors, concealed atomic.Uint64
	maxArrivalGap, maxProcessing                                          atomic.Int64
}

func maxCounter(c *atomic.Int64, value int64) {
	for old := c.Load(); value > old; old = c.Load() {
		if c.CompareAndSwap(old, value) {
			return
		}
	}
}

// Diagnostics returns a fixed-size snapshot; no packet history or SDP is retained.
func (c *Client) Diagnostics() any {
	result := map[string]any{
		"packets": c.stats.packets.Load(), "queue_overflow": c.stats.overflow.Load(),
		"rtp_loss": c.stats.lost.Load(), "rtp_reordered": c.stats.reordered.Load(), "rtp_rejected": c.stats.rejected.Load(),
		"decode_errors": c.stats.decodeErrors.Load(), "concealed_samples": c.stats.concealed.Load(),
		"max_arrival_gap_ns": c.stats.maxArrivalGap.Load(), "max_receive_processing_ns": c.stats.maxProcessing.Load(),
	}
	c.tbMu.Lock()
	tb := c.tb
	c.tbMu.Unlock()
	if tb != nil {
		tb.mu.Lock()
		result["talkback_queue_frames"] = len(tb.queue)
		result["talkback_queue_high_water"] = tb.queueHighWater
		result["talkback_silence_ticks_during_playback"] = tb.underruns
		result["last_first_audio_delay_ns"] = tb.firstAudioDelay
		result["talkback_write_failures"] = tb.writeFailures
		result["talkback_route_failures"] = tb.routeFailures
		result["max_pacing_lateness_ns"] = tb.maxLateness
		tb.mu.Unlock()
	}
	return result
}
