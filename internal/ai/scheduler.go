package ai

// JobOptions carries scheduling metadata for a submitted ASR/TTS job. It keeps
// the scheduling concern (priority ordering on the internal work queues) out of
// the core domain call signatures, which describe *what* to do, not *when* the
// scheduler should run it relative to other work.
type JobOptions struct {
	// Priority orders queued jobs; a higher value runs before a lower one.
	// The zero value is the normal/default priority.
	Priority int
}
