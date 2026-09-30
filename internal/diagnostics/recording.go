// Package diagnostics retains a bounded microphone window only when explicitly
// enabled. Each node overwrites two files, preventing recording accumulation.
package diagnostics

import (
	"crypto/sha256"
	"fmt"
	"gocalis/internal/audio"
	"os"
	"path/filepath"
	"sync"
)

type Recorder struct {
	mu          sync.Mutex
	dir, prefix string
	mic         []float32
}

func NewRecorder(dir, node string) *Recorder {
	if dir == "" {
		return nil
	}
	return &Recorder{dir: dir, prefix: fmt.Sprintf("%x", sha256.Sum256([]byte(node)))[:16]}
}
func (r *Recorder) Add(samples []float32) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	const limit = 16000 * 120
	if len(samples) > limit {
		samples = samples[len(samples)-limit:]
	}
	excess := len(r.mic) + len(samples) - limit
	if excess > 0 {
		copy(r.mic, r.mic[excess:])
		r.mic = r.mic[:len(r.mic)-excess]
	}
	r.mic = append(r.mic, samples...)
}

// Save writes both pre-gate microphone PCM and the exact ASR submission.
func (r *Recorder) Save(submitted []float32) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	mic := append([]float32(nil), r.mic...)
	r.mu.Unlock()
	if len(submitted) > 16000*120 {
		submitted = submitted[:16000*120]
	}
	if err := os.MkdirAll(r.dir, 0700); err != nil {
		return err
	}
	for suffix, samples := range map[string][]float32{"microphone": mic, "asr": submitted} {
		path := filepath.Join(r.dir, r.prefix+"-"+suffix+".wav")
		tmp, err := os.CreateTemp(r.dir, ".recording-*")
		if err != nil {
			return err
		}
		name := tmp.Name()
		_, err = tmp.Write(audio.EncodeWAVFloat32(samples, 16000))
		closeErr := tmp.Close()
		if err == nil {
			err = closeErr
		}
		if err == nil {
			err = os.Rename(name, path)
		}
		_ = os.Remove(name)
		if err != nil {
			return err
		}
	}
	return nil
}
