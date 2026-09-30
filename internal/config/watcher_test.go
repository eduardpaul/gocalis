package config

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSpeakerWatcherAtomicReplacementAndShutdown(t *testing.T) {
	dir := t.TempDir()
	reloaded := make(chan struct{}, 2)
	stop, err := WatchSpeakers(context.Background(), dir, func() { reloaded <- struct{}{} })
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	temp := filepath.Join(dir, "profile.tmp")
	if err := os.WriteFile(temp, []byte("test WAV contents"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(temp, filepath.Join(dir, "profile.wav")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reloaded:
	case <-time.After(3 * time.Second):
		t.Fatal("atomic replacement did not reload")
	}
	stop()
	if err := os.WriteFile(filepath.Join(dir, "other.wav"), []byte("ignored"), 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case <-reloaded:
		t.Fatal("reload after watcher shutdown")
	case <-time.After(600 * time.Millisecond):
	}
}
