package config

import (
	"context"
	"io/fs"
	"log"
	"path/filepath"
	"strings"
	"time"

	"github.com/fsnotify/fsnotify"
)

// WatchSpeakers watches profile WAVs, including atomic replacements, rather than
// suggesting that edits to application configuration are hot-applied. The returned
// stop function joins reload work before the speaker engine can be closed.
func WatchSpeakers(parent context.Context, directory string, onReload func()) (func(), error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	addDirectories := func(root string) error {
		return filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if entry.IsDir() {
				return watcher.Add(path)
			}
			return nil
		})
	}
	if err := addDirectories(directory); err != nil {
		watcher.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(parent)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer watcher.Close()
		timer := time.NewTimer(time.Hour)
		timer.Stop()
		defer timer.Stop()
		var tick <-chan time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Create) {
					_ = addDirectories(event.Name)
				}
				if strings.EqualFold(filepath.Ext(event.Name), ".wav") || event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
					timer.Reset(500 * time.Millisecond)
					tick = timer.C
				}
			case <-tick:
				tick = nil
				if ctx.Err() == nil {
					onReload()
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("[SpeakerWatcher] %v", err)
			}
		}
	}()
	return func() { cancel(); <-done }, nil
}
