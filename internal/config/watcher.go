package config

import (
	"log"
	"time"

	"github.com/fsnotify/fsnotify"
)

// WatchSpeakers watches the config file and, on each change, invokes onReload so
// the caller can refresh speaker profiles. It only triggers speaker reloading:
// other settings (models, nodes, transports) are NOT hot-reapplied, because they
// require rebuilding engines and node runtimes. The file is re-parsed purely to
// validate it still loads before firing the callback.
func WatchSpeakers(configPath string, onReload func()) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Printf("[Watcher] Error creating file watcher: %v\n", err)
		return
	}

	go func() {
		defer watcher.Close()

		// fsnotify sometimes fires duplicate write events quickly, we debounce them
		var lastReload time.Time

		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				if event.Has(fsnotify.Write) {
					if time.Since(lastReload) < 500*time.Millisecond {
						continue
					}
					lastReload = time.Now()

					log.Printf("[Watcher] Config file modified: %s. Reloading speaker profiles...\n", event.Name)
					if _, err := LoadConfig(configPath); err != nil {
						log.Printf("[Watcher] Error loading new config, skipping reload: %v\n", err)
						continue
					}

					onReload()
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("[Watcher] File watcher error: %v\n", err)
			}
		}
	}()

	err = watcher.Add(configPath)
	if err != nil {
		log.Printf("[Watcher] Error adding file to watcher: %v\n", err)
	}
}
