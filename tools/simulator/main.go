package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"syscall"
	"time"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "serve", "serve, generate, probe, or scenario")
	manifest := flag.String("manifest", "devices.yaml", "device manifest")
	out := flag.String("out", "runs", "artifact directory")
	base := flag.String("base-config", "", "base application config for generation")
	scenario := flag.String("scenario", "", "YAML scenario")
	endpoint := flag.String("url", "http://127.0.0.1:18080", "simulator control URL")
	flag.Parse()
	if *mode == "probe" {
		return codecProbe()
	}
	if *mode == "scenario" {
		return scenarioCLI(*endpoint, *scenario, *out)
	}
	m, e := loadManifest(*manifest)
	if e != nil {
		return e
	}
	if *mode == "generate" {
		return generate(m, *out, *base)
	}
	if *mode != "serve" {
		return fmt.Errorf("unknown mode %s", *mode)
	}
	if e = codecProbe(); e != nil {
		return e
	}
	engine := newEngine(m)
	if e = engine.start(m.RTSP); e != nil {
		return e
	}
	defer engine.close()
	server := newServer(engine, m, *out)
	srv := &http.Server{Addr: m.HTTP, Handler: server, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 310 * time.Second, IdleTimeout: 30 * time.Second}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.ListenAndServe() }()
	log.Printf("simulator UI http://%s; RTSP %s", m.HTTP, m.RTSP)
	select {
	case <-ctx.Done():
		server.cancel()
		server.closeSockets()
		engine.cancel()
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		e = srv.Shutdown(shutdown)
		<-done
		server.wg.Wait()
		return e
	case e = <-done:
		return e
	}
}
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
func saveArtifacts(dir string, e *Engine, report any) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	events := []Event{}
	statuses := []Status{}
	for _, id := range e.order {
		d := e.devices[id]
		if err := os.WriteFile(filepath.Join(dir, id+".wav"), wav(d.audio()), 0644); err != nil {
			return err
		}
		statuses = append(statuses, d.snapshot())
		d.mu.Lock()
		events = append(events, d.events...)
		d.mu.Unlock()
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Time.Before(events[j].Time) })
	for name, v := range map[string]any{"report.json": report, "events.json": events, "diagnostics.json": statuses} {
		b, err := json.MarshalIndent(v, "", "  ")
		if err != nil {
			return err
		}
		if err = os.WriteFile(filepath.Join(dir, name), b, 0644); err != nil {
			return err
		}
	}
	return nil
}
func readLimited(r *http.Request, max int64) ([]byte, error) {
	return io.ReadAll(http.MaxBytesReader(nil, r.Body, max))
}
