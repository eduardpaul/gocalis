// Package webserver serves the React dashboard and exposes REST/WebSocket APIs
// for monitoring and controlling the Gocalis speech agent.
package webserver

import (
	"context"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"gocalis/internal/ai"
	"gocalis/internal/audio"
	"gocalis/internal/brain"
	"gocalis/internal/httpsec"
	"gocalis/internal/protocol"
	"gocalis/internal/taskgroup"
	"gocalis/internal/wshub"

	"github.com/gorilla/websocket"
)

//go:embed all:dist
var distFS embed.FS

// Server hosts the dashboard static files and API endpoints.
type Server struct {
	addr          string
	brain         *brain.Brain
	executor      *protocol.Executor
	speakerEngine ai.SpeakerIdentifier
	startTime     time.Time
	authToken     string

	hub      *wshub.Hub
	upgrader websocket.Upgrader

	httpMutex sync.Mutex
	httpSrv   *http.Server
}

// NewServer creates a dashboard web server. authToken, when non-empty, is
// required on controls and the events socket; allowedOrigins restricts which browser Origins
// may open the events WebSocket (empty => localhost/same-origin only).
func NewServer(addr string, b *brain.Brain, executor *protocol.Executor, speakerEngine ai.SpeakerIdentifier, authToken string, allowedOrigins []string) *Server {
	return &Server{
		addr:          addr,
		brain:         b,
		executor:      executor,
		speakerEngine: speakerEngine,
		startTime:     time.Now(),
		authToken:     authToken,
		hub:           wshub.New(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     httpsec.OriginChecker(allowedOrigins),
		},
	}
}

// Start launches the HTTP server. It blocks until the server exits.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// API routes. Read-only status endpoints stay open so the dashboard works
	// without embedding credentials; control endpoints require the auth token.
	mux.HandleFunc("/api/status", s.withCORS(s.handleStatus))
	mux.HandleFunc("/api/nodes", s.withCORS(s.handleNodes))
	mux.HandleFunc("/api/execute", s.withCORS(httpsec.RequireToken(s.authToken, s.handleExecute)))
	mux.HandleFunc("/api/synthesize", s.control(s.handleSynthesize))
	mux.HandleFunc("/api/ask", s.control(s.handleAsk))
	mux.HandleFunc("/ask", s.control(s.handleAsk))
	mux.HandleFunc("/api/reload-speakers", s.control(s.handleReloadSpeakers))
	mux.HandleFunc("/api/events", s.handleEvents)

	// Static files from embedded React build
	staticFS, err := fs.Sub(distFS, "dist")
	if err != nil {
		log.Printf("[WebServer] Failed to open embedded dist: %v\n", err)
	} else {
		fileServer := http.FileServer(http.FS(staticFS))
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			// If the file exists, serve it; otherwise fall back to index.html for SPA routing.
			path := strings.TrimPrefix(r.URL.Path, "/")
			if path == "" {
				path = "index.html"
			}
			if _, err := fs.Stat(staticFS, path); err == nil {
				fileServer.ServeHTTP(w, r)
				return
			}
			index, err := distFS.ReadFile("dist/index.html")
			if err != nil {
				http.Error(w, "index.html not found", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write(index)
		})
	}

	log.Printf("[WebServer] Dashboard listening on http://localhost%s\n", s.addr)
	srv := &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	s.httpMutex.Lock()
	s.httpSrv = srv
	s.httpMutex.Unlock()

	return srv.ListenAndServe()
}

// Shutdown gracefully stops the HTTP server, waiting for in-flight requests to
// drain until ctx is cancelled.
func (s *Server) Shutdown(ctx context.Context) error {
	s.hub.Close()
	s.httpMutex.Lock()
	srv := s.httpSrv
	s.httpMutex.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// Publish implements protocol.EventPublisher by broadcasting events to all dashboard clients.
func (s *Server) Publish(event protocol.Response) { s.hub.Publish(event) }

func (s *Server) withCORS(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, X-Auth-Token")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next(w, r)
	}
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	resp := map[string]any{
		"status":     "running",
		"uptime":     time.Since(s.startTime).Seconds(),
		"node_count": s.brain.NodeCount(),
		"nodes":      s.brain.ListNodes(),
	}
	s.writeJSON(w, resp)
}

func (s *Server) handleNodes(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.writeJSON(w, s.brain.ListNodes())
}

func (s *Server) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.Request
	if !httpsec.DecodeJSON(w, r, &req) {
		return
	}

	if err := s.executor.Submit(req); err != nil {
		code := http.StatusBadRequest
		if errors.Is(err, taskgroup.ErrBusy) || errors.Is(err, taskgroup.ErrClosed) {
			code = http.StatusServiceUnavailable
		}
		http.Error(w, err.Error(), code)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)

	s.writeJSON(w, protocol.Response{
		Event:  req.Action + "_accepted",
		NodeID: req.NodeID,
		Status: "accepted",
	})
}

// ttsOutputDir is where the /api/synthesize endpoint writes generated WAV files.
// It lives under the working directory so the files are also reachable by the
// ASR endpoint (which confines audio_file paths to the working directory).
const ttsOutputDir = "models/tts_cache"

// safeFilename keeps only characters that are safe in a filename, so a
// user-supplied name can never escape ttsOutputDir via "/" or "..".
var safeFilename = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

// SynthesizeRequest asks the TTS engine to render text to a WAV file on disk
// without playing it on any node.
type SynthesizeRequest struct {
	Text     string `json:"text"`
	Filename string `json:"filename"`
	Priority int    `json:"priority"`
}

// SynthesizeResponse reports where the rendered WAV file was written.
type SynthesizeResponse struct {
	Status          string  `json:"status"`
	File            string  `json:"file"`
	Filename        string  `json:"filename"`
	SampleRate      int     `json:"sample_rate"`
	Samples         int     `json:"samples"`
	DurationSeconds float64 `json:"duration_seconds"`
	AudioWavBase64  string  `json:"audio_wav_base64"`
	ErrorMessage    string  `json:"error_message,omitempty"`
}

func (s *Server) handleSynthesize(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SynthesizeRequest
	if !httpsec.DecodeJSON(w, r, &req) {
		return
	}

	if strings.TrimSpace(req.Text) == "" || len(req.Text) > ai.MaxTextBytes || len(req.Filename) > 200 {
		http.Error(w, "text must contain 1-4000 bytes and filename at most 200 bytes", http.StatusBadRequest)
		return
	}

	samples, sampleRate, err := s.brain.Synthesize(r.Context(), req.Text, req.Priority)
	if err != nil {
		s.writeJSON(w, SynthesizeResponse{Status: "error", ErrorMessage: err.Error()})
		return
	}

	// Build a safe filename confined to ttsOutputDir.
	name := safeFilename.ReplaceAllString(filepath.Base(strings.TrimSpace(req.Filename)), "_")
	name = strings.Trim(name, "._-")
	if name == "" {
		name = fmt.Sprintf("tts_%d", time.Now().UnixNano())
	}
	if !strings.HasSuffix(strings.ToLower(name), ".wav") {
		name += ".wav"
	}

	if err := os.MkdirAll(ttsOutputDir, 0o755); err != nil {
		s.writeJSON(w, SynthesizeResponse{Status: "error", ErrorMessage: "failed to create output dir: " + err.Error()})
		return
	}

	outPath := filepath.Join(ttsOutputDir, name)
	if err := audio.WriteWAVPCM16(outPath, samples, sampleRate); err != nil {
		s.writeJSON(w, SynthesizeResponse{Status: "error", ErrorMessage: "failed to write WAV: " + err.Error()})
		return
	}

	absPath, err := filepath.Abs(outPath)
	if err != nil {
		absPath = outPath
	}

	duration := 0.0
	if sampleRate > 0 {
		duration = float64(len(samples)) / float64(sampleRate)
	}

	log.Printf("[WebServer] Synthesized %d samples to %s\n", len(samples), absPath)
	s.writeJSON(w, SynthesizeResponse{
		Status:          "success",
		File:            absPath,
		Filename:        name,
		SampleRate:      sampleRate,
		Samples:         len(samples),
		DurationSeconds: duration,
		AudioWavBase64:  base64.StdEncoding.EncodeToString(audio.EncodeWAVPCM16(samples, sampleRate)),
	})
}

// AskResponse is the result returned to the Node-RED gocalis-ask node.
type AskResponse struct {
	ContextID      string `json:"context_id"`
	NodeID         string `json:"node_id"`
	Status         string `json:"status"`
	Transcription  string `json:"transcription"`
	Speaker        string `json:"speaker"`
	AudioWavBase64 string `json:"audio_wav_base64"`
	ErrorMessage   string `json:"error_message"`
}

func (s *Server) handleAsk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req protocol.Request
	if !httpsec.DecodeJSON(w, r, &req) {
		return
	}

	req.Action = "ask"
	if err := req.Validate(); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	result := s.executor.AskEngine.Run(r.Context(), req.AskConfig())
	var audioBase64 string
	if (req.OutputFormat == "audio" || req.OutputFormat == "both") && len(result.Audio) > 0 {
		audioBase64 = base64.StdEncoding.EncodeToString(audio.EncodeWAVFloat32(result.Audio, result.SampleRate))
	}

	s.writeJSON(w, AskResponse{
		ContextID:      req.ContextID,
		NodeID:         req.NodeID,
		Status:         result.Status,
		Transcription:  result.Transcription,
		Speaker:        result.Speaker,
		AudioWavBase64: audioBase64,
		ErrorMessage:   result.ErrorMessage,
	})
}

func (s *Server) handleReloadSpeakers(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := s.speakerEngine.ReloadSpeakers(); err != nil {
		s.writeJSON(w, protocol.Response{
			Event:   "reload_speakers_failed",
			Status:  "error",
			Message: err.Error(),
		})
		return
	}

	s.writeJSON(w, protocol.Response{
		Event:  "reload_speakers_completed",
		Status: "success",
	})
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if !httpsec.TokenValid(r, s.authToken) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[WebServer] WebSocket upgrade failed: %v\n", err)
		return
	}

	client := s.hub.Add(conn)
	if client == nil {
		return
	}
	defer client.Close()
	client.Send(protocol.Response{Event: "status", Status: "running"})

	// Keep connection open and read until client disconnects.
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			break
		}
	}
}

func (s *Server) writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("[WebServer] Failed to encode JSON response: %v\n", err)
	}
}

// control tracks synchronous API work alongside detached commands.
func (s *Server) control(next http.HandlerFunc) http.HandlerFunc {
	return s.withCORS(httpsec.RequireToken(s.authToken, func(w http.ResponseWriter, r *http.Request) {
		ctx, finish, err := s.executor.Tasks.Start(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		defer finish()
		ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
		defer cancel()
		next(w, r.WithContext(ctx))
	}))
}
