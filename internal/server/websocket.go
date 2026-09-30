package server

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
	"time"

	"gocalis/internal/httpsec"
	"gocalis/internal/protocol"
	"gocalis/internal/wshub"

	"github.com/gorilla/websocket"
)

// Server orchestrates the Node-RED WebSocket API server.
type Server struct {
	addr      string
	hub       *wshub.Hub
	upgrader  websocket.Upgrader
	executor  *protocol.Executor
	authToken string

	httpMutex sync.Mutex
	httpSrv   *http.Server
}

// NewServer creates a new Node-RED WebSocket proxy server. authToken, when
// non-empty, is required to open a connection; allowedOrigins restricts which
// browser Origins may connect (empty => localhost/same-origin only).
func NewServer(addr string, executor *protocol.Executor, authToken string, allowedOrigins []string) *Server {
	return &Server{
		addr:      addr,
		executor:  executor,
		authToken: authToken,
		hub:       wshub.New(),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin:     httpsec.OriginChecker(allowedOrigins),
		},
	}
}

// Start launches the WebSocket server on the configured address.
func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleConnection)
	log.Printf("[Server] WebSocket Server listening on %s/ws...\n", s.addr)

	srv := &http.Server{
		Addr:              s.addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
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

// Publish implements protocol.EventPublisher by sending a JSON event payload to all connected clients.
func (s *Server) Publish(event protocol.Response) { s.hub.Publish(event) }

func (s *Server) handleConnection(w http.ResponseWriter, r *http.Request) {
	if !httpsec.TokenValid(r, s.authToken) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("[Server] WebSocket Upgrade failed: %v\n", err)
		return
	}

	client := s.hub.Add(conn)
	if client == nil {
		return
	}
	defer client.Close()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var req protocol.Request
		if err := json.Unmarshal(message, &req); err != nil {
			client.Send(protocol.Response{Event: "error", Status: "error", Message: "invalid JSON payload"})
			continue
		}

		if err := s.executor.Submit(req); err != nil {
			client.Send(protocol.Response{Event: "error", NodeID: req.NodeID, Status: "error", Message: err.Error()})
			continue
		}
		client.Send(protocol.Response{Event: req.Action + "_accepted", NodeID: req.NodeID, Status: "accepted"})
	}
}
