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

	"github.com/gorilla/websocket"
)

// Server orchestrates the Node-RED WebSocket API server.
type Server struct {
	addr         string
	clients      map[*websocket.Conn]*sync.Mutex
	clientsMutex sync.Mutex
	upgrader     websocket.Upgrader
	executor     *protocol.Executor
	authToken    string

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
		clients:   make(map[*websocket.Conn]*sync.Mutex),
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
	s.httpMutex.Lock()
	srv := s.httpSrv
	s.httpMutex.Unlock()
	if srv == nil {
		return nil
	}
	return srv.Shutdown(ctx)
}

// Publish implements protocol.EventPublisher by sending a JSON event payload to all connected clients.
func (s *Server) Publish(event protocol.Response) {
	s.clientsMutex.Lock()
	clients := make(map[*websocket.Conn]*sync.Mutex, len(s.clients))
	for c, m := range s.clients {
		clients[c] = m
	}
	s.clientsMutex.Unlock()

	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("[Server] Failed to marshal broadcast event: %v\n", err)
		return
	}

	for client, writeMutex := range clients {
		writeMutex.Lock()
		err := client.WriteMessage(websocket.TextMessage, data)
		writeMutex.Unlock()
		if err != nil {
			log.Printf("[Server] Failed to write to client, closing: %v\n", err)
			client.Close()
			s.clientsMutex.Lock()
			delete(s.clients, client)
			s.clientsMutex.Unlock()
		}
	}
}

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

	writeMutex := &sync.Mutex{}
	s.clientsMutex.Lock()
	s.clients[conn] = writeMutex
	s.clientsMutex.Unlock()

	log.Printf("[Server] Node-RED client connected: %s\n", conn.RemoteAddr().String())

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Server] Panic recovered in WebSocket connection loop: %v\n", r)
		}
		s.clientsMutex.Lock()
		delete(s.clients, conn)
		s.clientsMutex.Unlock()
		conn.Close()
		log.Printf("[Server] Node-RED client disconnected: %s\n", conn.RemoteAddr().String())
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			break
		}

		var req protocol.Request
		if err := json.Unmarshal(message, &req); err != nil {
			s.sendError(conn, "", "invalid JSON payload")
			continue
		}

		go s.processRequest(conn, req)
	}
}

func (s *Server) processRequest(conn *websocket.Conn, req protocol.Request) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[Server] Panic recovered in processRequest: %v\n", r)
			s.sendError(conn, req.NodeID, "internal server panic recovered")
		}
	}()

	log.Printf("[Server] Request received: action=%s node=%s\n", req.Action, req.NodeID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	s.executor.Execute(ctx, req)

	s.sendResponse(conn, protocol.Response{
		Event:  req.Action + "_accepted",
		NodeID: req.NodeID,
		Status: "accepted",
	})
}

func (s *Server) sendResponse(conn *websocket.Conn, resp protocol.Response) {
	data, err := json.Marshal(resp)
	if err != nil {
		return
	}

	s.clientsMutex.Lock()
	writeMutex, ok := s.clients[conn]
	s.clientsMutex.Unlock()
	if !ok {
		return
	}

	writeMutex.Lock()
	_ = conn.WriteMessage(websocket.TextMessage, data)
	writeMutex.Unlock()
}

func (s *Server) sendError(conn *websocket.Conn, nodeID string, errMsg string) {
	s.sendResponse(conn, protocol.Response{
		Event:   "error",
		NodeID:  nodeID,
		Status:  "error",
		Message: errMsg,
	})
}
