// Package wshub owns WebSocket event delivery independently of publishers.
package wshub

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const MaxMessageBytes = 8 << 20
const outboundCapacity = 32
const writeTimeout = 5 * time.Second

type Client struct {
	conn *websocket.Conn
	out  chan []byte
	done chan struct{}
	once sync.Once
}

func (c *Client) Close() { c.once.Do(func() { close(c.done); _ = c.conn.Close() }) }

// Send is non-blocking. A slow consumer is disconnected instead of losing order.
func (c *Client) Send(value any) {
	data, err := json.Marshal(value)
	if err == nil {
		c.send(data)
	}
}
func (c *Client) send(data []byte) {
	select {
	case <-c.done:
		return
	default:
	}
	select {
	case c.out <- data:
	case <-c.done:
	default:
		c.Close()
	}
}

type Hub struct {
	mu      sync.Mutex
	clients map[*Client]struct{}
	closed  bool
	wg      sync.WaitGroup
}

func New() *Hub { return &Hub{clients: make(map[*Client]struct{})} }

func (h *Hub) Add(conn *websocket.Conn) *Client {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || len(h.clients) >= 64 {
		_ = conn.Close()
		return nil
	}
	conn.SetReadLimit(MaxMessageBytes)
	c := &Client{conn: conn, out: make(chan []byte, outboundCapacity), done: make(chan struct{})}
	h.clients[c] = struct{}{}
	h.wg.Add(1)
	go h.writeLoop(c)
	return c
}

func (h *Hub) writeLoop(c *Client) {
	defer h.wg.Done()
	defer func() {
		c.Close()
		h.mu.Lock()
		delete(h.clients, c)
		h.mu.Unlock()
	}()
	for {
		select {
		case <-c.done:
			return
		case data := <-c.out:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeTimeout))
			if err := c.conn.WriteMessage(websocket.TextMessage, data); err != nil {
				return
			}
		}
	}
}

func (h *Hub) Publish(value any) {
	data, err := json.Marshal(value)
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.clients {
		c.send(data)
	}
}

func (h *Hub) Close() {
	h.mu.Lock()
	h.closed = true
	for c := range h.clients {
		c.Close()
	}
	h.mu.Unlock()
	h.wg.Wait()
}
