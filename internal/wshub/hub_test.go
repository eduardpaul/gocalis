package wshub

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestHubOrderedDeliveryAndJoinedShutdown(t *testing.T) {
	hub := New()
	joined := make(chan struct{})
	registered := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		client := hub.Add(conn)
		if client == nil {
			return
		}
		close(registered)
		defer close(joined)
		defer client.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	<-registered
	for i := 0; i < 10; i++ {
		hub.Publish(i)
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	for i := 0; i < 10; i++ {
		var got int
		if err := conn.ReadJSON(&got); err != nil {
			t.Fatal(err)
		}
		if got != i {
			t.Fatalf("message = %d, want %d", got, i)
		}
	}
	hub.Close()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("read handler was not unblocked")
	}
	hub.Close()
}

func TestSlowConsumerIsDisconnectedWithoutBlockingPublish(t *testing.T) {
	// A bounded client without a writer represents a consumer that has stopped
	// draining. No kernel socket buffering assumptions are needed for this test.
	c := &Client{out: make(chan []byte, 1), done: make(chan struct{})}
	// Close uses a real socket; arrange an in-process connection for that cleanup.
	ready := make(chan *websocket.Conn, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err == nil {
			ready <- conn
		}
	}))
	defer srv.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c.conn = <-ready
	hub := New()
	hub.clients[c] = struct{}{}
	defer hub.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { hub.Publish("one"); hub.Publish("two"); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("slow consumer blocked publication")
	}
	select {
	case <-c.done:
	default:
		t.Fatal("slow consumer remained registered")
	}
}
