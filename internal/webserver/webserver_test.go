package webserver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"gocalis/internal/brain"
	"gocalis/internal/protocol"
	"gocalis/internal/taskgroup"
)

func TestEventsRequireConfiguredToken(t *testing.T) {
	s := NewServer("", nil, nil, nil, "secret", nil)
	defer s.hub.Close()
	r := httptest.NewRequest(http.MethodGet, "/api/events", nil)
	w := httptest.NewRecorder()
	s.handleEvents(w, r)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("events code = %d", w.Code)
	}
}

func TestExecuteRejectsOverloadAndInvalidCommands(t *testing.T) {
	group := taskgroup.New(context.Background(), 1)
	defer group.Close()
	s := NewServer("", brain.New(nil), &protocol.Executor{Tasks: group}, nil, "", nil)
	defer s.hub.Close()
	_, finish, err := group.Start(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer finish()
	for _, tc := range []struct {
		body string
		code int
	}{
		{`{"action":"tts","node_id":"room","text":"hello"}`, 503},
		{`{"action":"unknown"}`, 400},
		{`{"action":"tts","node_id":"room","text":"hello","unknown":true}`, 400},
	} {
		w := httptest.NewRecorder()
		s.handleExecute(w, httptest.NewRequest(http.MethodPost, "/api/execute", strings.NewReader(tc.body)))
		if w.Code != tc.code {
			t.Fatalf("execute status = %d, want %d; body=%s", w.Code, tc.code, w.Body.String())
		}
	}
}

func TestControlContextIsOwnedByService(t *testing.T) {
	parent, cancel := context.WithCancel(context.Background())
	group := taskgroup.New(parent, 1)
	defer group.Close()
	s := NewServer("", nil, &protocol.Executor{Tasks: group}, nil, "", nil)
	defer s.hub.Close()
	invoked := make(chan struct{})
	finished := make(chan struct{})
	handler := s.control(func(_ http.ResponseWriter, r *http.Request) {
		close(invoked)
		<-r.Context().Done()
	})
	go func() {
		handler(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/api/ask", nil))
		close(finished)
	}()
	<-invoked
	cancel()
	<-finished
}
