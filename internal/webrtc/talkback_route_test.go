package webrtc

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTalkbackRouteActive(t *testing.T) {
	body := ""
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/streams" || r.URL.Query().Get("src") != "doorbell_raw_homekit" {
			t.Errorf("unexpected request %s", r.URL)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	defer srv.Close()

	tb := &talkbackSender{apiBaseURL: srv.URL, inStream: "talkback_in", dstStream: "doorbell_raw_homekit"}
	cases := []struct {
		name   string
		body   string
		status int
		want   bool
		err    bool
	}{
		{"route added via dst (real go2rtc shape)", `{"producers":[{"url":"homekit://x"},{"url":"rtsp://127.0.0.1:8554/talkback_in?audio&source=ffmpeg:talkback_in%23audio%3Deld"}]}`, 200, true, false},
		{"route declared in yaml", `{"producers":[{"url":"homekit://x"},{"url":"ffmpeg:talkback_in#audio=eld"}]}`, 200, true, false},
		{"other ffmpeg bridge", `{"producers":[{"url":"rtsp://127.0.0.1:8554/other_in?audio&source=ffmpeg:other_in%23audio%3Deld"}]}`, 200, false, false},
		{"only the camera", `{"producers":[{"url":"homekit://x"}]}`, 200, false, false},
		{"no producers", `{"producers":null}`, 200, false, false},
		{"api error", ``, 500, false, true},
	}
	for _, c := range cases {
		body, status = c.body, c.status
		got, err := tb.routeActive(context.Background())
		if got != c.want || (err != nil) != c.err {
			t.Errorf("%s: got %v, %v; want %v, err=%v", c.name, got, err, c.want, c.err)
		}
	}
}

// TestTalkbackEnsureRoute checks that a live bridge is left alone: go2rtc's
// Stream.Play stops and restarts the bridge on every POST, so the route is only
// re-posted when go2rtc does not list it (or the listing fails).
func TestTalkbackEnsureRoute(t *testing.T) {
	listing := ""
	listStatus := http.StatusOK
	posts := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			w.WriteHeader(listStatus)
			_, _ = w.Write([]byte(listing))
		case http.MethodPost:
			posts++
			if r.URL.Query().Get("dst") != "doorbell_raw_homekit" || r.URL.Query().Get("src") != "ffmpeg:talkback_in#audio=eld" {
				t.Errorf("unexpected route POST %s", r.URL)
			}
			_, _ = w.Write([]byte(`{}`))
		}
	}))
	defer srv.Close()

	tb := &talkbackSender{apiBaseURL: srv.URL, inStream: "talkback_in", dstStream: "doorbell_raw_homekit"}
	cases := []struct {
		name      string
		listing   string
		status    int
		wantPosts int
	}{
		{"bridge live", `{"producers":[{"url":"homekit://x"},{"url":"rtsp://127.0.0.1:8554/talkback_in?audio&source=ffmpeg:talkback_in%23audio%3Deld"}]}`, 200, 0},
		{"bridge missing", `{"producers":[{"url":"homekit://x"}]}`, 200, 1},
		{"listing failed", ``, 500, 1},
	}
	for _, c := range cases {
		listing, listStatus, posts = c.listing, c.status, 0
		if err := tb.ensureRoute(context.Background()); err != nil {
			t.Fatalf("%s: ensureRoute: %v", c.name, err)
		}
		if posts != c.wantPosts {
			t.Errorf("%s: %d route POSTs, want %d", c.name, posts, c.wantPosts)
		}
	}
}
