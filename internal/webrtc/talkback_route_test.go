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
