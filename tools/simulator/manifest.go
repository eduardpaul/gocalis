package main

import (
	"bytes"
	"fmt"
	"gopkg.in/yaml.v3"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

type DeviceSpec struct {
	KWS              map[string]any `yaml:"kws,omitempty" json:"-"`
	EchoCancellation *bool          `yaml:"echo_cancellation,omitempty" json:"-"`
	ID               string         `yaml:"id" json:"id"`
	Profile          string         `yaml:"profile" json:"profile"`
}
type Manifest struct {
	Go2RTCRTSP   string       `yaml:"go2rtc_rtsp"`
	Go2RTCWebRTC string       `yaml:"go2rtc_webrtc"`
	Devices      []DeviceSpec `yaml:"devices"`
	HTTP         string       `yaml:"http"`
	RTSP         string       `yaml:"rtsp"`
	Go2RTC       string       `yaml:"go2rtc"`
	App          string       `yaml:"app"`
	Token        string       `yaml:"token"`
}

func loadManifest(path string) (Manifest, error) {
	var m Manifest
	b, e := os.ReadFile(path)
	if e != nil {
		return m, e
	}
	d := yaml.NewDecoder(bytes.NewReader(b))
	d.KnownFields(true)
	if e = d.Decode(&m); e != nil {
		return m, e
	}
	if d.Decode(new(any)) != io.EOF {
		return m, fmt.Errorf("one manifest document required")
	}
	if len(m.Devices) < 1 || len(m.Devices) > 16 {
		return m, fmt.Errorf("manifest needs 1–16 devices")
	}
	seen := map[string]bool{}
	for _, v := range m.Devices {
		if !regexp.MustCompile(`^[a-zA-Z0-9_-]{1,40}$`).MatchString(v.ID) || seen[v.ID] || (v.Profile != "bidirectional" && v.Profile != "doorbell") {
			return m, fmt.Errorf("invalid or duplicate device: %+v", v)
		}
		seen[v.ID] = true
	}
	if m.HTTP == "" {
		m.HTTP = "127.0.0.1:18080"
	}
	if m.RTSP == "" {
		m.RTSP = "127.0.0.1:18554"
	}
	if m.Go2RTC == "" {
		m.Go2RTC = "http://127.0.0.1:11984"
	}
	if m.App == "" {
		m.App = "http://127.0.0.1:18081"
	}
	if m.Go2RTCRTSP == "" {
		m.Go2RTCRTSP = "127.0.0.1:18555"
	}
	if m.Go2RTCWebRTC == "" {
		m.Go2RTCWebRTC = "127.0.0.1:18556"
	}
	for _, addr := range []string{m.HTTP, m.RTSP, m.Go2RTCRTSP, m.Go2RTCWebRTC} {
		host, port, e := net.SplitHostPort(addr)
		if e != nil || port == "" || (host != "127.0.0.1" && host != "::1" && host != "localhost") {
			return m, fmt.Errorf("listen address must be localhost with a port: %s", addr)
		}
	}
	for _, base := range []string{m.Go2RTC, m.App} {
		u, e := url.Parse(base)
		if e != nil || u.Scheme != "http" || u.Port() == "" || (u.Hostname() != "127.0.0.1" && u.Hostname() != "::1" && u.Hostname() != "localhost") || u.RawQuery != "" || u.Fragment != "" || u.User != nil || strings.Trim(u.Path, "/") != "" {
			return m, fmt.Errorf("API URL must be local HTTP with a port: %s", base)
		}
	}
	return m, nil
}
func generate(m Manifest, dir, base string) error {
	if e := os.MkdirAll(dir, 0755); e != nil {
		return e
	}
	if m.Go2RTCRTSP == "" {
		m.Go2RTCRTSP = "127.0.0.1:18555"
	}
	if m.Go2RTCWebRTC == "" {
		m.Go2RTCWebRTC = "127.0.0.1:18556"
	}
	streams := map[string]any{}
	nodes := []any{}
	for _, d := range m.Devices {
		streams[d.ID] = "rtsp://" + m.RTSP + "/" + d.ID
		echo := true
		if d.EchoCancellation != nil {
			echo = *d.EchoCancellation
		}
		rtc := map[string]any{"api_url": m.Go2RTC, "stream_name": d.ID, "ice_servers": []any{}, "echo_cancellation": echo}
		if d.Profile == "doorbell" {
			streams[d.ID+"_in"] = nil
			rtc["talkback_stream"] = d.ID
			rtc["talkback_in_stream"] = d.ID + "_in"
		}
		kws := d.KWS
		if kws == nil {
			kws = map[string]any{"enabled": false}
		}
		nodes = append(nodes, map[string]any{"node_id": d.ID, "type": "rtc_stream", "rtc_stream": rtc, "kws": kws})
	}
	apiURL, _ := url.Parse(m.Go2RTC)
	_, mediaPort, _ := net.SplitHostPort(m.Go2RTCWebRTC)
	cfg := map[string]any{"api": map[string]any{"listen": apiURL.Host}, "rtsp": map[string]any{"listen": m.Go2RTCRTSP}, "webrtc": map[string]any{"listen": ":" + mediaPort, "candidates": []string{m.Go2RTCWebRTC}, "ice_servers": []any{}}, "streams": streams, "ffmpeg": map[string]any{"eld": "-c:a libfdk_aac -profile:a aac_eld -ar:a 16000 -ac:a 1"}}
	b, e := yaml.Marshal(cfg)
	if e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(dir, "go2rtc.yaml"), b, 0644); e != nil {
		return e
	}
	app := map[string]any{}
	if base != "" {
		b, e = os.ReadFile(base)
		if e != nil {
			return e
		}
		if e = yaml.Unmarshal(b, &app); e != nil {
			return e
		}
	}
	app["mqtt"] = map[string]any{"enabled": false}
	app["security"] = map[string]any{"auth_token": m.Token}
	app["nodes"] = nodes
	b, e = yaml.Marshal(app)
	if e != nil {
		return e
	}
	return os.WriteFile(filepath.Join(dir, "gocalis.yaml"), b, 0644)
}
