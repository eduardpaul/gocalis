package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"os/exec"
	"strings"
	"time"
)

const rate = 16000
const maxSamples = rate * 300

func wav(pcm []int16) []byte {
	b := new(bytes.Buffer)
	b.WriteString("RIFF")
	binary.Write(b, binary.LittleEndian, uint32(36+len(pcm)*2))
	b.WriteString("WAVEfmt ")
	binary.Write(b, binary.LittleEndian, uint32(16))
	binary.Write(b, binary.LittleEndian, uint16(1))
	binary.Write(b, binary.LittleEndian, uint16(1))
	binary.Write(b, binary.LittleEndian, uint32(rate))
	binary.Write(b, binary.LittleEndian, uint32(rate*2))
	binary.Write(b, binary.LittleEndian, uint16(2))
	binary.Write(b, binary.LittleEndian, uint16(16))
	b.WriteString("data")
	binary.Write(b, binary.LittleEndian, uint32(len(pcm)*2))
	binary.Write(b, binary.LittleEndian, pcm)
	return b.Bytes()
}
func parseWAV(b []byte) ([]int16, error) {
	if len(b) >= 8 && int(binary.LittleEndian.Uint32(b[4:])) != len(b)-8 {
		return nil, fmt.Errorf("invalid RIFF length")
	}
	if len(b) < 12 || string(b[:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return nil, fmt.Errorf("expected PCM16 mono 16 kHz WAV")
	}
	valid := false
	for i := 12; i+8 <= len(b); {
		n := int(binary.LittleEndian.Uint32(b[i+4:]))
		tag := string(b[i : i+4])
		i += 8
		if n > len(b)-i {
			return nil, fmt.Errorf("truncated WAV chunk")
		}
		if tag == "fmt " {
			valid = n >= 16 && binary.LittleEndian.Uint16(b[i:]) == 1 && binary.LittleEndian.Uint16(b[i+2:]) == 1 && binary.LittleEndian.Uint32(b[i+4:]) == rate && binary.LittleEndian.Uint16(b[i+14:]) == 16
		}
		if tag == "data" {
			if !valid || n%2 != 0 || n/2 > maxSamples || n == 0 {
				return nil, fmt.Errorf("unsupported or oversized WAV")
			}
			p := make([]int16, n/2)
			binary.Read(bytes.NewReader(b[i:i+n]), binary.LittleEndian, p)
			return p, nil
		}
		i += n + (n % 2)
	}
	return nil, fmt.Errorf("missing WAV data")
}
func tone(hz float64, seconds float64) []int16 {
	p := make([]int16, int(seconds*rate))
	for i := range p {
		p[i] = int16(10000 * math.Sin(2*math.Pi*hz*float64(i)/rate))
	}
	return p
}
func level(p []int16) float64 {
	if len(p) == 0 {
		return 0
	}
	var v float64
	for _, s := range p {
		v += float64(s) * float64(s)
	}
	return math.Sqrt(v/float64(len(p))) / 32768
}
func marker(p []int16, hz float64) float64 {
	if len(p) == 0 {
		return 0
	}
	var s, c, energy float64
	for i, v := range p {
		a := 2 * math.Pi * hz * float64(i) / rate
		s += float64(v) * math.Sin(a)
		c += float64(v) * math.Cos(a)
		energy += float64(v) * float64(v)
	}
	if energy == 0 {
		return 0
	}
	return 2 * (s*s + c*c) / (float64(len(p)) * energy)
}

// Probe the exact FFmpeg encoder and FDK decoder, including their real ELD profile.
func codecProbe() error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	version, err := exec.CommandContext(ctx, "ffmpeg", "-version").Output()
	if err != nil || !strings.HasPrefix(string(version), "ffmpeg version 7.1.1") {
		return fmt.Errorf("FFmpeg 7.1.1 is required")
	}
	dir, err := os.MkdirTemp("", "sim-codecs-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	in, out := dir+"/input.wav", dir+"/eld.m4a"
	os.WriteFile(in, wav(tone(700, 0.5)), 0600)
	cmd := exec.CommandContext(ctx, "ffmpeg", "-v", "error", "-i", in, "-c:a", "libfdk_aac", "-profile:a", "aac_eld", "-ar", "16000", "-ac", "1", out)
	if b, e := cmd.CombinedOutput(); e != nil {
		return fmt.Errorf("ELD encode: %w: %s", e, b)
	}
	probe, e := exec.CommandContext(ctx, "ffprobe", "-v", "error", "-show_entries", "stream=profile,sample_rate,channels", "-of", "json", out).Output()
	if e != nil {
		return fmt.Errorf("ELD profile probe: %w", e)
	}
	var info struct {
		Streams []struct {
			Profile  string `json:"profile"`
			Rate     string `json:"sample_rate"`
			Channels int    `json:"channels"`
		} `json:"streams"`
	}
	if e = json.Unmarshal(probe, &info); e != nil || len(info.Streams) != 1 || info.Streams[0].Profile != "ELD" || info.Streams[0].Rate != "16000" || info.Streams[0].Channels != 1 {
		return fmt.Errorf("expected mono 16 kHz ELD, got %s", probe)
	}
	cmd = exec.CommandContext(ctx, "ffmpeg", "-v", "error", "-c:a", "libfdk_aac", "-i", out, "-f", "s16le", "-ac", "1", "-ar", "16000", "pipe:1")
	b, e := cmd.Output()
	if e != nil {
		return fmt.Errorf("ELD decode: %w", e)
	}
	p := make([]int16, len(b)/2)
	binary.Read(bytes.NewReader(b), binary.LittleEndian, p)
	if len(p) < rate/3 || marker(p, 700) < 0.4 {
		return fmt.Errorf("ELD roundtrip content failed")
	}
	return nil
}
