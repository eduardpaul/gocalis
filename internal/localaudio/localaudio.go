package localaudio

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"gocalis/internal/audionode"
	"gocalis/internal/config"
)

// LocalAudioNode implements audionode.AudioNode by running arecord and aplay subprocesses.
type LocalAudioNode struct {
	nodeCfg   config.NodeConfig
	onAudioCb func([]float32)
	recordCmd *exec.Cmd
	recordOut io.ReadCloser
	mu        sync.Mutex
	running   bool
}

// New creates a new LocalAudioNode.
func New(nodeCfg config.NodeConfig) *LocalAudioNode {
	return &LocalAudioNode{
		nodeCfg: nodeCfg,
	}
}

// resolveALSADevice parses aplay -l or arecord -l to find the card shortname matching deviceStr.
func resolveALSADevice(deviceStr string, isCapture bool) string {
	if deviceStr == "" || strings.ToLower(deviceStr) == "default" {
		return "default"
	}

	cmdName := "aplay"
	if isCapture {
		cmdName = "arecord"
	}

	out, err := exec.Command(cmdName, "-l").Output()
	if err != nil {
		return "default"
	}

	// Format: card <num>: <shortname> [<longname>], device <dev>...
	re := regexp.MustCompile(`card\s+(\d+):\s+([^\s\[]+)\s+\[([^\]]+)\]`)
	lines := strings.Split(string(out), "\n")
	for _, line := range lines {
		matches := re.FindStringSubmatch(line)
		if len(matches) >= 4 {
			num := matches[1]
			shortname := matches[2]
			longname := matches[3]

			if strings.Contains(strings.ToLower(shortname), strings.ToLower(deviceStr)) ||
				strings.Contains(strings.ToLower(longname), strings.ToLower(deviceStr)) ||
				strings.Contains(strings.ToLower(num), strings.ToLower(deviceStr)) {
				return fmt.Sprintf("plughw:%s", shortname)
			}
		}
	}

	return "default"
}

// Connect implements audionode.AudioNode.
func (l *LocalAudioNode) Connect(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.running {
		return nil
	}

	device := resolveALSADevice(l.nodeCfg.Audio.InputDeviceIndex, true)
	sampleRate := l.nodeCfg.Audio.SampleRate
	if sampleRate <= 0 {
		sampleRate = 16000
	}

	log.Printf("[LocalAudioNode:%s] Starting arecord on device %s (sample rate: %d)...\n", l.nodeCfg.NodeID, device, sampleRate)

	// arecord -t raw -f S16_LE -r <rate> -c 1 -D <device>
	cmd := exec.CommandContext(ctx, "arecord",
		"-t", "raw",
		"-f", "S16_LE",
		"-r", fmt.Sprintf("%d", sampleRate),
		"-c", "1",
		"-D", device,
	)

	cmd.Stderr = os.Stderr

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return fmt.Errorf("failed to create arecord stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start arecord: %w", err)
	}

	l.recordCmd = cmd
	l.recordOut = stdout
	l.running = true

	// Read audio in a loop in a background goroutine
	go l.readLoop()

	return nil
}

func (l *LocalAudioNode) readLoop() {
	// 20ms chunk size in samples.
	// At 16000Hz, 20ms is 320 samples. Each sample is 2 bytes (int16).
	chunkSamples := 320
	byteBuf := make([]byte, chunkSamples*2)

	for {
		l.mu.Lock()
		if !l.running {
			l.mu.Unlock()
			break
		}
		out := l.recordOut
		l.mu.Unlock()

		if out == nil {
			break
		}

		_, err := io.ReadFull(out, byteBuf)
		if err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Printf("[LocalAudioNode] Read error: %v\n", err)
			}
			break
		}

		// Convert bytes to float32 samples in range [-1.0, 1.0]
		floatSamples := make([]float32, chunkSamples)
		for i := 0; i < chunkSamples; i++ {
			rawSample := int16(binary.LittleEndian.Uint16(byteBuf[i*2 : (i+1)*2]))
			floatSamples[i] = float32(rawSample) / 32767.0
		}

		// Apply gain if configured
		gain := l.nodeCfg.Audio.Gain
		if gain > 0 && gain != 1.0 {
			for i := range floatSamples {
				floatSamples[i] *= gain
				if floatSamples[i] > 1.0 {
					floatSamples[i] = 1.0
				} else if floatSamples[i] < -1.0 {
					floatSamples[i] = -1.0
				}
			}
		}

		l.mu.Lock()
		cb := l.onAudioCb
		l.mu.Unlock()

		if cb != nil {
			cb(floatSamples)
		}
	}

	_ = l.Close()
}

// Play implements audionode.AudioNode.
func (l *LocalAudioNode) Play(ctx context.Context, pcm16 []int16, sampleRate int) error {
	device := resolveALSADevice(l.nodeCfg.Audio.OutputDeviceIndex, false)
	log.Printf("[LocalAudioNode:%s] Playing %d samples on device %s via aplay...\n", l.nodeCfg.NodeID, len(pcm16), device)

	cmd := exec.CommandContext(ctx, "aplay",
		"-t", "raw",
		"-f", "S16_LE",
		"-r", fmt.Sprintf("%d", sampleRate),
		"-c", "1",
		"-D", device,
	)

	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create aplay stdin pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start aplay: %w", err)
	}

	// Write raw bytes to stdin
	byteBuf := new(bytes.Buffer)
	err = binary.Write(byteBuf, binary.LittleEndian, pcm16)
	if err != nil {
		return fmt.Errorf("failed to encode PCM data: %w", err)
	}

	_, _ = stdin.Write(byteBuf.Bytes())
	_ = stdin.Close()

	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("aplay play failed: %w", err)
	}

	return nil
}

// PlayStream implements audionode.AudioNode.
func (l *LocalAudioNode) PlayStream(ctx context.Context, src audionode.PCM16Source) error {
	device := resolveALSADevice(l.nodeCfg.Audio.OutputDeviceIndex, false)
	sampleRate := src.SampleRate()
	log.Printf("[LocalAudioNode:%s] Playing stream on device %s via aplay...\n", l.nodeCfg.NodeID, device)

	cmd := exec.CommandContext(ctx, "aplay",
		"-t", "raw",
		"-f", "S16_LE",
		"-r", fmt.Sprintf("%d", sampleRate),
		"-c", "1",
		"-D", device,
	)

	cmd.Stderr = os.Stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create aplay stdin pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("failed to start aplay: %w", err)
	}

	defer func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	}()

	chunkSize := 1024
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunk, err := src.ReadPCM16(chunkSize)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		if len(chunk) > 0 {
			byteBuf := new(bytes.Buffer)
			_ = binary.Write(byteBuf, binary.LittleEndian, chunk)
			_, err = stdin.Write(byteBuf.Bytes())
			if err != nil {
				return err
			}
		}
	}

	return nil
}

// OnAudio implements audionode.AudioNode.
func (l *LocalAudioNode) OnAudio(callback func(samples []float32)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onAudioCb = callback
}

// Close implements audionode.AudioNode.
func (l *LocalAudioNode) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.running {
		return nil
	}

	l.running = false
	if l.recordCmd != nil && l.recordCmd.Process != nil {
		_ = l.recordCmd.Process.Signal(syscall.SIGINT)

		go func(cmd *exec.Cmd) {
			done := make(chan error, 1)
			go func() {
				done <- cmd.Wait()
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				if cmd.Process != nil {
					_ = cmd.Process.Kill()
				}
			}
		}(l.recordCmd)
	}

	l.recordCmd = nil
	l.recordOut = nil
	return nil
}
