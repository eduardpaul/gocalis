package localaudio

import (
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
	closed    bool
	cancel    context.CancelFunc
	done      chan struct{}
}

// New creates a new LocalAudioNode.
func New(nodeCfg config.NodeConfig) *LocalAudioNode {
	return &LocalAudioNode{
		nodeCfg: nodeCfg,
	}
}

// resolveALSADevice parses aplay -l or arecord -l to find the card shortname matching deviceStr.
func resolveALSADevice(ctx context.Context, deviceStr string, isCapture bool) string {
	if deviceStr == "" || strings.ToLower(deviceStr) == "default" {
		return "default"
	}

	cmdName := "aplay"
	if isCapture {
		cmdName = "arecord"
	}

	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, cmdName, "-l").Output()
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

	if l.closed {
		return fmt.Errorf("audio node is closed")
	}
	if l.running {
		return nil
	}

	device := resolveALSADevice(ctx, l.nodeCfg.Audio.InputDeviceIndex, true)
	sampleRate := l.nodeCfg.Audio.SampleRate
	if sampleRate <= 0 {
		sampleRate = 16000
	}

	log.Printf("[LocalAudioNode:%s] Starting arecord on device %s (sample rate: %d)...\n", l.nodeCfg.NodeID, device, sampleRate)

	// arecord -t raw -f S16_LE -r <rate> -c 1 -D <device>
	ctx, cancel := context.WithCancel(ctx)
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
		cancel()
		return fmt.Errorf("failed to create arecord stdout pipe: %w", err)
	}

	if err := cmd.Start(); err != nil {
		cancel()
		_ = stdout.Close()
		return fmt.Errorf("failed to start arecord: %w", err)
	}

	l.recordCmd = cmd
	l.recordOut = stdout
	l.running = true
	l.cancel = cancel
	l.done = make(chan struct{})

	// Read audio in a loop in a background goroutine
	go l.readLoop(cmd, stdout, l.done)

	return nil
}

func (l *LocalAudioNode) readLoop(cmd *exec.Cmd, out io.ReadCloser, done chan struct{}) {
	defer close(done)
	defer func() { _ = out.Close(); _ = cmd.Wait(); l.mu.Lock(); l.running = false; l.mu.Unlock() }()
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

}

// Play implements audionode.AudioNode.
func (l *LocalAudioNode) Play(ctx context.Context, pcm16 []int16, sampleRate int) error {
	return l.PlayStream(ctx, audionode.NewSliceSource(pcm16, sampleRate))
}

// PlayStream implements audionode.AudioNode.
func (l *LocalAudioNode) PlayStream(ctx context.Context, src audionode.PCM16Source) error {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return fmt.Errorf("audio node is closed")
	}
	device := resolveALSADevice(ctx, l.nodeCfg.Audio.OutputDeviceIndex, false)
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
		_ = stdin.Close()
		return fmt.Errorf("failed to start aplay: %w", err)
	}

	defer stdin.Close()
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()

	chunkSize := 1024
	byteBuf := make([]byte, chunkSize*2)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		chunk, readErr := src.ReadPCM16(ctx, chunkSize)
		if len(chunk) > 0 {
			for i, value := range chunk {
				binary.LittleEndian.PutUint16(byteBuf[i*2:], uint16(value))
			}
			if _, err := stdin.Write(byteBuf[:len(chunk)*2]); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return readErr
		}
	}
	_ = stdin.Close()
	if err := cmd.Wait(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("aplay failed: %w", err)
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
	l.closed = true
	l.running = false
	if l.cancel != nil {
		l.cancel()
	}
	if l.recordOut != nil {
		_ = l.recordOut.Close()
	}
	done := l.done
	l.mu.Unlock()
	if done != nil {
		<-done
	}
	return nil
}
