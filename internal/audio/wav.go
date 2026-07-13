package audio

import (
	"bufio"
	"fmt"
	"os"
)

// EncodeWAVPCM16 returns a canonical 44-byte-header mono PCM16 WAV file for the
// given samples and sample rate.
func EncodeWAVPCM16(samples []int16, sampleRate int) []byte {
	const (
		numChannels   = 1
		bitsPerSample = 16
	)
	byteRate := sampleRate * numChannels * bitsPerSample / 8
	blockAlign := numChannels * bitsPerSample / 8
	subChunk2Size := len(samples) * numChannels * bitsPerSample / 8
	chunkSize := 36 + subChunk2Size

	buf := make([]byte, 44+subChunk2Size)
	copy(buf[0:4], "RIFF")
	putUint32LE(buf[4:8], uint32(chunkSize))
	copy(buf[8:12], "WAVE")
	copy(buf[12:16], "fmt ")
	putUint32LE(buf[16:20], 16) // PCM fmt chunk size
	putUint16LE(buf[20:22], 1)  // audio format = PCM
	putUint16LE(buf[22:24], uint16(numChannels))
	putUint32LE(buf[24:28], uint32(sampleRate))
	putUint32LE(buf[28:32], uint32(byteRate))
	putUint16LE(buf[32:34], uint16(blockAlign))
	putUint16LE(buf[34:36], bitsPerSample)
	copy(buf[36:40], "data")
	putUint32LE(buf[40:44], uint32(subChunk2Size))

	offset := 44
	for _, s := range samples {
		buf[offset] = byte(s)
		buf[offset+1] = byte(s >> 8)
		offset += 2
	}
	return buf
}

// EncodeWAVFloat32 returns a mono PCM16 WAV file for float32 samples in the
// range [-1.0, 1.0].
func EncodeWAVFloat32(samples []float32, sampleRate int) []byte {
	return EncodeWAVPCM16(FloatToPCM16(samples), sampleRate)
}

// WriteWAVPCM16 writes a mono PCM16 WAV file to disk.
func WriteWAVPCM16(filePath string, samples []int16, sampleRate int) (err error) {
	return writeWAV(filePath, EncodeWAVPCM16(samples, sampleRate))
}

// WriteWAVFloat32 writes mono float32 samples (range [-1.0, 1.0]) to a WAV file.
func WriteWAVFloat32(filePath string, samples []float32, sampleRate int) (err error) {
	return writeWAV(filePath, EncodeWAVFloat32(samples, sampleRate))
}

func writeWAV(filePath string, data []byte) (err error) {
	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := f.Close(); cerr != nil && err == nil {
			err = cerr
		}
	}()

	w := bufio.NewWriter(f)
	if _, err = w.Write(data); err != nil {
		return err
	}
	return w.Flush()
}

func putUint16LE(b []byte, v uint16) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
}

func putUint32LE(b []byte, v uint32) {
	b[0] = byte(v)
	b[1] = byte(v >> 8)
	b[2] = byte(v >> 16)
	b[3] = byte(v >> 24)
}

func uint16LE(b []byte) uint16 {
	return uint16(b[0]) | uint16(b[1])<<8
}

func uint32LE(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// DecodeWAVPCM16 parses an uncompressed PCM16 WAV byte slice and returns the
// mono samples together with the sample rate. Multi-channel input is downmixed
// to mono by averaging channels. It only supports PCM format (1) with 16 bits
// per sample, which is what every recording produced by Gocalis uses.
func DecodeWAVPCM16(data []byte) ([]int16, int, error) {
	if len(data) < 12 {
		return nil, 0, fmt.Errorf("wav too short: %d bytes", len(data))
	}
	if string(data[0:4]) != "RIFF" || string(data[8:12]) != "WAVE" {
		return nil, 0, fmt.Errorf("not a RIFF/WAVE file")
	}

	var (
		sampleRate    int
		numChannels   int
		bitsPerSample int
		haveFmt       bool
		haveData      bool
		pcm           []byte
	)

	// Walk the chunk list that follows the 12-byte RIFF/WAVE header.
	off := 12
	for off+8 <= len(data) {
		id := string(data[off : off+4])
		size := int(uint32LE(data[off+4 : off+8]))
		body := off + 8
		if size < 0 || body+size > len(data) {
			size = len(data) - body // tolerate a truncated final chunk
		}
		switch id {
		case "fmt ":
			if size < 16 {
				return nil, 0, fmt.Errorf("fmt chunk too small: %d bytes", size)
			}
			format := int(uint16LE(data[body : body+2]))
			numChannels = int(uint16LE(data[body+2 : body+4]))
			sampleRate = int(uint32LE(data[body+4 : body+8]))
			bitsPerSample = int(uint16LE(data[body+14 : body+16]))
			if format != 1 {
				return nil, 0, fmt.Errorf("unsupported WAV format %d (only PCM)", format)
			}
			haveFmt = true
		case "data":
			pcm = data[body : body+size]
			haveData = true
		}
		// Chunks are word-aligned: an odd size is followed by a pad byte.
		off = body + size
		if size%2 == 1 {
			off++
		}
	}

	if !haveFmt {
		return nil, 0, fmt.Errorf("missing fmt chunk")
	}
	if !haveData {
		return nil, 0, fmt.Errorf("missing data chunk")
	}
	if bitsPerSample != 16 {
		return nil, 0, fmt.Errorf("unsupported bits per sample %d (only 16)", bitsPerSample)
	}
	if numChannels < 1 {
		return nil, 0, fmt.Errorf("invalid channel count %d", numChannels)
	}
	if sampleRate <= 0 {
		return nil, 0, fmt.Errorf("invalid sample rate %d", sampleRate)
	}

	total := len(pcm) / 2
	interleaved := make([]int16, total)
	for i := 0; i < total; i++ {
		interleaved[i] = int16(uint16LE(pcm[2*i : 2*i+2]))
	}

	if numChannels == 1 {
		return interleaved, sampleRate, nil
	}

	// Downmix interleaved multi-channel audio to mono by averaging channels.
	frames := total / numChannels
	mono := make([]int16, frames)
	for f := 0; f < frames; f++ {
		var acc int
		for c := 0; c < numChannels; c++ {
			acc += int(interleaved[f*numChannels+c])
		}
		mono[f] = int16(acc / numChannels)
	}
	return mono, sampleRate, nil
}
