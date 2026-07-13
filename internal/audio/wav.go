package audio

import (
	"bufio"
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
