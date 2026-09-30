package main

/*
#cgo pkg-config: fdk-aac
#include <fdk-aac/aacdecoder_lib.h>
#include <stdlib.h>
static HANDLE_AACDECODER eld_open(unsigned char *config, unsigned int size) {
 HANDLE_AACDECODER h = aacDecoder_Open(TT_MP4_RAW, 1);
 if (aacDecoder_ConfigRaw(h, &config, &size) != AAC_DEC_OK) { aacDecoder_Close(h); return NULL; }
 return h;
}
static int eld_decode(HANDLE_AACDECODER h, unsigned char *data, unsigned int size, short *pcm) {
 unsigned int valid = size;
 if (aacDecoder_Fill(h, &data, &size, &valid) != AAC_DEC_OK || valid != 0) return -1;
 if (aacDecoder_DecodeFrame(h, pcm, 2048, 0) != AAC_DEC_OK) return -2;
 CStreamInfo *info = aacDecoder_GetStreamInfo(h);
 if (info->sampleRate != 16000 || info->numChannels != 1 || info->aot != 39) return -3;
 return info->frameSize;
}
*/
import "C"
import (
	"encoding/hex"
	"fmt"
	"unsafe"
)

type eldDecoder struct{ handle C.HANDLE_AACDECODER }

func newELDDecoder(config string) (*eldDecoder, error) {
	b, err := hex.DecodeString(config)
	if err != nil || len(b) == 0 {
		return nil, fmt.Errorf("invalid ELD config")
	}
	h := C.eld_open((*C.uchar)(unsafe.Pointer(&b[0])), C.uint(len(b)))
	if h == nil {
		return nil, fmt.Errorf("FDK cannot configure ELD decoder")
	}
	return &eldDecoder{h}, nil
}
func (d *eldDecoder) close() { C.aacDecoder_Close(d.handle) }
func (d *eldDecoder) decode(b []byte) ([]int16, error) {
	if len(b) == 0 {
		return nil, fmt.Errorf("empty ELD packet")
	}
	pcm := make([]int16, 2048)
	n := int(C.eld_decode(d.handle, (*C.uchar)(unsafe.Pointer(&b[0])), C.uint(len(b)), (*C.short)(unsafe.Pointer(&pcm[0]))))
	if n < 0 {
		return nil, fmt.Errorf("ELD decode error %d", n)
	}
	return pcm[:n], nil
}
