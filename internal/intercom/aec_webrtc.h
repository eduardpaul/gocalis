// C interface to WebRTC's audio processing module (AEC3), implemented in
// aec_webrtc.cc, for the cgo echo canceller in aec_webrtc.go.
#ifndef GOCALIS_AEC_WEBRTC_H
#define GOCALIS_AEC_WEBRTC_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct gocalis_apm gocalis_apm;


// gocalis_apm_new builds a mono echo canceller at sample_rate_hz, or returns
// NULL. noise_suppression switches WebRTC's noise suppressor on as well.
gocalis_apm *gocalis_apm_new(int sample_rate_hz, int noise_suppression);
void gocalis_apm_free(gocalis_apm *a);

// gocalis_apm_frame_samples is the number of samples in one 10 ms frame.
int gocalis_apm_frame_samples(gocalis_apm *a);

// gocalis_apm_render feeds one 10 ms frame of the far end (what the speaker
// plays). gocalis_apm_capture processes one 10 ms frame of the microphone in
// place, removing the far end's echo. Both return an AudioProcessing error code
// (0 is success).
int gocalis_apm_render(gocalis_apm *a, const int16_t *frame);
int gocalis_apm_capture(gocalis_apm *a, int16_t *frame);

// gocalis_apm_delay_ms stores AEC3's current echo delay estimate in *delay_ms
// and returns 1, or returns 0 when there is none. The module refreshes it about
// once per second of wall-clock time.
int gocalis_apm_delay_ms(gocalis_apm *a, int *delay_ms);

#ifdef __cplusplus
}
#endif

#endif
