// extern "C" shim over webrtc::AudioProcessing (webrtc-audio-processing-1) so
// cgo can drive AEC3 without C++ on the Go side.
#include "aec_webrtc.h"

#include <modules/audio_processing/include/audio_processing.h>

struct gocalis_apm {
	rtc::scoped_refptr<webrtc::AudioProcessing> apm;
	webrtc::StreamConfig stream;
};

extern "C" {

gocalis_apm *gocalis_apm_new(int sample_rate_hz, int noise_suppression) {
	webrtc::AudioProcessing *raw = webrtc::AudioProcessingBuilder().Create();
	if (raw == nullptr) {
		return nullptr;
	}
	gocalis_apm *a = new gocalis_apm{rtc::scoped_refptr<webrtc::AudioProcessing>(raw),
	                                  webrtc::StreamConfig(sample_rate_hz, 1)};

	webrtc::AudioProcessing::Config cfg;
	// AEC3 (the full canceller, not the mobile one): linear filter, its own
	// render/capture delay estimator and the non-linear residual echo
	// suppressor that cleans up what a lossy codec on the echo path leaves.
	cfg.echo_canceller.enabled = true;
	cfg.echo_canceller.mobile_mode = false;
	cfg.high_pass_filter.enabled = true;
	cfg.noise_suppression.enabled = noise_suppression != 0;
	cfg.noise_suppression.level = webrtc::AudioProcessing::Config::NoiseSuppression::kModerate;
	cfg.gain_controller1.enabled = false;
	cfg.gain_controller2.enabled = false;
	cfg.transient_suppression.enabled = false;
	a->apm->ApplyConfig(cfg);

	webrtc::ProcessingConfig pc = {{a->stream, a->stream, a->stream, a->stream}};
	if (a->apm->Initialize(pc) != webrtc::AudioProcessing::kNoError) {
		delete a;
		return nullptr;
	}
	return a;
}

void gocalis_apm_free(gocalis_apm *a) { delete a; }

int gocalis_apm_frame_samples(gocalis_apm *a) { return static_cast<int>(a->stream.num_frames()); }

int gocalis_apm_render(gocalis_apm *a, const int16_t *frame) {
	// The output (the far end after render-side processing) is not used: the
	// node plays the original.
	int16_t scratch[480];  // 10 ms at up to 48 kHz, mono
	return a->apm->ProcessReverseStream(frame, a->stream, a->stream, scratch);
}

int gocalis_apm_capture(gocalis_apm *a, int16_t *frame) {
	return a->apm->ProcessStream(frame, a->stream, a->stream, frame);
}

int gocalis_apm_delay_ms(gocalis_apm *a, int *delay_ms) {
	webrtc::AudioProcessingStats s = a->apm->GetStatistics();
	if (!s.delay_ms.has_value()) {
		return 0;
	}
	*delay_ms = *s.delay_ms;
	return 1;
}

}  // extern "C"
