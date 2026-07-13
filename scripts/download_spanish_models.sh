#!/bin/bash
set -e

mkdir -p models/whisper models/vits-es

echo "Downloading Whisper Tiny (Multilingual/Spanish) ASR model..."
WHISPER_URL="https://github.com/k2-fsa/sherpa-onnx/releases/download/asr-models/sherpa-onnx-whisper-tiny.tar.bz2"
wget -q --show-progress -O whisper.tar.bz2 "${WHISPER_URL}"
echo "Extracting Whisper model..."
tar -xf whisper.tar.bz2 -C models/whisper --strip-components=1
rm whisper.tar.bz2

echo "Downloading VITS es_ES Sharvard Medium TTS model..."
VITS_URL="https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/vits-piper-es_ES-sharvard-medium.tar.bz2"
wget -q --show-progress -O vits-es.tar.bz2 "${VITS_URL}"
echo "Extracting VITS model..."
tar -xf vits-es.tar.bz2 -C models/vits-es --strip-components=1
rm vits-es.tar.bz2

echo "Spanish models downloaded and extracted successfully!"
ls -la models/whisper
ls -la models/vits-es
