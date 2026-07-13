#!/bin/bash
set -e

mkdir -p models/moonshine models/supertonic

echo "Downloading Moonshine Tiny ASR model files..."
HF_MOONSHINE_URL="https://huggingface.co/csukuangfj/sherpa-onnx-moonshine-tiny-en-int8/resolve/main"

wget -q --show-progress -O models/moonshine/preprocess.onnx "${HF_MOONSHINE_URL}/preprocess.onnx"
wget -q --show-progress -O models/moonshine/encode.int8.onnx "${HF_MOONSHINE_URL}/encode.int8.onnx"
wget -q --show-progress -O models/moonshine/uncached_decode.int8.onnx "${HF_MOONSHINE_URL}/uncached_decode.int8.onnx"
wget -q --show-progress -O models/moonshine/cached_decode.int8.onnx "${HF_MOONSHINE_URL}/cached_decode.int8.onnx"
wget -q --show-progress -O models/moonshine/tokens.txt "${HF_MOONSHINE_URL}/tokens.txt"

echo "Downloading Supertonic 3 TTS model..."
TTS_URL="https://github.com/k2-fsa/sherpa-onnx/releases/download/tts-models/sherpa-onnx-supertonic-3-tts-int8-2026-05-11.tar.bz2"

wget -q --show-progress -O supertonic.tar.bz2 "${TTS_URL}"
echo "Extracting Supertonic model..."
tar -xf supertonic.tar.bz2 -C models/supertonic --strip-components=1
rm supertonic.tar.bz2

echo "Models downloaded and extracted successfully!"
ls -la models/moonshine
ls -la models/supertonic
