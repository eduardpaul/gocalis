#!/bin/bash
set -e

mkdir -p models/known_speakers

echo "Downloading Wespeaker CAM++ Speaker ID model..."
MODEL_URL="https://github.com/k2-fsa/sherpa-onnx/releases/download/speaker-recongition-models/wespeaker_en_voxceleb_CAM++.onnx"
wget -q --show-progress -O models/wespeaker_en_voxceleb_CAM++.onnx "${MODEL_URL}"

echo "Model downloaded successfully!"
ls -la models/wespeaker_en_voxceleb_CAM++.onnx
