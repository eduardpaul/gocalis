import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { runInNewContext } from 'node:vm';
test('worklet sends paced mono PCM16 frames and clamps samples', () => {
  let Processor;
  const outputs = [];
  runInNewContext(readFileSync(new URL('./worklet.js', import.meta.url), 'utf8'), {
    AudioWorkletProcessor: class { constructor() { this.port = { postMessage: buffer => outputs.push(buffer) }; } },
    registerProcessor: (name, implementation) => { assert.equal(name, 'pcm-input'); Processor = implementation; },
  });
  const processor = new Processor();
  for (let i = 0; i < 5; i++) processor.process([[new Float32Array(128).fill(2), new Float32Array(128).fill(0)]]);
  assert.equal(outputs.length, 2);
  assert.equal(outputs[0].byteLength, 640);
  assert.equal(new DataView(outputs[0]).getInt16(0, true), 32767);
  assert.equal(processor.samples.length, 0);
});
