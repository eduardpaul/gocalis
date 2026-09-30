// AudioContext is requested at 16 kHz; verify it before attaching this processor.
class PCMInput extends AudioWorkletProcessor {
  constructor() { super(); this.samples = []; }
  process(inputs) {
    const channels = inputs[0];
    if (!channels?.length) return true;
    for (let i = 0; i < channels[0].length; i++) {
      let v = 0;
      for (const channel of channels) v += channel[i] / channels.length;
      this.samples.push(Math.round(Math.max(-1, Math.min(1, v)) * 32767));
      if (this.samples.length === 320) {
        const buffer = new ArrayBuffer(640), view = new DataView(buffer);
        this.samples.forEach((sample, index) => view.setInt16(index * 2, sample, true));
        this.port.postMessage(buffer, [buffer]);
        this.samples = [];
      }
    }
    return true;
  }
}
registerProcessor('pcm-input', PCMInput);
