package audio

// PCMResampler retains interpolation phase across chunks. Flush emits the final
// fractional samples using the last value, matching whole-clip resampling.
type PCMResampler struct {
	src, dst            int
	input               []int16
	base, total, output int64
}

func NewPCMResampler(src, dst int) *PCMResampler { return &PCMResampler{src: src, dst: dst} }
func (r *PCMResampler) Push(samples []int16) []int16 {
	r.input = append(r.input, samples...)
	r.total += int64(len(samples))
	return r.emit(false)
}
func (r *PCMResampler) Flush() []int16 { return r.emit(true) }
func (r *PCMResampler) emit(final bool) []int16 {
	if r.src <= 0 || r.dst <= 0 {
		return nil
	}
	var out []int16
	limit := r.total * int64(r.dst) / int64(r.src)
	for r.output < limit {
		numerator := r.output * int64(r.src)
		index := numerator / int64(r.dst)
		fraction := float64(numerator%int64(r.dst)) / float64(r.dst)
		if !final && index+1 >= r.total {
			break
		}
		i := int(index - r.base)
		value := float64(r.input[i])
		if i+1 < len(r.input) {
			value += (float64(r.input[i+1]) - value) * fraction
		}
		out = append(out, int16(value))
		r.output++
	}
	next := r.output * int64(r.src) / int64(r.dst)
	drop := min(int(next-r.base), len(r.input))
	r.input = r.input[drop:]
	r.base += int64(drop)
	return out
}
