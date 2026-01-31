package residualencoder

import (
	"github.com/KitchenMishap/pudding-huffman/compress"
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"math"
)

type Rice struct {
	huffman Huffman
	slice   []huffman.BitCode
}

// Check that implements
var _ Encoder = (*Huffman)(nil)

func (r *Rice) Init(freqMap map[int64]int64) {
	r.huffman = Huffman{}
	r.huffman.Init(freqMap)
}
func (r *Rice) InitSlice(freqSlice []int64, offset int, escapeVal int64) {
	r.huffman = Huffman{}
	r.huffman.InitSlice(freqSlice, offset, escapeVal)

	bestOffset := r.huffman.PopularVal()

	// 3) Find the optimimum Rice K
	// Heuristic: k = log2(mean(|centered_residuals|))
	var sumAbs int64
	var count int64
	for r, freq := range freqSlice {
		centred := int64(r) - bestOffset
		uVal := (centred << 1) ^ (centred >> 63) // Zig zag
		sumAbs += uVal * freq
		count += freq
	}
	optimalK := 0
	if count > 0 {
		mean := float64(sumAbs) / float64(count)
		optimalK = int(math.Max(0, math.Round(math.Log2(mean))))
	}
	r.slice = huffmanMapToMidpointRiceSlice(r.huffman.Map(), escapeVal, optimalK)
}
func (r *Rice) Encode(val int64) huffman.BitCode {
	if r.slice != nil {
		zeroOffset := (len(r.slice) - 1) / 2
		index := int64(zeroOffset) + val
		return r.slice[index]
	}
	panic("!")
}
func (r *Rice) PopularVal() int64 {
	return r.huffman.PopularVal()
}
func (r *Rice) Map() map[int64]huffman.BitCode {
	return r.huffman.Map()
}
func (r *Rice) Slice() []huffman.BitCode {
	return r.slice
}

// len(result) is odd
// The midpoint (corresponding to a key of 0) is therefore at (len(result)-1)/2
func huffmanMapToMidpointRiceSlice(m map[int64]huffman.BitCode, escapeCode int64, kForRice int) []huffman.BitCode {
	if len(m) == 0 {
		return nil
	}

	// 1. Find the absolute furthest residual from zero
	var maxAbs int64
	for k := range m {
		// escapeCode is a VERY LARGE number that deserves special treatment
		if k != escapeCode {
			absK := k
			if absK < 0 {
				absK = -absK
			}
			if absK > maxAbs {
				maxAbs = absK
			}
		}
	}

	// 2. Safety cap to prevent "The Beast" from eating too much RAM
	if maxAbs > compress.MaxResidual {
		return nil
	}

	// 3. Create a slice that can hold [-maxAbs ... 0 ... +maxAbs]
	// Size is 2 * maxAbs + 1. +1 is for the zero itself.
	span := 2*maxAbs + 1
	slice := make([]huffman.BitCode, span)

	midpoint := maxAbs

	for key, _ := range m {
		// Remember the key's are the residual values we're interested in! (not the huffman codes values)
		slice[midpoint+key] = huffman.GenerateRiceBitCode(uint64(key), kForRice)
	}

	return slice
}
