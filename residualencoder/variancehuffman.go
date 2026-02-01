package residualencoder

import (
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"math"
)

type VarianceHuffman struct {
	root       *huffman.Node
	codes      map[int64]huffman.BitCode
	codesSlice []huffman.BitCode
	variance   int64
}

// Check that implements
var _ Encoder = (*VarianceHuffman)(nil)

func (vh *VarianceHuffman) Init(freqMap map[int64]int64) {
	mean, variance := MeanVariance(freqMap)
	fakeFreq := FakeFreq(mean, variance)
	vh.root = huffman.BuildHuffmanTree(fakeFreq)
	vh.codes = make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(vh.root, 0, 0, vh.codes)
	vh.variance = variance
}
func (vh *VarianceHuffman) InitSlice(freqSlice []int64, offset int, escapeVal int64, maxResidual int64) {
	mean := offset
	variance := VarianceSlice(freqSlice, int64(offset), escapeVal)
	fakeSlice := FakeFreqSlice(int64(mean), variance)
	vh.root = huffman.BuildHuffmanTreeFromSlice(fakeSlice, offset)
	vh.codes = make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(vh.root, 0, 0, vh.codes)
	vh.codesSlice = HuffmanMapToMidpointSlice(vh.codes, escapeVal, maxResidual)
	vh.variance = variance
}
func (vh *VarianceHuffman) Encode(val int64) huffman.BitCode {
	if vh.codesSlice != nil {
		zeroOffset := (len(vh.codesSlice) - 1) / 2
		index := int64(zeroOffset) + val
		return vh.codesSlice[index]
	}
	return vh.codes[val]
}
func (vh *VarianceHuffman) PopularVal() int64 {
	if vh.codesSlice != nil {
		shortestCodeLength := math.MaxInt64
		var popularVal int64
		for v, code := range vh.codesSlice {
			if code.Length < shortestCodeLength {
				shortestCodeLength = code.Length
				popularVal = int64(v)
			}
		}
		return popularVal
	} else {
		shortestCodeLength := math.MaxInt64
		var popularVal int64
		for v, code := range vh.codes {
			if code.Length < shortestCodeLength {
				shortestCodeLength = code.Length
				popularVal = v
			}
		}
		return popularVal
	}
}
func (vh *VarianceHuffman) Map() map[int64]huffman.BitCode {
	return vh.codes
}
func (vh *VarianceHuffman) Slice() []huffman.BitCode { return vh.codesSlice }

func MeanVariance(freqMap map[int64]int64) (int64, int64) {
	var totalHits int64
	var sum int64
	for val, freq := range freqMap {
		sum += val * freq
		totalHits += freq
	}
	if totalHits == 0 {
		return 0, 0
	}
	mean := sum / totalHits

	var sumSqDiff int64
	for val, freq := range freqMap {
		diff := val - mean
		sumSqDiff += (diff * diff) * freq
	}
	variance := sumSqDiff / totalHits
	return mean, variance
}

func FakeFreq(mean, variance int64) map[int64]int64 {
	fakeMap := make(map[int64]int64)
	// We simulate out to 4 sigma (99.9% of data)
	sigma := math.Sqrt(float64(variance))
	if sigma < 1 {
		sigma = 1
	}

	const scale = 1e9

	// Generate values around the mean
	for i := int64(-4 * sigma); i <= int64(4*sigma); i++ {
		val := mean + i
		// Gaussian PDF formula
		exponent := -float64(i*i) / (2 * float64(variance))
		prob := (1.0 / (sigma * math.Sqrt(2*math.Sqrt(math.Pi)))) * math.Exp(exponent)

		// Convert to integer frequency for Huffman builder
		freq := int64(prob * scale)
		if freq > 0 {
			fakeMap[val] = freq
		}
	}
	return fakeMap
}

func VarianceSlice(freqSlice []int64, offset int64, escapeVal int64) int64 {
	var totalHits int64
	var sumSqDiff int64
	// In Slice mode, the index is the value + offset.
	// The midpoint of the slice is the mean (usually 0 for residuals)
	midpoint := int64(len(freqSlice) / 2)

	for i, freq := range freqSlice {
		if int64(i) == escapeVal {
			continue
		}
		val := int64(i) - midpoint
		sumSqDiff += (val * val) * freq
		totalHits += freq
	}
	if totalHits == 0 {
		return 1
	}
	return sumSqDiff / totalHits
}

func FakeFreqSlice(mean, variance int64) []int64 {
	// We want an odd length to maintain a perfect midpoint
	sigma := math.Sqrt(float64(variance))
	if sigma < 1 {
		sigma = 1
	}

	span := int64(sigma * 8) // 4 sigma each way
	if span%2 == 0 {
		span++
	}
	if span < 11 {
		span = 11
	} // Minimum floor

	fakeSlice := make([]int64, span)
	midpoint := span / 2
	const scale = 1e9

	for i := int64(0); i < span; i++ {
		diff := i - midpoint
		exponent := -float64(diff*diff) / (2 * float64(variance))
		prob := (1.0 / (sigma * math.Sqrt(2*math.Pi))) * math.Exp(exponent)
		fakeSlice[i] = int64(prob * scale)
	}
	return fakeSlice
}

func (vh *VarianceHuffman) Variance() int64 { return vh.variance }
