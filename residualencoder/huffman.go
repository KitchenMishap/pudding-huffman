package residualencoder

import (
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"math"
)

type Huffman struct {
	root       *huffman.Node
	codes      map[int64]huffman.BitCode
	codesSlice []huffman.BitCode
}

// Check that implements
var _ Encoder = (*Huffman)(nil)

func (h *Huffman) Init(freqMap map[int64]int64) {
	h.root = huffman.BuildHuffmanTree(freqMap)
	h.codes = make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(h.root, 0, 0, h.codes)
}
func (h *Huffman) InitSlice(freqSlice []int64, offset int, escapeVal int64, maxResidual int64) {
	h.root = huffman.BuildHuffmanTreeFromSlice(freqSlice, offset)
	h.codes = make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(h.root, 0, 0, h.codes)
	h.codesSlice = HuffmanMapToMidpointSlice(h.codes, escapeVal, maxResidual)
}
func (h *Huffman) Encode(val int64) huffman.BitCode {
	if h.codesSlice != nil {
		zeroOffset := (len(h.codesSlice) - 1) / 2
		index := int64(zeroOffset) + val
		if index < 0 || index >= int64(len(h.codesSlice)) {
			return huffman.BigCode()
		}
		return h.codesSlice[index]
	}
	return h.codes[val]
}
func (h *Huffman) PopularVal() int64 {
	if h.codesSlice != nil {
		shortestCodeLength := math.MaxInt64
		var popularVal int64
		for v, code := range h.codesSlice {
			if code.Length < shortestCodeLength {
				shortestCodeLength = code.Length
				popularVal = int64(v)
			}
		}
		return popularVal
	} else {
		shortestCodeLength := math.MaxInt64
		var popularVal int64
		for v, code := range h.codes {
			if code.Length < shortestCodeLength {
				shortestCodeLength = code.Length
				popularVal = v
			}
		}
		return popularVal
	}
}
func (h *Huffman) Map() map[int64]huffman.BitCode {
	return h.codes
}

func huffmanSliceMidpoint(length int) int { return length / 2 }

// len(result) is odd
// The midpoint (corresponding to a key of 0) is therefore at (len(result)-1)/2
func HuffmanMapToMidpointSlice(m map[int64]huffman.BitCode, escapeCode int64, maxResidual int64) []huffman.BitCode {
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
	if maxAbs > maxResidual {
		return nil
	}

	// 3. Create a slice that can hold [-maxAbs ... 0 ... +maxAbs]
	// Size is 2 * maxAbs + 1. +1 is for the zero itself.
	span := 2*maxAbs + 1
	slice := make([]huffman.BitCode, span)

	midpoint := maxAbs

	for k, v := range m {
		slice[midpoint+k] = v
	}

	return slice
}
func (h *Huffman) Variance() int64 { return -1 }
