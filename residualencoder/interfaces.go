package residualencoder

import "github.com/KitchenMishap/pudding-huffman/huffman"

type Encoder interface {
	Init(freqMap map[int64]int64)
	InitSlice(freqSlice []int64, offset int, escapeVal int64, maxResidual int64)
	Encode(val int64) huffman.BitCode
	PopularVal() int64
	Map() map[int64]huffman.BitCode
	Variance() int64 // -1 for "not available"
}
