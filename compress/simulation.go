package compress

import (
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"github.com/KitchenMishap/pudding-huffman/kmeans"
)

type CompressionStats struct {
	TotalBits      uint64
	CelebrityHits  uint64
	ScientificHits uint64
	LiteralHits    uint64
	KMeansHits     uint64
}

func SimulateCompression(amounts []int64,
	blocksPerEpoch int,
	blockToTxo []int64,
	celebCodes map[int64]huffman.BitCode,
	mantissaCodes map[int64]huffman.BitCode,
	expCodes map[int64]huffman.BitCode) (CompressionStats, [][]int64) {

	blocks := len(blockToTxo)
	epochs := blocks/blocksPerEpoch + 1 // +1 because of the partial epoch at the end
	resultLiteralsPerEpoch := make([][]int64, epochs)

	var stats CompressionStats
	// Escape lengths (the length of the Huffman code for "Everything Else")
	esc1Len := celebCodes[-1].Length
	esc2Len := mantissaCodes[-1].Length
	// exponents encode 100% of values, so have no escape code

	block := 0
	for txo := int64(0); txo < int64(len(amounts)); txo++ {
		if block+1 < blocks && txo >= blockToTxo[block+1] {
			block++
		}
		amount := amounts[txo]
		epochID := block / blocksPerEpoch
		if resultLiteralsPerEpoch[epochID] == nil {
			resultLiteralsPerEpoch[epochID] = make([]int64, 0)
		}

		// Stage 1: Celebrity check
		if aCode, ok := celebCodes[amount]; ok {
			stats.TotalBits += uint64(aCode.Length)
			stats.CelebrityHits++
			continue
		}

		// Stage 2: Scientific check
		stats.TotalBits += uint64(esc1Len) // Pay the first escape penalty
		ff := NewForensicFloat(amount)

		if mCode, ok := mantissaCodes[ff.Mantissa]; ok {
			stats.TotalBits += uint64(mCode.Length)
			// Also need the exponent code. It should always be there (no "everything else" escape for exponents)
			if eCode, ok := expCodes[ff.Exponent]; ok {
				stats.TotalBits += uint64(eCode.Length)
			} else {
				panic("missing exponent code")
			}
			stats.ScientificHits++
			continue
		}

		// Stage 3: Literal
		stats.TotalBits += uint64(esc2Len + 64)
		stats.LiteralHits++
		// Also append the literal (and ONLY the literals) to the per-epoch result, for our k-means stuff later
		resultLiteralsPerEpoch[epochID] = append(resultLiteralsPerEpoch[epochID], amount)
	}
	return stats, resultLiteralsPerEpoch
}

// This fn is somewhat copied from above! only the k-means is new
func SimulateCompressionWithKMeans(amounts []int64,
	blocksPerEpoch int,
	blockToTxo []int64,
	celebCodes map[int64]huffman.BitCode,
	mantissaCodes map[int64]huffman.BitCode,
	expCodes map[int64]huffman.BitCode,
	residualCodes map[int64]huffman.BitCode,
	epochToPhasePeaks [][]float64) CompressionStats {

	blocks := len(blockToTxo)

	var stats CompressionStats
	// Escape lengths (the length of the Huffman code for "Everything Else")
	esc1Len := celebCodes[-1].Length
	esc2Len := mantissaCodes[-1].Length
	esc2bLen := residualCodes[2100000000000000].Length // Can't use -1 for this one, -1 is a valid residual
	// exponents encode 100% of values, so have no escape code

	block := 0
	for txo := int64(0); txo < int64(len(amounts)); txo++ {
		if block+1 < blocks && txo >= blockToTxo[block+1] {
			block++
		}
		amount := amounts[txo]
		epochID := block / blocksPerEpoch

		// Stage 1: Celebrity check
		if aCode, ok := celebCodes[amount]; ok {
			stats.TotalBits += uint64(aCode.Length)
			stats.CelebrityHits++
			continue
		}

		// Stage 2: Scientific check
		stats.TotalBits += uint64(esc1Len) // Pay the first escape penalty
		ff := NewForensicFloat(amount)

		if mCode, ok := mantissaCodes[ff.Mantissa]; ok {
			stats.TotalBits += uint64(mCode.Length)
			// Also need the exponent code. It should always be there (no "everything else" escape for exponents)
			if eCode, ok := expCodes[ff.Exponent]; ok {
				stats.TotalBits += uint64(eCode.Length)
			} else {
				panic("missing exponent code")
			}
			stats.ScientificHits++
			continue
		}

		// Stage 2b: k-means check
		stats.TotalBits += uint64(esc2bLen)
		if epochToPhasePeaks[epochID] != nil {
			e, _, r := kmeans.ExpPeakResidual(amount, epochToPhasePeaks[epochID])
			if rCode, ok := residualCodes[r]; ok {
				stats.TotalBits += uint64(3) // 3 bits for the peak number (0..6)
				// Also need the exponent code. It should always be there (no "everything else" escape for exponents)
				if eCode, ok := expCodes[int64(e)]; ok {
					stats.TotalBits += uint64(eCode.Length)
				} else {
					panic("missing exponent code")
				}
				stats.TotalBits += uint64(rCode.Length)
				stats.KMeansHits++
				continue
			}
		} else {
			// k-means not worthwhile here
		}

		// Stag 3: Literal
		stats.TotalBits += uint64(esc2Len + 64)
		stats.LiteralHits++
	}
	return stats
}
