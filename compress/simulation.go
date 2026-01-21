package compress

import (
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"github.com/KitchenMishap/pudding-huffman/kmeans"
	"math/bits"
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
	expCodes map[int64]huffman.BitCode) (CompressionStats, [][]int64, []int64) {

	blocks := len(blockToTxo)
	epochs := blocks/blocksPerEpoch + 1 // +1 because of the partial epoch at the end
	resultLiteralsPerEpoch := make([][]int64, epochs)

	var stats CompressionStats
	// Escape lengths (the length of the Huffman code for "Everything Else")
	esc1Len := celebCodes[-1].Length
	esc2Len := mantissaCodes[-1].Length
	// exponents encode 100% of values, so have no escape code

	// A simple array to hold the frequencies for bit lengths 0 to 64
	magnitudeFreqs := make([]int64, 65)

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

		// Stage 3: "Literal" (actually, we're gonna Huffman-encode the magnitude bitcount)
		stats.TotalBits += uint64(esc2Len + 56) // Literal is 7 bytes not 8
		stats.LiteralHits++
		// Also append the literal (and ONLY the literals) to the per-epoch result, for our k-means stuff later
		resultLiteralsPerEpoch[epochID] = append(resultLiteralsPerEpoch[epochID], amount)
		// Also count the number of bits
		mag := bits.Len64(uint64(amount))
		magnitudeFreqs[mag]++
	}
	return stats, resultLiteralsPerEpoch, magnitudeFreqs
}

// This fn is somewhat copied from above! only the k-means is new
func SimulateCompressionWithKMeans(amounts []int64,
	blocksPerEpoch int,
	blockToTxo []int64,
	celebCodes map[int64]huffman.BitCode,
	mantissaCodes map[int64]huffman.BitCode,
	expCodes map[int64]huffman.BitCode,
	residualCodes map[int64]huffman.BitCode,
	magnitudeCodes map[int64]huffman.BitCode,
	epochToPhasePeaks [][]float64) (CompressionStats, [][7]int64) {

	blocks := len(blockToTxo)
	epochs := blocks/blocksPerEpoch + 1

	var stats CompressionStats
	// Escape lengths (the length of the Huffman code for "Everything Else")
	esc1Len := celebCodes[-1].Length
	esc2Len := mantissaCodes[-1].Length
	// There is no distinct escape code for stage 2b. It is included in the 3 bit "peak number" (when it is 7)
	// esc2bLen := residualCodes[2100000000000000].Length  Can't use -1 for this one, -1 is a valid residual
	// exponents encode 100% of values, so have no escape code

	// Counter of peak-index popularities for each epoch
	peakStrengths := make([][7]int64, epochs)

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
		stats.TotalBits += uint64(esc2Len) // Pay the second escape penalty
		if epochToPhasePeaks[epochID] != nil {
			e, peakIdx, r := kmeans.ExpPeakResidual(amount, epochToPhasePeaks[epochID])
			if rCode, ok := residualCodes[r]; ok {
				stats.TotalBits += uint64(3) // 3 bits for the peak number (0..6 or 7 for escape)

				// Strengthen the appropriate peak index for the appropriate epochID
				peakStrengths[epochID][peakIdx]++

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
			stats.TotalBits += uint64(3) // Still have to pay the escape penalty, which is in the peak number
		}

		// Stage 3: Literal (actually, a Huffman-encoded magnitude, followed by that number of bits)
		// stats.TotalBits += uint64(esc2bLen)	There is no escape penalty (it was covered in the 3 bits of peak number)
		mag := int64(bits.Len64(uint64(amount)))
		magCodeBits := magnitudeCodes[mag].Length
		stats.TotalBits += uint64(magCodeBits) // Encoded magnitude
		stats.TotalBits += uint64(mag)         // The bits themselves
		stats.LiteralHits++
	}
	return stats, peakStrengths
}
