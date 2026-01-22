package compress

import (
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"github.com/KitchenMishap/pudding-huffman/kmeans"
	"math/bits"
	"sync"
)

type CompressionStats struct {
	TotalBits      uint64
	CelebrityHits  uint64
	ScientificHits uint64
	LiteralHits    uint64
	KMeansHits     uint64
}

func ParallelSimulateCompression(amounts []int64,
	blocksPerEpoch int,
	blockToTxo []int64,
	celebCodes map[int64]huffman.BitCode,
	mantissaCodes map[int64]huffman.BitCode,
	expCodes map[int64]huffman.BitCode) (CompressionStats, [][]int64, []int64) {

	blocks := len(blockToTxo)
	epochs := blocks/blocksPerEpoch + 1
	numWorkers := 40

	// Channels for distribution and collection
	jobs := make(chan int, 100)
	type workerResult struct {
		stats    CompressionStats
		literals [][]int64
		mags     []int64
	}
	resultsChan := make(chan workerResult, numWorkers)
	var wg sync.WaitGroup

	// Escape lengths
	esc1Len := celebCodes[-1].Length
	esc2Len := mantissaCodes[-1].Length

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := workerResult{
				literals: make([][]int64, epochs),
				mags:     make([]int64, 65),
			}

			for blockIdx := range jobs {
				epochID := blockIdx / blocksPerEpoch
				firstTxo := blockToTxo[blockIdx]
				lastTxo := int64(len(amounts))
				if blockIdx+1 < blocks {
					lastTxo = blockToTxo[blockIdx+1]
				}

				for txo := firstTxo; txo < lastTxo; txo++ {
					amount := amounts[txo]

					// Stage 1: Celebrity
					if aCode, ok := celebCodes[amount]; ok {
						local.stats.TotalBits += uint64(aCode.Length)
						local.stats.CelebrityHits++
						continue
					}

					// Stage 2: Scientific
					local.stats.TotalBits += uint64(esc1Len)
					ff := NewForensicFloat(amount)
					if mCode, ok := mantissaCodes[ff.Mantissa]; ok {
						local.stats.TotalBits += uint64(mCode.Length)
						if eCode, ok := expCodes[ff.Exponent]; ok {
							local.stats.TotalBits += uint64(eCode.Length)
						}
						local.stats.ScientificHits++
						continue
					}

					// Stage 3: Literal (Initial Pass)
					local.stats.TotalBits += uint64(esc2Len + 56)
					local.stats.LiteralHits++
					local.literals[epochID] = append(local.literals[epochID], amount)
					local.mags[bits.Len64(uint64(amount))]++
				}
			}
			resultsChan <- local
		}()
	}

	// Feed the workers
	for b := 0; b < blocks; b++ {
		jobs <- b
	}
	close(jobs)
	wg.Wait()
	close(resultsChan)

	// --- REDUCE PHASE ---
	finalStats := CompressionStats{}
	finalMags := make([]int64, 65)
	finalLiterals := make([][]int64, epochs)

	for res := range resultsChan {
		finalStats.TotalBits += res.stats.TotalBits
		finalStats.CelebrityHits += res.stats.CelebrityHits
		finalStats.ScientificHits += res.stats.ScientificHits
		finalStats.LiteralHits += res.stats.LiteralHits

		for i := 0; i < 65; i++ {
			finalMags[i] += res.mags[i]
		}

		for e := 0; e < epochs; e++ {
			if len(res.literals[e]) > 0 {
				finalLiterals[e] = append(finalLiterals[e], res.literals[e]...)
			}
		}
	}

	return finalStats, finalLiterals, finalMags
}

func ParallelSimulateCompressionWithKMeans(amounts []int64,
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
	numWorkers := 40

	jobs := make(chan int, 100)
	type workerResult struct {
		stats         CompressionStats
		peakStrengths [][7]int64
	}
	resultsChan := make(chan workerResult, numWorkers)
	var wg sync.WaitGroup

	esc1Len := celebCodes[-1].Length
	esc2Len := mantissaCodes[-1].Length

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := workerResult{
				peakStrengths: make([][7]int64, epochs),
			}

			for blockIdx := range jobs {
				epochID := blockIdx / blocksPerEpoch
				firstTxo := blockToTxo[blockIdx]
				lastTxo := int64(len(amounts))
				if blockIdx+1 < blocks {
					lastTxo = blockToTxo[blockIdx+1]
				}

				for txo := firstTxo; txo < lastTxo; txo++ {
					amount := amounts[txo]

					// Stage 1: Celebrity
					if aCode, ok := celebCodes[amount]; ok {
						local.stats.TotalBits += uint64(aCode.Length)
						local.stats.CelebrityHits++
						continue
					}

					// Stage 2: Scientific
					local.stats.TotalBits += uint64(esc1Len)
					ff := NewForensicFloat(amount)
					if mCode, ok := mantissaCodes[ff.Mantissa]; ok {
						local.stats.TotalBits += uint64(mCode.Length)
						if eCode, ok := expCodes[ff.Exponent]; ok {
							local.stats.TotalBits += uint64(eCode.Length)
						}
						local.stats.ScientificHits++
						continue
					}

					// Stage 2b: k-means
					local.stats.TotalBits += uint64(esc2Len)
					if epochToPhasePeaks[epochID] != nil {
						e, peakIdx, r := kmeans.ExpPeakResidual(amount, epochToPhasePeaks[epochID])
						if rCode, ok := residualCodes[r]; ok {
							local.stats.TotalBits += 3 // 3 bits peak index
							local.peakStrengths[epochID][peakIdx]++
							if eCode, ok := expCodes[int64(e)]; ok {
								local.stats.TotalBits += uint64(eCode.Length)
							}
							local.stats.TotalBits += uint64(rCode.Length)
							local.stats.KMeansHits++
							continue
						}
					} else {
						local.stats.TotalBits += 3 // Peak index escape
					}

					// Stage 3: Magnitude-encoded Literal
					mag := int64(bits.Len64(uint64(amount)))
					local.stats.TotalBits += uint64(magnitudeCodes[mag].Length) + uint64(mag)
					local.stats.LiteralHits++
				}
			}
			resultsChan <- local
		}()
	}

	// Dispatcher
	for b := 0; b < blocks; b++ {
		jobs <- b
	}
	close(jobs)
	wg.Wait()
	close(resultsChan)

	// Final Reduction
	globalStats := CompressionStats{}
	globalStrengths := make([][7]int64, epochs)
	for res := range resultsChan {
		globalStats.TotalBits += res.stats.TotalBits
		globalStats.CelebrityHits += res.stats.CelebrityHits
		globalStats.ScientificHits += res.stats.ScientificHits
		globalStats.KMeansHits += res.stats.KMeansHits
		globalStats.LiteralHits += res.stats.LiteralHits

		for e := 0; e < epochs; e++ {
			for p := 0; p < 7; p++ {
				globalStrengths[e][p] += res.peakStrengths[e][p]
			}
		}
	}

	return globalStats, globalStrengths
}
