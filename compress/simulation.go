package compress

import (
	"fmt"
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"github.com/KitchenMishap/pudding-huffman/kmeans"
	"math"
	"math/bits"
	"runtime"
	"sync"
)

type CompressionStats struct {
	TotalBits     uint64
	CelebrityHits uint64
	KMeansHits    uint64
	LiteralHits   uint64
}

func ParallelAmountStatistics(amounts []int64,
	blocksPerEpoch int,
	blockToTxo []int64,
	epochToCelebCodes []map[int64]huffman.BitCode,
	max_base_10_exp int) (CompressionStats, [][]int64, []int64, []int64) {

	blocks := len(blockToTxo)
	epochs := blocks/blocksPerEpoch + 1

	fmt.Printf("Parallel phase...")

	numWorkers := runtime.NumCPU()
	if numWorkers > 4 {
		numWorkers -= 2
	} // Leave some free for OS

	// Channels for distribution and collection
	jobs := make(chan int, 100)
	type workerResult struct {
		stats          CompressionStats
		literalsSample [][]int64
		mags           []int64 // Base-2 magnitudes (for literals)
		expFreqs       []int64 // Base-10 exponents (for K-Means)
	}
	resultsChan := make(chan workerResult, numWorkers)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := workerResult{
				literalsSample: make([][]int64, epochs),
				mags:           make([]int64, 65),
				expFreqs:       make([]int64, max_base_10_exp),
			}
			// Optimization: Pre-allocate a 'slab' for each epoch in this worker
			// Based on 5% sampling of a typical block, 5,000 is a very safe starting capacity
			for e := 0; e < epochs; e++ {
				local.literalsSample[e] = make([]int64, 0, 5000)
			}

			for blockIdx := range jobs {
				epochID := blockIdx / blocksPerEpoch
				firstTxo := blockToTxo[blockIdx]
				lastTxo := int64(len(amounts)) // Rare fallback
				if blockIdx+1 < blocks {       // Usual case
					lastTxo = blockToTxo[blockIdx+1]
				}

				for txo := firstTxo; txo < lastTxo; txo++ {
					amount := amounts[txo]

					// Stage 1: Celebrity
					if _, ok := epochToCelebCodes[epochID][amount]; ok {
						local.stats.CelebrityHits++
						continue
					}

					// (The new) Stage 2: Literal (Initial Pass)
					local.stats.LiteralHits++
					// The following append was probably a RAM Killer (froze my 768GB RAM machine!)
					// We now only sample only 5% of data in a block to train the kmeans with.
					// But we retain the first 100 samples, to avoid data starvation when blocks are small
					if txo-firstTxo < 100 || txo%20 == 0 {
						local.literalsSample[epochID] = append(local.literalsSample[epochID], amount)
					}
					local.mags[bits.Len64(uint64(amount))]++ // Increment for EVERY amount including zero
					if amount > 0 {                          // Guard against log10(0)
						exponent := int(math.Floor(math.Log10(float64(amount))))
						if exponent >= 0 && exponent < len(local.expFreqs) {
							local.expFreqs[exponent]++
						}
					}
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

	fmt.Printf("Reduce phase (serial)...")
	// --- REDUCE PHASE ---
	finalStats := CompressionStats{}
	finalMags := make([]int64, 65)
	finalLiterals := make([][]int64, epochs)
	finalExpFreqs := make([]int64, max_base_10_exp)

	for res := range resultsChan {
		finalStats.CelebrityHits += res.stats.CelebrityHits
		finalStats.LiteralHits += res.stats.LiteralHits

		for i := 0; i < 65; i++ {
			finalMags[i] += res.mags[i]
		}
		for i := 0; i < max_base_10_exp; i++ {
			finalExpFreqs[i] += res.expFreqs[i]
		}

		for e := 0; e < epochs; e++ {
			if len(res.literalsSample[e]) > 0 {
				finalLiterals[e] = append(finalLiterals[e], res.literalsSample[e]...)
			}
		}
	}

	return finalStats, finalLiterals, finalMags, finalExpFreqs
}

func ParallelGatherResidualFrequenciesByExp10(amounts []int64,
	blocksPerEpoch int,
	blockToTxo []int64,
	epochToCelebCodes []map[int64]huffman.BitCode,
	epochToPhasePeaks [][]float64,
	max_base_10_exp int) [20]map[int64]int64 { // The result: outer array index is the exponent (number of decimal zeros). Inner map is freq for each possible residual

	fmt.Printf("Stage 1.5, gather frequencies of residuals by exp magnitude")

	blocks := len(blockToTxo)

	fmt.Printf("Parallel phase...")

	numWorkers := runtime.NumCPU()
	if numWorkers > 4 {
		numWorkers -= 2
	} // Leave some free for OS

	// Channels for distribution and collection
	jobs := make(chan int, 100)
	if max_base_10_exp != 20 {
		panic("You changed a constant!")
	}
	type workerResult struct {
		// A separate map for each exponent level
		localResidualsByExp [20]map[int64]int64
	}
	resultsChan := make(chan workerResult, numWorkers)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if max_base_10_exp != 20 {
				panic("You changed a constant!")
			}
			local := workerResult{}
			for i := 0; i < max_base_10_exp; i++ {
				local.localResidualsByExp[i] = make(map[int64]int64, 1000)
			}

			for blockIdx := range jobs {
				epochID := blockIdx / blocksPerEpoch
				firstTxo := blockToTxo[blockIdx]
				lastTxo := int64(len(amounts)) // Rare fallback
				if blockIdx+1 < blocks {       // Usual case
					lastTxo = blockToTxo[blockIdx+1]
				}

				for txo := firstTxo; txo < lastTxo; txo++ {
					amount := amounts[txo]

					// Stage 1: Celebrity
					if _, ok := epochToCelebCodes[epochID][amount]; ok {
						continue
					}

					if epochToPhasePeaks[epochID] == nil || len(epochToPhasePeaks[epochID]) == 0 {
						// This is probably an "early" week (epoch) where there weren't enough amount peaks to
						// do the k-means analysis on (other than common "celebrity" amounts which bypass this already)
						continue
					}

					e, _, r := kmeans.ExpPeakResidual(amount, epochToPhasePeaks[epochID])

					if e >= 0 && e < max_base_10_exp {
						local.localResidualsByExp[e][r]++
					}
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

	fmt.Printf("Reduce phase (serial)...")
	if max_base_10_exp != 20 {
		panic("You changed a constant!")
	}
	finalResidualsByExp := [20]map[int64]int64{}
	for i := 0; i < max_base_10_exp; i++ {
		finalResidualsByExp[i] = make(map[int64]int64)
	}

	for res := range resultsChan {
		// Merge the 20 exponent maps from this worker
		for e := 0; e < max_base_10_exp; e++ {
			for r, count := range res.localResidualsByExp[e] {
				finalResidualsByExp[e][r] += count
			}
		}
	}

	return finalResidualsByExp
}

func ParallelSimulateCompressionWithKMeans(amounts []int64,
	blocksPerEpoch int,
	blockToTxo []int64,
	epochToCelebCodes []map[int64]huffman.BitCode,
	expCodes map[int64]huffman.BitCode,
	residualCodesByExp []map[int64]huffman.BitCode,
	magnitudeCodes map[int64]huffman.BitCode,
	epochToPhasePeaks [][]float64) (CompressionStats, [][7]int64) {

	blocks := len(blockToTxo)
	epochs := blocks/blocksPerEpoch + 1

	numWorkers := runtime.NumCPU()
	if numWorkers > 4 {
		numWorkers -= 2
	} // Leave some free for OS

	jobs := make(chan int, 100)
	type workerResult struct {
		stats         CompressionStats
		peakStrengths [][7]int64
	}
	resultsChan := make(chan workerResult, numWorkers)
	var wg sync.WaitGroup

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
				lastTxo := int64(len(amounts)) // Rare fallback
				if blockIdx+1 < blocks {       // Usual case
					lastTxo = blockToTxo[blockIdx+1]
				}

				for txo := firstTxo; txo < lastTxo; txo++ {
					amount := amounts[txo]

					// Stage 1: Celebrity cost (cost of MAXINT means celebrity status not available)
					// Intended to capture common numbers of satoshis like 50BTC and 0 sats
					// The celebrity codes are now PER EPOCH
					celebCost := math.MaxInt
					if aCode, ok := epochToCelebCodes[epochID][amount]; ok {
						celebCost = aCode.Length
					}

					// Stage 2: Ghost cost (maxint means ghost status not available)
					// Intended to capture the "ghosts" of round numbers in fiat-land, when they are converted to satoshis
					ghostCost := math.MaxInt
					// Amount 0 will trigger a log10(0) and things will go wrong. But we know amount 0 will be treated as a celeb or literal so we're not interested in the "ghost" cost of a zero
					if amount > 0 && epochToPhasePeaks[epochID] != nil && len(epochToPhasePeaks[epochID]) > 0 {
						e, peakIdx, r := kmeans.ExpPeakResidual(amount, epochToPhasePeaks[epochID])
						if rCode, ok := residualCodesByExp[e][r]; ok {
							ghostCost = 3                           // Firstly there is a 3 bit cost to select which of the 7 stored peaks (for this epoch) we're near
							local.peakStrengths[epochID][peakIdx]++ // Yes this IS supposed to be here. It's for oracle price prediction
							if eCode, ok := expCodes[int64(e)]; ok {
								ghostCost += eCode.Length // Secondly there are some bits to encode the number of decimal points (exp)
							} else {
								panic("missing exp code")
							}
							ghostCost += rCode.Length // Thirdly there are some bits to encode the residual distance from the peak
						}
					}

					// Stage 3: Magnitude-encoded Literal cost. Always available.
					mag := int64(bits.Len64(uint64(amount))) // Number of bits in the literal (after the binary 0's)
					// COULD BE 0 BITS! BE AWARE!
					// One bit saving is clever. Because we can assume "0" is a celebrity (in fact we found that
					// it's the most popular celebrity!), we know that amount is non zero. So we don't need
					// to store mag bits, because we ALWAYS ALREADY KNOW that the first bit will be a 1. Why store it?
					const oneBitSaving = 1
					literalCost := magnitudeCodes[mag].Length // A huffman code telling us the magnitude (number of bits)
					if mag > 0 {
						literalCost += int(mag) - oneBitSaving // The bits themselves (minus the clever one bit saving)
					} else {
						// The magnitude is zero. The number is zero bits long. The NUMBER IS ZERO. There are no bits
						literalCost += 0
					}

					selectorCost := 2 // Two bits to select between (00) Literal, (01) Celebrity, (10) Ghost, and (11) That which is prophesied ;-)
					// Choose whichever is cheapest
					choice := 0
					chosenCost := literalCost
					if celebCost < chosenCost {
						choice = 1
						chosenCost = celebCost
					}
					if ghostCost < chosenCost {
						choice = 2
						chosenCost = ghostCost
					}
					cost := selectorCost + chosenCost

					if choice == 0 {
						local.stats.LiteralHits++
					} else if choice == 1 {
						local.stats.CelebrityHits++
					} else if choice == 2 {
						local.stats.KMeansHits++
					}

					if cost > 200 {
						// That's just silly.
						cost = 64 // Fallback to let the simulation continue
					}

					local.stats.TotalBits += uint64(cost)
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
