package compress

import (
	"fmt"
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"github.com/KitchenMishap/pudding-huffman/kmeans"
	"math"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
)

type CompressionStats struct {
	TotalBits     uint64
	CelebrityHits uint64
	KMeansHits    uint64
	LiteralHits   uint64
}

func ParallelAmountStatistics(amounts []int64,
	blocksPerEpoch int,
	blocksPerPeriod int, // A period might be an epoch, or a microEpoch
	blockToTxo []int64,
	epochToCelebCodes []map[int64]huffman.BitCode,
	max_base_10_exp int) (CompressionStats, []int64, []int64) {

	blocks := len(blockToTxo)
	periods := blocks/blocksPerPeriod + 1

	fmt.Printf("Stage 1: ParallelAmountStatistics()\n")

	fmt.Printf("Parallel phase...\n")

	numWorkers := runtime.NumCPU()
	if numWorkers > 4 {
		numWorkers -= 2
	} // Leave some free for OS

	// Channels for distribution and collection
	jobs := make(chan int, 100) // Block numbers get squirted into here
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
				literalsSample: make([][]int64, periods),
				mags:           make([]int64, 65),
				expFreqs:       make([]int64, max_base_10_exp),
			}
			// Optimization: Pre-allocate a 'slab' for each epoch in this worker
			// Based on 5% sampling of a typical block, 5,000 is a very safe starting capacity
			for e := 0; e < periods; e++ {
				local.literalsSample[e] = make([]int64, 0, 5000)
			}

			for blockIdx := range jobs {
				epochID := blockIdx / blocksPerEpoch
				periodID := blockIdx / blocksPerPeriod
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
						local.literalsSample[periodID] = append(local.literalsSample[periodID], amount)
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

	fmt.Printf("Reduce phase (serial)...\n")
	// --- REDUCE PHASE ---
	finalStats := CompressionStats{}
	finalMags := make([]int64, 65)
	finalLiterals := make([][]int64, periods)
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

		for p := 0; p < periods; p++ {
			if len(res.literalsSample[p]) > 0 {
				finalLiterals[p] = append(finalLiterals[p], res.literalsSample[p]...)
			}
		}
	}

	return finalStats, finalMags, finalExpFreqs
}

func ParallelGatherResidualFrequenciesByExp10(amounts []int64,
	blocksPerEpoch int,
	blocksPerMicroEpoch int,
	blockToTxo []int64,
	epochToCelebCodes []map[int64]huffman.BitCode,
	microEpochToPhasePeaks [][]float64,
	max_base_10_exp int) ([20]map[int64]int64, // First result: outer array index is the exponent (number of decimal zeros). Inner map is freq for each possible residual
	map[int64]int64) { // Second result: frequencies of combined peak/harmonic index

	fmt.Printf("Stage 1.5, gather frequencies of residuals by exp magnitude\n")

	blocks := len(blockToTxo)

	fmt.Printf("Parallel phase...\n")

	numWorkers := runtime.NumCPU()
	if numWorkers > 4 {
		numWorkers -= 2
	} // Leave some free for OS

	// Channels for distribution and collection
	jobs := make(chan int, 100) // Block numbers get squirted into here
	if max_base_10_exp != 20 {
		panic("You changed a constant!")
	}
	type workerResult struct {
		// A separate map for each exponent level
		localResidualsByExp [20]map[int64]int64
		localCombinedFreq   map[int64]int64
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
			local.localCombinedFreq = make(map[int64]int64)
			for i := 0; i < max_base_10_exp; i++ {
				local.localResidualsByExp[i] = make(map[int64]int64, MAX_PHASE_PEAKS)
			}

			for blockIdx := range jobs {
				epochID := blockIdx / blocksPerEpoch
				microEpochID := blockIdx / blocksPerMicroEpoch
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

					if microEpochToPhasePeaks[microEpochID] == nil || len(microEpochToPhasePeaks[microEpochID]) == 0 {
						// This is probably an "early" week (epoch) where there weren't enough amount peaks to
						// do the k-means analysis on (other than common "celebrity" amounts which bypass this already)
						continue
					}

					e, peak, harmonic, r := kmeans.ExpPeakResidual(amount, microEpochToPhasePeaks[microEpochID])
					combined := peak*3 + harmonic
					local.localCombinedFreq[int64(combined)]++

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

	fmt.Printf("Reduce phase (serial)...\n")
	if max_base_10_exp != 20 {
		panic("You changed a constant!")
	}
	finalResidualsByExp := [20]map[int64]int64{}
	for i := 0; i < max_base_10_exp; i++ {
		finalResidualsByExp[i] = make(map[int64]int64)
	}
	finalCombinedFreqs := make(map[int64]int64)

	for res := range resultsChan {
		// Merge the 20 exponent maps from this worker
		for e := 0; e < max_base_10_exp; e++ {
			for r, count := range res.localResidualsByExp[e] {
				finalResidualsByExp[e][r] += count
			}
		}
		for combined := 0; combined < 24; combined++ {
			if freq, ok := res.localCombinedFreq[int64(combined)]; ok {
				finalCombinedFreqs[int64(combined)] += freq
			}
		}
	}

	return finalResidualsByExp, finalCombinedFreqs
}

const MAX_PHASE_PEAKS = 1000
const CSV_COLUMNS = 3

func ParallelSimulateCompressionWithKMeans(amounts []int64,
	blocksPerEpoch int,
	blocksPerMicroEpoch int,
	blockToTxo []int64,
	epochToCelebCodes []map[int64]huffman.BitCode,
	expCodes map[int64]huffman.BitCode,
	residualCodesByExp []map[int64]huffman.BitCode,
	magnitudeCodes map[int64]huffman.BitCode,
	combinedCodes map[int64]huffman.BitCode,
	microEpochToPhasePeaks [][]float64) (CompressionStats, [][CSV_COLUMNS]int64) {

	completed := int64(0) // Atomic int

	blocks := len(blockToTxo)
	microEpochs := blocks/blocksPerMicroEpoch + 1

	numWorkers := runtime.NumCPU()
	if numWorkers > 4 {
		numWorkers -= 2
	} // Leave some free for OS

	jobs := make(chan int, 100)
	type workerResult struct {
		stats         CompressionStats
		peakStrengths [][CSV_COLUMNS]int64
	}
	resultsChan := make(chan workerResult, numWorkers)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := workerResult{
				peakStrengths: make([][CSV_COLUMNS]int64, microEpochs),
			}

			for blockIdx := range jobs {
				epochID := blockIdx / blocksPerEpoch
				microEpochID := blockIdx / blocksPerMicroEpoch
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
					if amount > 0 && microEpochToPhasePeaks[microEpochID] != nil && len(microEpochToPhasePeaks[microEpochID]) > 0 {
						e, peakIdx, harmonic, r := kmeans.ExpPeakResidual(amount, microEpochToPhasePeaks[microEpochID])
						if rCode, ok := residualCodesByExp[e][r]; ok {
							//ghostCost = 3                           // Firstly there is a 3 bit cost to select which of the 7 stored peaks (for this epoch) we're near
							//ghostCost += 2                          // And some bits to store the harmonic
							// Now we have a huffman code for the combination of peak index and harmonic index.
							// This is the initial cost...
							ghostCost = combinedCodes[int64(3*peakIdx+harmonic)].Length
							if peakIdx < CSV_COLUMNS {
								local.peakStrengths[epochID][peakIdx]++ // Yes this IS supposed to be here. It's for oracle price prediction
							}
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

				// Report progress on completion
				done := atomic.AddInt64(&completed, 1)
				if done%1000 == 0 || done == int64(blocks) {
					fmt.Printf("\r> Progress: [%d/%d] blocks (%.1f%%)    ",
						done, blocks, float64(done)/float64(blocks)*100)
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
	fmt.Printf("\nDone that now\n")

	// Final Reduction
	globalStats := CompressionStats{}
	globalStrengths := make([][CSV_COLUMNS]int64, microEpochs)
	for res := range resultsChan {
		globalStats.TotalBits += res.stats.TotalBits
		globalStats.CelebrityHits += res.stats.CelebrityHits
		globalStats.KMeansHits += res.stats.KMeansHits
		globalStats.LiteralHits += res.stats.LiteralHits

		for me := 0; me < microEpochs; me++ {
			for p := 0; p < CSV_COLUMNS; p++ {
				globalStrengths[me][p] += res.peakStrengths[me][p]
			}
		}
	}

	return globalStats, globalStrengths
}
