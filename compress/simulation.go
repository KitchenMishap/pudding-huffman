package compress

import (
	"context"
	"errors"
	"fmt"
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"github.com/KitchenMishap/pudding-huffman/kmeans"
	"github.com/KitchenMishap/pudding-shed/chainreadinterface"
	"golang.org/x/sync/errgroup"
	"math"
	"math/bits"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type CompressionStats struct {
	TotalBits     uint64
	LiteralHits   uint64
	LiteralBits   uint64
	CelebrityHits uint64
	CelebrityBits uint64
	GhostHits     uint64
	GhostBits     uint64
	RestHits      uint64
	RestBits      uint64
}

func ParallelAmountStatistics(chain chainreadinterface.IBlockChain,
	handles chainreadinterface.IHandleCreator,
	blocks int64,
	blocksPerEpoch int64,
	epochToCelebCodes []map[int64]huffman.BitCode,
	max_base_10_exp int) (CompressionStats, []int64, []int64, error) {

	sJob := "Stage 1: ParallelAmountStatistics() (PARALLEL by block)"
	fmt.Printf("\t%s\n", sJob)
	tJob := time.Now()

	blocksDone := uint64(0) // atomic int

	workersDivider := 1
	numWorkers := runtime.NumCPU() / workersDivider
	if numWorkers > 8 {
		numWorkers -= 4 // Save some for OS
	}
	//numWorkers = 1 // Serial test! I may be some time

	// Channels for distribution and collection
	jobsChan := make(chan int64, 100) // Block numbers get squirted into here
	type workerResult struct {
		stats    CompressionStats
		mags     []int64 // Base-2 magnitudes (for literals)
		expFreqs []int64 // Base-10 exponents (for K-Means)
	}
	resultsChan := make(chan workerResult, numWorkers)
	var wg sync.WaitGroup

	// Create an errgroup and a context
	g, ctx := errgroup.WithContext(context.Background())

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		g.Go(func() error { // Use the errgroup instead of "go func() {"
			defer wg.Done()
			local := workerResult{
				mags:     make([]int64, 65),
				expFreqs: make([]int64, max_base_10_exp),
			}

			for blockIdx := range jobsChan {
				// Check if another worker already failed
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				epochID := blockIdx / blocksPerEpoch

				blockHandle, err := handles.BlockHandleByHeight(int64(blockIdx))
				if err != nil {
					return err
				}
				block, err := chain.BlockInterface(blockHandle)
				if err != nil {
					return err
				}
				tCount, err := block.TransactionCount()
				if err != nil {
					return err
				}
				for t := int64(0); t < tCount; t++ {
					transHandle, err := block.NthTransaction(t)
					if err != nil {
						return err
					}
					trans, err := chain.TransInterface(transHandle)
					if err != nil {
						return err
					}
					txoAmounts, err := trans.AllTxoSatoshis()
					if err != nil {
						return err
					}
					for _, sats := range txoAmounts {
						amount := sats

						// Stage 1: Celebrity
						if _, ok := epochToCelebCodes[epochID][amount]; ok {
							//local.stats.CelebrityHits++	No statistics in this run!
							continue
						}

						// (The new) Stage 2: Literal (Initial Pass)
						//local.stats.LiteralHits++			No statistics in this run!
						local.mags[bits.Len64(uint64(amount))]++ // Increment for EVERY amount including zero
						if amount > 0 {                          // Guard against log10(0)
							exponent := int(math.Floor(math.Log10(float64(amount))))
							if exponent >= 0 && exponent < len(local.expFreqs) {
								local.expFreqs[exponent]++
							}
						}
					}
				} // For transaction
				done := atomic.AddUint64(&blocksDone, 1)
				if done%100000 == 0 || done == uint64(blocks) {
					fmt.Printf("\r\tProgress: %.1f   ", float64(done*100)/float64(blocks))
				}
			} // For block
			resultsChan <- local
			return nil
		})
	}

	// Feed the Channel (The Producer). This is now errgroup context-aware
	go func() {
		defer close(jobsChan)
		for b := int64(0); b < blocks; b++ {
			select { // Note: NOT a switch statement!
			case jobsChan <- b: // This happens if a worker is free to be fed an epoch ID
			case <-ctx.Done(): // This happens if a worker returned an err
				return
			}
		}
	}()
	// Wait for completion and handle the error
	if err := g.Wait(); err != nil {
		return CompressionStats{}, nil, nil, err
	}

	wg.Wait()
	close(resultsChan)
	fmt.Printf("\n")
	jobElapsed := time.Since(tJob)
	fmt.Printf("\t%s: Job took: [%5.1f min]\n", sJob, jobElapsed.Minutes())

	// --- REDUCE PHASE ---
	sJob = "Stage 1: ParallelAmountStatistics() (SERIAL Reduction)"
	fmt.Printf("%s\n", sJob)
	tJob = time.Now()

	finalStats := CompressionStats{}
	finalMags := make([]int64, 65)
	finalExpFreqs := make([]int64, max_base_10_exp)

	for res := range resultsChan {
		// No statistics for this run!
		//finalStats.CelebrityHits += res.stats.CelebrityHits
		//finalStats.LiteralHits += res.stats.LiteralHits

		for i := 0; i < 65; i++ {
			finalMags[i] += res.mags[i]
		}
		for i := 0; i < max_base_10_exp; i++ {
			finalExpFreqs[i] += res.expFreqs[i]
		}
	}

	jobElapsed = time.Since(tJob)
	fmt.Printf("\t%s: Job took: [%5.1f min]\n", sJob, jobElapsed.Minutes())

	return finalStats, finalMags, finalExpFreqs, nil
}

// const MaxResidual = 500_000
const MaxResidual = 500_000
const ResidualSliceWidth = (MaxResidual * 2) + 1
const MaxCombined = 24

func ParallelGatherResidualFrequenciesByExp10(chain chainreadinterface.IBlockChain, handles chainreadinterface.IHandleCreator,
	blocksPerEpoch int64,
	blocksPerMicroEpoch int64,
	blocks int64,
	epochToCelebCodes []map[int64]huffman.BitCode,
	microEpochToPhasePeaks []kmeans.MantissaArray,
	max_base_10_exp int) (*[20][ResidualSliceWidth]int64, // First result: outer array index is the exponent (number of decimal zeros). Inner array is freq for each possible residual
	*[MaxCombined]int64) { // Second result: frequencies of combined peak/harmonic index

	// TotalResidualFreqs is a flat array of all possible residuals
	if max_base_10_exp != 20 {
		panic("You changed a constant!")
	}
	var TotalResidualFreqsByExp = [20][ResidualSliceWidth]int64{}
	var TotalCombinedFreq = [MaxCombined]int64{}

	tJob := time.Now()
	sJob := "Stage 1.5, gather frequencies of residuals by exp magnitude (PARALLEL by block)"
	fmt.Printf("%s\n", sJob)

	fmt.Printf("\tParallel phase...\n")
	workersDivider := 1
	numWorkers := runtime.NumCPU() / workersDivider
	if numWorkers > 8 {
		numWorkers -= 4 // Save some for OS
	}
	//numWorkers = 1 // Serial test! I may be some time

	// Channels for distribution and collection
	jobsChan := make(chan int64, 100) // Block numbers get squirted into here
	if max_base_10_exp != 20 {
		panic("You changed a constant!")
	}

	resultsChan := make(chan int, numWorkers)
	var wg sync.WaitGroup

	// Create an errgroup and a context
	g, ctx := errgroup.WithContext(context.Background())

	blocksDone := int64(0) // atomic

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		g.Go(func() error { // Use the errgroup instead of "go func() {"
			defer wg.Done()

			for blockIdx := range jobsChan {
				// Check if another worker already failed
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				epochID := blockIdx / blocksPerEpoch
				microEpochID := blockIdx / blocksPerMicroEpoch

				blockHandle, err := handles.BlockHandleByHeight(int64(blockIdx))
				if err != nil {
					return err
				}
				block, err := chain.BlockInterface(blockHandle)
				if err != nil {
					return err
				}
				tCount, err := block.TransactionCount()
				if err != nil {
					return err
				}
				for t := int64(0); t < tCount; t++ {
					transHandle, err := block.NthTransaction(t)
					if err != nil {
						return err
					}
					trans, err := chain.TransInterface(transHandle)
					if err != nil {
						return err
					}
					txoAmounts, err := trans.AllTxoSatoshis()
					if err != nil {
						return err
					}
					for _, sats := range txoAmounts {
						amount := sats

						// Stage 1: Celebrity
						if _, ok := epochToCelebCodes[epochID][amount]; ok {
							continue
						}

						if microEpochToPhasePeaks[microEpochID] == nil || microEpochToPhasePeaks[microEpochID].Len() == 0 {
							// This is probably an "early" week (epoch) where there weren't enough amount peaks to
							// do the k-means analysis on (other than common "celebrity" amounts which bypass this already)
							continue
						}

						if amount == 0 {
							// We don't want to count residuals for amount 0.
							// It blows up the exp in ExpPeakResidual
							continue
						}

						e, peak, harmonic, r := kmeans.ExpPeakResidual(amount, microEpochToPhasePeaks[microEpochID])
						combined := peak*3 + harmonic
						atomic.AddInt64(&(TotalCombinedFreq[combined]), 1)

						if e >= 0 && e < max_base_10_exp {
							if r >= -MaxResidual && r <= MaxResidual {
								const zeroOffset = MaxResidual
								atomic.AddInt64(&(TotalResidualFreqsByExp[e][r+zeroOffset]), 1)
							}
						}
					}
				} // for tranaction
				done := atomic.AddInt64(&blocksDone, 1)
				fmt.Printf("\r\tProgress %.1f   ", float64(done*100)/float64(blocks))
			} // for block
			resultsChan <- 123
			return nil
		})
	}

	// Feed the workers
	go func() {
		defer close(jobsChan)
		for b := int64(0); b < blocks; b++ {
			select { // Note: NOT a switch statement!
			case jobsChan <- b: // This happens if a worker is free to be fed an epoch ID
			case <-ctx.Done(): // This happens if a worker returned an err
				return
			}
		}
	}()

	wg.Wait()
	close(resultsChan)

	fmt.Printf("\n")
	jobElapsed := time.Since(tJob)
	fmt.Printf("\t%s: Job took: [%5.1f min]\n", sJob, jobElapsed.Minutes())

	tJob = time.Now()
	sJob = "\t(Serial Reduction)"
	fmt.Printf("%s\n", sJob)

	if max_base_10_exp != 20 {
		panic("You changed a constant!")
	}

	for _ = range resultsChan {
	}

	jobElapsed = time.Since(tJob)
	fmt.Printf("\t%s: Job took: [%5.1f min]\n", sJob, jobElapsed.Minutes())

	return &TotalResidualFreqsByExp, &TotalCombinedFreq
}

func bucketCount(beans int64, beansPerBucket int64) int64 {
	return (beans + beansPerBucket - 1) / beansPerBucket
}

const CSV_COLUMNS = 3

const GHOSTS_ARE_RICE = true

func ParallelSimulateCompressionWithKMeans(chain chainreadinterface.IBlockChain, handles chainreadinterface.IHandleCreator,
	blocksPerEpoch int64,
	blocksPerMicroEpoch int64,
	blocks int64,
	epochToCelebCodes []map[int64]huffman.BitCode,
	expCodes map[int64]huffman.BitCode,
	residualCodesSlicesByExp [][]huffman.BitCode,
	residualCodesRiceSlicesByExp [][]huffman.BitCode,
	magnitudeCodes map[int64]huffman.BitCode,
	combinedCodes map[int64]huffman.BitCode,
	microEpochToPhasePeaks []kmeans.MantissaArray) (CompressionStats,
	[][CSV_COLUMNS]int64,
	*[2000000000]byte, // Which outputs of each transaction to exclude from next k-means peak detection
	[]map[int64]bool) { // Which celebrities to exclude (per epoch) from next k-means peak detection

	fmt.Printf("\t(PARALLEL, by block)\n")

	const blocksInBatch = 1000

	mutex := &sync.Mutex{}
	podiumForLiterals := huffman.NewPodium()
	podiumForCelebrities := huffman.NewPodium()
	podiumForGhosts := huffman.NewPodium()
	podiumForRests := huffman.NewPodium()
	podiumForGL := huffman.NewPodium()

	completed := int64(0) // Atomic int

	microEpochs := bucketCount(blocks, blocksPerMicroEpoch)
	epochs := bucketCount(blocks, blocksPerEpoch)

	workersDivider := 1
	numWorkers := runtime.NumCPU() / workersDivider
	if numWorkers > 8 {
		numWorkers -= 4 // Save some for OS
	}
	//numWorkers = 1 // Serial test! I may be some time

	blocksChan := make(chan int64, 100)
	type workerResult struct {
		stats         CompressionStats
		peakStrengths [][CSV_COLUMNS]int64
	}
	resultsChan := make(chan workerResult, numWorkers)
	var wg sync.WaitGroup

	// Create an errgroup and a context
	g, ctx := errgroup.WithContext(context.Background())

	transToExcludedOutput := [2000000000]byte{}
	celebsToExcludeOutput := make([]map[int64]bool, epochs)
	celebsMutexes := make([]sync.Mutex, epochs)
	for i := int64(0); i < epochs; i++ {
		celebsToExcludeOutput[i] = make(map[int64]bool)
		celebsMutexes[i] = sync.Mutex{}
	}

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		g.Go(func() error { // Use the errgroup instead of "go func() {"
			defer wg.Done()
			local := workerResult{
				peakStrengths: make([][CSV_COLUMNS]int64, microEpochs),
			}

			for blockThousand := range blocksChan {
				// Check if another worker already failed
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				for blockIdx := blockThousand; blockIdx < blockThousand+blocksInBatch && blockIdx < blocks; blockIdx++ {
					epochID := blockIdx / blocksPerEpoch
					microEpochID := blockIdx / blocksPerMicroEpoch
					doPodium := (epochID == epochs-1)

					blockHandle, err := handles.BlockHandleByHeight(int64(blockIdx))
					if err != nil {
						return err
					}
					block, err := chain.BlockInterface(blockHandle)
					if err != nil {
						return err
					}
					tCount, err := block.TransactionCount()
					if err != nil {
						return err
					}
					for t := int64(0); t < tCount; t++ {
						outputsAndFeesAmounts := make([]int64, 0, 100)

						transHandle, err := block.NthTransaction(t)
						if err != nil {
							return err
						}
						if !transHandle.HeightSpecified() {
							return errors.New("transaction height not specified")
						}
						transIndex := transHandle.Height()
						transToExcludedOutput[transIndex] = 255 // Unitl we find an actual output to be excluded
						trans, err := chain.TransInterface(transHandle)
						if err != nil {
							return err
						}
						txoAmounts, err := trans.AllTxoSatoshis()
						if err != nil {
							return err
						}
						for _, sats := range txoAmounts {
							amount := sats
							outputsAndFeesAmounts = append(outputsAndFeesAmounts, amount)
						}
						// There is an extra amount we have to encode... fees.
						// This is because fees are needed to infer the output that gets the two bit "the rest" code.
						// For a transaction with n outputs, the encoded fees are added after the n outpus' codes
						fees := rand.Intn(1000) + 1000 // Just a simulation for now... ToDo
						outputsAndFeesAmounts = append(outputsAndFeesAmounts, int64(fees))

						// We have all the outputs and the fees for the transaction
						// Work out the bit cost for every one of these (BEFORE we choose which is most bit-expensive)
						bigCode := huffman.BitCode{0xFFFFFFFFFFFFFFFF, 64}
						celebSelector := huffman.BitCode{0b00, 2}
						ghostSelector := huffman.BitCode{0b01, 2}
						literalSelector := huffman.BitCode{0b10, 2}
						restSelector := huffman.BitCode{0b11, 2}

						outputsAndFeesCodes := make([]huffman.BitCode, len(outputsAndFeesAmounts))
						outputsAndFeesEncodingChoice := make([]huffman.BitCode, len(outputsAndFeesAmounts))
						outputsAndFeesQuotes := make([]string, len(outputsAndFeesAmounts))
						for c, amount := range outputsAndFeesAmounts {

							// Stage 1: Celebrity cost
							// Intended to capture common numbers of satoshis like 50BTC and 0 sats
							// The celebrity codes are now PER EPOCH
							celebCode := bigCode
							celebQuote := "?"
							if aCode, ok := epochToCelebCodes[epochID][amount]; ok {
								celebCode = huffman.JoinBitCodes(celebSelector, aCode)
								if doPodium {
									if amount >= 100000 {
										celebQuote = "Celeb BTC amount: " + strconv.FormatFloat(float64(amount)/100000000, 'f', 8, 64) + " BTC"
									} else {
										celebQuote = "Celeb BTC amount: " + strconv.FormatInt(amount, 10) + " sats"
									}
								}
								// amount is a winning (cheap) celeb for this epoch
								// exclude amount from the next k-means peak detection run
								celebsMutexes[epochID].Lock()
								celebsToExcludeOutput[epochID][amount] = true
								celebsMutexes[epochID].Unlock()
							}

							// Stage 2: Ghost cost (maxint means ghost status not available)
							// Intended to capture the "ghosts" of round numbers in fiat-land, when they are converted to satoshis
							ghostCode := bigCode
							ghostQuote := "?"
							// Amount 0 will trigger a log10(0) and things will go wrong. But we know amount 0 will
							// be treated as a celeb or literal so we're not interested in the "ghost" cost of a zero
							if amount > 0 && microEpochToPhasePeaks[microEpochID] != nil && microEpochToPhasePeaks[microEpochID].Len() > 0 {
								e, peakIdx, harmonic, r := kmeans.ExpPeakResidual(amount, microEpochToPhasePeaks[microEpochID])
								residualCodes := residualCodesSlicesByExp[e]
								zeroOffset := (len(residualCodes) - 1) / 2
								huffmanIndex := r + int64(zeroOffset)
								if huffmanIndex < 0 || huffmanIndex >= int64(len(residualCodes)) {
									// residual is too -ve or too +ve
									// We won't be storing this as a ghost!
								} else {
									rCode := residualCodes[huffmanIndex]

									// Now we have a huffman code for the combination of peak index and harmonic index.
									// This is the initial cost...
									combinedCode := combinedCodes[int64(3*peakIdx+harmonic)]
									if peakIdx < CSV_COLUMNS {
										local.peakStrengths[epochID][peakIdx]++ // Yes this IS supposed to be here. It's for oracle price prediction
									}
									if eCode, ok := expCodes[int64(e)]; ok {
										ghostCode = huffman.JoinBitCodes(ghostSelector, combinedCode, eCode, rCode)
										if doPodium {
											// 4 digit peak value in sats
											digitsSats := int64(math.Round(microEpochToPhasePeaks[microEpochID].Get10toPow(peakIdx, 3)))
											ghostQuote = strconv.FormatInt(digitsSats, 10) + "sats (being harmonic "
											ghostQuote += strconv.FormatInt(int64(harmonic), 10) + " of peak "
											ghostQuote += strconv.FormatInt(int64(peakIdx), 10) + ") of the era, x 10e"
											ghostQuote += strconv.FormatInt(int64(e-3), 10) + " and residual "
											ghostQuote += strconv.FormatInt(r, 10)
										}
									} else {
										panic("missing exp code")
									}
								}
							}

							riceGhostCode := bigCode
							riceGhostQuote := "?"
							// Amount 0 will trigger a log10(0) and things will go wrong. But we know amount 0 will
							// be treated as a celeb or literal so we're not interested in the "ghost" cost of a zero
							if amount > 0 && microEpochToPhasePeaks[microEpochID] != nil && microEpochToPhasePeaks[microEpochID].Len() > 0 {
								e, peakIdx, harmonic, r := kmeans.ExpPeakResidual(amount, microEpochToPhasePeaks[microEpochID])
								residualCodes := residualCodesRiceSlicesByExp[e]
								zeroOffset := (len(residualCodes) - 1) / 2
								riceIndex := r + int64(zeroOffset)
								if riceIndex < 0 || riceIndex >= int64(len(residualCodes)) {
									// residual is too -ve or too +ve
									// We won't be storing this as a ghost!
								} else {
									rCode := residualCodes[riceIndex]

									// Now we have a HUFFMAN (yes) code for the combination of peak index and harmonic index.
									// This is the initial cost...
									combinedCode := combinedCodes[int64(3*peakIdx+harmonic)]
									if peakIdx < CSV_COLUMNS {
										//										local.peakStrengths[epochID][peakIdx]++ // Already done this for huffman ghosts
									}
									if eCode, ok := expCodes[int64(e)]; ok {
										riceGhostCode = huffman.JoinBitCodes(ghostSelector, combinedCode, eCode, rCode)
										if doPodium {
											// 4 digit peak value in sats
											digitsSats := int64(math.Round(microEpochToPhasePeaks[microEpochID].Get10toPow(peakIdx, 3)))
											riceGhostQuote = strconv.FormatInt(digitsSats, 10) + "sats (being harmonic "
											riceGhostQuote += strconv.FormatInt(int64(harmonic), 10) + " of peak "
											riceGhostQuote += strconv.FormatInt(int64(peakIdx), 10) + ") of the era, x 10e"
											riceGhostQuote += strconv.FormatInt(int64(e-3), 10) + " and residual "
											riceGhostQuote += strconv.FormatInt(r, 10)
										}
									} else {
										panic("missing exp code")
									}
								}
							}

							// Stage 3: Magnitude-encoded Literal cost. Always available.
							literalCode := bigCode
							literalQuote := "?"
							mag := int64(bits.Len64(uint64(amount))) // Number of bits in the literal (after the binary 0's)
							// COULD BE 0 BITS! BE AWARE!
							// One bit saving is clever. Because we can assume "0" is a celebrity (in fact we found that
							// it's the most popular celebrity!), we know that amount is non zero. So we don't need
							// to store mag bits, because we ALWAYS ALREADY KNOW that the first bit will be a 1. Why store it?
							const oneBitSaving = 1
							magCode := magnitudeCodes[mag] // A huffman code telling us the magnitude (number of bits)
							var bts uint64
							var bitsCount int
							var bitsCode huffman.BitCode
							if mag > 0 {
								bts = uint64(amount ^ (1 << (mag - 1))) // Take off the clever missing 1
								bitsCount = int(mag - oneBitSaving)
								bitsCode = huffman.BitCode{bts, bitsCount}
							} else {
								// The magnitude is zero. The number is zero bits long. The NUMBER IS ZERO. There are no bits
								bitsCode = huffman.BitCode{0, 0}
							}
							literalCode = huffman.JoinBitCodes(literalSelector, magCode, bitsCode)
							if doPodium {
								literalQuote = "Literal: " + strconv.FormatInt(amount, 10) + " sats"
							}
							// Choose whichever choice of encoding is cheapest (ignoring restSelector for now)
							choice := literalSelector
							chosenCode := literalCode
							chosenQuote := literalQuote
							if celebCode.Length < chosenCode.Length {
								choice = celebSelector
								chosenCode = celebCode
								chosenQuote = celebQuote
							}
							if GHOSTS_ARE_RICE {
								ghostCode = riceGhostCode
								ghostQuote = riceGhostQuote
							}
							if ghostCode.Length < chosenCode.Length {
								choice = ghostSelector
								chosenCode = ghostCode
								chosenQuote = ghostQuote
							}

							outputsAndFeesCodes[c] = chosenCode
							outputsAndFeesEncodingChoice[c] = choice
							outputsAndFeesQuotes[c] = chosenQuote
						}
						// Find the most costly output (or fees) of this transaction in terms of bitcount
						mostExpensive := int(0)
						loser := -1
						for c, code := range outputsAndFeesCodes {
							if code.Length > mostExpensive {
								mostExpensive = code.Length
								loser = c
							}
						}
						outputsAndFeesCodes[loser] = restSelector // Nothing else is needed for this output!
						outputsAndFeesEncodingChoice[loser] = restSelector
						outputsAndFeesQuotes[loser] = "Rest: You can work out this amount from the rest of the transaction"

						transactionBitcount := 0
						for c, code := range outputsAndFeesCodes {
							transactionBitcount += code.Length
							if outputsAndFeesEncodingChoice[c] == literalSelector {
								local.stats.LiteralHits++
								local.stats.LiteralBits += uint64(code.Length)
							}
							if outputsAndFeesEncodingChoice[c] == celebSelector {
								local.stats.CelebrityHits++
								local.stats.CelebrityBits += uint64(code.Length)
							}
							if outputsAndFeesEncodingChoice[c] == ghostSelector {
								local.stats.GhostHits++
								local.stats.GhostBits += uint64(code.Length)
							}
							if outputsAndFeesEncodingChoice[c] == restSelector {
								local.stats.RestHits++
								local.stats.RestBits += uint64(code.Length)
								// For this transaction (using the transaction's height as an index), we
								// make a note of which transaction output (c) is to be excluded from the next
								// round of ghost k-means peak estimation. We have room to store this as a byte.
								// For a transaction with n outputs, if we specify a greater number (ie we store >=n),
								// this means DO NOT EXCLUDE ANY OUTPUT. Also, regardless of n, 255 is a special value
								// that also means "do not exclude any output"
								if c < 255 {
									// make a note to exclude output c from next k-means ghost peak estimation phase
									transToExcludedOutput[transIndex] = byte(c)
								} else {
									// Not enough room in the byte to encode c.
									// We are forced to use the special code 255 which always means "do not exclude any"
									transToExcludedOutput[transIndex] = 255
								}
							}
						}

						local.stats.TotalBits += uint64(transactionBitcount)

						if doPodium {
							mutex.Lock()
							for c, code := range outputsAndFeesCodes {
								if outputsAndFeesEncodingChoice[c] == literalSelector {
									podiumForLiterals.Submit(code, outputsAndFeesQuotes[c])
									podiumForGL.Submit(code, outputsAndFeesQuotes[c])
								}
								if outputsAndFeesEncodingChoice[c] == celebSelector {
									podiumForCelebrities.Submit(code, outputsAndFeesQuotes[c])
								}
								if outputsAndFeesEncodingChoice[c] == ghostSelector {
									podiumForGhosts.Submit(code, outputsAndFeesQuotes[c])
									podiumForGL.Submit(code, outputsAndFeesQuotes[c])
								}
								if outputsAndFeesEncodingChoice[c] == restSelector {
									podiumForRests.Submit(code, outputsAndFeesQuotes[c])
								}
							}
							mutex.Unlock()
						}
					} // For transactions
				} // for blockIdx
				// Report progress on completion
				done := atomic.AddInt64(&completed, blocksInBatch)
				if done%10000 == 0 || done == blocks {
					fmt.Printf("\r\tProgress: %.1f%%    ", float64(100*done)/float64(blocks))
				}
			}
			resultsChan <- local
			return nil
		})
	}
	go func() {
		defer close(blocksChan)
		for b := int64(0); b < blocks; b += blocksInBatch {
			select { // Note: NOT a switch statement!
			case blocksChan <- b: // This happens if a worker is free to be fed an epoch ID
			case <-ctx.Done(): // This happens if a worker returned an err
				return
			}
		}
	}()

	wg.Wait()
	close(resultsChan)
	fmt.Printf("\nDone that now\n")

	// Final Reduction
	globalStats := CompressionStats{}
	globalStrengths := make([][CSV_COLUMNS]int64, microEpochs)
	for res := range resultsChan {
		globalStats.TotalBits += res.stats.TotalBits
		globalStats.CelebrityHits += res.stats.CelebrityHits
		globalStats.CelebrityBits += res.stats.CelebrityBits
		globalStats.GhostHits += res.stats.GhostHits
		globalStats.GhostBits += res.stats.GhostBits
		globalStats.LiteralHits += res.stats.LiteralHits
		globalStats.LiteralBits += res.stats.LiteralBits
		globalStats.RestHits += res.stats.RestHits
		globalStats.RestBits += res.stats.RestBits

		for me := int64(0); me < microEpochs; me++ {
			for p := 0; p < CSV_COLUMNS; p++ {
				globalStrengths[me][p] += res.peakStrengths[me][p]
			}
		}
	}

	n := 10
	fmt.Printf("=========================================================\n")
	fmt.Printf("==== HERE ARE THE PODIUMS! (For just the last Epoch) ====\n")
	fmt.Printf("=========================================================\n")
	fmt.Printf("Top %d Ghost & Literal codes together\n", n)
	podiumForGL.Rank(n)
	fmt.Printf("=====================\n")
	fmt.Printf("Top %d Literal codes:\n", n)
	podiumForLiterals.Rank(n)
	fmt.Printf("=======================\n")
	fmt.Printf("Top %d Celebrity codes:\n", n)
	podiumForCelebrities.Rank(n)
	fmt.Printf("===================\n")
	fmt.Printf("Top %d Ghost codes:\n", n)
	podiumForGhosts.Rank(n)
	fmt.Printf("=====================\n")
	fmt.Printf("Top %d TheRest codes:\n", n)
	podiumForRests.Rank(n)

	return globalStats, globalStrengths, &transToExcludedOutput, celebsToExcludeOutput
}
