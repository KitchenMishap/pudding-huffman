package kmeans

import (
	"context"
	"errors"
	"fmt"
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"github.com/KitchenMishap/pudding-shed/chainreadinterface"
	"golang.org/x/sync/errgroup"
	"math"
	"math/rand"
	"runtime"
	"sync/atomic"
	"time"
)

// Switch between float32 and float64 here
type KFloat = float32

func FindEpochPeaksMain(amounts []int64, deterministic *rand.Rand) []float64 {
	// 1. Map all mantissas to the 0.0 to 1.0 "Clock face"
	phases := make([]KFloat, len(amounts))
	for i, v := range amounts {
		// log10(v) % 1 gives the position on the clock
		_, ph := math.Modf(math.Log10(float64(v)))
		phases[i] = KFloat(ph)
		if phases[i] < 0.0 {
			phases[i] += 1.0
		}
	}

	n := 4
	nPeaks := findEpochPeaks(amounts, n, deterministic)
	if len(nPeaks) < n {
		return nil
	}
	bestPeak := nPeaks[0]
	_, bestBadness := FindBestAnchor(phases, bestPeak)
	for _, peak := range nPeaks {
		peak, badness := FindBestAnchor(phases, peak)
		if badness < bestBadness {
			bestBadness = badness
			bestPeak = peak
		}
	}

	result := []float64{}

	// fundamental times logs representing 1.0, 1.1, 1.2, ..., 9.9
	//for i := float64(1.00); i < 10.00; i += 0.1 {
	//	result = append(result, math.Mod(float64(bestPeak)+math.Log10(i), 1))
	//}

	// OR just 1,2,5
	result = append(result, math.Mod(float64(bestPeak)+math.Log10(1), 1))
	result = append(result, math.Mod(float64(bestPeak)+math.Log10(2), 1))
	result = append(result, math.Mod(float64(bestPeak)+math.Log10(5), 1))

	return result
}

func FindBestAnchor(phases []KFloat, initialPeak KFloat) (bestAnchor KFloat, score KFloat) {
	spokes := []KFloat{0.0, 0.30103, 0.69897} // log10 of 1, 2, 5

	bestScore := KFloat(math.MaxFloat32)
	var absoluteBest KFloat

	for _, shift := range spokes {
		// Hypothesis: What if the initial peak is actually the '1', '2', or '5'?
		// We shift the anchor so the template aligns the initial peak with that spoke.
		testAnchor := KFloat(math.Mod(float64(initialPeak)-float64(shift)+1.0, 1.0))

		refined, currentBadness := refineAndScore(phases, testAnchor, spokes)

		if currentBadness < bestScore {
			bestScore = currentBadness
			absoluteBest = refined
		}
	}
	return absoluteBest, bestScore
}

func refineAndScore(phases []KFloat, startAnchor KFloat, spokes []KFloat) (KFloat, KFloat) {
	const guffThreshold = 0.05 // Should never be more than 0.12. If it gets to 0.15, we lose the ability to
	// recognize the "10" of a "5-10-20" peak pattern and everything falls apart
	const iterations = 4
	currentAnchor := startAnchor

	for iter := 0; iter < iterations; iter++ {
		var totalTorque KFloat
		var validHits KFloat

		for i, p := range phases {
			if i%1000 == 0 {
				runtime.Gosched()
			} //...and breathe
			bestError := KFloat(1.0) // Initialize with max possible

			for _, spokeOffset := range spokes {
				// Calculate where this specific spoke is on the clock
				targetPos := KFloat(math.Mod(float64(currentAnchor+spokeOffset), 1.0))

				// We need SIGNED distance: how far and in which direction?
				// Result should be between -0.5 and 0.5
				diff := p - targetPos
				if diff > 0.5 {
					diff -= 1.0
				}
				if diff < -0.5 {
					diff += 1.0
				}

				if math.Abs(float64(diff)) < math.Abs(float64(bestError)) {
					bestError = diff
				}
			}

			// Only let "near misses" pull the template.
			// This ignores the 'guff' between the 2 and 5 spokes.
			if math.Abs(float64(bestError)) < guffThreshold {
				totalTorque += bestError
				validHits++
			}
		}

		if validHits > 0 {
			// Adjust the anchor by the average torque (the M-step)
			//currentAnchor = math.Mod(currentAnchor+(totalTorque/validHits)+1.0, 1.0)
			currentAnchor = KFloat(math.Mod(float64(currentAnchor-(totalTorque/validHits)+1.0), 1.0)) // Gemini test, reverse the torque
			if math.IsNaN(float64(currentAnchor)) {
				return startAnchor, math.MaxFloat32
			}
		}
	}

	// Final Pass: Calculate "Badness"
	var totalAbsError KFloat
	var hits KFloat
	for i, p := range phases {
		if i%1000 == 0 {
			runtime.Gosched()
		} //...and breathe
		bestError := KFloat(1.0) // Initialize with max possible
		for _, spokeOffset := range spokes {
			// Calculate where this specific spoke is on the clock
			targetPos := KFloat(math.Mod(float64(currentAnchor+spokeOffset), 1.0))

			// We need SIGNED distance: how far and in which direction?
			// Result should be between -0.5 and 0.5
			diff := p - targetPos
			if diff > 0.5 {
				diff -= 1.0
			}
			if diff < -0.5 {
				diff += 1.0
			}

			if math.Abs(float64(diff)) < math.Abs(float64(bestError)) {
				bestError = diff
			}
		}
		err := bestError

		if err < guffThreshold {
			//totalSqError += (err * err)
			totalAbsError += KFloat(math.Abs(float64(err * err)))
			hits++
		}
	}

	// If it captures very few points, it's a "bad" fit regardless of tightness
	if hits == 0 {
		return startAnchor, math.MaxFloat32
	}

	// Badness = Variance / CaptureRate
	captureRate := hits / KFloat(len(phases))
	badness := (totalAbsError / hits) / (captureRate * captureRate)

	return currentAnchor, badness
}

func findEpochPeaks(amounts []int64, k int, deterministic *rand.Rand) []KFloat {
	result := make([]KFloat, 0)
	bestBadness := KFloat(math.MaxFloat32)
	for try := 0; try < 4; try++ {
		guess, badness := guessEpochPeaksClock(amounts, k, deterministic)
		if badness < bestBadness {
			bestBadness = badness
			result = guess
		}
	}
	return result
}

func guessEpochPeaksClock(amounts []int64, k int, deterministic *rand.Rand) (logCentroids []KFloat, badnessScore KFloat) {
	// 1. Map all mantissas to the 0.0 to 1.0 "Clock face"
	phases := make([]KFloat, len(amounts))
	for i, v := range amounts {
		// log10(v) % 1 gives the position on the clock
		_, ph := math.Modf(math.Log10(float64(v)))
		phases[i] = KFloat(ph)
		if phases[i] < 0.0 {
			phases[i] += 1.0
		}
	}

	logCentroids = initializeCentroids(phases, k, deterministic)

	for i := 0; i < 8; i++ { // 10 iterations is usually enough for 1D
		clusters := make([][]KFloat, k)

		// 2. Assign to nearest centroid
		badnessScore = KFloat(0)
		for i, val := range phases {
			if i%1000 == 0 {
				runtime.Gosched()
			} //...and breathe
			best := 0
			minDist := cyclicDistance(val, logCentroids[0])
			for j := 1; j < k; j++ {
				d := cyclicDistance(val, logCentroids[j])
				if d < minDist {
					minDist = d
					best = j
				}
			}
			clusters[best] = append(clusters[best], val)
			badnessScore += minDist
		}

		// 3. Update centroids using a "circular median" or mean
		for j := 0; j < k; j++ {
			if len(clusters[j]) > 0 {
				if len(clusters[j]) > 2 {
					logCentroids[j] = circularMean(clusters[j])
				} else {
					logCentroids[j] = KFloat(rand.Float32()) // Give it a kick
				}
			}
		}
		// badnessScore is one iteration out of date, but let's not get too picky!
	}
	return
}

func cyclicDistance(a, b KFloat) KFloat {
	diff := KFloat(math.Abs(float64(a - b)))
	if diff > 0.5 {
		return 1.0 - diff
	}
	return diff
}

func initializeCentroids(mantissas []KFloat, k int, deterministic *rand.Rand) []KFloat {
	result := make([]KFloat, k)
	count := len(mantissas)
	for i := 0; i < k; i++ {
		var r int
		if deterministic != nil {
			r = deterministic.Intn(count)
		} else {
			r = rand.Intn(count)
		}
		c := mantissas[r]
		result[i] = c
	}
	return result
}

func circularMean(phases []KFloat) KFloat {
	if len(phases) == 0 {
		return 0
	}

	var sumSin, sumCos float64
	for i, p := range phases {
		if i%1000 == 0 {
			runtime.Gosched()
		} //...and breathe
		// 1. Convert phase (0..1) to radians (0..2π)
		angle := float64(p * 2.0 * math.Pi)

		// 2. Sum the Cartesian coordinates
		sumSin += math.Sin(angle)
		sumCos += math.Cos(angle)
	}

	// Safety check
	if math.Abs(sumSin) < 1e-9 && math.Abs(sumCos) < 1e-9 {
		return phases[0] // Just pick the first point to break the symmetry
	}

	// 3. Use Atan2 to find the angle of the average vector
	avgAngle := math.Atan2(sumSin, sumCos)

	// 4. Convert back from radians to phase (0..1)
	avgPhase := avgAngle / (2.0 * math.Pi)

	// 5. Ensure the result is in the [0, 1) range
	if avgPhase < 0 {
		avgPhase += 1.0
	}
	return KFloat(avgPhase)
}

func ExpPeakResidual(amount int64, logCentroids []float64) (exp int, peak int, harmonic int, residual int64) {
	// If we-re not doing 1-2-5 harmonics, we'll just have to specify the harmonic as zero
	harmonic = 0

	// log10(v) % 1 gives the position on the clock
	e, lc := math.Modf(math.Log10(float64(amount)))
	logCentroid := KFloat(lc)
	if logCentroid < 0.0 {
		logCentroid += 1.0
		e -= 1.0
	}
	exp = int(e)

	bestPeak := 0
	bestDiff := cyclicDistance(logCentroid, KFloat(logCentroids[bestPeak]))
	for p := 1; p < len(logCentroids); p++ {
		diff := cyclicDistance(logCentroid, KFloat(logCentroids[p]))
		if diff < bestDiff {
			bestDiff = diff
			bestPeak = p
		}
	}
	peak = bestPeak

	peakAmount := int64(math.Round(math.Pow(10, float64(logCentroids[bestPeak])+float64(exp))))

	residual = amount - peakAmount

	return
}

const MIN_AMOUNT_COUNT_FOR_ANALYSIS = 100

// "beans/beansperbucket+1" usually works, but you get black swans when the division is exact
func bucketCount(beans int64, beansPerBucket int64) int64 {
	return (beans + beansPerBucket - 1) / beansPerBucket
}

func ParallelKMeans(chain chainreadinterface.IBlockChain, handles chainreadinterface.IHandleCreator, blocks int64, blocksPerMicroEpoch int64,
	celebCodesPerEpoch []map[int64]huffman.BitCode, blocksPerEpoch int64, deterministic *rand.Rand,
	transToExcludedOutput *[2000000000]byte,
	epochToExcludedCelebs []map[int64]bool) ([][]float64, error) {

	sJob := "Peak detection: PARALLEL by micro-epoch"
	fmt.Printf("\t%s\n", sJob)
	tJob := time.Now()

	epochs := bucketCount(blocks, blocksPerEpoch)
	microEpochs := bucketCount(blocks, blocksPerMicroEpoch)
	microEpochToPhasePeaks := make([][]float64, microEpochs)
	microEpochsPerEpoch := blocksPerEpoch / blocksPerMicroEpoch

	microEpochsToTxos := make([]int64, microEpochs)
	transactionsInChain := int64(0)
	blocksInChain := int64(0)

	var completed int64 // atomic counter

	// Use a semaphore to limit concurrency to CPU count

	workersDivider := 1
	numWorkers := runtime.NumCPU() / workersDivider
	if numWorkers > 8 {
		numWorkers -= 4 // Save some for OS
	}
	//numWorkers = 1 // Serial test! I may be some time

	g, ctx := errgroup.WithContext(context.Background())
	sem := make(chan struct{}, numWorkers)

	for i := int64(0); i < epochs; i++ {
		epochID := i // Capture for closure
		g.Go(func() error {
			sem <- struct{}{}
			defer func() { <-sem }()

			// Check if another worker failed
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			// Rest of my logic...
			buffer := make([]int64, 0, 5000)

			// Create a local source unique to THIS epoch
			// No matter which thread runs this, eID 866 (the suspect) always gets the same seed.
			//salt := int64(0)	// This didn't freeze my PC
			salt := int64(1) // Tried this salt next... YES AND NO (froze once, sailed once, froze a second time)
			src := rand.NewSource(int64(epochID) + salt)
			localRand := rand.New(src)

			var celebsToExclude map[int64]bool
			if epochToExcludedCelebs == nil {
				celebsToExclude = nil
			} else {
				celebsToExclude = epochToExcludedCelebs[epochID]
			}

			// Go through the microEpochs in this epoch
			firstMe := epochID * microEpochsPerEpoch
			lastMe := (epochID + 1) * microEpochsPerEpoch // Usually
			if lastMe > microEpochs {
				lastMe = microEpochs // Finally (last, possibly partial, epoch)
			}
			for me := firstMe; me < lastMe; me++ {
				txoCount := 0
				buffer = buffer[:0] // Reset buffer but keep allocated memory
				firstBlock := epochID*blocksPerEpoch + (me-firstMe)*blocksPerMicroEpoch
				if firstBlock >= blocks {
					break
				}
				lastBlock := firstBlock + blocksPerMicroEpoch
				if lastBlock > blocks {
					lastBlock = blocks
				}

				// To emulate the old code, we need to know the txoIndex of the first txo of the first trans
				// in the first block of this microepoch
				blockHandle, err := handles.BlockHandleByHeight(firstBlock)
				if err != nil {
					return err
				}
				block, err := chain.BlockInterface(blockHandle)
				if err != nil {
					return err
				}
				transHandle, err := block.NthTransaction(0)
				if err != nil {
					return err
				}
				trans, err := chain.TransInterface(transHandle)
				if err != nil {
					return err
				}
				txoHandle, err := trans.NthTxo(0)
				if err != nil {
					return err
				}
				if !txoHandle.TxoHeightSpecified() {
					return errors.New("txo height not specified")
				}
				firstTxoOfMe := txoHandle.TxoHeight()

				// Go through the blocks in the microEpoch
				for blockIdx := firstBlock; blockIdx < lastBlock; blockIdx++ {
					atomic.AddInt64(&blocksInChain, 1)
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
						atomic.AddInt64(&transactionsInChain, 1)
						transHandle, err := block.NthTransaction(t)
						if !transHandle.HeightSpecified() {
							return errors.New("transaction height not specified")
						}
						transIndex := transHandle.Height()
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
						// For the thinning below to match the "old" code, in which txo indexed all the txo's
						// in the entire chain, we'll need to know the firstTxo of the transaction
						txoHandle, err := trans.NthTxo(0)
						if err != nil {
							return err
						}
						if !txoHandle.TxoHeightSpecified() {
							return errors.New("txo height not specified")
						}
						firstTxoOfTrans := txoHandle.TxoHeight()

						for txo, sats := range txoAmounts {
							txoCount++
							amount := sats

							excluded := false
							found := false
							exclusionListExists := false
							if celebsToExclude != nil {
								exclusionListExists = true
								excluded, found = celebsToExclude[amount]
							}
							if exclusionListExists && found {
								excluded = true
							}

							if excluded {
								// The amount is specified as an excluded celebrity for this epoch
							} else {
								oldCodeTxoIndex := firstTxoOfTrans + int64(txo)
								if _, ok := celebCodesPerEpoch[epochID][amount]; !ok {
									// Only if NOT a celeb
									if transToExcludedOutput == nil {
										// First pass, thin it down
										if oldCodeTxoIndex-firstTxoOfMe < 1000 || oldCodeTxoIndex%20 == 0 {
											buffer = append(buffer, amount)
										}
									} else {
										// Second pass
										// Is it excluded? (recognized as high entropy change?)
										excludeCode := transToExcludedOutput[transIndex]
										// 255 is a special code meaning "do not exclude any of the outputs"
										// 0 to 254 are codes meaning "exclude this output, one of output index 0 to 254"
										if excludeCode == 255 || int64(excludeCode) != int64(txo) {
											// Do not exclude txo. So use it.
											// But maybe do some thinning
											if oldCodeTxoIndex-firstTxoOfMe < 1000 || oldCodeTxoIndex%20 == 0 {
												buffer = append(buffer, amount)
											}
										}
									}
								}
							}
						}
					} // for transactions
				} // for blocks
				atomic.AddInt64(&microEpochsToTxos[me], int64(txoCount))
				if len(buffer) < MIN_AMOUNT_COUNT_FOR_ANALYSIS {
					microEpochToPhasePeaks[me] = nil
				} else {
					// This is the heavy lifting
					microEpochToPhasePeaks[me] = FindEpochPeaksMain(buffer, localRand)
				}
			} // for micro epochs

			// Report progress on completion of epoch
			done := atomic.AddInt64(&completed, 1)
			fmt.Printf("\r\tProgress %.1f%%    ", float64(100*done)/float64(epochs))

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	fmt.Printf("\n")
	jobElapsed := time.Since(tJob)
	fmt.Printf("\t%s: Job took: [%5.1f min]\n", sJob, jobElapsed.Minutes())

	txosInChain := int64(0)
	for i := int64(0); i < microEpochs; i++ {
		txosInChain += microEpochsToTxos[i]
	}
	fmt.Printf("\tConsidered %d blocks (should be 888,888\n", blocksInChain)
	fmt.Printf("\tTODO! Considered %d transactions (should be 1169006472)\n", transactionsInChain)
	fmt.Printf("\tTODO! Considered %d txos (should be 3,244,970,783)\n", txosInChain)
	return microEpochToPhasePeaks, nil
}
