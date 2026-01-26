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
)

const USE_125 = false

func FindEpochPeaksMain(amounts []int64, deterministic *rand.Rand) []float64 {
	// 1. Map all mantissas to the 0.0 to 1.0 "Clock face"
	phases := make([]float64, len(amounts))
	for i, v := range amounts {
		// log10(v) % 1 gives the position on the clock
		_, phases[i] = math.Modf(math.Log10(float64(v)))
		if phases[i] < 0.0 {
			phases[i] += 1.0
		}
	}

	onePeak := findEpochPeaks(amounts, 1, deterministic)
	if len(onePeak) == 0 {
		return nil
	}
	fundamentalPhase := onePeak[0]
	betterFundamental, _ := FindBestAnchor(phases, fundamentalPhase)

	result := []float64{}
	// fundamental times logs representing 1.0, 1.1, 1.2, ..., 9.9
	for i := float64(1.00); i < 10.00; i += 0.1 {
		result = append(result, math.Mod(betterFundamental+math.Log10(i), 1))
	}

	// OR... Just the plain 1,2,5
	//result = append(result, math.Mod(betterFundamental+math.Log10(1), 1))
	//result = append(result, math.Mod(betterFundamental+math.Log10(2), 1))
	//result = append(result, math.Mod(betterFundamental+math.Log10(5), 1))

	return result
}

func FindBestAnchor(phases []float64, initialPeak float64) (bestAnchor float64, score float64) {
	spokes := []float64{0.0, 0.30103, 0.69897} // log10 of 1, 2, 5

	bestScore := math.MaxFloat64
	var absoluteBest float64

	for _, shift := range spokes {
		// Hypothesis: What if the initial peak is actually the '1', '2', or '5'?
		// We shift the anchor so the template aligns the initial peak with that spoke.
		testAnchor := math.Mod(initialPeak-shift+1.0, 1.0)

		refined, currentBadness := refineAndScore(phases, testAnchor, spokes)

		if currentBadness < bestScore {
			bestScore = currentBadness
			absoluteBest = refined
		}
	}
	return absoluteBest, bestScore
}

func refineAndScore(phases []float64, startAnchor float64, spokes []float64) (float64, float64) {
	const guffThreshold = 0.05
	const iterations = 5
	currentAnchor := startAnchor

	for iter := 0; iter < iterations; iter++ {
		var totalTorque float64
		var validHits float64

		for _, p := range phases {
			bestError := 1.0 // Initialize with max possible

			for _, spokeOffset := range spokes {
				// Calculate where this specific spoke is on the clock
				targetPos := math.Mod(currentAnchor+spokeOffset, 1.0)

				// We need SIGNED distance: how far and in which direction?
				// Result should be between -0.5 and 0.5
				diff := p - targetPos
				if diff > 0.5 {
					diff -= 1.0
				}
				if diff < -0.5 {
					diff += 1.0
				}

				if math.Abs(diff) < math.Abs(bestError) {
					bestError = diff
				}
			}

			// Only let "near misses" pull the template.
			// This ignores the 'guff' between the 2 and 5 spokes.
			if math.Abs(bestError) < guffThreshold {
				totalTorque += bestError
				validHits++
			}
		}

		if validHits > 0 {
			// Adjust the anchor by the average torque (the M-step)
			currentAnchor = math.Mod(currentAnchor+(totalTorque/validHits)+1.0, 1.0)
		}
	}

	// Final Pass: Calculate "Badness"
	var totalSqError float64
	var hits float64
	for _, p := range phases {
		bestError := 1.0 // Initialize with max possible
		for _, spokeOffset := range spokes {
			// Calculate where this specific spoke is on the clock
			targetPos := math.Mod(currentAnchor+spokeOffset, 1.0)

			// We need SIGNED distance: how far and in which direction?
			// Result should be between -0.5 and 0.5
			diff := p - targetPos
			if diff > 0.5 {
				diff -= 1.0
			}
			if diff < -0.5 {
				diff += 1.0
			}

			if math.Abs(diff) < math.Abs(bestError) {
				bestError = diff
			}
		}
		err := bestError

		if err < guffThreshold {
			totalSqError += (err * err)
			hits++
		}
	}

	// If it captures very few points, it's a "bad" fit regardless of tightness
	if hits == 0 {
		return startAnchor, math.MaxFloat64
	}

	// Badness = Variance / CaptureRate
	captureRate := hits / float64(len(phases))
	badness := (totalSqError / hits) / captureRate

	return currentAnchor, badness
}

func findEpochPeaks(amounts []int64, k int, deterministic *rand.Rand) []float64 {

	if USE_125 {
		return findEpochPeaks125(amounts, k, deterministic)
	}

	result := make([]float64, 0)
	bestBadness := math.MaxFloat64
	for try := 0; try < 5; try++ {
		guess, badness := guessEpochPeaksClock(amounts, k, deterministic)
		if badness < bestBadness {
			bestBadness = badness
			result = guess
		}
	}
	return result
}

func findEpochPeaks125(amounts []int64, k int, deterministic *rand.Rand) []float64 {
	result := make([]float64, 0)
	bestBadness := math.MaxFloat64
	for try := 0; try < 5; try++ {
		guess, badness := guessEpochPeaksClock125(amounts, k, deterministic)
		if badness < bestBadness {
			bestBadness = badness
			result = guess
		}
	}
	return result
}

func guessEpochPeaksClock(amounts []int64, k int, deterministic *rand.Rand) (logCentroids []float64, badnessScore float64) {
	// 1. Map all mantissas to the 0.0 to 1.0 "Clock face"
	phases := make([]float64, len(amounts))
	for i, v := range amounts {
		// log10(v) % 1 gives the position on the clock
		_, phases[i] = math.Modf(math.Log10(float64(v)))
		if phases[i] < 0.0 {
			phases[i] += 1.0
		}
	}

	logCentroids = initializeCentroids(phases, k, deterministic)

	for i := 0; i < 10; i++ { // 10 iterations is usually enough for 1D
		clusters := make([][]float64, k)

		// 2. Assign to nearest centroid
		badnessScore = float64(0)
		for _, val := range phases {
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
				logCentroids[j] = circularMean(clusters[j])
			}
		}
		// badnessScore is one iteration out of date, but let's not get too picky!
	}
	return
}

const (
	log1 = 0.0           // math.Log10(1)
	log2 = 0.30102999566 // math.Log10(2)
	log5 = 0.69897000433 // math.Log10(5)
)

func guessEpochPeaksClock125(amounts []int64, k int, deterministic *rand.Rand) (logCentroids []float64, badnessScore float64) {
	phases := make([]float64, len(amounts))
	for i, v := range amounts {
		_, phases[i] = math.Modf(math.Log10(float64(v)))
		if phases[i] < 0.0 {
			phases[i] += 1.0
		}
	}

	logCentroids = initializeCentroids(phases, k, deterministic)

	for i := 0; i < 10; i++ {
		clusters := make([][]float64, k)
		badnessScore = 0

		for _, val := range phases {
			bestCentroidIdx := 0
			minDist := 2.0 // Sentinel

			// Try every centroid's three harmonic possibilities
			for j := 0; j < k; j++ {
				// Find which harmonic of centroid[j] is closest to 'val'
				// We use the 1x base phase (logCentroids[j]
				d, _ := cyclicDistance125(val, logCentroids[j])

				if d < minDist {
					minDist = d
					bestCentroidIdx = j
				}
			}

			// NORMALIZE the value before appending it to the cluster
			// This rotates 2x and 5x points back to the 1x 'Root'
			normalizedVal := normalizeToFundamental(val, logCentroids[bestCentroidIdx])
			clusters[bestCentroidIdx] = append(clusters[bestCentroidIdx], normalizedVal)
			badnessScore += minDist
		}

		// Update centroids (now purely on normalized 1x bases)
		for j := 0; j < k; j++ {
			if len(clusters[j]) > 0 {
				logCentroids[j] = circularMean(clusters[j])
			}
		}
	}
	return
}

func normalizeToFundamental(val, centroid float64) float64 {
	d1 := cyclicDistance(val, centroid)
	d2 := cyclicDistance(val, math.Mod(centroid+log2, 1.0))
	d5 := cyclicDistance(val, math.Mod(centroid+log5, 1.0))

	if d1 <= d2 && d1 <= d5 {
		return val // Already at 1x
	} else if d2 < d5 {
		// It's a 2x hit, rotate back by log2
		res := val - log2
		if res < 0 {
			res += 1.0
		}
		return res
	} else {
		// It's a 5x hit, rotate back by log5
		res := val - log5
		if res < 0 {
			res += 1.0
		}
		return res
	}
}

func cyclicDistance(a, b float64) float64 {
	diff := math.Abs(a - b)
	if diff > 0.5 {
		return 1.0 - diff
	}
	return diff
}

func cyclicDistance125(a, b float64) (float64, int) {
	// The three harmonic "images" of the centroid in log space
	h1 := b                  // 1x
	h2 := fmod(b+0.301, 1.0) // 2x (b + log10(2))
	h5 := fmod(b+0.699, 1.0) // 5x (b + log10(5))

	d1 := cyclicDistance(a, h1)
	d2 := cyclicDistance(a, h2)
	d5 := cyclicDistance(a, h5)

	// Find the winner
	if d1 <= d2 && d1 <= d5 {
		return d1, 1
	} else if d2 <= d1 && d2 <= d5 {
		return d2, 2
	}
	return d5, 5
}

// Helper to keep the log phase within [0, 1)
func fmod(x, y float64) float64 {
	res := math.Mod(x, y)
	if res < 0 {
		res += y
	}
	return res
}

func initializeCentroids(mantissas []float64, k int, deterministic *rand.Rand) []float64 {
	result := make([]float64, k)
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

func circularMean(phases []float64) float64 {
	if len(phases) == 0 {
		return 0
	}

	var sumSin, sumCos float64
	for _, p := range phases {
		// 1. Convert phase (0..1) to radians (0..2π)
		angle := p * 2.0 * math.Pi

		// 2. Sum the Cartesian coordinates
		sumSin += math.Sin(angle)
		sumCos += math.Cos(angle)
	}

	// 3. Use Atan2 to find the angle of the average vector
	avgAngle := math.Atan2(sumSin, sumCos)

	// 4. Convert back from radians to phase (0..1)
	avgPhase := avgAngle / (2.0 * math.Pi)

	// 5. Ensure the result is in the [0, 1) range
	if avgPhase < 0 {
		avgPhase += 1.0
	}
	return avgPhase
}

func ExpPeakResidual(amount int64, logCentroids []float64) (exp int, peak int, harmonic int, residual int64) {
	if USE_125 {
		exp, peak, harmonic, residual = expPeakResidual125(amount, logCentroids)
		return
	}
	// If we-re not doing 1-2-5 harmonics, we'll just have to specify the harmonic as zero
	harmonic = 0

	// log10(v) % 1 gives the position on the clock
	e, logCentroid := math.Modf(math.Log10(float64(amount)))
	if logCentroid < 0.0 {
		logCentroid += 1.0
		e -= 1.0
	}
	exp = int(e)

	bestPeak := 0
	bestDiff := cyclicDistance(logCentroid, logCentroids[bestPeak])
	for p := 1; p < len(logCentroids); p++ {
		diff := cyclicDistance(logCentroid, logCentroids[p])
		if diff < bestDiff {
			bestDiff = diff
			bestPeak = p
		}
	}
	peak = bestPeak

	peakAmount := int64(math.Round(math.Pow(10, logCentroids[bestPeak]+float64(exp))))

	residual = amount - peakAmount

	return
}

func expPeakResidual125(amount int64, logCentroids []float64) (exp int, peak int, harmonic int, residual int64) {
	e, logVal := math.Modf(math.Log10(float64(amount)))
	if logVal < 0.0 {
		logVal += 1.0
		e -= 1.0
	}
	exp = int(e)

	bestPeak := 0
	bestHarmonic := 1
	minDist := 2.0

	for p := 0; p < len(logCentroids); p++ {
		d, h := cyclicDistance125(logVal, logCentroids[p])
		if d < minDist {
			minDist = d
			bestPeak = p
			bestHarmonic = h
		}
	}

	peak = bestPeak
	harmonic = bestHarmonic

	// Calculate the target amount based on the harmonic winner
	// peakAmount = 10^(exp) * 10^(logCentroid) * harmonic
	// Using Round to avoid floating point 49.999999 issues
	baseAmount := math.Pow(10, logCentroids[peak]+float64(exp))
	targetAmount := int64(math.Round(baseAmount * float64(harmonic)))

	residual = amount - targetAmount
	return
}

const MIN_AMOUNT_COUNT_FOR_ANALYSIS = 100

func ParallelKMeans(chain chainreadinterface.IBlockChain, handles chainreadinterface.IHandleCreator, blocks int64, blocksPerMicroEpoch int64,
	celebCodesPerEpoch []map[int64]huffman.BitCode, blocksPerEpoch int64, deterministic *rand.Rand) ([][]float64, error) {

	fmt.Printf("Parallel peak detection by micro-epoch...\n")

	epochs := blocks/blocksPerEpoch + 1
	microEpochs := blocks/blocksPerMicroEpoch + 1
	microEpochToPhasePeaks := make([][]float64, microEpochs)
	microEpochsPerEpoch := blocksPerEpoch / blocksPerMicroEpoch

	var completed int64 // atomic counter

	// Use a semaphore to limit concurrency to CPU count
	numWorkers := runtime.NumCPU()
	if numWorkers > 4 {
		numWorkers -= 2 // Save some for OS
	}

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

			// Go through the microEpochs in this epoch
			firstMe := epochID * microEpochsPerEpoch
			for me := firstMe; me < firstMe+microEpochsPerEpoch; me++ {
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
							amount := sats
							oldCodeTxoIndex := firstTxoOfTrans + int64(txo)
							if _, ok := celebCodesPerEpoch[epochID][amount]; !ok {
								// Only if NOT a celeb
								// And thin it down
								if oldCodeTxoIndex-firstTxoOfMe < 1000 || oldCodeTxoIndex%10 == 0 {
									buffer = append(buffer, amount)
								}
							}
						}
					} // for transactions
				} // for blocks
				if len(buffer) < MIN_AMOUNT_COUNT_FOR_ANALYSIS {
					microEpochToPhasePeaks[me] = nil
				} else {
					// This is the heavy lifting
					microEpochToPhasePeaks[me] = FindEpochPeaksMain(buffer, localRand)
				}
			} // for micro epochs

			// Report progress on completion of epoch
			done := atomic.AddInt64(&completed, 1)
			fmt.Printf("\r> Peak detection progress: [%d/%d] epochs (%.1f%%)    ",
				done, epochs, float64(done)/float64(epochs)*100)

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	fmt.Println("\n Peak detection done.")
	return microEpochToPhasePeaks, nil
}
