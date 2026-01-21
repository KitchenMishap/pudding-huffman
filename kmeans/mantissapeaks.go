package kmeans

import (
	"math"
	"math/rand"
	"runtime"
	"sync"
)

func FindEpochPeaks(amounts []int64, k int) []float64 {
	result := make([]float64, 0)
	bestBadness := math.MaxFloat64
	for try := 0; try < 5; try++ {
		guess, badness := guessEpochPeaksClock(amounts, k)
		if badness < bestBadness {
			bestBadness = badness
			result = guess
		}
	}
	return result
}

func guessEpochPeaksClock(amounts []int64, k int) (logCentroids []float64, badnessScore float64) {
	// 1. Map all mantissas to the 0.0 to 1.0 "Clock face"
	phases := make([]float64, len(amounts))
	for i, v := range amounts {
		// log10(v) % 1 gives the position on the clock
		_, phases[i] = math.Modf(math.Log10(float64(v)))
		if phases[i] < 0.0 {
			phases[i] += 1.0
		}
	}

	logCentroids = initializeCentroids(phases, k)

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

func cyclicDistance(a, b float64) float64 {
	diff := math.Abs(a - b)
	if diff > 0.5 {
		return 1.0 - diff
	}
	return diff
}

func initializeCentroids(mantissas []float64, k int) []float64 {
	result := make([]float64, k)
	count := len(mantissas)
	for i := 0; i < k; i++ {
		r := rand.Intn(count)
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

func ExpPeakResidual(amount int64, logCentroids []float64) (exp int, peak int, residual int64) {
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

func ParallelKMeans(amountsEachEpoch [][]int64, epochs int64) [][]float64 {
	epochToPhasePeaks := make([][]float64, epochs)
	var wg sync.WaitGroup

	// Use a semaphore to limit concurrency to CPU count
	sem := make(chan struct{}, runtime.NumCPU())

	for i := int64(0); i < epochs; i++ {
		wg.Add(1)
		go func(epochID int64) {
			defer wg.Done()
			sem <- struct{}{}        // Acquire token
			defer func() { <-sem }() // Release token

			if len(amountsEachEpoch[epochID]) < 7 {
				epochToPhasePeaks[epochID] = nil
			} else {
				// This is the heavy lifting
				epochToPhasePeaks[epochID] = FindEpochPeaks(amountsEachEpoch[epochID], 7)
			}
		}(i)
	}

	wg.Wait()
	return epochToPhasePeaks
}
