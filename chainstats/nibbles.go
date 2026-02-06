package chainstats

import (
	"context"
	"errors"
	"fmt"
	"github.com/KitchenMishap/pudding-huffman/bcd"
	"github.com/KitchenMishap/pudding-huffman/graphics"
	"github.com/KitchenMishap/pudding-shed/chainreadinterface"
	"golang.org/x/sync/errgroup"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

func WholeChainNibbleComplexity(chain chainreadinterface.IBlockChain,
	handles chainreadinterface.IHandleCreator,
	interestedBlock int64, interestedBlocks int64) error {

	const blocksInBatch = 1000
	const maxComplexity = 8
	const minComplexity = 14

	mut := sync.Mutex{}
	pgm := graphics.PgmHist{}

	completedBlocks := int64(0) // Atomic int

	workersDivider := 1
	numWorkers := runtime.NumCPU() / workersDivider
	if numWorkers > 8 {
		numWorkers -= 4 // Save some for OS
	}

	blockBatchChan := make(chan int64)

	type workerResult struct {
		csvRows  string
		csvPrice string
	}
	resultsChan := make(chan workerResult, numWorkers)

	// Create an errgroup and a context
	g, ctx := errgroup.WithContext(context.Background())

	for w := 0; w < numWorkers; w++ {
		g.Go(func() error { // Use the errgroup instead of "go func() {"

			var sb strings.Builder
			var sbPrice strings.Builder

			amounts := make([]int64, 0, 10000)

			for blockBatch := range blockBatchChan {
				// Check if another worker already failed
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				for blockIdx := blockBatch; blockIdx < blockBatch+blocksInBatch && blockIdx < interestedBlock+interestedBlocks; blockIdx++ {
					blockHandle, err := handles.BlockHandleByHeight(blockIdx)
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

					amounts = amounts[:0]

					for t := int64(0); t < tCount; t++ {
						transHandle, err := block.NthTransaction(t)
						if err != nil {
							return err
						}
						if !transHandle.HeightSpecified() {
							return errors.New("transaction height not specified")
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
							metrics := bcd.GetDivineComplexity(uint64(sats))
							if metrics.Complexity >= minComplexity {
								// Complex enough to try to match with a fiat price
								amounts = append(amounts, sats)
							}
							/*
								if metrics.Complexity <= maxComplexity {
									// Simple enough to look at the sats price
									sb.WriteString(strconv.FormatInt(blockIdx, 10))
									sb.WriteByte(',')
									sb.WriteByte('\t')
									sb.WriteString(strconv.FormatUint(uint64(sats), 10))
									sb.WriteByte(',')
									sb.WriteByte('\t')
									sb.WriteString(strconv.FormatFloat(metrics.Complexity, 'f', 0, 64))
									sb.WriteByte(',')
									sb.WriteByte('\t')
									sb.WriteString(strconv.FormatFloat(metrics.TotalDigits, 'f', 0, 64))
									sb.WriteByte(',')
									sb.WriteByte('\t')
									sb.WriteString(strconv.FormatFloat(metrics.TrailingZeros, 'f', 0, 64))
									sb.WriteByte(',')
									sb.WriteByte('\t')
									sb.WriteString(strconv.FormatFloat(metrics.UniqueDigits, 'f', 0, 64))
									sb.WriteByte('\n')
								}*/
						} // for txo amounts
					} // for transactions

					contenders := getPriceByInverse(amounts)
					if len(contenders) >= winners {
						//fmt.Printf("Block %d, Price1 %d, Price2 %d, Price3 %d\n", blockIdx, contenders[0].price, contenders[1].price, contenders[2].price)
						//						sbPrice.WriteString(strconv.FormatInt(blockIdx, 10))
						//						for _, contender := range contenders {
						//							sbPrice.WriteByte(',')
						//							sbPrice.WriteByte('\t')
						//							sbPrice.WriteString(strconv.FormatInt(contender.price, 10))
						//						}
						//						sbPrice.WriteByte('\n')

						mut.Lock()
						x := float64(blockIdx) / 888888
						for _, contender := range contenders {
							y := math.Log10(float64(contender.price) / 100000)
							pgm.PlotPoint(x, y)
						}
						mut.Unlock()
					} // if contenders
				} // for block

				done := atomic.AddInt64(&completedBlocks, blocksInBatch)
				if done%1000 == 0 || done == interestedBlocks {
					fmt.Printf("\r\tProgress: %.1f%%    ", float64(100*done)/float64(interestedBlocks))
					runtime.Gosched()
				}

			} // for blockBatch from chan
			resultsChan <- workerResult{csvRows: sb.String(), csvPrice: sbPrice.String()}

			runtime.Gosched()
			return nil
		}) // gofunc
	} // for workers
	go func() {
		defer close(blockBatchChan)
		for blockBatch := interestedBlock; blockBatch < interestedBlock+interestedBlocks; blockBatch += blocksInBatch {
			select { // Note: NOT a switch statement!
			case blockBatchChan <- blockBatch: // This happens if a worker is free to be fed an epoch ID
			case <-ctx.Done(): // This happens if a worker returned an err
				return
			}
		}
	}()

	err := g.Wait()

	close(resultsChan)
	pgm.Output("Price888888.pgm")
	fmt.Printf("\n\tDone that now\n")

	// Final Reduction
	fComplexity, err := os.Create("AmountComplexity.csv")
	if err != nil {
		panic("Could not create complexity file")
	}
	defer fComplexity.Close()
	fmt.Fprintf(fComplexity, "Block,\tSats,\tComplexity,\tTotalDigits,\tTrailingZeros,\tUniqueDigits\n")

	fPrice, err := os.Create("PriceComplexity.csv")
	if err != nil {
		panic("Could not create price file")
	}
	defer fPrice.Close()
	fmt.Fprintf(fPrice, "Block,\tPrice,\tSatsTotalDigits,\tSatsTrailingZeros,\tSatsUniqueDigits,\tSatsComplexity,\tFiatTotalDigits,\tFiatTrailingZeros,\tFiatUniqueDigits,\tFiatComplexity\n")

	for result := range resultsChan {
		fmt.Fprintf(fComplexity, "%s", result.csvRows)
		fmt.Fprintf(fPrice, "%s", result.csvPrice)
	}
	return err
}

func getPrice(amounts []int64) (price int64, satsMetrics *bcd.NibbleMetrics, fiatMetrics *bcd.NibbleMetrics) {
	satsMetrics = bcd.NewBlankNibbleMetrics()
	bestGuess := -1
	bestScore := bcd.NewMaxNibbleMetrics()
	priceCount := 0
	for priceOfSat := 20; priceOfSat < 90; priceOfSat++ {
		priceCount++
		totalComplexityThisPrice := bcd.NibbleMetrics{}
		for _, sats := range amounts {
			complexityStats := bcd.GetDivineComplexity(uint64(sats) * uint64(priceOfSat))
			totalComplexityThisPrice.Incorporate(&complexityStats)
		}
		if priceOfSat == 20 {
			*satsMetrics = totalComplexityThisPrice
		}
		averageComplexityThisPrice := totalComplexityThisPrice
		averageComplexityThisPrice.Averages()
		if SecondIsBetter(bestScore, &averageComplexityThisPrice) {
			bestScore = &averageComplexityThisPrice
			bestGuess = priceOfSat
		}
	}
	satsMetrics.Averages()
	return int64(bestGuess), satsMetrics, bestScore
}

type Contender struct {
	price      int64
	complexity int64
	count      int64
}

const winners = 10

func getPriceByInverse(amounts []int64) (fiatWinners []Contender) {
	fiatContenders := make(map[uint64]Contender, 100000)
	putativeRoundFiatAmounts := []int64{1}
	for _, sats := range amounts {
		satsComplexity := bcd.BcdComplexity(uint64(sats), 6, 1)
		if sats > 10000 && satsComplexity >= 5 {
			for _, fiatAmount := range putativeRoundFiatAmounts {
				roundFiatMantissa, _ := bcd.IntDecMantissa(uint64(fiatAmount), 6)
				uglySatsMantissa, _ := bcd.IntDecMantissa(uint64(sats), 6)
				priceOfSat := roundFiatMantissa * 10000000 / uglySatsMantissa
				priceOfSatMantissa, _ := bcd.IntDecMantissa(uint64(priceOfSat), 6)
				priceOfTransMantissa, _ := bcd.IntDecMantissa(uint64(priceOfSatMantissa)*uint64(sats), 6)
				// You wally! complexity will always come out as one!
				complexityThisAmount := bcd.BcdComplexity(priceOfTransMantissa, 6, 1)

				if complexityThisAmount < satsComplexity-1 {
					oldVal, ok := fiatContenders[priceOfSatMantissa]
					if ok {
						oldVal.price = int64(priceOfSatMantissa)
						oldVal.complexity = int64(complexityThisAmount)
						oldVal.count++
						fiatContenders[priceOfSatMantissa] = oldVal
					} else {
						fiatContenders[priceOfSatMantissa] = Contender{int64(priceOfSatMantissa), int64(complexityThisAmount), 1}
					}
				}
			}
		}
	}

	fiatSingles := make([]Contender, 0, len(fiatContenders))
	for _, contender := range fiatContenders {
		contender.complexity /= contender.count
		fiatSingles = append(fiatSingles, contender)
	}
	fiatWinners = FindPairs(fiatSingles, 1000000)
	sort.Slice(fiatWinners, func(i, j int) bool {
		return fiatWinners[i].complexity > fiatWinners[j].complexity
	})

	if len(fiatWinners) >= winners {
		return fiatWinners[0:winners]
	} else {
		return make([]Contender, 0)
	}
}

func SecondIsBetter(first *bcd.NibbleMetrics, second *bcd.NibbleMetrics) bool {
	return second.Complexity < first.Complexity
}

func FindPairs(contenders []Contender, maxPrice int64) []Contender {
	// Stage 1 vote for bins
	binSize := int64(5)
	bins := make([]Contender, maxPrice/binSize)
	for _, cont1 := range contenders {
		if cont1.complexity <= 2 {
			binIndex := cont1.price / binSize
			bins[binIndex].price = binIndex * binSize
			bins[binIndex].count++
		}
	}

	sort.Slice(bins, func(i, j int) bool {
		return bins[i].count > bins[j].count
	})
	return bins[:winners]
}

/*	results := make([]Contender, 0, len(contenders))
	for _, cont1 := range contenders {
		for _, cont2 := range contenders {
			diff := cont1.price - cont2.price
			if diff < 0 {
				diff = -diff
			}
			if diff > 0 && diff < 10 && cont1.complexity < 3 && cont2.complexity < 3 {
				results = append(results, cont1)
				break // One confirmation is good enough!
				//fmt.Printf("Found a pair! at price=%d diff=%d\n", cont1.price, diff)
			}
		}
	}
	return results
}
*/
