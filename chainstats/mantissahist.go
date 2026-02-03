package chainstats

import (
	"context"
	"errors"
	"fmt"
	"github.com/KitchenMishap/pudding-shed/chainreadinterface"
	"golang.org/x/sync/errgroup"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
)

const blocksInBatch = 1000

func WholeChainMantissaHistogram(chain chainreadinterface.IBlockChain,
	handles chainreadinterface.IHandleCreator,
	interestedBlock int64, interestedBlocks int64) (xAxis5Digits *[100000]int64,
	medians *[100000]int64, bannedMantissas map[int]bool) {

	completedBlocks := int64(0) // Atomic int

	workersDivider := 1
	numWorkers := runtime.NumCPU() / workersDivider
	if numWorkers > 8 {
		numWorkers -= 4 // Save some for OS
	}

	blockBatchChan := make(chan int64)

	type workerResult struct {
		local100000 []int64
	}
	resultsChan := make(chan workerResult, numWorkers)

	// Create an errgroup and a context
	g, ctx := errgroup.WithContext(context.Background())
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		g.Go(func() error { // Use the errgroup instead of "go func() {"
			defer wg.Done()

			local := workerResult{}
			local.local100000 = make([]int64, 100000)

			for blockBatch := range blockBatchChan {
				// Check if another worker already failed
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				for blockIdx := blockBatch; blockIdx < blockBatch+blocksInBatch && blockIdx < interestedBlock+interestedBlocks; blockIdx++ {
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
							if sats > 0 {
								log10 := math.Log10(float64(sats))
								integer := int(log10)
								frac := log10 - float64(integer)
								zeroToOne := frac
								oneToTen := math.Pow(10, zeroToOne)
								index100000 := int(math.Round(oneToTen * 10000))
								if index100000 >= 100000 {
									index100000 = 99999 // Just to be sure
								}
								local.local100000[index100000]++
							}
						} // for txo amounts
					} // for transactions
				} // for block

				done := atomic.AddInt64(&completedBlocks, blocksInBatch)
				if done%1000 == 0 || done == interestedBlocks {
					fmt.Printf("\r\tProgress: %.1f%%    ", float64(100*done)/float64(interestedBlocks))
					runtime.Gosched()
				}

			} // for blockBatch from chan
			resultsChan <- local
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

	wg.Wait()
	close(resultsChan)
	fmt.Printf("\nDone that now\n")

	// Final Reduction
	result100000 := [100000]int64{}
	for result := range resultsChan {
		for i100000 := 0; i100000 < 100000; i100000++ {
			result100000[i100000] += result.local100000[i100000]
		}
	}
	f100000, err := os.Create("100000Hist.csv")
	fMedian, err := os.Create("100000Median.csv")
	if err != nil {
		panic("could not create file 1000Hist.csv")
	}
	resultMedian := [100000]int64{}
	resultBannedMantissas := make(map[int]bool, 1000)
	bannedSequenceStart := -1
	bannedSequenceEnd := -1
	for i100000 := 0; i100000 < 100000; i100000++ {
		hist := result100000[i100000]
		fmt.Fprintf(f100000, "%d, %d\n", i100000, hist)

		winSize := 5
		winReach := (winSize - 1) / 2
		windowLeft := int(math.Max(0, float64(i100000-winReach)))
		windowRight := int(math.Min(99999, float64(i100000+winReach)))
		srt := make([]int, 0, winSize)
		for i := windowLeft; i <= windowRight; i++ {
			srt = append(srt, int(result100000[i]))
		}
		sort.Ints(srt)
		median := srt[winReach]
		resultMedian[i100000] = int64(median)
		if float64(hist) > 1.2*float64(median) {
			if bannedSequenceStart == -1 {
				bannedSequenceStart = i100000 // Start of sequence
			}
			bannedSequenceEnd = i100000 // Start or within or end of sequence
			resultBannedMantissas[i100000] = true
			if i100000 == 99999 { // End of whole series is within a banned sequence
				if bannedSequenceStart == bannedSequenceEnd {
					fmt.Printf("Banned 5 digit mantissa: %d\n", bannedSequenceStart)
				} else {
					fmt.Printf("Banned 5 digit mantissas: %d-%d\n", bannedSequenceStart, bannedSequenceEnd)
				}
			}
		} else if bannedSequenceStart != -1 { // Just ended a banned sequence
			if bannedSequenceStart == bannedSequenceEnd {
				fmt.Printf("Banned 5 digit mantissa: %d\n", bannedSequenceStart)
			} else {
				fmt.Printf("Banned 5 digit mantissas: %d-%d\n", bannedSequenceStart, bannedSequenceEnd)
			}
			bannedSequenceStart = -1
		}
		fmt.Fprintf(fMedian, "%d,%d\n", i100000, srt[winReach])
	}
	f100000.Close()
	fMedian.Close()
	return &result100000, &resultMedian, resultBannedMantissas
}
