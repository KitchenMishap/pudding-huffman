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
	"sync"
	"sync/atomic"
)

const blocksInBatch = 1000

func WholeChainMantissaHistogram(chain chainreadinterface.IBlockChain,
	handles chainreadinterface.IHandleCreator,
	interestedBlock int64, interestedBlocks int64) (xAxis5Digits *[100000]int64) {

	completedBlocks := int64(0) // Atomic int
	result100000 := [100000]int64{}

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
								index100000 := int(oneToTen * 10000)
								local.local100000[index100000]++
							}
						} // for txo amounts
					} // for transactions
				} // for block

				done := atomic.AddInt64(&completedBlocks, blocksInBatch)
				if done%1000 == 0 || done == interestedBlocks {
					fmt.Printf("\r\tProgress: %.1f%%    ", float64(100*done)/float64(interestedBlocks))
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
	for result := range resultsChan {
		for i100000 := 0; i100000 < 100000; i100000++ {
			result100000[i100000] += result.local100000[i100000]
		}
	}
	f100000, err := os.Create("100000Hist.csv")
	if err != nil {
		panic("could not create file 1000Hist.csv")
	}
	for i100000 := 0; i100000 < 100000; i100000++ {
		fmt.Fprintf(f100000, "%d, %d\n", i100000, result100000[i100000])
	}
	f100000.Close()
	return &result100000
}
