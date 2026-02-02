package jobs

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/KitchenMishap/pudding-huffman/blockchain"
	"github.com/KitchenMishap/pudding-huffman/compress"
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"github.com/KitchenMishap/pudding-huffman/kmeans"
	"golang.org/x/sync/errgroup"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Histograms struct {
	// Sharding the histogram maps reduces lock contention on the maps
	shardsAmount [256]map[int64]int64
	mu           [256]sync.Mutex
}

func (h *Histograms) Add(amount int64) {
	idx := amount & 0xFF
	h.mu[idx].Lock()
	if h.shardsAmount[idx] == nil {
		h.shardsAmount[idx] = make(map[int64]int64, 100000)
	}
	h.shardsAmount[idx][amount]++
	h.mu[idx].Unlock()
}

type Entry struct {
	Value int64
	Count int64
}

func (h *Histograms) MergeAndSort() (shardsAmount map[int64]int64) {
	amountsMap := make(map[int64]int64)

	// Merge
	for i := 0; i < 256; i++ {
		h.mu[i].Lock()
		for amount, count := range h.shardsAmount[i] {
			amountsMap[amount] += count
		}
		h.mu[i].Unlock()
	}

	fmt.Printf("Truncating...\n")
	amountTruncated, reason := TruncateMapWithEscapeCode(amountsMap, 100000, 0.99, -1)
	if reason == 0 {
		fmt.Printf(REASON_STRING_0)
	}
	if reason == 1 {
		fmt.Printf(REASON_STRING_1)
	}
	if reason == 2 {
		fmt.Printf(REASON_STRING_2)
	}
	fmt.Printf("Amount: truncated to %d (celebs)", len(amountTruncated))

	return amountTruncated
}

const REASON_STRING_0 = "NoReason"
const REASON_STRING_1 = "MaxCodesReached"
const REASON_STRING_2 = "CoverageReached"
const NO_REASON_FLAG = 0
const MAXCODES_REACHED_FLAG = 1
const COVERAGE_REACHED_FLAG = 2

func TruncateMapWithEscapeCode(all map[int64]int64, maxCodes int, captureCoverage float64, escapeCode int64) (map[int64]int64, int) {
	reasonFlag := NO_REASON_FLAG
	entries := make([]Entry, 0, len(all))
	total := int64(0)
	for k, v := range all {
		total += v
		entries = append(entries, Entry{Value: k, Count: v})
	}
	// Sort
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Count > entries[j].Count
	})
	// Truncate
	some := make(map[int64]int64)
	soFar := int64(0)
	for entry := range entries {
		soFar += entries[entry].Count
		some[entries[entry].Value] = entries[entry].Count
		if entry+1 >= maxCodes {
			reasonFlag = MAXCODES_REACHED_FLAG
			break // maxCodes reached
		}
		if float64(soFar)/float64(total) >= captureCoverage {
			reasonFlag = COVERAGE_REACHED_FLAG
			break // captureCoverage ratio met
		}
	}
	some[escapeCode] = total - soFar
	return some, reasonFlag
}

func bucketCount(beans int64, beansPerBucket int64) int64 {
	return (beans + beansPerBucket - 1) / beansPerBucket
}

// Numbers up to 21 million btc (in sats) are unsafe to use as an escape code, because a txo amount could match.
// So we go to 22 million (times 100,000,000 sats) and make it -ve for good measure
const ESCAPE_VALUE = -2200000000000000

// The maximum number of zeroes at the end of a base 10 number. 15 is about enough for max supply of sats.
const MAX_BASE_10_EXP = 20

func GatherStatistics(folder string, deterministic *rand.Rand) error {
	reader, err := blockchain.NewChainReader(folder)

	var startTime = time.Now()
	elapsed := time.Since(startTime)
	fmt.Printf("The time is now: %s\n", startTime.Format(time.TimeOnly))
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Very start Kinda (after user has typed!) **==")

	if err != nil {
		return err
	}
	chain := reader.Blockchain()
	handles := reader.HandleCreator()
	latestBlock, err := chain.LatestBlock()
	if err != nil {
		return err
	}

	//const blocksPerEpoch = 144 * 7 // Roughly a week
	//const blocksPerMicroEpoch = 6  // Roughly an hour
	const blocksPerEpoch = 144 * 28    // Roughly a month
	const blocksPerMicroEpoch = 6 * 24 // Roughly a day

	totalBlocks := latestBlock.Height() + 1
	totalNumEpochs := bucketCount(totalBlocks, blocksPerEpoch)

	interestedEpoch := 7 * totalNumEpochs / 8 // Integer math, approximate
	interestedEpochs := totalNumEpochs/8 - 10 // 10 leeway for approximation
	interestedBlock := interestedEpoch * blocksPerEpoch
	interestedBlocks := interestedEpochs * blocksPerEpoch
	interestedMicroEpoch := interestedBlock / blocksPerMicroEpoch

	fmt.Printf("** In this run, we are looking at %d blocks, starting at %d **\n", interestedBlocks, interestedBlock)
	sSpokes := "1-2-5 spokes"
	fmt.Printf("** In this run, we are using %s **\n", sSpokes)
	fmt.Printf("** In this run, there are %d blocks in a micro-epoch **\n", blocksPerMicroEpoch)
	fmt.Printf("** In this run, there are %d blocks in an epoch **\n", blocksPerEpoch)

	elapsed = time.Since(startTime)
	sJob := "Creating the celebrity histograms per epoch (PARALLEL by epoch)"
	tJob := time.Now()
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), sJob)

	workersDivider := 1
	numWorkers := runtime.NumCPU() / workersDivider
	if numWorkers > 8 {
		numWorkers -= 4 // Save some for OS
	}
	//numWorkers = 1 // Serial test! I may be some time
	fmt.Printf("\tNUMWORKERS:%d\n", numWorkers)

	// Here we use the "worker pool" ("feed the  beast") pattern
	// 1. Create a channel to hold the epochIDs
	epochChan := make(chan int64) // epochIDs get squirted into here

	// 2. Create a slice to store the results (one map per epoch)
	epochToCelebsMap := make([]map[int64]int64, interestedEpochs)

	var wg sync.WaitGroup

	// Create an errgroup and a context
	g, ctx := errgroup.WithContext(context.Background())

	// 3. Start the pool of workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		g.Go(func() error { // Use the errgroup instead of "go func() {"
			defer wg.Done()

			// Workers pull from the channel until it's closed
			for eID := range epochChan {
				// Check if another worker already failed
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}

				// --- WORKER LOGIC START ---
				localMap := make(map[int64]int64) // ToDo initial size?

				startBlock := eID * blocksPerEpoch
				endBlock := startBlock + blocksPerEpoch
				if endBlock > interestedBlock+interestedBlocks {
					endBlock = interestedBlock + interestedBlocks
				}

				blockHandle, err := handles.BlockHandleByHeight(startBlock)
				if err != nil {
					return err
				}
				for b := startBlock; b < endBlock; b++ {
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
							localMap[sats]++
						}
					}
					blockHandle, err = chain.NextBlock(blockHandle)
					if err != nil {
						return err
					}
				}

				// Save result to shared slice (this IS threadsafe because eID is unique)
				epochToCelebsMap[eID-interestedEpoch] = localMap
				// --- WORKER LOGIC END ---
			}
			return nil
		})
	}
	// 4. Feed the Channel (The Producer). This is now errgroup context-aware
	go func() {
		defer close(epochChan)
		for eID := interestedEpoch; eID < interestedEpoch+interestedEpochs; eID++ {
			select { // Note: NOT a switch statement!
			case epochChan <- eID: // This happens if a worker is free to be fed an epoch ID
			case <-ctx.Done(): // This happens if a worker returned an err
				return
			}
		}
	}()

	// Wait for completion and handle the error
	if err := g.Wait(); err != nil {
		return err
	}

	jobElapsed := time.Since(tJob)
	fmt.Printf("\t%s: Job took: [%5.1f min]\n", sJob, jobElapsed.Minutes())

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Huffman per Epoch (now parallel) **==")

	lock := sync.Mutex{}
	reasonHist := make(map[int]int64)

	// Build the Forest of Trees
	// 1. Create the results slice
	epochToCelebCodes := make([]map[int64]huffman.BitCode, interestedEpochs)

	// 2. Setup WaitGroup and Channel
	//var wg sync.WaitGroup (use the previous one)
	epochChan2 := make(chan int64, numWorkers)

	// 3. Start Workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for eID := range epochChan2 {
				// Check for empty data
				if len(epochToCelebsMap[eID-interestedEpoch]) == 0 {
					continue
				}

				captureCoverage := 0.7
				epochCelebsTruncated, reason := TruncateMapWithEscapeCode(
					epochToCelebsMap[eID-interestedEpoch], 100000, captureCoverage, ESCAPE_VALUE,
				)
				lock.Lock()
				reasonHist[reason]++
				lock.Unlock()

				huffCelebRoot := huffman.BuildHuffmanTree(epochCelebsTruncated)
				localCodes := make(map[int64]huffman.BitCode)
				huffman.GenerateBitCodes(huffCelebRoot, 0, 0, localCodes)

				// Thread-safe write to independent slice index
				epochToCelebCodes[eID-interestedEpoch] = localCodes
			}
		}()
	}

	// 4. Feed the Workers
	for eID := interestedEpoch; eID < interestedEpoch+interestedEpochs; eID++ {
		epochChan2 <- eID
	}
	close(epochChan2)
	wg.Wait()

	fmt.Printf("\tStatistics of why each map was truncated before being sent for Huffman encoding:\n")
	fmt.Printf("\t%s: %d occurances\n", REASON_STRING_0, reasonHist[0])
	fmt.Printf("\t%s: %d occurances\n", REASON_STRING_1, reasonHist[1])
	fmt.Printf("\t%s: %d occurances\n", REASON_STRING_2, reasonHist[2])

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Simulating compression **==")
	result, magFreqs, expFreqs, err := compress.ParallelAmountStatistics(chain, handles, interestedBlock, interestedBlocks, blocksPerEpoch, epochToCelebCodes, MAX_BASE_10_EXP)
	if err != nil {
		return err
	}

	fmt.Printf("\tCelebrity hits: %d\n", result.CelebrityHits)
	fmt.Printf("\tLiteral hits: %d\n", result.LiteralHits)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Identifying fiat peaks (parallel) **==")

	interestedMicroEpochs := bucketCount(interestedBlocks, blocksPerMicroEpoch)

	var microEpochToPhasePeaks [][]float64
	var microEpochToPeakStrengths [][3]int64

	var excludeTransOutputs *[2000000000]byte = nil // Comes out of pass 0, goes into pass 1
	var excludeCelebs []map[int64]bool = nil        // Comes out of pass 0, goes into pass 1
	for pass := 0; pass < 2; pass++ {
		fmt.Printf("\t==== Pass %d ====\n", pass)

		microEpochToPhasePeaks, err := kmeans.ParallelKMeans(chain, handles, interestedBlock, interestedBlocks, blocksPerMicroEpoch,
			epochToCelebCodes, blocksPerEpoch, deterministic, excludeTransOutputs, excludeCelebs,
			sSpokes, nil)
		if err != nil {
			return err
		}

		if pass == 0 {
			f, err := os.Create("FourDigits.csv")
			if err != nil {
				panic("couldn't open file")
			}

			//microEpochsPerEpoch := blocksPerEpoch / blocksPerMicroEpoch
			for meID := interestedMicroEpoch; meID < interestedMicroEpoch+interestedMicroEpochs; meID++ {
				dayCounter := meID
				//TimeOfDay := meID % 24
				year := 2009 + math.Round(100.0*(float64(dayCounter)/(52.1775*7.0)))/100.0
				if microEpochToPhasePeaks[meID-interestedMicroEpoch] != nil && microEpochToPhasePeaks[meID-interestedMicroEpoch].Len() > 0 {
					L := microEpochToPhasePeaks[meID-interestedMicroEpoch].Get(0)
					val := math.Pow(10, -float64(L))
					for val < 1000 {
						val *= 10
					}
					for val >= 10000 {
						val /= 10
					}
					digits := int(val)
					fmt.Fprintf(f, "%.2f, %d\n", year, digits)
				}
			}
			f.Close()
		} // if pass==1

		//	for meID := interestedMicroEpoch; meID < int(interestedMicroEpoch + interestedMicroEpochs); meID++ {
		// Sort the peaks for this epoch so Peak 0 is always the smallest phase
		//			sort.Float64s(microEpochToPhasePeaks[meID - interestedMicroEpoch])	ToDo
		//	}

		if pass == 0 {
			elapsed = time.Since(startTime)
			fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "Build residuals map (PARALLEL per exp) ")
			residualsEncoderByExp, combinedFreq := compress.ParallelGatherResidualFrequenciesByExp10(chain, handles, blocksPerEpoch, blocksPerMicroEpoch, interestedBlock, interestedBlocks, epochToCelebCodes, microEpochToPhasePeaks, MAX_BASE_10_EXP, ESCAPE_VALUE)

			fmt.Printf("SECOND round of KMeans...")
			microEpochToPhasePeaks, err = kmeans.ParallelKMeans(chain, handles, interestedBlock, interestedBlocks, blocksPerMicroEpoch,
				epochToCelebCodes, blocksPerEpoch, deterministic, excludeTransOutputs, excludeCelebs,
				sSpokes, residualsEncoderByExp)
			if err != nil {
				return err
			}

			elapsed = time.Since(startTime)
			fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** More Huffman stuff **==")

			fmt.Printf("Huffman tree for combined peak and harmonic selection\n")
			huffCombinedRoot := huffman.BuildHuffmanTreeFromSlice(combinedFreq[:], 0)
			combinedCodes := make(map[int64]huffman.BitCode)
			huffman.GenerateBitCodes(huffCombinedRoot, 0, 0, combinedCodes)

			fmt.Printf("\tStatistics of why each map was truncated before being sent for Huffman encoding:\n")
			fmt.Printf("\t%s: %d occurances\n", REASON_STRING_0, reasonHist[0])
			fmt.Printf("\t%s: %d occurances\n", REASON_STRING_1, reasonHist[1])
			fmt.Printf("\t%s: %d occurances\n", REASON_STRING_2, reasonHist[2])

			fmt.Printf("Huffman tree for literal magnitudes...\n")
			magnitudesMap := make(map[int64]int64)
			for mag := int64(0); mag <= 64; mag++ {
				freq := magFreqs[mag]
				magnitudesMap[mag] = freq
			}
			huffMagnitudeRoot := huffman.BuildHuffmanTree(magnitudesMap)
			magnitudeCodes := make(map[int64]huffman.BitCode)
			huffman.GenerateBitCodes(huffMagnitudeRoot, 0, 0, magnitudeCodes)

			fmt.Printf("Huffman tree for base 10 exps...\n")
			expsMap := make(map[int64]int64)
			for exp := int64(0); exp < MAX_BASE_10_EXP; exp++ {
				freq := expFreqs[exp]
				expsMap[exp] = freq
			}
			huffExpRoot := huffman.BuildHuffmanTree(expsMap)
			expCodes := make(map[int64]huffman.BitCode)
			huffman.GenerateBitCodes(huffExpRoot, 0, 0, expCodes)

			elapsed = time.Since(startTime)
			sJob = "==** Simulating compression with fiat peaks **=="
			tJob := time.Now()
			fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), sJob)
			result, microEpochToPeakStrengths, excludeTransOutputs, excludeCelebs = compress.ParallelSimulateCompressionWithKMeans(chain, handles,
				blocksPerEpoch, blocksPerMicroEpoch, interestedBlock, interestedBlocks,
				epochToCelebCodes, expCodes, residualsEncoderByExp,
				magnitudeCodes, combinedCodes, microEpochToPhasePeaks)
			jobElapsed := time.Since(tJob)
			fmt.Printf("\t%s: Job took: [%5.1f min]\n", sJob, jobElapsed.Minutes())

			bitsPerGB := float64(8 * 1024 * 1024 * 1024)
			p := message.NewPrinter(language.English) // For commas between thousands
			p.Printf("TotalBits: %d (%f GB)\n", result.TotalBits, float64(result.TotalBits)/bitsPerGB)
			p.Printf("-----\n")
			p.Printf("BTC Celebrity bits: %d (%f GB)\n", result.CelebrityBits, float64(result.CelebrityBits)/bitsPerGB)
			p.Printf("BTC Celebrity hits: %d\n", result.CelebrityHits)
			p.Printf("BTC Celebrity average bits: %.1f\n", float64(result.CelebrityBits)/float64(result.CelebrityHits))
			p.Printf("-----\n")
			p.Printf("Fiat Ghost bits: %d (%f GB)\n", result.GhostBits, float64(result.GhostBits)/bitsPerGB)
			p.Printf("Fiat Ghost hits: %d\n", result.GhostHits)
			p.Printf("Fiat Ghost average bits: %.1f\n", float64(result.GhostBits)/float64(result.GhostHits))
			p.Printf("-----\n")
			p.Printf("Literal Satoshis bits: %d (%f GB)\n", result.LiteralBits, float64(result.LiteralBits)/bitsPerGB)
			p.Printf("Literal Satoshis hits: %d\n", result.LiteralHits)
			p.Printf("Literal Satoshis average bits: %.1f\n", float64(result.LiteralBits)/float64(result.LiteralHits))
			p.Printf("-----\n")
			p.Printf("The Rest bits: %d (%f GB)\n", result.RestBits, float64(result.RestBits)/bitsPerGB)
			p.Printf("TheRest Satoshis hits: %d\n", result.RestHits)
			p.Printf("TheRest Satoshs average bits: %.1f\n", float64(result.RestBits)/float64(result.RestHits))
			p.Printf("-----\n")

			p.Printf("Fiat Ghost hits: %d\n", result.GhostHits)
			p.Printf("Literal hits: %d\n", result.LiteralHits)
			p.Printf("Rest hits: %d\n", result.RestHits)
			elapsed = time.Since(startTime)
		}

		fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Finished Pass **==")
	} // for pass
	exportOracleCSV("Oracle.csv", microEpochToPhasePeaks, microEpochToPeakStrengths)

	return nil
}

type PeakResult struct {
	Value    float64
	Strength int64
}

func GetSensibleMaxCodes(exponent int) int {
	// W is our "Roundness Window" in log10 space (e.g., 0.01 for 1%)
	const W = 0.05

	// Calculate the integer width of that 1% wedge
	upper := math.Pow(10, float64(exponent)+W/2)
	lower := math.Pow(10, float64(exponent)-W/2)

	// The number of integers we need to cover the window
	needed := int(math.Ceil(upper - lower))

	// Guardrails
	if needed < 10 {
		return 10 // Minimum to capture even tiny peaks
	}
	if needed > 1000000 {
		return 1000000 // Your "Silliness" cap
	}
	return needed
}

func exportOracleCSV(filename string, microEpochToPhasePeaks [][]float64, peakStrengths [][compress.CSV_COLUMNS]int64) {
	f, _ := os.Create(filename)
	defer f.Close()
	w := csv.NewWriter(f)

	header := []string{"microEpoch"}
	for i := 0; i < compress.CSV_COLUMNS; i++ {
		header = append(header, fmt.Sprintf("P%d_Value", i), fmt.Sprintf("P%d_Strength", i))
	}
	w.Write(header)

	for microEpochID, peaks := range microEpochToPhasePeaks {
		if peaks == nil {
			continue
		}
		row := []string{fmt.Sprintf("%d", microEpochID)}

		results := make([]PeakResult, compress.CSV_COLUMNS)
		for peakIdx := 0; peakIdx < compress.CSV_COLUMNS; peakIdx++ {
			results[peakIdx] = PeakResult{
				Value:    peaks[peakIdx],
				Strength: peakStrengths[microEpochID][peakIdx],
			}
		}
		sort.Slice(results, func(i, j int) bool {
			return results[i].Strength > results[j].Strength
		})

		for peakPriority := 0; peakPriority < compress.CSV_COLUMNS; peakPriority++ {
			row = append(row, fmt.Sprintf("%.4f", results[peakPriority].Value), fmt.Sprintf("%d", results[peakPriority].Strength))
		}
		w.Write(row)
	}
	w.Flush()
}
