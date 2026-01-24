package jobs

import (
	"encoding/csv"
	"errors"
	"fmt"
	"github.com/KitchenMishap/pudding-huffman/blockchain"
	"github.com/KitchenMishap/pudding-huffman/compress"
	"github.com/KitchenMishap/pudding-huffman/derived"
	"github.com/KitchenMishap/pudding-huffman/huffman"
	"github.com/KitchenMishap/pudding-huffman/kmeans"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"math"
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

	println("Truncating...")
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

// Numbers up to 21 million btc (in sats) are unsafe to use as an escape code, because a txo amount could match.
// So we go to 22 million (times 100,000,000 sats) and make it -ve for good measure
const ESCAPE_VALUE = -2200000000000000

// The maximum number of zeroes at the end of a base 10 number. 15 is about enough for max supply of sats.
const MAX_BASE_10_EXP = 20

func GatherStatistics(folder string) error {
	var startTime = time.Now()
	elapsed := time.Since(startTime)
	fmt.Printf("The time is now: %s\n", startTime.Format(time.TimeOnly))
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Very start **==")

	println("Please wait... opening files")
	reader, err := blockchain.NewChainReader(folder)
	if err != nil {
		return err
	}
	println("Finished opening files.")
	blockchainInterface := reader.Blockchain()
	latestBlock, err := blockchainInterface.LatestBlock()
	if err != nil {
		return err
	}
	println("The last block height is:", latestBlock.Height())

	blocks := latestBlock.Height() + 1
	blockToTxo := make([]int64, blocks)

	println("Gathering txo indices for each block...")
	blockHeight := int64(0)
	blockHandle := blockchainInterface.GenesisBlock()
	for {
		if blockHeight%100000 == 0 {
			println("Block: ", blockHeight)
		}

		block, err := blockchainInterface.BlockInterface(blockHandle)
		if err != nil {
			return err
		}
		transHandle, err := block.NthTransaction(0)
		if err != nil {
			return err
		}
		trans, err := blockchainInterface.TransInterface(transHandle)
		if err != nil {
			return err
		}
		txoHandle, err := trans.NthTxo(0)
		if err != nil {
			return err
		}
		if !txoHandle.TxoHeightSpecified() {
			return errors.New("txo height not specified by handle")
		}
		blockToTxo[blockHeight] = txoHandle.TxoHeight()

		blockHeight++
		if blockHeight == blocks {
			break
		}
		blockHandle, err = blockchainInterface.NextBlock(blockHandle)
	}
	numTxos := blockToTxo[blocks-1]
	p := message.NewPrinter(language.English) // For commas between thousands
	p.Printf("There are: %d txos in the first %d blocks\n", numTxos, blocks)

	fmt.Printf("Gathering the amounts (big file read coming...)\n")
	derivedFiles, err := derived.NewDerivedFiles(folder)
	if err != nil {
		return err
	}
	err = derivedFiles.OpenReadOnly()
	if err != nil {
		return err
	}
	satsFile := derivedFiles.PrivilegedFiles().TxoSatsFile()
	amounts, err := satsFile.ReadWholeFileAsInt64s()
	if err != nil {
		return err
	}
	fmt.Printf("Gathering the amounts (...finished file read)\n")

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Creating the celebrity histograms per epoch **==")

	const blocksPerEpoch = 144 * 7 // Roughly a week
	const blocksPerMicroEpoch = 6  // Roughly an hour
	numEpochs := int64(blocks/blocksPerEpoch + 1)
	numWorkers := int(runtime.NumCPU())
	if numWorkers > 4 {
		numWorkers -= 2 // Some spare for the OS
	}

	// Here we use the "worker pool" ("feed the  beast") pattern
	// 1. Create a channel to hold the epochIDs
	epochChan := make(chan int, 0)

	// 2. Create a slice to store the results (one map per epoch)
	epochToCelebsMap := make([]map[int64]int64, numEpochs)

	var wg sync.WaitGroup

	// 3. Start the pool of workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Workers pull from the channel until it's closed
			for eID := range epochChan {
				// --- WORKER LOGIC START ---
				localMap := make(map[int64]int64)

				startBlock := int64(eID * blocksPerEpoch)
				endBlock := startBlock + blocksPerEpoch
				if endBlock > blocks {
					endBlock = blocks
				}

				for b := startBlock; b < endBlock; b++ {
					txoStart := blockToTxo[b]
					txoEnd := numTxos // Rare fallback
					if b+1 < blocks {
						txoEnd = blockToTxo[b+1] // Common case
					}

					for m := txoStart; m < txoEnd; m++ {
						localMap[amounts[m]]++
					}
				}

				// Save result to shared slice (this IS threadsafe because eID is unique)
				epochToCelebsMap[eID] = localMap
				// --- WORKER LOGIC END ---
			}
		}()
	}
	// 4. Feed the Channel (The Producer)
	for eID := 0; eID < int(numEpochs); eID++ {
		if eID%100 == 0 {
			fmt.Println("Feeding epoch: ", eID)
		}
		epochChan <- eID
	}
	close(epochChan) // Crucial: Workers stop when the channel is empty and closed

	wg.Wait()
	// END Stuff suggested by Gemini

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Huffman per Epoch (now parallel) **==")

	lock := sync.Mutex{}
	reasonHist := make(map[int]int64)

	// Build the Forest of Trees
	// 1. Create the results slice
	epochToCelebCodes := make([]map[int64]huffman.BitCode, numEpochs)

	// 2. Setup WaitGroup and Channel
	//var wg sync.WaitGroup (use the previous one)
	epochChan2 := make(chan int, numWorkers)

	// 3. Start Workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for eID := range epochChan2 {
				// Check for empty data
				if len(epochToCelebsMap[eID]) == 0 {
					continue
				}

				// --- THE ACTUAL LOGIC ---
				captureCoverage := 0.7
				epochCelebsTruncated, reason := TruncateMapWithEscapeCode(
					epochToCelebsMap[eID], 100000, captureCoverage, ESCAPE_VALUE,
				)
				lock.Lock()
				reasonHist[reason]++
				lock.Unlock()

				huffCelebRoot := huffman.BuildHuffmanTree(epochCelebsTruncated)
				localCodes := make(map[int64]huffman.BitCode)
				huffman.GenerateBitCodes(huffCelebRoot, 0, 0, localCodes)

				// Thread-safe write to independent slice index
				epochToCelebCodes[eID] = localCodes
			}
		}()
	}

	// 4. Feed the Workers
	for eID := 0; eID < int(numEpochs); eID++ {
		epochChan2 <- eID
	}
	close(epochChan2)
	wg.Wait()

	fmt.Printf("Statistics of why each map was truncated before being sent for Huffman encoding:\n")
	fmt.Printf("%s: %d occurances\n", REASON_STRING_0, reasonHist[0])
	fmt.Printf("%s: %d occurances\n", REASON_STRING_1, reasonHist[1])
	fmt.Printf("%s: %d occurances\n", REASON_STRING_2, reasonHist[2])

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Simulating compression **==")
	result, magFreqs, expFreqs := compress.ParallelAmountStatistics(amounts, blocksPerEpoch, blockToTxo, epochToCelebCodes, MAX_BASE_10_EXP)

	fmt.Printf("Celebrity hits: %d\n", result.CelebrityHits)
	fmt.Printf("Literal hits: %d\n", result.LiteralHits)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Identifying fiat peaks (parallel) **==")

	microEpochs := blocks/blocksPerMicroEpoch + 1
	microEpochToPhasePeaks := kmeans.ParallelKMeans(amounts, blockToTxo, blocksPerMicroEpoch, epochToCelebCodes, blocksPerEpoch)
	for meID := 0; meID < int(microEpochs); meID++ {
		// Sort the peaks for this epoch so Peak 0 is always the smallest phase
		sort.Float64s(microEpochToPhasePeaks[meID])
	}

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Build residuals map (now parallel, now per EXP) **==")

	residualsMapByExp, combinedFreq := compress.ParallelGatherResidualFrequenciesByExp10(amounts, blocksPerEpoch, blocksPerMicroEpoch, blockToTxo, epochToCelebCodes, microEpochToPhasePeaks, MAX_BASE_10_EXP)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** More Huffman stuff **==")

	fmt.Printf("Huffman tree for combined peak and harmonic selection\n")
	combinedTruncated, reason := TruncateMapWithEscapeCode(combinedFreq, 24, 1.0, ESCAPE_VALUE)
	huffCombinedRoot := huffman.BuildHuffmanTree(combinedTruncated)
	combinedCodes := make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(huffCombinedRoot, 0, 0, combinedCodes)
	fmt.Printf("Reason (if any) why frequencies map was truncated\n")
	if reason == 0 {
		println(REASON_STRING_0)
	}
	if reason == 1 {
		println(REASON_STRING_1)
	}
	if reason == 2 {
		println(REASON_STRING_2)
	}

	fmt.Printf("Huffman trees for clockPhase residuals AT EACH EXP MAGNITUDE\n")
	residualCodesByExp := make([]map[int64]huffman.BitCode, MAX_BASE_10_EXP)
	reasonHist = make(map[int]int64)
	for exp := 0; exp < MAX_BASE_10_EXP; exp++ {
		// Build a specific tree for this exponent
		// Lets pick a max number of codes.
		maxCodes := GetSensibleMaxCodes(exp)
		residualTruncated, reason := TruncateMapWithEscapeCode(residualsMapByExp[exp], maxCodes, 0.99, ESCAPE_VALUE)
		reasonHist[reason]++
		huffResidualRoot := huffman.BuildHuffmanTree(residualTruncated)
		residualCodesByExp[exp] = make(map[int64]huffman.BitCode)
		huffman.GenerateBitCodes(huffResidualRoot, 0, 0, residualCodesByExp[exp])
	}
	fmt.Printf("Statistics of why each map was truncated before being sent for Huffman encoding:\n")
	fmt.Printf("%s: %d occurances\n", REASON_STRING_0, reasonHist[0])
	fmt.Printf("%s: %d occurances\n", REASON_STRING_1, reasonHist[1])
	fmt.Printf("%s: %d occurances\n", REASON_STRING_2, reasonHist[2])

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
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Simulating compression with fiat peaks **==")

	result, microEpochToPeakStrengths := compress.ParallelSimulateCompressionWithKMeans(amounts, blocksPerEpoch, blocksPerMicroEpoch, blockToTxo, epochToCelebCodes, expCodes, residualCodesByExp, magnitudeCodes, combinedCodes, microEpochToPhasePeaks)

	p = message.NewPrinter(language.English) // For commas between thousands
	p.Printf("TotalBits: %d\n", result.TotalBits)
	p.Printf("BTC Celebrity hits: %d\n", result.CelebrityHits)
	p.Printf("Fiat Ghost hits: %d\n", result.KMeansHits)
	p.Printf("Literal hits: %d\n", result.LiteralHits)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Finished **==")

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
