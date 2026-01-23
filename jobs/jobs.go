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
	amountTruncated := TruncateMapWithEscapeCode(amountsMap, 100000, 0.99, -1)
	println("Amount: truncated to (celebs)", len(amountTruncated))

	return amountTruncated
}

func TruncateMapWithEscapeCode(all map[int64]int64, maxCodes int, captureCoverage float64, escapeCode int64) map[int64]int64 {
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
			println("maxCodes reached")
			break // maxCodes reached
		}
		if float64(soFar)/float64(total) >= captureCoverage {
			println("capture coverage reached")
			break // captureCoverage ratio met
		}
	}
	some[escapeCode] = total - soFar
	return some
}

// Numbers up to 21 million btc (in sats) are unsafe to use as an escape code, because a txo amount could match.
// So we go to 22 million (times 100,000,000 sats) and make it -ve for good measure
const ESCAPE_VALUE = -2200000000000000

// The maximum number of zeroes at the end of a base 10 number. 15 is about enough for max supply of sats.
const MAX_BASE_10_EXP = 20

func GatherStatistics(folder string) error {
	var startTime = time.Now()
	elapsed := time.Since(startTime)
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
	println("There are: ", numTxos, " txos in the first: ", blocks, " blocks.")

	println("Gathering the amounts...")
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

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Creating the celebrity histogramS PER EPOCH **==")

	const blocksPerEpoch = 144 * 7 // Roughly a week
	numEpochs := int64(blocks/blocksPerEpoch + 1)
	numWorkers := int(runtime.NumCPU())
	if numWorkers > 4 {
		numWorkers -= 2 // Some spare for the OS
	}

	// Here we use the "worker pool" ("feed the  beast") pattern
	// 1. Create a channel to hold the epochIDs
	epochChan := make(chan int, numEpochs)

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
		epochChan <- eID
	}
	close(epochChan) // Crucial: Workers stop when the channel is empty and closed

	wg.Wait()
	// END Stuff suggested by Gemini

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Huffman per Epoch **==\n")

	// Build the Forest of Trees
	epochToCelebCodes := make([]map[int64]huffman.BitCode, numEpochs)
	for eID := 0; eID < int(numEpochs); eID++ {
		if len(epochToCelebsMap[eID]) == 0 {
			continue
		}

		captureCoverage := 0.7 // If we allowed capture of more celebrities (say, 0.99) the kmeans would be starved of info
		epochCelebsTruncated := TruncateMapWithEscapeCode(epochToCelebsMap[eID], 100000, captureCoverage, ESCAPE_VALUE)
		huffCelebRoot := huffman.BuildHuffmanTree(epochCelebsTruncated)
		epochToCelebCodes[eID] = make(map[int64]huffman.BitCode)
		huffman.GenerateBitCodes(huffCelebRoot, 0, 0, epochToCelebCodes[eID])
	}

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Simulating compression **==")
	result, amountsEachEpoch, magFreqs, expFreqs := compress.ParallelAmountStatistics(amounts, blocksPerEpoch, blockToTxo, epochToCelebCodes, MAX_BASE_10_EXP)

	println("Celebrity hits: ", result.CelebrityHits)
	println("Literal hits: ", result.LiteralHits)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Doing kmeans stuff (parallel) **==")

	epochs := blocks/blocksPerEpoch + 1 // +1 for the partial epoch at the end
	epochToPhasePeaks := kmeans.ParallelKMeans(amountsEachEpoch, epochs)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Build residuals map (now parallel, now per EXP) **==")

	residualsMapByExp := compress.ParallelGatherResidualFrequenciesByExp10(amounts, blocksPerEpoch, blockToTxo, epochToCelebCodes, epochToPhasePeaks, MAX_BASE_10_EXP)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** More Huffman stuff **==")

	println("Huffman trees for clockPhase residuals AT EACH EXP MAGNITUDE")
	residualCodesByExp := make([]map[int64]huffman.BitCode, MAX_BASE_10_EXP)
	for exp := 0; exp < MAX_BASE_10_EXP; exp++ {
		// Build a specific tree for this exponent
		// Lets pick a max number of codes.
		maxCodes := GetSensibleMaxCodes(exp)
		residualTruncated := TruncateMapWithEscapeCode(residualsMapByExp[exp], maxCodes, 0.99, ESCAPE_VALUE)
		println("Huffman tree for residuals...")
		huffResidualRoot := huffman.BuildHuffmanTree(residualTruncated)
		residualCodesByExp[exp] = make(map[int64]huffman.BitCode)
		huffman.GenerateBitCodes(huffResidualRoot, 0, 0, residualCodesByExp[exp])
	}

	println("Huffman tree for literal magnitudes...")
	magnitudesMap := make(map[int64]int64)
	for mag := int64(0); mag <= 64; mag++ {
		freq := magFreqs[mag]
		magnitudesMap[mag] = freq
	}
	huffMagnitudeRoot := huffman.BuildHuffmanTree(magnitudesMap)
	magnitudeCodes := make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(huffMagnitudeRoot, 0, 0, magnitudeCodes)

	println("Huffman tree for base 10 exps...")
	expsMap := make(map[int64]int64)
	for exp := int64(0); exp < MAX_BASE_10_EXP; exp++ {
		freq := expFreqs[exp]
		expsMap[exp] = freq
	}
	huffExpRoot := huffman.BuildHuffmanTree(expsMap)
	expCodes := make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(huffExpRoot, 0, 0, expCodes)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Simulating compression with kmeans **==")

	result, peakStrengths := compress.ParallelSimulateCompressionWithKMeans(amounts, blocksPerEpoch, blockToTxo, epochToCelebCodes, expCodes, residualCodesByExp, magnitudeCodes, epochToPhasePeaks)

	println("TotalBits: ", result.TotalBits)
	println("Celebrity hits: ", result.CelebrityHits)
	println("KMeans hits: ", result.KMeansHits)
	println("Literal hits: ", result.LiteralHits)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Finished **==")

	exportOracleCSV("Oracle.csv", epochToPhasePeaks, peakStrengths)

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

func exportOracleCSV(filename string, epochToPhasePeaks [][]float64, peakStrengths [][7]int64) {
	f, _ := os.Create(filename)
	defer f.Close()
	w := csv.NewWriter(f)

	header := []string{"Epoch"}
	for i := 0; i < 7; i++ {
		header = append(header, fmt.Sprintf("P%d_Value", i), fmt.Sprintf("P%d_Strength", i))
	}
	w.Write(header)

	for epochID, peaks := range epochToPhasePeaks {
		if peaks == nil {
			continue
		}
		row := []string{fmt.Sprintf("%d", epochID)}

		results := make([]PeakResult, 7)
		for peakIdx := range peaks {
			results[peakIdx] = PeakResult{
				Value:    peaks[peakIdx],
				Strength: peakStrengths[epochID][peakIdx],
			}
		}
		sort.Slice(results, func(i, j int) bool {
			return results[i].Strength > results[j].Strength
		})

		for peakPriority := 0; peakPriority < 7; peakPriority++ {
			row = append(row, fmt.Sprintf("%.4f", results[peakPriority].Value), fmt.Sprintf("%d", results[peakPriority].Strength))
		}
		w.Write(row)
	}
	w.Flush()
}
