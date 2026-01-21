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
	"os"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Histograms struct {
	// Sharding the histogram maps reduces lock contention on the maps
	shardsAmount   [256]map[int64]int64
	shardsMantissa [256]map[int64]int64
	shardsExponent [256]map[int64]int64
	mu             [256]sync.Mutex
}

func (h *Histograms) Add(amount int64) {
	idx := amount & 0xFF
	h.mu[idx].Lock()
	if h.shardsAmount[idx] == nil {
		h.shardsAmount[idx] = make(map[int64]int64, 100000)
		h.shardsMantissa[idx] = make(map[int64]int64, 100000)
		h.shardsExponent[idx] = make(map[int64]int64, 100000)
	}
	ff := compress.NewForensicFloat(amount)
	h.shardsAmount[idx][amount]++
	h.shardsMantissa[idx][ff.Mantissa]++
	h.shardsExponent[idx][ff.Exponent]++
	h.mu[idx].Unlock()
}

type Entry struct {
	Value int64
	Count int64
}

func (h *Histograms) MergeAndSort() (amountsMap map[int64]int64, mantissasMap map[int64]int64, exponentsMap map[int64]int64) {
	amountMap := make(map[int64]int64)
	mantissaMap := make(map[int64]int64)
	exponentMap := make(map[int64]int64)

	// Merge
	for i := 0; i < 256; i++ {
		h.mu[i].Lock()
		for amount, count := range h.shardsAmount[i] {
			amountMap[amount] += count
		}
		for mantissa, count := range h.shardsMantissa[i] {
			mantissaMap[mantissa] += count
		}
		for exponent, count := range h.shardsExponent[i] {
			exponentMap[exponent] += count
		}
		h.mu[i].Unlock()
	}

	println("Truncating...")
	amountTruncated := TruncateMapWithEscapeCode(amountMap, 10000, 0.99, -1)
	println("Amount: truncated to ", len(amountTruncated))
	mantissaTruncated := TruncateMapWithEscapeCode(mantissaMap, 10000, 0.99, -1)
	println("mantissa: truncated to ", len(mantissaTruncated))
	exponentTruncated := TruncateMapWithEscapeCode(exponentMap, 16, 1.0, -1)
	println("exponent: truncated to ", len(exponentTruncated))

	return amountTruncated, mantissaTruncated, exponentTruncated
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
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Creating the histogram **==")

	println("Creating the histogram...")
	hist := Histograms{}

	numWorkers := int64(runtime.NumCPU())
	var wg sync.WaitGroup
	chunkSize := int64(numTxos / numWorkers)
	for i := int64(0); i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int64) {
			defer wg.Done()

			start := int64(workerID) * chunkSize
			end := start + chunkSize
			if workerID == numWorkers-1 {
				end = numTxos
			}

			for m := start; m < end; m++ {
				hist.Add(amounts[m])
			}
		}(i)
	}
	wg.Wait()

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Merge and sort **==")
	amountsMap, mantissasMap, exponentsMap := hist.MergeAndSort()

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Huffman stuff **==")

	println("Huffman tree for amounts...")
	huffAmountRoot := huffman.BuildHuffmanTree(amountsMap)
	amountCodes := make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(huffAmountRoot, 0, 0, amountCodes)
	println("Huffman tree for mantissas...")
	huffMantissaRoot := huffman.BuildHuffmanTree(mantissasMap)
	mantissaCodes := make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(huffMantissaRoot, 0, 0, mantissaCodes)
	println("Huffman tree for exponents...")
	huffExponentRoot := huffman.BuildHuffmanTree(exponentsMap)
	exponentCodes := make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(huffExponentRoot, 0, 0, exponentCodes)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Simulating compression **==")
	const blocksPerEpoch = 144 * 7 // Roughly a week
	result, amountsEachEpoch, magFreqs := compress.SimulateCompression(amounts, blocksPerEpoch, blockToTxo, amountCodes, mantissaCodes, exponentCodes)

	println("TotalBits: ", result.TotalBits)
	println("Celebrity hits: ", result.CelebrityHits)
	println("Scientific hits: ", result.ScientificHits)
	println("Literal hits: ", result.LiteralHits)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Doing kmeans stuff (parallel) **==")

	epochs := blocks/blocksPerEpoch + 1 // +1 for the partial epoch at the end
	epochToPhasePeaks := kmeans.ParallelKMeans(amountsEachEpoch, epochs)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Build residuals map (serial) **==")
	residualsMap := make(map[int64]int64)
	block := int64(0)
	epochID := int64(0)
	for txo, amount := range amounts {
		if block+1 < blocks && txo >= int(blockToTxo[block+1]) {
			block++
			epochID = block / blocksPerEpoch
		}
		if epochToPhasePeaks[epochID] != nil {
			_, _, r := kmeans.ExpPeakResidual(amount, epochToPhasePeaks[epochID])
			residualsMap[r]++
		}
	}

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** More Huffman stuff **==")

	println("Huffman tree for clockPhase residuals")
	esc := int64(2100000000000000) // As we can't use -1
	residualTruncated := TruncateMapWithEscapeCode(residualsMap, 10000, 0.99, esc)
	println("Huffman tree for residuals...")
	huffResidualRoot := huffman.BuildHuffmanTree(residualTruncated)
	residualCodes := make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(huffResidualRoot, 0, 0, residualCodes)

	elapsed = time.Since(startTime)
	fmt.Printf("[%5.1f min] %s\n", elapsed.Minutes(), "==** Simulating compression with kmeans **==")

	println("Huffman tree for literal magnitudes...")
	magnitudesMap := make(map[int64]int64)
	for mag := int64(0); mag <= 64; mag++ {
		freq := magFreqs[mag]
		magnitudesMap[mag] = freq
	}
	huffMagnitudeRoot := huffman.BuildHuffmanTree(magnitudesMap)
	magnitudeCodes := make(map[int64]huffman.BitCode)
	huffman.GenerateBitCodes(huffMagnitudeRoot, 0, 0, magnitudeCodes)

	result, peakStrengths := compress.SimulateCompressionWithKMeans(amounts, blocksPerEpoch, blockToTxo, amountCodes, mantissaCodes, exponentCodes, residualCodes, magnitudeCodes, epochToPhasePeaks)

	println("TotalBits: ", result.TotalBits)
	println("Celebrity hits: ", result.CelebrityHits)
	println("Scientific hits: ", result.ScientificHits)
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
			row = append(row, fmt.Sprintf("%f", results[peakPriority]), fmt.Sprintf("%d", results[peakPriority]))
		}
		w.Write(row)
	}
	w.Flush()
}
