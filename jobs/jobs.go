package jobs

import (
	"errors"
	"github.com/KitchenMishap/pudding-huffman/blockchain"
	"github.com/KitchenMishap/pudding-huffman/derived"
	"runtime"
	"sort"
	"sync"
)

type ForensicFloat struct {
	Mantissa int64
	Exponent int64
}

func NewForensicFloat(amount int64) *ForensicFloat {
	if amount == 0 {
		return &ForensicFloat{Mantissa: 0, Exponent: 0}
	}
	exponent := int64(0)
	for amount > 0 && amount%10 == 0 {
		amount /= 10
		exponent++
	}
	return &ForensicFloat{Mantissa: amount, Exponent: exponent}
}

func (f ForensicFloat) Recover() int64 {
	exp := f.Exponent
	result := f.Mantissa
	if result == 0 {
		return result
	}
	for exp > 0 {
		result *= exp
		exp--
	}
	return result
}

type Histogram struct {
	// Sharding the histogram map reduces lock contention on the map(s)
	shards [256]map[int64]int64
	mu     [256]sync.Mutex
}

func (h *Histogram) Add(amount int64) {
	idx := amount & 0xFF
	h.mu[idx].Lock()
	if h.shards[idx] == nil {
		h.shards[idx] = make(map[int64]int64, 100000)
	}
	ff := NewForensicFloat(amount)
	h.shards[idx][ff.Mantissa]++
	h.mu[idx].Unlock()
}

type Entry struct {
	Mantissa int64
	Count    int64
}

func (h *Histogram) MergeAndSort() []Entry {
	theMap := make(map[int64]int64)

	// Merge
	for i := 0; i < 256; i++ {
		h.mu[i].Lock()
		for mantissa, count := range h.shards[i] {
			theMap[mantissa] += count
		}
		h.mu[i].Unlock()
	}
	// Extract
	allEntries := make([]Entry, 0)
	for mantissa, count := range theMap {
		allEntries = append(allEntries, Entry{Mantissa: mantissa, Count: count})
	}
	// Sort
	sort.Slice(allEntries, func(i, j int) bool {
		return allEntries[i].Count > allEntries[j].Count
	})

	return allEntries
}

func GatherStatistics(folder string) error {
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

	println("Creating the histogram...")
	hist := Histogram{}

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

	entries := hist.MergeAndSort()
	for i := 0; i < 20; i++ {
		println(entries[i].Mantissa)
	}

	return nil
}
