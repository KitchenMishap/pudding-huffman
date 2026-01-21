package huffman

import "container/heap"

type Node struct {
	Value       int64 // For example, mantissa, exponent, minute of clockface
	Freq        int64 // How often it appeared
	Left, Right *Node
}

// The compressed representation
type BitCode struct {
	Bits   uint64 // The actual bit pattern
	Length int    // How many bits used
}

type PriorityQueue []*Node

func (pq PriorityQueue) Len() int            { return len(pq) }
func (pq PriorityQueue) Less(i, j int) bool  { return pq[i].Freq < pq[j].Freq }
func (pq PriorityQueue) Swap(i, j int)       { pq[i], pq[j] = pq[j], pq[i] }
func (pq *PriorityQueue) Push(x interface{}) { *pq = append(*pq, x.(*Node)) }
func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func BuildHuffmanTree(freqs map[int64]int64) *Node {
	pq := make(PriorityQueue, 0, len(freqs))
	for val, freq := range freqs {
		pq = append(pq, &Node{Value: val, Freq: freq})
	}
	heap.Init(&pq)

	for pq.Len() > 1 {
		left := heap.Pop(&pq).(*Node)
		right := heap.Pop(&pq).(*Node)

		// Create a parent with sum of frequencies
		parent := &Node{
			Freq:  left.Freq + right.Freq,
			Left:  left,
			Right: right,
		}
		heap.Push(&pq, parent)
	}
	return heap.Pop(&pq).(*Node)
}

func GenerateBitCodes(node *Node, currentBits uint64, depth int, table map[int64]BitCode) {
	if node.Left == nil && node.Right == nil {
		// Leaf node - store the result
		table[node.Value] = BitCode{Bits: currentBits, Length: depth}
		return
	}
	// Left = 0, Right = 1
	if node.Left != nil {
		GenerateBitCodes(node.Left, currentBits<<1, depth+1, table)
	}
	if node.Right != nil {
		GenerateBitCodes(node.Right, (currentBits<<1)|1, depth+1, table)
	}
}

type BitWriter struct {
	buffer []byte
	accum  uint64 // Temporary storage for bits
	nbits  uint   // Number of bits currently in accum
}

func (bw *BitWriter) WriteBits(code uint64, length uint) {
	// Add new bits to the accumulator
	bw.accum |= (code << (64 - length - bw.nbits))
	bw.nbits += length

	// If we have 8 or more bits, flush the full bytes
	for bw.nbits >= 8 {
		bw.buffer = append(bw.buffer, byte(bw.accum>>56))
		bw.accum <<= 8
		bw.nbits -= 8
	}
}

func (bw *BitWriter) Flush() {
	if bw.nbits > 0 {
		bw.buffer = append(bw.buffer, byte(bw.accum>>56))
		bw.accum = 0
		bw.nbits = 0
	}
}
