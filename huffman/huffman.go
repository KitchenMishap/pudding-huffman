package huffman

import (
	"container/heap"
	"fmt"
	"sort"
)

// --- Core Types ---

type Node struct {
	Value       int64
	Freq        int64
	Left, Right *Node
}

type BitCode struct {
	Bits   uint64
	Length int
}

// String returns the binary representation (e.g., "0011")
func (bc BitCode) String() string {
	if bc.Length == 0 {
		return ""
	}
	return fmt.Sprintf("%0*b", bc.Length, bc.Bits)
}

// --- Construction & Generation ---

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
	if len(freqs) == 0 {
		return nil
	}
	pq := make(PriorityQueue, 0, len(freqs))
	for val, freq := range freqs {
		pq = append(pq, &Node{Value: val, Freq: freq})
	}
	heap.Init(&pq)

	for pq.Len() > 1 {
		left := heap.Pop(&pq).(*Node)
		right := heap.Pop(&pq).(*Node)
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
	if node == nil {
		return
	}
	if node.Left == nil && node.Right == nil {
		table[node.Value] = BitCode{Bits: currentBits, Length: depth}
		return
	}
	if node.Left != nil {
		GenerateBitCodes(node.Left, currentBits<<1, depth+1, table)
	}
	if node.Right != nil {
		GenerateBitCodes(node.Right, (currentBits<<1)|1, depth+1, table)
	}
}

// --- Bit Manipulation & Concatenation ---

func AppendBitCodes(a, b BitCode) BitCode {
	if a.Length == 0 {
		return b
	}
	if b.Length == 0 {
		return a
	}
	// Note: Total length must be <= 64
	return BitCode{
		Bits:   (a.Bits << b.Length) | b.Bits,
		Length: a.Length + b.Length,
	}
}

func JoinBitCodes(codes ...BitCode) BitCode {
	result := BitCode{}
	for _, c := range codes {
		result = AppendBitCodes(result, c)
	}
	return result
}

// --- The Podium & The Judges ---

type Contender struct {
	Words string
	Count int64
}

type Podium struct {
	// Buckets[length][bits] -> Contender info
	Buckets map[int]map[uint64]*Contender
}

func NewPodium() *Podium {
	return &Podium{Buckets: make(map[int]map[uint64]*Contender)}
}

func (p *Podium) Submit(code BitCode, words string) {
	if code.Length > 8 || code.Length == 0 {
		return // Disqualified from the short-code podium
	}

	if p.Buckets[code.Length] == nil {
		p.Buckets[code.Length] = make(map[uint64]*Contender)
	}

	if _, exists := p.Buckets[code.Length][code.Bits]; !exists {
		p.Buckets[code.Length][code.Bits] = &Contender{Words: words}
	}
	p.Buckets[code.Length][code.Bits].Count++
}

// Rank returns the Top N fullest buckets across all lengths
func (p *Podium) Rank(topN int) {
	type result struct {
		Code  string
		Count int64
		Words string
	}
	var flat []result

	for length, bitsMap := range p.Buckets {
		for bits, contender := range bitsMap {
			bc := BitCode{Bits: bits, Length: length}
			flat = append(flat, result{bc.String(), contender.Count, contender.Words})
		}
	}

	sort.Slice(flat, func(i, j int) bool {
		return flat[i].Count > flat[j].Count
	})

	fmt.Printf("\n🏆 --- THE PODIUM (Top %d Fullest Buckets) ---\n", topN)
	for i := 0; i < topN && i < len(flat); i++ {
		fmt.Printf("[%d] Code: %-10s | Hits: %-12d | %s\n", i+1, flat[i].Code, flat[i].Count, flat[i].Words)
	}
}
