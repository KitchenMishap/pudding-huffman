package verify

import (
	"bytes"
	"errors"
	"github.com/KitchenMishap/pudding-huffman/huffman"
)

// A simple in-memory bit writer
type BitStream struct {
	Buffer    bytes.Buffer
	Byte      byte
	Count     uint8 // bits filled in current byte
	ReadPos   int   // for reading back
	ReadByte  byte
	ReadCount uint8
}

func (bs *BitStream) WriteCode(bc huffman.BitCode) {
	// Write bits from MSB to LSB (or LSB to MSB, just be consistent)
	// Here we write MSB of the code first
	for i := bc.Length - 1; i >= 0; i-- {
		bit := (bc.Bits >> i) & 1
		bs.Byte = (bs.Byte << 1) | byte(bit)
		bs.Count++
		if bs.Count == 8 {
			bs.Buffer.WriteByte(bs.Byte)
			bs.Byte = 0
			bs.Count = 0
		}
	}
}

func (bs *BitStream) Flush() {
	if bs.Count > 0 {
		bs.Byte = bs.Byte << (8 - bs.Count) // Pad with zeros
		bs.Buffer.WriteByte(bs.Byte)
	}
}

// Reset for reading
func (bs *BitStream) StartRead() {
	bs.ReadPos = 0
	bs.ReadCount = 0
	if bs.Buffer.Len() > 0 {
		bs.ReadByte = bs.Buffer.Bytes()[0]
	}
}

func (bs *BitStream) ReadBit() (uint64, error) {
	if bs.ReadPos >= bs.Buffer.Len() && bs.ReadCount == 0 {
		return 0, errors.New("EOF")
	}

	// Extract MSB
	bit := (bs.ReadByte >> 7) & 1
	bs.ReadByte = bs.ReadByte << 1
	bs.ReadCount++

	if bs.ReadCount == 8 {
		bs.ReadPos++
		bs.ReadCount = 0
		if bs.ReadPos < bs.Buffer.Len() {
			bs.ReadByte = bs.Buffer.Bytes()[bs.ReadPos]
		}
	}
	return uint64(bit), nil
}

// The Decoder needs the Tree to walk
func (bs *BitStream) ReadValue(root *huffman.Node) (int64, error) {
	current := root
	for current.Left != nil || current.Right != nil {
		bit, err := bs.ReadBit()
		if err != nil {
			return 0, err
		}

		if bit == 0 {
			current = current.Left
		} else {
			current = current.Right
		}
	}
	return current.Value, nil
}
