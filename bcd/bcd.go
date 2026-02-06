package bcd

import "fmt"

type Nibble = byte
type Bcd struct {
	Data []Nibble // LSD first
}

func TestBcdComplexity() {
	nums := []uint64{123456, 1000000, 999999, 1000001, 10001, 99999, 42999}
	for _, num := range nums {
		fmt.Printf("complexity(%d,5,2)=%d\n", num, BcdComplexity(num, 5, 2))
	}
	for _, num := range nums {
		fmt.Printf("complexity(%d,4,1)=%d\n", num, BcdComplexity(num, 4, 1))
	}
}

func BcdComplexity(num uint64, totalDigits int, maxResidualDigits int) int {
	numBcd := NewBCD(num)
	numBcd.ToMantissa(totalDigits)
	return numBcd.MantissaResidualDigitCount(maxResidualDigits)
}

func extractLSD(num uint64) (nibble Nibble, left uint64) {
	nibble = Nibble(num % 10)
	left = num - uint64(nibble)
	return
}

func NewBCD(num uint64) *Bcd {
	result := make([]Nibble, 0, 20)
	left := num
	var nibble Nibble
	for left > 0 {
		nibble, left = extractLSD(left)
		result = append(result, nibble)
		left /= 10
	}
	return &Bcd{result}
}

func (b *Bcd) ToMantissa(digits int) {
	for len(b.Data) > digits {
		// Take off the LSD
		b.Data = b.Data[1:]
	}
	if len(b.Data) < digits {
		// Insert some zero LSDs
		data := make([]Nibble, digits-len(b.Data))
		b.Data = append(data, b.Data...)
	}
}

func (b *Bcd) SigFig() int {
	result := len(b.Data)
	if result == 0 {
		return 0
	}
	for _, nibble := range b.Data {
		if nibble == 0 {
			result--
		} else {
			break
		}
	}
	return result
}

func (b *Bcd) ToUint64() uint64 {
	result := uint64(0)
	multiplier := uint64(1)
	for _, v := range b.Data {
		result += multiplier * uint64(v) // Shift
		multiplier *= 10
	}
	return result
}

func (b *Bcd) MantissaResidualDigitCount(maxResidualDigits int) int {
	totalDigits := len(b.Data)
	u := b.ToUint64()
	pow10Factor := uint64(1)
	for _ = range maxResidualDigits {
		pow10Factor *= 10
	}

	mantissa := pow10Factor * ((u + (pow10Factor / 2)) / pow10Factor)
	mantissaBcd := NewBCD(mantissa)
	mantissaBcd.ToMantissa(totalDigits - maxResidualDigits)
	mantissaDigits := mantissaBcd.SigFig()

	var residual uint64
	if mantissa > u {
		residual = mantissa - u
	} else {
		residual = u - mantissa
	}
	residualBcd := NewBCD(residual)
	residualBcd.ToMantissa(maxResidualDigits)
	residualDigits := residualBcd.SigFig()

	return mantissaDigits + residualDigits
}
