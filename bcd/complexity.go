package bcd

import "math"

// NibbleMetrics holds our "Humanity" indicators
type NibbleMetrics struct {
	TotalDigits   float64
	TrailingZeros float64
	UniqueDigits  float64
	Complexity    float64
	Count         int64
}

func NewBlankNibbleMetrics() *NibbleMetrics {
	metrics := NibbleMetrics{}
	return &metrics
}

func NewMaxNibbleMetrics() *NibbleMetrics {
	metrics := NibbleMetrics{math.MaxFloat64, math.MaxFloat64, math.MaxFloat64, math.MaxFloat64, 1}
	return &metrics
}

func (nm *NibbleMetrics) Incorporate(other *NibbleMetrics) {
	nm.TotalDigits += other.TotalDigits
	nm.TrailingZeros += other.TrailingZeros
	nm.UniqueDigits += other.UniqueDigits
	nm.Complexity += other.Complexity
	nm.Count += other.Count
}

func (nm *NibbleMetrics) Averages() {
	if nm.Count == 0 {
		*nm = NibbleMetrics{}
		return
	}
	nm.TotalDigits = nm.TotalDigits / float64(nm.Count)
	nm.TrailingZeros = nm.TrailingZeros / float64(nm.Count)
	nm.UniqueDigits = nm.UniqueDigits / float64(nm.Count)
	nm.Complexity = nm.Complexity / float64(nm.Count)
}

func GetDivineComplexity(amount uint64) NibbleMetrics {
	// If the amount is 0, it's perfectly simple.
	if amount == 0 {
		return GetNibbleComplexity(0)
	}

	// Strip trailing zeros to remove "Scale Bias"
	for amount > 0 && amount%10 == 0 {
		amount /= 10
	}

	// Now score what remains (the Mantissa)
	return GetNibbleComplexity(amount)
}

// GetNibbleComplexity analyzes a uint64 without expensive string conversions.
func GetNibbleComplexity(n uint64) NibbleMetrics {
	if n == 0 {
		return NibbleMetrics{TotalDigits: 1, TrailingZeros: 1, UniqueDigits: 1, Complexity: 0, Count: 1}
	}

	var digits []int
	temp := n
	trailing := true
	trailingCount := 0
	uniqueMap := uint16(0) // Bitmask to track digits 0-9

	for temp > 0 {
		d := int(temp % 10)
		digits = append(digits, d)

		// Track Trailing Zeros
		if d == 0 && trailing {
			trailingCount++
		} else {
			trailing = false
		}

		// Track Unique Digits using bitmask
		uniqueMap |= (1 << d)
		temp /= 10
	}

	// Count bits in uniqueMap
	uniqueCount := 0
	for i := 0; i < 10; i++ {
		if (uniqueMap & (1 << i)) != 0 {
			uniqueCount++
		}
	}

	total := len(digits)

	// Divine Formula: High complexity = harder to compress
	// We penalize long numbers but reward trailing zeros (roundness)
	complexity := (total - trailingCount) + (uniqueCount * 2)

	return NibbleMetrics{
		TotalDigits:   float64(total),
		TrailingZeros: float64(trailingCount),
		UniqueDigits:  float64(uniqueCount),
		Complexity:    float64(complexity),
		Count:         1,
	}
}

func GetNigelNumComplexity(num uint64) int {
	if num == 0 {
		return 0
	}

	const numDigits = 5        // n for the whole thing
	const numResidueDigits = 2 // m for the residue
	const residueMult = 100    // 10^2

	// This is the "whole" number, n digits long
	nDigitNumber, countDigits := IntDecMantissa(num, numDigits)
	if countDigits < numResidueDigits-numResidueDigits {
		return numDigits
	}

	// Mantissa is n-m digit number
	// residueMult/2 is for rounding
	mantissa, mantissaLen := IntDecMantissa(nDigitNumber, numDigits-numResidueDigits)
	mantissaRounded, mantissaRoundedLen := IntDecMantissa(nDigitNumber+residueMult/2, numDigits-numResidueDigits)
	crossed := mantissaRoundedLen < mantissaRoundedLen

	// (Exponent is considered "Free")
	// (Sign of residue is considered "Free")
	var absResidue uint64
	if !crossed {
		absResidue = uint64(math.Abs(float64(nDigitNumber) - float64(mantissaRounded*residueMult)))
	} else {
		absResidue = uint64(math.Abs(float64(nDigitNumber) - float64(mantissaRounded*residueMult/10)))
	}
	// The rounding might have ticked it over a power of 10 boundary
	absResidue2 := uint64(math.Abs(float64(nDigitNumber) - float64(mantissa*residueMult*10)))
	if absResidue2 < absResidue {
		absResidue = absResidue2
	}
	_, residueLen := IntDecMantissa(absResidue, numResidueDigits)

	return mantissaLen + residueLen
}

func IntDecMantissa(num uint64, n int) (mantissa uint64, numDigits int) {
	if num == 0 {
		return 0, 0
	}

	// Step 1: Strip trailing zeros first so we find the
	// true "end" of the significant digits.
	for num > 0 && num%10 == 0 {
		num /= 10
	}

	// Step 2: Find how many digits we actually have.
	// We could use math.Log10, but a simple loop is safer for uint64.
	temp := num
	digits := 0
	for temp > 0 {
		temp /= 10
		digits++
	}
	numDigits = digits

	// Step 3: Shift the window to capture exactly 'n' digits.
	if digits > n {
		// Too many digits? Shrink it down.
		for digits > n {
			num /= 10
			digits--
		}
	} else if digits < n {
		// Too few digits? Pad it up with zeros at the END.
		// (This maintains the most significant digit position)
		for digits < n {
			num *= 10
			digits++
		}
	}

	return num, numDigits
}
