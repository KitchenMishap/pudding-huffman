package compress

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
