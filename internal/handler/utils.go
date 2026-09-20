package handler

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
)

func normalizeCurrency(currency string) (string, error) {
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if len(currency) != 3 {
		return "", fmt.Errorf("invalid currency")
	}

	return currency, nil
}

func normalizeAmount(raw json.Number) (string, error) {
	value, ok := new(big.Rat).SetString(raw.String())
	if !ok || value.Sign() <= 0 {
		return "", fmt.Errorf("amount must be positive")
	}

	// Переводим в единицы по 0.0001 без округления.
	scaled := new(big.Rat).Mul(value, big.NewRat(10000, 1))
	if !scaled.IsInt() {
		return "", fmt.Errorf("amount must be a multiple of 0.0001")
	}

	units := scaled.Num()

	maxUnits := big.NewInt(999999999999999999)
	if units.Cmp(maxUnits) > 0 {
		return "", fmt.Errorf("amount exceeds NUMERIC(18,4)")
	}

	n := units.Int64()
	return fmt.Sprintf("%d.%04d", n/10000, n%10000), nil
}
