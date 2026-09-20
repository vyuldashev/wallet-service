package handler

import (
	"fmt"
	"math"
	"strings"
)

func validateAmount(amount float64) error {
	if amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return fmt.Errorf("invalid amount")
	}
	return nil
}

func normalizeCurrency(currency string) (string, error) {
	currency = strings.ToUpper(strings.TrimSpace(currency))
	if len(currency) != 3 {
		return "", fmt.Errorf("invalid currency")
	}

	return currency, nil
}
