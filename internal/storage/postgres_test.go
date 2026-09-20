package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestIdempotentDeposit(t *testing.T) {
	store := newTestStore(t)
	walletID := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	currency := "USD"

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 0, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 100", walletID, currency)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, walletID)
	})

	requestID := uuid.New()

	err = store.Deposit(requestID, walletID, 100, currency)
	if err != nil {
		t.Fatal(err)
	}

	err = store.Deposit(requestID, walletID, 100, currency)
	if err != nil {
		t.Fatal(err)
	}

	balances, err := store.GetWalletBalances(walletID)
	if err != nil {
		t.Fatal(err)
	}

	if balances[currency] != 100 {
		t.Fatalf("expected 100 balance, got %f", balances[currency])
	}
}

func TestConcurrentWithdraw(t *testing.T) {
	store := newTestStore(t)

	walletID := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	currency := "USD"

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 100, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 100", walletID, currency)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = store.DB.Exec(
			`DELETE FROM wallets WHERE wallet_id = $1`, walletID,
		)
	})

	var wg sync.WaitGroup

	tx, err := store.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
		wg.Wait()
	}()

	var lockedBalance float64
	err = tx.QueryRowContext(t.Context(), "SELECT balance FROM wallets WHERE wallet_id = $1 AND currency = $2 FOR UPDATE", walletID, currency).Scan(&lockedBalance)
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)

	for range 2 {
		wg.Go(func() {
			results <- store.Withdraw(uuid.New(), walletID, 80, currency)
		})
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		var blocked int

		select {
		case err := <-results:
			t.Fatalf("withdraw failed: %v", err)
		default:
		}

		err := store.DB.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&blocked)
		if err != nil {
			t.Fatalf("waiting for two blocked withdrawals: %v", err)
		}

		if blocked == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	successes := 0
	for range 2 {
		if err := <-results; err != nil {
			t.Logf("withdraw failed: %v", err)
			continue
		}

		successes++
	}

	balances, err := store.GetWalletBalances(walletID)
	if err != nil {
		t.Fatal(err)
	}

	if successes != 1 || balances[currency] != 20 {
		t.Fatalf("expected 1 success and 20 balance, got %d and %f", successes, balances[currency])
	}
}

func TestConcurrentTransfer(t *testing.T) {
	store := newTestStore(t)

	srcWallet := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	dstWallet := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	currency := "USD"

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 100, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 100", srcWallet, currency)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 0, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 0", dstWallet, currency)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, srcWallet)
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, dstWallet)
	})

	var wg sync.WaitGroup

	tx, err := store.DB.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tx.Rollback()
		wg.Wait()
	}()

	var lockedBalance float64
	err = tx.QueryRowContext(t.Context(), "SELECT balance FROM wallets WHERE wallet_id = $1 AND currency = $2 FOR UPDATE", srcWallet, currency).Scan(&lockedBalance)
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)

	for range 2 {
		wg.Go(func() {
			results <- store.Transfer(uuid.New(), srcWallet, dstWallet, 80, currency)
		})
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		var blocked int

		select {
		case err := <-results:
			t.Fatalf("withdraw failed: %v", err)
		default:
		}

		err := store.DB.QueryRowContext(ctx, `SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'`).Scan(&blocked)
		if err != nil {
			t.Fatalf("waiting for two blocked withdrawals: %v", err)
		}

		if blocked == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	successes := 0
	for range 2 {
		if err := <-results; err != nil {
			t.Logf("withdraw failed: %v", err)
			continue
		}

		successes++
	}

	srcBalances, err := store.GetWalletBalances(srcWallet)
	if err != nil {
		t.Fatal(err)
	}

	dstBalances, err := store.GetWalletBalances(dstWallet)
	if err != nil {
		t.Fatal(err)
	}

	if successes != 1 || srcBalances[currency] != 20 || dstBalances[currency] != 80 {
		t.Fatalf("expected 1 success, 20 balance for source wallet and 80 balance for destination wallet, got %d, %f and %f", successes, srcBalances[currency], dstBalances[currency])
	}
}

func TestSuccessfulTransfer(t *testing.T) {
	store := newTestStore(t)

	// source USD=100 EUR=50
	// destination USD=0 EUR=0
	// transfer 100 USD

	srcWallet := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	dstWallet := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	currency := "USD"

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 100, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 100", srcWallet, currency)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 50, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 50", srcWallet, "EUR")
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 0, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 0", dstWallet, currency)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 0, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 0", dstWallet, "EUR")
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, srcWallet)
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, dstWallet)
	})

	if err := store.Transfer(uuid.New(), srcWallet, dstWallet, 100, currency); err != nil {
		t.Fatal(err)
	}

	srcBalances, err := store.GetWalletBalances(srcWallet)
	if err != nil {
		t.Fatal(err)
	}

	dstBalances, err := store.GetWalletBalances(dstWallet)
	if err != nil {
		t.Fatal(err)
	}

	if srcBalances[currency] != 0 || dstBalances[currency] != 100 {
		t.Fatalf("expected 0 balance for source wallet and 100 balance for destination wallet, got %f and %f", srcBalances[currency], dstBalances[currency])
	}

	if srcBalances["EUR"] != 50 || dstBalances["EUR"] != 0 {
		t.Fatalf("expected 50 EUR and 0 EUR, got %f and %f", srcBalances["EUR"], dstBalances["EUR"])
	}
}

func TestInsufficientFundsForTransfer(t *testing.T) {
	store := newTestStore(t)

	srcWallet := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	dstWallet := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	currency := "USD"

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 100, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 100", srcWallet, currency)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 0, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 0", dstWallet, currency)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, srcWallet)
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, dstWallet)
	})

	err = store.Transfer(uuid.New(), srcWallet, dstWallet, 200, currency)

	if err == nil {
		t.Fatal("expected insufficient funds error")
	}

	if err.Error() != "insufficient funds" {
		t.Fatalf("expected insufficient funds error, got %v", err)
	}
}

func TestInsufficientFundsWithAnotherCurrencyForTransfer(t *testing.T) {
	store := newTestStore(t)

	// source USD=100 EUR=50
	// destination USD=0 EUR=0
	// transfer 80 USD

	srcWallet := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	dstWallet := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	currency := "USD"

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 100, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 100", srcWallet, currency)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 50, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 50", srcWallet, "EUR")
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance, currency) VALUES($1, 0, $2) ON CONFLICT (wallet_id, currency) DO UPDATE SET balance = 0", dstWallet, currency)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, srcWallet)
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, dstWallet)
	})

	err = store.Transfer(uuid.New(), srcWallet, dstWallet, 200, "EUR")

	if err == nil {
		t.Fatal("expected insufficient funds error")
	}

	if err.Error() != "insufficient funds" {
		t.Fatalf("expected insufficient funds error, got %v", err)
	}

	balances, err := store.GetWalletBalances(srcWallet)
	if err != nil {
		t.Fatal(err)
	}

	if balances["USD"] != 100 || balances["EUR"] != 50 {
		t.Fatalf("expected 100 USD and 50 EUR, got %f and %f", balances["USD"], balances["EUR"])
	}
}
