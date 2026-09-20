package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestConcurrentWithdraw(t *testing.T) {
	store := newTestStore(t)

	walletID := uuid.MustParse("00000000-0000-0000-0000-000000000000")

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance) VALUES($1, 100) ON CONFLICT (wallet_id) DO UPDATE SET balance = 100", walletID)
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
	err = tx.QueryRowContext(t.Context(), "SELECT balance FROM wallets WHERE wallet_id = $1 FOR UPDATE", walletID).Scan(&lockedBalance)
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)

	for range 2 {
		wg.Go(func() {
			results <- store.Withdraw(walletID, 80)
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

	balance, _, err := store.GetWalletBalance(walletID)
	if err != nil {
		t.Fatal(err)
	}

	if successes != 1 || balance != 20 {
		t.Fatalf("expected 1 success and 20 balance, got %d and %f", successes, balance)
	}
}

func TestConcurrentTransfer(t *testing.T) {
	store := newTestStore(t)

	srcWallet := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	dstWallet := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance) VALUES($1, 100) ON CONFLICT (wallet_id) DO UPDATE SET balance = 100", srcWallet)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance) VALUES($1, 0) ON CONFLICT (wallet_id) DO UPDATE SET balance = 0", dstWallet)
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
	err = tx.QueryRowContext(t.Context(), "SELECT balance FROM wallets WHERE wallet_id = $1 FOR UPDATE", srcWallet).Scan(&lockedBalance)
	if err != nil {
		t.Fatal(err)
	}

	results := make(chan error, 2)

	for range 2 {
		wg.Go(func() {
			results <- store.Transfer(srcWallet, dstWallet, 80)
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

	srcBalance, _, err := store.GetWalletBalance(srcWallet)
	if err != nil {
		t.Fatal(err)
	}

	dstBalance, _, err := store.GetWalletBalance(dstWallet)
	if err != nil {
		t.Fatal(err)
	}

	if successes != 1 || srcBalance != 20 || dstBalance != 80 {
		t.Fatalf("expected 1 success, 20 balance for source wallet and 80 balance for destination wallet, got %d, %f and %f", successes, srcBalance, dstBalance)
	}
}

func TestSuccessfulTransfer(t *testing.T) {
	store := newTestStore(t)

	srcWallet := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	dstWallet := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance) VALUES($1, 100) ON CONFLICT (wallet_id) DO UPDATE SET balance = 100", srcWallet)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance) VALUES($1, 0) ON CONFLICT (wallet_id) DO UPDATE SET balance = 0", dstWallet)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, srcWallet)
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, dstWallet)
	})

	if err := store.Transfer(srcWallet, dstWallet, 100); err != nil {
		t.Fatal(err)
	}

	srcBalance, _, err := store.GetWalletBalance(srcWallet)
	if err != nil {
		t.Fatal(err)
	}

	dstBalance, _, err := store.GetWalletBalance(dstWallet)
	if err != nil {
		t.Fatal(err)
	}

	if srcBalance != 0 || dstBalance != 100 {
		t.Fatalf("expected 0 balance for source wallet and 100 balance for destination wallet, got %f and %f", srcBalance, dstBalance)
	}
}

func TestInsufficientFundsForTransfer(t *testing.T) {
	store := newTestStore(t)

	srcWallet := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	dstWallet := uuid.MustParse("11111111-1111-1111-1111-111111111111")

	_, err := store.DB.Exec("INSERT INTO wallets(wallet_id, balance) VALUES($1, 100) ON CONFLICT (wallet_id) DO UPDATE SET balance = 100", srcWallet)
	if err != nil {
		t.Fatal(err)
	}

	_, err = store.DB.Exec("INSERT INTO wallets(wallet_id, balance) VALUES($1, 0) ON CONFLICT (wallet_id) DO UPDATE SET balance = 0", dstWallet)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, srcWallet)
		_, _ = store.DB.Exec(`DELETE FROM wallets WHERE wallet_id = $1`, dstWallet)
	})

	err = store.Transfer(srcWallet, dstWallet, 200)

	if err == nil {
		t.Fatal("expected insufficient funds error")
	}

	if err.Error() != "insufficient funds" {
		t.Fatalf("expected insufficient funds error, got %v", err)
	}
}
