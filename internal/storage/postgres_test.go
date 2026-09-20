package storage

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConcurrentWithdraw(t *testing.T) {
	store := newTestStore(t)

	walletID := "00000000-0000-0000-0000-000000000000"

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
