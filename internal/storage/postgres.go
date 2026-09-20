package storage

import (
	"database/sql"
	"fmt"
	"math"
	"sync"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

type Store struct {
	DB *sql.DB
	mu sync.Mutex
}

func NewStore(pgURL string) (*Store, error) {
	db, err := sql.Open("postgres", pgURL)
	if err != nil {
		return nil, fmt.Errorf("open db: %w", err)
	}
	db.SetMaxOpenConns(10)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping db: %w", err)
	}
	return &Store{DB: db}, nil
}

func (s *Store) Close() error {
	return s.DB.Close()
}

func (s *Store) GetWalletBalance(walletID uuid.UUID) (float64, string, error) {
	var balance float64
	var currency string
	err := s.DB.QueryRow(`SELECT balance, currency FROM wallets WHERE wallet_id = $1`, walletID).Scan(&balance, &currency)
	return balance, currency, err
}

func (s *Store) RecordTransaction(requestID, operation string, fromWallet, toWallet *uuid.UUID, amount float64, status string) error {
	_, err := s.DB.Exec(
		`INSERT INTO transactions (request_id, operation, from_wallet, to_wallet, amount, status) VALUES ($1, $2, $3, $4, $5, $6)`,
		requestID, operation, fromWallet, toWallet, amount, status,
	)
	return err
}

func (s *Store) Deposit(walletID uuid.UUID, amount float64) error {
	if amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return fmt.Errorf("invalid amount")
	}

	_, err := s.DB.Exec(`
		INSERT INTO wallets (wallet_id, balance)
		VALUES ($1, $2)
		ON CONFLICT (wallet_id)
		DO UPDATE SET balance = wallets.balance + EXCLUDED.balance, updated_at = NOW()
	`, walletID, amount)
	return err
}

func (s *Store) Withdraw(walletID uuid.UUID, amount float64) error {
	if amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return fmt.Errorf("invalid amount")
	}

	result, err := s.DB.Exec("UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE wallet_id = $2 AND balance >= $1", amount, walletID)
	if err != nil {
		return err
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return fmt.Errorf("insufficient funds")
	}

	return nil
}

func (s *Store) Transfer(fromWallet, toWallet uuid.UUID, amount float64) error {
	if fromWallet == toWallet {
		return fmt.Errorf("cannot transfer to the same wallet")
	}

	if amount <= 0 || math.IsNaN(amount) || math.IsInf(amount, 0) {
		return fmt.Errorf("invalid amount")
	}

	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.Query("SELECT wallet_id, balance FROM wallets WHERE wallet_id IN($1, $2) ORDER BY wallet_id FOR UPDATE", fromWallet, toWallet)
	if err != nil {
		return err
	}
	defer rows.Close()

	hasFrom := false
	hasTo := false

	for rows.Next() {
		var walletID uuid.UUID
		var balance float64
		if err := rows.Scan(&walletID, &balance); err != nil {
			return err
		}

		if walletID == fromWallet {
			hasFrom = true
			if balance < amount {
				return fmt.Errorf("insufficient funds")
			}
		}

		if walletID == toWallet {
			hasTo = true
		}
	}

	if rows.Err() != nil {
		return rows.Err()
	}

	if !hasFrom || !hasTo {
		return fmt.Errorf("insufficient funds")
	}

	_, err = tx.Exec("UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE wallet_id = $2", amount, fromWallet)
	if err != nil {
		return err
	}
	_, err = tx.Exec("UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE wallet_id = $2", amount, toWallet)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) SumBalanceFromTransactions(walletID uuid.UUID) (float64, error) {
	var balance float64
	err := s.DB.QueryRow(`
		SELECT COALESCE(
			SUM(CASE WHEN to_wallet = $1 THEN amount ELSE 0 END) -
			SUM(CASE WHEN from_wallet = $1 THEN amount ELSE 0 END),
		0)
		FROM transactions WHERE from_wallet = $1 OR to_wallet = $1
	`, walletID).Scan(&balance)
	return balance, err
}
