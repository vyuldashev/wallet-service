package storage

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

var (
	errDuplicateRequestID = errors.New("duplicate request_id: transaction already processed")
)

type Store struct {
	DB *sql.DB
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

func (s *Store) GetWalletBalances(walletID uuid.UUID) (map[string]string, error) {
	rows, err := s.DB.Query(`SELECT balance, currency FROM wallets WHERE wallet_id = $1`, walletID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	balances := make(map[string]string)

	for rows.Next() {
		var currency string
		var balance string
		if err := rows.Scan(&balance, &currency); err != nil {
			return nil, err
		}

		balances[currency] = balance
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return balances, nil
}

func (s *Store) Deposit(requestID, walletID uuid.UUID, amount string, currency string) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	alreadyProcessed, err := s.recordTransaction(tx, requestID, "deposit", nil, &walletID, amount, currency, "completed")
	if err != nil {
		return err
	}

	if alreadyProcessed {
		return errDuplicateRequestID
	}

	_, err = tx.Exec(`
		INSERT INTO wallets (wallet_id, balance, currency)
		VALUES ($1, $2, $3)
		ON CONFLICT (wallet_id, currency)
		DO UPDATE SET balance = wallets.balance + EXCLUDED.balance, updated_at = NOW()
	`, walletID, amount, currency)

	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Store) Withdraw(requestID, walletID uuid.UUID, amount string, currency string) error {
	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	alreadyProcessed, err := s.recordTransaction(tx, requestID, "withdraw", &walletID, nil, amount, currency, "completed")
	if err != nil {
		return err
	}

	if alreadyProcessed {
		return errDuplicateRequestID
	}

	result, err := tx.Exec("UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE wallet_id = $2 AND currency = $3 AND balance >= $1", amount, walletID, currency)
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

	return tx.Commit()
}

func (s *Store) Transfer(requestID, fromWallet, toWallet uuid.UUID, amount string, currency string) error {
	if fromWallet == toWallet {
		return fmt.Errorf("cannot transfer to the same wallet")
	}

	tx, err := s.DB.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	alreadyProcessed, err := s.recordTransaction(tx, requestID, "transfer", &fromWallet, &toWallet, amount, currency, "completed")
	if err != nil {
		return err
	}

	if alreadyProcessed {
		return errDuplicateRequestID
	}

	rows, err := tx.Query("SELECT wallet_id FROM wallets WHERE wallet_id IN($1, $2) AND currency = $3 ORDER BY wallet_id FOR UPDATE", fromWallet, toWallet, currency)
	if err != nil {
		return err
	}
	defer rows.Close()

	hasFrom := false
	hasTo := false

	for rows.Next() {
		var walletID uuid.UUID
		if err := rows.Scan(&walletID); err != nil {
			return err
		}

		if walletID == fromWallet {
			hasFrom = true
		}

		if walletID == toWallet {
			hasTo = true
		}
	}

	if err := rows.Err(); err != nil {
		return err
	}

	if !hasFrom || !hasTo {
		return fmt.Errorf("insufficient funds")
	}

	result, err := tx.Exec("UPDATE wallets SET balance = balance - $1, updated_at = NOW() WHERE wallet_id = $2 AND currency = $3 AND balance >= $1::numeric", amount, fromWallet, currency)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected == 0 {
		return fmt.Errorf("insufficient funds")
	}

	_, err = tx.Exec("UPDATE wallets SET balance = balance + $1, updated_at = NOW() WHERE wallet_id = $2 AND currency = $3", amount, toWallet, currency)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Store) SumBalanceFromTransactions(walletID uuid.UUID) (map[string]string, error) {
	rows, err := s.DB.Query(`
		SELECT currency, COALESCE(
			SUM(CASE WHEN to_wallet = $1 THEN amount ELSE 0 END) -
			SUM(CASE WHEN from_wallet = $1 THEN amount ELSE 0 END),
		0)
		FROM transactions WHERE status = 'completed' AND (from_wallet = $1 OR to_wallet = $1)
		GROUP BY currency
	`, walletID)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	balances := make(map[string]string)
	for rows.Next() {
		var currency string
		var balance string
		if err := rows.Scan(&currency, &balance); err != nil {
			return nil, err
		}

		balances[currency] = balance
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return balances, nil
}

func (s *Store) recordTransaction(
	tx *sql.Tx,
	requestID uuid.UUID,
	operation string,
	fromWallet, toWallet *uuid.UUID,
	amount string,
	currency string,
	status string,
) (bool, error) {
	result, err := tx.Exec(
		`INSERT INTO transactions (request_id, operation, from_wallet, to_wallet, amount, currency, status) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT(request_id) DO NOTHING`,
		requestID, operation, fromWallet, toWallet, amount, currency, status,
	)
	if err != nil {
		return false, err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return false, err
	}

	return rows == 0, nil
}
