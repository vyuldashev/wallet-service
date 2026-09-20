package handler

import (
	"encoding/json"
	"fmt"

	"github.com/fundingpips/wallet-service/internal/nats"
	"github.com/fundingpips/wallet-service/internal/storage"
	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
)

type WithdrawRequest struct {
	RequestID string    `json:"request_id"`
	WalletID  uuid.UUID `json:"wallet_id"`
	Amount    float64   `json:"amount"`
	Currency  string    `json:"currency"`
}

func HandleWithdraw(store *storage.Store, nc *nats.Client) natsgo.MsgHandler {
	return func(msg *natsgo.Msg) {
		var req WithdrawRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			fmt.Println("withdraw: bad payload:", err)
			return
		}

		if err := validateAmount(req.Amount); err != nil {
			fmt.Println("withdraw: bad amount:", err)
			return
		}

		currency, err := normalizeCurrency(req.Currency)
		if err != nil {
			fmt.Println("withdraw: bad currency:", err)
			return
		}

		if err := store.Withdraw(req.WalletID, req.Amount, currency); err != nil {
			fmt.Println("withdraw failed:", err)
			publishFailed(nc, req.RequestID, "withdraw", err.Error())
			return
		}

		publishCompleted(nc, req.RequestID, "withdraw")

		fromWallet := req.WalletID
		if err := store.RecordTransaction(req.RequestID, "withdraw", &fromWallet, nil, req.Amount, currency, "completed"); err != nil {
			fmt.Println("withdraw: failed to record transaction:", err)
		}
	}
}
