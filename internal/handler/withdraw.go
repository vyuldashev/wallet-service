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
	RequestID uuid.UUID `json:"request_id"`
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

		if req.RequestID == uuid.Nil || req.WalletID == uuid.Nil {
			fmt.Println("withdraw: bad request_id or wallet_id")
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

		if err := store.Withdraw(req.RequestID, req.WalletID, req.Amount, currency); err != nil {
			fmt.Println("withdraw failed:", err)
			publishFailed(nc, req.RequestID, "withdraw", err.Error())
			return
		}

		publishCompleted(nc, req.RequestID, "withdraw")
	}
}
