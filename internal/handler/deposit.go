package handler

import (
	"encoding/json"
	"fmt"

	"github.com/fundingpips/wallet-service/internal/nats"
	"github.com/fundingpips/wallet-service/internal/storage"
	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
)

type DepositRequest struct {
	RequestID uuid.UUID   `json:"request_id"`
	WalletID  uuid.UUID   `json:"wallet_id"`
	Amount    json.Number `json:"amount"`
	Currency  string      `json:"currency"`
}

func HandleDeposit(store *storage.Store, nc *nats.Client) natsgo.MsgHandler {
	return func(msg *natsgo.Msg) {
		var req DepositRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			fmt.Println("deposit: bad payload:", err)
			return
		}

		if req.RequestID == uuid.Nil || req.WalletID == uuid.Nil {
			fmt.Println("deposit: bad request_id or wallet_id")
			return
		}

		amount, err := normalizeAmount(req.Amount)
		if err != nil {
			fmt.Println("deposit: bad amount:", err)
			return
		}

		currency, err := normalizeCurrency(req.Currency)
		if err != nil {
			fmt.Println("deposit: bad currency:", err)
			return
		}

		if err := store.Deposit(req.RequestID, req.WalletID, amount, currency); err != nil {
			fmt.Println("deposit failed:", err)
			publishFailed(nc, req.RequestID, "deposit", err.Error())
			return
		}

		publishCompleted(nc, req.RequestID, "deposit")
	}
}
