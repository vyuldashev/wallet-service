package handler

import (
	"encoding/json"
	"fmt"

	"github.com/fundingpips/wallet-service/internal/nats"
	"github.com/fundingpips/wallet-service/internal/storage"
	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
)

type TransferRequest struct {
	RequestID    uuid.UUID   `json:"request_id"`
	FromWalletID uuid.UUID   `json:"from_wallet_id"`
	ToWalletID   uuid.UUID   `json:"to_wallet_id"`
	Amount       json.Number `json:"amount"`
	Currency     string      `json:"currency"`
}

func HandleTransfer(store *storage.Store, nc *nats.Client) natsgo.MsgHandler {
	return func(msg *natsgo.Msg) {
		var req TransferRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			fmt.Println("transfer: bad payload:", err)
			return
		}

		if req.RequestID == uuid.Nil || req.FromWalletID == uuid.Nil || req.ToWalletID == uuid.Nil {
			fmt.Println("transfer: bad request_id or wallet_id")
			return
		}

		amount, err := normalizeAmount(req.Amount)
		if err != nil {
			fmt.Println("transfer: bad amount:", err)
			return
		}

		currency, err := normalizeCurrency(req.Currency)
		if err != nil {
			fmt.Println("transfer: bad currency:", err)
			return
		}

		if err := store.Transfer(req.RequestID, req.FromWalletID, req.ToWalletID, amount, currency); err != nil {
			fmt.Println("transfer failed:", err)
			publishFailed(nc, req.RequestID, "transfer", err.Error())
			return
		}

		publishCompleted(nc, req.RequestID, "transfer")
	}
}
