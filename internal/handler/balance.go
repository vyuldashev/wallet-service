package handler

import (
	"encoding/json"
	"fmt"

	"github.com/fundingpips/wallet-service/internal/storage"
	"github.com/google/uuid"
	natsgo "github.com/nats-io/nats.go"
)

type BalanceRequest struct {
	WalletID uuid.UUID `json:"wallet_id"`
}

type BalanceResponse struct {
	WalletID uuid.UUID          `json:"wallet_id"`
	Balances map[string]float64 `json:"balances"`
}

func HandleBalance(store *storage.Store) natsgo.MsgHandler {
	return func(msg *natsgo.Msg) {
		var req BalanceRequest
		if err := json.Unmarshal(msg.Data, &req); err != nil {
			fmt.Println("balance: bad payload:", err)
			return
		}

		balances, err := store.GetWalletBalances(req.WalletID)
		if err != nil {
			fmt.Println("balance: wallet lookup failed:", err)
			return
		}

		resp := BalanceResponse{
			WalletID: req.WalletID,
			Balances: balances,
		}
		data, _ := json.Marshal(resp)
		msg.Respond(data)
	}
}
