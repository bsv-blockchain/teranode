package redis

import (
	"context"
	"fmt"
	"strings"

	_ "embed"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/redis/go-redis/v9"
)

//go:embed spend_utxo.lua
var scriptString string

var luaScript = redis.NewScript(scriptString)

func spend(ctx context.Context, rdb redis.Scripter, hash *chainhash.Hash, spendingTxID *chainhash.Hash, blockHeight uint32) (*utxostore.UTXOResponse, error) {
	res, err := luaScript.Run(ctx, rdb, []string{hash.String()}, spendingTxID.String(), blockHeight).Result()
	if err != nil {
		return nil, err
	}

	s, ok := res.(string)
	if !ok {
		return nil, fmt.Errorf("unknown response from spend: %v", res)
	}

	if s == "OK" {
		return &utxostore.UTXOResponse{
			Status: int(utxostore_api.Status_OK),
		}, nil
	}

	if s == "NOT_FOUND" {
		return &utxostore.UTXOResponse{
			Status: int(utxostore_api.Status_NOT_FOUND),
		}, nil
	}

	if s == "LOCKED" {
		return &utxostore.UTXOResponse{
			Status: int(utxostore_api.Status_LOCKED),
		}, nil
	}

	if strings.HasPrefix(s, "SPENT") {
		spendingTxID, err := chainhash.NewHashFromStr(s[6:])
		if err != nil {
			return nil, err
		}

		return &utxostore.UTXOResponse{
			Status:       int(utxostore_api.Status_SPENT),
			SpendingTxID: spendingTxID,
		}, nil
	}

	return nil, fmt.Errorf("unknown response from spend: %v", res)
}
