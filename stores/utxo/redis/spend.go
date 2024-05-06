package redis

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"

	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/redis/go-redis/v9"
)

//go:embed spend_utxo.lua
var scriptString string

var luaScript = redis.NewScript(scriptString)

func spendUtxo(ctx context.Context, rdb redis.Scripter, spend *utxostore.Spend, blockHeight, ttl uint32) error {
	// ttl is in seconds, convert to milliseconds
	res, err := luaScript.Run(ctx, rdb, []string{spend.Hash.String()}, spend.SpendingTxID.String(), blockHeight, ttl).Result()
	if err != nil {
		return err
	}

	s, ok := res.(string)
	if !ok {
		return fmt.Errorf("unknown response from spend: %v", res)
	}

	if s == "OK" {
		return nil
	}

	// if s == "NOT_FOUND" {
	// panic("NOT_FOUND")
	// return utxostore.ErrNotFound
	//}

	if strings.HasPrefix(s, "LOCKED") {
		parts := strings.Split(s[7:], ",")
		if len(parts) != 2 {
			return fmt.Errorf("%w: No extra data returned", utxostore.ErrTypeLockTime)
		}

		lockTime, err := strconv.Atoi(parts[0])
		if err != nil {
			return fmt.Errorf("%w: %v", utxostore.ErrTypeLockTime, err)
		}

		blockHeight, err := strconv.Atoi(parts[1])
		if err != nil {
			return fmt.Errorf("%w: %v", utxostore.ErrTypeLockTime, err)
		}

		return utxostore.NewErrLockTime(uint32(lockTime), uint32(blockHeight))
	}

	if strings.HasPrefix(s, "SPENT") {
		hash, err := chainhash.NewHashFromStr(s[6:])
		if err != nil {
			return err
		}

		return utxostore.NewErrSpent(spend.TxID, spend.Vout, spend.Hash, hash)
	}

	return fmt.Errorf("unknown response from spend: %v", res)
}
