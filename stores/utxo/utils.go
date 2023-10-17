package utxo

import (
	"time"

	"github.com/bitcoin-sv/ubsv/services/utxo/utxostore_api"
	"github.com/libsv/go-bt/v2/chainhash"
)

func CalculateUtxoStatus(spendingTxId *chainhash.Hash, lockTime uint32, blockHeight uint32) utxostore_api.Status {
	status := utxostore_api.Status_OK
	if spendingTxId != nil {
		status = utxostore_api.Status_SPENT
	} else if lockTime > 0 {
		if lockTime < 500000000 && uint32(lockTime) > blockHeight {
			status = utxostore_api.Status_LOCKED
		} else if lockTime >= 500000000 && uint32(lockTime) > uint32(time.Now().Unix()) {
			status = utxostore_api.Status_LOCKED
		}
	}

	return status
}
