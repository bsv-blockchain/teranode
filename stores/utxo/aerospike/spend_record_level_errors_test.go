package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestCreateSpendError_RecordLevelCodesKeepTheirType pins the typed errors for
// the three record-level answers when they arrive per index. spendMulti sends
// them that way whenever another spend in the same Lua call hits a replay
// marker, so the per-spend path must map them exactly as createGeneralError
// maps the whole-record answer. A bare StorageError here turned a conflicting
// parent into a hard block failure and stopped the validator retrying a locked
// one.
func TestCreateSpendError_RecordLevelCodesKeepTheirType(t *testing.T) {
	s := &Store{}
	txID := &chainhash.Hash{0x02}
	item := &batchSpend{spend: &utxo.Spend{TxID: txID, Vout: 5, UTXOHash: &chainhash.Hash{0x03}}}

	for _, tc := range []struct {
		code LuaErrorCode
		want error
	}{
		{LuaErrorCodeConflicting, errors.ErrTxConflicting},
		{LuaErrorCodeLocked, errors.ErrTxLocked},
		{LuaErrorCodeCoinbaseImmature, errors.ErrTxCoinbaseImmature},
	} {
		t.Run(string(tc.code), func(t *testing.T) {
			perIndex := s.createSpendError(LuaErrorInfo{ErrorCode: tc.code, Message: "detail"}, item, txID)
			require.ErrorIs(t, perIndex, tc.want)
			require.NotErrorIs(t, perIndex, errors.ErrStorageError)

			wholeRecord := s.createGeneralError(tc.code, txID, 100, 1, "detail")
			require.ErrorIs(t, wholeRecord, tc.want, "the per-index and whole-record answers must carry the same type")
		})
	}
}
