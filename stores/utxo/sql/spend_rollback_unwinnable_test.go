package sql

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/require"
)

// TestConsensusFrozenSpendIsUnwinnable pins that a spend refused by the consensus tier
// of an alert-system freeze marks the transaction unwinnable (issue #1422), so a batch
// that also saw a transient lock still rolls back its sibling spends rather than leave
// them naming a spender that can never be created. Mirrors the Aerospike store's
// sawUnwinnable (TestResolveSpendCompletionsConsensusFrozenIsUnwinnable).
func TestConsensusFrozenSpendIsUnwinnable(t *testing.T) {
	spends := []*utxo.Spend{
		{TxID: &chainhash.Hash{}, Vout: 0},
		{TxID: &chainhash.Hash{}, Vout: 1, Err: errors.NewTxLockedError("locked")},
		{TxID: &chainhash.Hash{}, Vout: 2, Err: errors.NewUtxoConsensusFrozenError("frozen at height")},
	}

	require.True(t, hasTransientLockFailure(spends))
	require.True(t, hasUnwinnableFailure(spends), "a consensus-frozen spend makes the txid unwinnable, so the sibling rollback fires")
}
