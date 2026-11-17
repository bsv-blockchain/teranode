package blockvalidation

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShouldSkipUnspendableCreate(t *testing.T) {
	newSettings := func(enabled bool) *settings.Settings {
		s := test.CreateBaseTestSettings(t)
		s.BlockValidation.SkipUnspendableTxStorageDuringCatchup = enabled
		return s
	}
	opReturnTx := func() *bt.Tx {
		tx := bt.NewTx()
		require.NoError(t, tx.AddOpReturnOutput([]byte("data")))
		return tx
	}
	spendableTx := func() *bt.Tx {
		tx := bt.NewTx()
		tx.AddOutput(&bt.Output{Satoshis: 1000, LockingScript: &bscript.Script{}})
		return tx
	}

	const h = uint32(1000)

	t.Run("lock on (lockUTXOs true) never skips", func(t *testing.T) {
		assert.False(t, shouldSkipUnspendableCreate(true, newSettings(true), opReturnTx(), h))
	})
	t.Run("lock off, setting off, no skip", func(t *testing.T) {
		assert.False(t, shouldSkipUnspendableCreate(false, newSettings(false), opReturnTx(), h))
	})
	t.Run("lock off, setting on, unspendable -> skip", func(t *testing.T) {
		assert.True(t, shouldSkipUnspendableCreate(false, newSettings(true), opReturnTx(), h))
	})
	t.Run("lock off, setting on, spendable -> no skip", func(t *testing.T) {
		assert.False(t, shouldSkipUnspendableCreate(false, newSettings(true), spendableTx(), h))
	})
}
