package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/stretchr/testify/assert"
)

func TestNeedsSpendRollback(t *testing.T) {
	t.Run("no errors returns false", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: nil},
			{Err: nil},
		}
		assert.False(t, needsSpendRollback(spends))
	})

	t.Run("infrastructure error returns false", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: nil},
			{Err: errors.NewStorageError("DEVICE_OVERLOAD")},
		}
		assert.False(t, needsSpendRollback(spends))
	})

	t.Run("service unavailable error returns false", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: errors.NewServiceUnavailableError("batch timeout")},
		}
		assert.False(t, needsSpendRollback(spends))
	})

	t.Run("context cancelled error returns false", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: errors.NewContextCanceledError("cancelled")},
		}
		assert.False(t, needsSpendRollback(spends))
	})

	t.Run("double spend returns true", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: nil},
			{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)},
		}
		assert.True(t, needsSpendRollback(spends))
	})

	t.Run("conflicting tx returns true", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: errors.NewTxConflictingError("conflicting")},
		}
		assert.True(t, needsSpendRollback(spends))
	})

	t.Run("frozen utxo returns true", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: errors.NewUtxoFrozenError("frozen")},
		}
		assert.True(t, needsSpendRollback(spends))
	})

	t.Run("hash mismatch returns true", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: errors.NewUtxoHashMismatchError("mismatch")},
		}
		assert.True(t, needsSpendRollback(spends))
	})

	t.Run("mixed infra and validation returns true", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: errors.NewStorageError("DEVICE_OVERLOAD")},
			{Err: errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)},
		}
		assert.True(t, needsSpendRollback(spends))
	})

	t.Run("multiple infra errors returns false", func(t *testing.T) {
		spends := []*utxo.Spend{
			{Err: errors.NewStorageError("DEVICE_OVERLOAD")},
			{Err: errors.NewStorageError("TIMEOUT")},
			{Err: errors.NewServiceUnavailableError("no connections")},
		}
		assert.False(t, needsSpendRollback(spends))
	})
}
