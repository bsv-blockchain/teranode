package teraslab

import (
	"testing"

	teraslab "github.com/icellan/teraslab/client/go"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMapErrorCode exercises every TeraSlab error code -> Teranode error mapping.
// This is the translation table the money-path relies on, so each arm is pinned
// to the exact sentinel/category the Aerospike backend would surface for the
// equivalent condition.
func TestMapErrorCode(t *testing.T) {
	t.Run("OK maps to nil", func(t *testing.T) {
		assert.NoError(t, mapErrorCode(teraslab.ErrCodeOK))
	})

	// Sentinel-error codes: callers branch on errors.Is, so identity matters.
	sentinels := []struct {
		name string
		code uint16
		want error
	}{
		{"TxNotFound", teraslab.ErrCodeTxNotFound, errors.ErrTxNotFound},
		{"AlreadySpent", teraslab.ErrCodeAlreadySpent, errors.ErrSpent},
		{"AlreadyFrozen", teraslab.ErrCodeAlreadyFrozen, errors.ErrFrozen},
		{"Frozen", teraslab.ErrCodeFrozen, errors.ErrFrozen},
		{"FrozenUntil", teraslab.ErrCodeFrozenUntil, errors.ErrFrozen},
		{"Conflicting", teraslab.ErrCodeConflicting, errors.ErrTxConflicting},
		{"Locked", teraslab.ErrCodeLocked, errors.ErrTxLocked},
		{"CoinbaseImmature", teraslab.ErrCodeCoinbaseImmature, errors.ErrNonFinal},
		{"AlreadyExists", teraslab.ErrCodeAlreadyExists, errors.ErrTxExists},
	}
	for _, tc := range sentinels {
		t.Run(tc.name, func(t *testing.T) {
			err := mapErrorCode(tc.code)
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.want)
		})
	}

	// Category codes: no shared sentinel, so assert a non-nil error is returned.
	categories := []struct {
		name string
		code uint16
	}{
		{"UtxoNotFrozen", teraslab.ErrCodeUtxoNotFrozen},
		{"VoutOutOfRange", teraslab.ErrCodeVoutOutOfRange},
		{"UtxoHashMismatch", teraslab.ErrCodeUtxoHashMismatch},
		{"InvalidSpend", teraslab.ErrCodeInvalidSpend},
		{"Internal", teraslab.ErrCodeInternal},
		{"unknown code falls through to storage error", 0xBEEF},
	}
	for _, tc := range categories {
		t.Run(tc.name, func(t *testing.T) {
			assert.Error(t, mapErrorCode(tc.code))
		})
	}
}

// TestPartialErrorToError covers the per-item aggregation used by the batch
// mutation paths (mining, alert, locked) so partial failures are never dropped.
func TestPartialErrorToError(t *testing.T) {
	t.Run("nil PartialError returns nil", func(t *testing.T) {
		assert.NoError(t, partialErrorToError("op", nil))
	})

	t.Run("empty Errors returns nil", func(t *testing.T) {
		assert.NoError(t, partialErrorToError("op", &teraslab.PartialError{}))
	})

	t.Run("single item failure surfaces mapped error", func(t *testing.T) {
		pe := &teraslab.PartialError{Errors: []teraslab.BatchItemError{
			{ItemIndex: 0, Code: teraslab.ErrCodeTxNotFound},
		}}
		err := partialErrorToError("SetMinedMulti", pe)
		require.Error(t, err)
		assert.ErrorIs(t, err, errors.ErrTxNotFound)
	})

	t.Run("multiple item failures are joined", func(t *testing.T) {
		pe := &teraslab.PartialError{Errors: []teraslab.BatchItemError{
			{ItemIndex: 1, Code: teraslab.ErrCodeTxNotFound},
			{ItemIndex: 3, Code: teraslab.ErrCodeConflicting},
		}}
		err := partialErrorToError("MarkTransactionsOnLongestChain", pe)
		require.Error(t, err)
		// errors.Join keeps both branches reachable via errors.Is.
		assert.ErrorIs(t, err, errors.ErrTxNotFound)
		assert.ErrorIs(t, err, errors.ErrTxConflicting)
	})
}
