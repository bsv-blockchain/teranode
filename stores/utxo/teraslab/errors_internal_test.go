package teraslab

import (
	"testing"

	"github.com/bsv-blockchain/teranode/errors"
	teraslab "github.com/icellan/teraslab/client/go"
	"github.com/stretchr/testify/require"
)

// TestMapErrorCode exercises every TeraSlab error code -> Teranode error mapping.
// This is the translation table the money-path relies on, so each arm is pinned
// to the exact sentinel/category the Aerospike backend would surface for the
// equivalent condition.
func TestMapErrorCode(t *testing.T) {
	t.Run("OK maps to nil", func(t *testing.T) {
		require.NoError(t, mapErrorCode(teraslab.ErrCodeOK))
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
		{"CoinbaseImmature", teraslab.ErrCodeCoinbaseImmature, errors.ErrTxCoinbaseImmature},
		{"UtxoHashMismatch", teraslab.ErrCodeUtxoHashMismatch, errors.ErrUtxoHashMismatch},
		{"AlreadyExists", teraslab.ErrCodeAlreadyExists, errors.ErrTxExists},
	}
	for _, tc := range sentinels {
		t.Run(tc.name, func(t *testing.T) {
			err := mapErrorCode(tc.code)
			require.Error(t, err)
			require.ErrorIs(t, err, tc.want)
		})
	}

	// Category codes: no shared sentinel, so assert a non-nil error is returned.
	categories := []struct {
		name string
		code uint16
	}{
		{"UtxoNotFrozen", teraslab.ErrCodeUtxoNotFrozen},
		{"VoutOutOfRange", teraslab.ErrCodeVoutOutOfRange},
		{"InvalidSpend", teraslab.ErrCodeInvalidSpend},
		{"Internal", teraslab.ErrCodeInternal},
		{"Redirect", teraslab.ErrCodeRedirect},
		{"unknown code falls through to storage error", 0xBEEF},
	}
	for _, tc := range categories {
		t.Run(tc.name, func(t *testing.T) {
			require.Error(t, mapErrorCode(tc.code))
		})
	}

	// Cluster / replication / migration codes (15-37). The client v0.5.1 has no
	// named constants for these, so they are pinned by their wire values from
	// src/protocol/opcodes.rs. Transient codes must be retryable so the caller's
	// backoff loop kicks in; the rest carry a descriptive storage/processing
	// error and must not be flagged retryable.
	transient := []struct {
		name string
		code uint16
	}{
		{"NoQuorum", 15},
		{"MigrationInProgress", 19},
		{"ReplicationFailed", 20},
		{"StaleEpoch", 24},
	}
	for _, tc := range transient {
		t.Run(tc.name+" is retryable", func(t *testing.T) {
			err := mapErrorCode(tc.code)
			require.Error(t, err)
			require.True(t, errors.IsRetryableError(err), "expected code %d to map to a retryable error", tc.code)
		})
	}

	t.Run("ClusterNotReady is a storage error mentioning cluster not ready", func(t *testing.T) {
		err := mapErrorCode(25)
		require.Error(t, err)
		var tErr *errors.Error
		require.True(t, errors.As(err, &tErr))
		require.Equal(t, errors.ERR_STORAGE_ERROR, tErr.Code())
		// Distinct message so startup gating can identify a not-ready cluster.
		require.Contains(t, tErr.Message(), "cluster not ready")
	})

	t.Run("PayloadMalformed is a processing error", func(t *testing.T) {
		err := mapErrorCode(28)
		require.Error(t, err)
		var tErr *errors.Error
		require.True(t, errors.As(err, &tErr))
		require.Equal(t, errors.ERR_PROCESSING, tErr.Code())
		require.Contains(t, tErr.Message(), "payload malformed")
	})
}

// TestPartialErrorToError covers the per-item aggregation used by the batch
// mutation paths (mining, alert, locked) so partial failures are never dropped.
func TestPartialErrorToError(t *testing.T) {
	t.Run("nil PartialError returns nil", func(t *testing.T) {
		require.NoError(t, partialErrorToError("op", nil))
	})

	t.Run("empty Errors returns nil", func(t *testing.T) {
		require.NoError(t, partialErrorToError("op", &teraslab.PartialError{}))
	})

	t.Run("single item failure surfaces mapped error", func(t *testing.T) {
		pe := &teraslab.PartialError{Errors: []teraslab.BatchItemError{
			{ItemIndex: 0, Code: teraslab.ErrCodeTxNotFound},
		}}
		err := partialErrorToError("SetMinedMulti", pe)
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrTxNotFound)
	})

	t.Run("multiple item failures are joined", func(t *testing.T) {
		pe := &teraslab.PartialError{Errors: []teraslab.BatchItemError{
			{ItemIndex: 1, Code: teraslab.ErrCodeTxNotFound},
			{ItemIndex: 3, Code: teraslab.ErrCodeConflicting},
		}}
		err := partialErrorToError("MarkTransactionsOnLongestChain", pe)
		require.Error(t, err)
		// errors.Join keeps both branches reachable via errors.Is.
		require.ErrorIs(t, err, errors.ErrTxNotFound)
		require.ErrorIs(t, err, errors.ErrTxConflicting)
	})
}
