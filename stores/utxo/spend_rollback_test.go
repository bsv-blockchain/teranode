package utxo

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// TestNeedsSpendRollback pins the one predicate both stores use to decide
// whether a failed Spend reverses the inputs it wrote. It moved here from the
// Aerospike package, where the slice form survived only as a test wrapper while
// the SQL store kept its own copy of the same body.
func TestNeedsSpendRollback(t *testing.T) {
	spent := errors.NewUtxoSpentError(chainhash.Hash{}, 0, chainhash.Hash{}, nil)

	tests := []struct {
		name    string
		errs    []error
		created bool
		want    bool
	}{
		{name: "no errors", errs: []error{nil, nil}},
		{name: "infrastructure error", errs: []error{nil, errors.NewStorageError("DEVICE_OVERLOAD")}},
		{name: "service unavailable", errs: []error{errors.NewServiceUnavailableError("batch timeout")}},
		{name: "context cancelled", errs: []error{errors.NewContextCanceledError("cancelled")}},
		{name: "double spend", errs: []error{nil, spent}, want: true},
		{name: "conflicting tx", errs: []error{errors.NewTxConflictingError("conflicting")}, want: true},
		{name: "frozen utxo", errs: []error{errors.NewUtxoFrozenError("frozen")}, want: true},
		{name: "hash mismatch", errs: []error{errors.NewUtxoHashMismatchError("mismatch")}, want: true},
		{name: "pruned replay", errs: []error{errors.NewUtxoSpendingTxPrunedError("pruned")}, want: true},
		{name: "mixed infrastructure and validation", errs: []error{errors.NewStorageError("DEVICE_OVERLOAD"), spent}, want: true},
		{
			name: "only infrastructure errors",
			errs: []error{errors.NewStorageError("DEVICE_OVERLOAD"), errors.NewStorageError("TIMEOUT"), errors.NewServiceUnavailableError("no connections")},
		},
		{
			name:    "missing parent, spender the caller created",
			errs:    []error{nil, errors.NewTxNotFoundError("parent not found")},
			created: true,
			want:    true,
		},
		{
			name: "missing parent, any other caller",
			errs: []error{nil, errors.NewTxNotFoundError("parent not found")},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			spends := make([]*Spend, 0, len(tt.errs))
			for _, err := range tt.errs {
				spends = append(spends, &Spend{Err: err})
			}

			require.Equal(t, tt.want, NeedsSpendRollback(spends, tt.created))

			anyRollbackError := false
			for _, err := range tt.errs {
				if err != nil && IsSpendRollbackError(err, tt.created) {
					anyRollbackError = true
				}
			}

			require.Equal(t, tt.want, anyRollbackError, "the per-error and slice forms must agree")
		})
	}
}
