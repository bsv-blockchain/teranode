package aerospike

import (
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/stretchr/testify/require"
)

// batchResults builds one CREATE_ONLY batch's per-record outcomes. A nil entry means the
// record was written by us; a non-nil one is the error Aerospike returned for it.
func batchResults(t *testing.T, errs ...aerospike.Error) []aerospike.BatchRecordIfc {
	t.Helper()

	records := make([]aerospike.BatchRecordIfc, len(errs))

	for i, err := range errs {
		key, keyErr := aerospike.NewKey("test", "txmeta", []byte{byte(i)})
		require.NoError(t, keyErr)

		record := aerospike.NewBatchWrite(nil, key, aerospike.PutOp(aerospike.NewBin("x", 1)))
		record.BatchRec().Err = err
		records[i] = record
	}

	return records
}

func keyExists() aerospike.Error {
	return aeroErr(types.KEY_EXISTS_ERROR)
}

// TestClassifyCreateBatchResults pins the rule that decides whether THIS writer created a
// transaction, which is the fix for issue 1442. The consequential case is the third one -
// master already present, child written by us. See the classifyCreateBatchResults doc
// comment for why that must not report success.
func TestClassifyCreateBatchResults(t *testing.T) {
	tests := []struct {
		name             string
		errs             []aerospike.Error
		wantMaster       bool
		wantFailures     bool
		wantPresentCount int
	}{
		{
			name:       "we wrote every record, including the master",
			errs:       []aerospike.Error{nil, nil},
			wantMaster: true,
		},
		{
			name:             "every record already existed",
			errs:             []aerospike.Error{keyExists(), keyExists()},
			wantMaster:       false,
			wantPresentCount: 2,
		},
		{
			// The regression: master already present, child written by us.
			name:             "master already present, we only filled in a child",
			errs:             []aerospike.Error{keyExists(), nil},
			wantMaster:       false,
			wantPresentCount: 1,
		},
		{
			// The mirror: we own the master, someone else had already written a child.
			// We did create the transaction, so this must report success.
			name:             "we wrote the master, a child already existed",
			errs:             []aerospike.Error{nil, keyExists()},
			wantMaster:       true,
			wantPresentCount: 1,
		},
		{
			name:         "a genuine failure is not mistaken for already-present",
			errs:         []aerospike.Error{nil, aeroErr(types.TIMEOUT)},
			wantMaster:   true,
			wantFailures: true,
		},
		{
			name:         "a failure on the master is a failure, not a creation",
			errs:         []aerospike.Error{aeroErr(types.TIMEOUT), nil},
			wantMaster:   false,
			wantFailures: true,
		},
		{
			name:       "single-record transaction we wrote",
			errs:       []aerospike.Error{nil},
			wantMaster: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			masterCreated, alreadyPresent, failed := classifyCreateBatchResults(batchResults(t, tt.errs...))

			require.Equal(t, tt.wantMaster, masterCreated,
				"masterCreated decides whether the caller is told it created this transaction")
			require.Equal(t, tt.wantFailures, len(failed) > 0)
			require.Len(t, alreadyPresent, tt.wantPresentCount)
		})
	}
}

// TestIsKeyExists pins the narrow error match the classifier relies on: only Aerospike's
// already-exists result counts as "someone got there first". Any other error, and any
// non-Aerospike error, must fall through to the failure path rather than being quietly
// treated as a completed previous attempt.
func TestIsKeyExists(t *testing.T) {
	require.True(t, isKeyExists(aeroErr(types.KEY_EXISTS_ERROR)))
	require.False(t, isKeyExists(aeroErr(types.TIMEOUT)))
	require.False(t, isKeyExists(aeroErr(types.KEY_NOT_FOUND_ERROR)))
	require.False(t, isKeyExists(errors.NewProcessingError("not an aerospike error")))
}
