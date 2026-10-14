package propagation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	terrors "github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/stores/blob/null"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"github.com/stretchr/testify/require"
)

// newPropagationServerForTest builds a minimal *PropagationServer backed by a
// real validator (SQLite-memory UTXO store, no Kafka) and a null blob store.
// It returns the server and a no-op cleanup function.
func newPropagationServerForTest(t *testing.T) (*PropagationServer, func()) {
	t.Helper()
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Propagation.GRPCListenAddress = ""
	tSettings.Propagation.HTTPListenAddress = ""
	tSettings.BlockAssembly.Disabled = true

	validatorInstance, _ := setupRealValidator(t, ctx)

	txStore, err := null.New(logger)
	require.NoError(t, err)

	ps := &PropagationServer{
		logger:    logger,
		settings:  tSettings,
		validator: validatorInstance,
		txStore:   txStore,
	}

	return ps, func() {}
}

// TestProcessTransactionBatch_PerTxErrorsMapByIndex asserts the
// response.Errors slice has one slot per input tx (positional), with
// per-tx outcomes correctly attributed. The contract must hold across
// the refactor.
func TestProcessTransactionBatch_PerTxErrorsMapByIndex(t *testing.T) {
	ctx := context.Background()
	ps, cleanup := newPropagationServerForTest(t)
	defer cleanup()

	// Two empty / invalid tx — both should fail validation.
	bads := []*bt.Tx{{}, {}}
	items := make([]*propagation_api.BatchTransactionItem, len(bads))
	for i, tx := range bads {
		items[i] = &propagation_api.BatchTransactionItem{Tx: tx.Bytes()}
	}

	resp, err := ps.ProcessTransactionBatch(ctx, &propagation_api.ProcessTransactionBatchRequest{Items: items})
	require.NoError(t, err, "whole-batch err should be nil even when all per-tx fail")
	require.Len(t, resp.Errors, len(bads))
	for i, e := range resp.Errors {
		require.NotNil(t, e, "index %d should have a non-nil error", i)
	}
}

// TestProcessTransactionBatch_EmptyBatch confirms the empty-input case
// is handled without panic and returns an empty response.
func TestProcessTransactionBatch_EmptyBatch(t *testing.T) {
	ctx := context.Background()
	ps, cleanup := newPropagationServerForTest(t)
	defer cleanup()

	resp, err := ps.ProcessTransactionBatch(ctx, &propagation_api.ProcessTransactionBatchRequest{Items: nil})
	require.NoError(t, err)
	require.Len(t, resp.Errors, 0)
}

// TestProcessTransactionBatch_NativePath_FlagOn exercises the flag-on code path
// (processTransactionBatchNative), confirming it is reachable and returns per-tx
// errors for invalid transactions just as the legacy path does.
func TestProcessTransactionBatch_NativePath_FlagOn(t *testing.T) {
	ctx := context.Background()
	ps, cleanup := newPropagationServerForTest(t)
	defer cleanup()
	// Flag-on: take the new batch path via validator.ValidateBatch
	ps.settings.Validator.UseBatchValidation = true

	bads := []*bt.Tx{{}, {}}
	items := make([]*propagation_api.BatchTransactionItem, len(bads))
	for i, tx := range bads {
		items[i] = &propagation_api.BatchTransactionItem{Tx: tx.Bytes()}
	}

	resp, err := ps.ProcessTransactionBatch(ctx, &propagation_api.ProcessTransactionBatchRequest{Items: items})
	require.NoError(t, err)
	require.Len(t, resp.Errors, len(bads))
	// Both empty tx must fail (different code path, same outcome shape).
	for i, e := range resp.Errors {
		require.NotNil(t, e, "index %d", i)
	}
}

// TestProcessTransactionBatch_FlagParityWithSqlitememory asserts that
// flag-on and flag-off produce the SAME per-tx outcomes for the same
// inputs when the UTXO store is sqlitememory (which does not implement
// batchUtxoStore, so flag-on still uses the fan-out fallback). This is
// the v1 "behavior-preserving when flag is off" contract; this test
// also confirms flag-on doesn't regress that contract through the
// fallback path.
func TestProcessTransactionBatch_FlagParityWithSqlitememory(t *testing.T) {
	ctx := context.Background()

	run := func(t *testing.T, useBatch bool) []*terrors.TError {
		ps, cleanup := newPropagationServerForTest(t)
		defer cleanup()
		ps.settings.Validator.UseBatchValidation = useBatch

		// Two empty/invalid tx — both will fail validation.
		bads := []*bt.Tx{{}, {}}
		items := make([]*propagation_api.BatchTransactionItem, len(bads))
		for i, tx := range bads {
			items[i] = &propagation_api.BatchTransactionItem{Tx: tx.Bytes()}
		}
		resp, err := ps.ProcessTransactionBatch(ctx, &propagation_api.ProcessTransactionBatchRequest{Items: items})
		require.NoError(t, err)
		require.Len(t, resp.Errors, len(bads))
		return resp.Errors
	}

	offErrors := run(t, false)
	onErrors := run(t, true)

	// Both runs should have errors in the same positions.
	require.Len(t, onErrors, len(offErrors))
	for i := range offErrors {
		require.Equal(t, offErrors[i] == nil, onErrors[i] == nil, "flag-off vs flag-on disagree at index %d", i)
	}
}
