package validator

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/stores/utxo/tests"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// runValidateNewTxBench validates a distinct transaction per iteration through
// the whole validator path into a real store, so each one spends its parent and
// creates its record. Script checks are skipped because the workload's unlocking
// scripts are dummies, and that cost is not what this measures.
//
// wrap lets a caller put something between the validator and the store.
func runValidateNewTxBench(b *testing.B, wrap func(utxo.Store) utxo.Store) {
	b.Helper()

	const height = 1_000

	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(b)
	// Flush a batch as soon as a worker is free, so the timing is the work and not
	// the batcher's flush timer.
	tSettings.BatcherDrainMode = true

	storeURL, err := url.Parse("sqlitememory:///validatebench")
	require.NoError(b, err)

	inner, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(b, err)

	v, err := New(ctx, ulogger.TestLogger{}, tSettings, wrap(inner), nil, nil, nil, nil, nil)
	require.NoError(b, err)

	w := tests.BuildMultiWorkload(b, 0xc8, 1, b.N)
	w.StoreRoots(b, inner, height-1)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := v.Validate(ctx, w.Txs[i], height,
			WithSkipPolicyChecks(true),
			WithSkipScriptValidation(true),
			WithAddTXToBlockAssembly(false),
			WithSkipTxMetaPublishing(true),
			WithCandidateParentMedianTime(1),
			WithUnconfirmedParentsAtCandidateHeight(true)); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkValidateNewTx times the validator's create path with nothing between
// it and the store, which is lane positions off.
func BenchmarkValidateNewTx(b *testing.B) {
	runValidateNewTxBench(b, func(s utxo.Store) utxo.Store { return s })
}
