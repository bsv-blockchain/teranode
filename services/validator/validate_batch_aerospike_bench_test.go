//go:build aerospike

package validator

// BenchmarkValidateBatch_RealAerospike benchmarks ValidateBatch at different
// batch sizes against a real Aerospike testcontainer, measuring the
// validator-to-Aerospike round-trip cost (one BatchOperate per phase).
//
// BA submission and TxMeta Kafka publish are overridden to no-ops so the
// benchmark isolates the UTXO store hot path. CPU validation (script verify)
// is also a no-op to exclude ECDSA/script cost.
//
// Run:
//
//	go test -count=1 -race=false -tags aerospike \
//	  -bench=BenchmarkValidateBatch_RealAerospike \
//	  -benchmem -benchtime=2x -run=NONE ./services/validator -v

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"

	utxotesthelper "github.com/bsv-blockchain/teranode/test/longtest/stores/utxo"
)

// BenchmarkValidateBatch_RealAerospike runs ValidateBatch at varying batch
// sizes against a real Aerospike testcontainer.
func BenchmarkValidateBatch_RealAerospike(b *testing.B) {
	for _, batchSize := range []int{32, 128, 512} {
		batchSize := batchSize
		b.Run(fmt.Sprintf("N=%d", batchSize), func(b *testing.B) {
			benchValidateBatchOnAerospike(b, batchSize)
		})
	}
}

func benchValidateBatchOnAerospike(b *testing.B, batchSize int) {
	b.Helper()
	ctx := context.Background()

	v, aeroStore, cleanup := newValidatorBackedByAerospike(b)
	defer cleanup()

	// Override CPU validation: make it a no-op so the benchmark measures
	// the UTXO store round-trip rather than script-verification cost.
	v.overrideCPUValidationForTest(func(_ *bt.Tx) error { return nil })

	// Override BA submission: always accept all txs to avoid BA latency.
	v.overrideBASubmitForTest(func(_ context.Context, _ []*bt.Tx) map[chainhash.Hash]error {
		return map[chainhash.Hash]error{}
	})

	// Override TxMeta Kafka publish: no-op so we don't need a live producer.
	v.overrideTxMetaPublishForTest(func(_ *bt.Tx, _ *meta.Data) {})

	// Pre-generate b.N batches so the timed loop excludes generation overhead.
	// Each batch has fresh child txs (unique tx IDs) to avoid CREATE_ONLY
	// conflicts across iterations. Parents are seeded once per batch so
	// Phase A (BatchGetParents) can resolve them.
	batches := make([][]*bt.Tx, b.N)
	for i := 0; i < b.N; i++ {
		parents := seedRandomParentsForBench(b, ctx, aeroStore, batchSize)
		batches[i] = buildChildrenSpendingParentsForBench(b, parents)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := v.ValidateBatch(ctx, batches[i], 100)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// newValidatorBackedByAerospike spins up a testcontainers Aerospike instance,
// constructs an *aerostore.Store pointing at it, and wires a *Validator using
// that store. Returns the validator, the store (for direct seeding), and a
// cleanup function that terminates the container.
func newValidatorBackedByAerospike(b testing.TB) (*Validator, *aerostore.Store, func()) {
	b.Helper()
	tracing.SetupMockTracer()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.Disabled = true

	// Start the Aerospike testcontainer.
	container, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport("test"))
	if err != nil {
		b.Skipf("Aerospike testcontainer unavailable: %v", err)
	}

	host, err := container.Host(ctx)
	require.NoError(b, err)

	port, err := container.ServicePort(ctx)
	require.NoError(b, err)

	aerospikeContainerURL := fmt.Sprintf(
		"aerospike://%s:%d/test?set=test&block_retention=1&externalStore=file:///tmp/bench-aero-external",
		host, port,
	)
	aeroURL, err := url.Parse(aerospikeContainerURL)
	require.NoError(b, err)

	aeroStore, err := aerostore.New(ctx, logger, tSettings, aeroURL)
	require.NoError(b, err)

	aeroStore.SetExternalStore(memory.New())

	iface, err := New(ctx, logger, tSettings, aeroStore, nil, nil, nil, nil)
	require.NoError(b, err)

	v := iface.(*Validator)

	cleanup := func() {
		if termErr := container.Terminate(ctx); termErr != nil {
			b.Logf("warning: failed to terminate Aerospike container: %v", termErr)
		}
	}

	return v, aeroStore, cleanup
}

// seedRandomParentsForBench creates batchSize parent txs in the Aerospike store
// and returns them. Each parent has one output so children can reference it.
func seedRandomParentsForBench(b testing.TB, ctx context.Context, s *aerostore.Store, n int) []*bt.Tx {
	b.Helper()
	parents := make([]*bt.Tx, n)
	for i := 0; i < n; i++ {
		tx, err := utxotesthelper.CreateTransaction(1)
		require.NoError(b, err)
		_, err = s.Create(ctx, tx, 0)
		require.NoError(b, err)
		parents[i] = tx
	}
	return parents
}

// buildChildrenSpendingParentsForBench builds one child tx per parent, each
// spending output 0 of its parent. The inputs are extended (PreviousTxScript
// and PreviousTxSatoshis set) so that utxo.GetSpends (Phase C) can compute
// UTXOHashes without a nil-pointer error. The parent's locking script is
// reused as the input's PreviousTxScript.
func buildChildrenSpendingParentsForBench(b testing.TB, parents []*bt.Tx) []*bt.Tx {
	b.Helper()
	children := make([]*bt.Tx, len(parents))
	for i, parent := range parents {
		ph := parent.TxIDChainHash()

		// Build a fresh child tx spending output 0 of the parent.
		child := bt.NewTx()

		var lockScript *bscript.Script
		if parent.Outputs[0].LockingScript != nil {
			lockScript = parent.Outputs[0].LockingScript
		} else {
			lockScript = &bscript.Script{}
		}

		input := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   lockScript,
			PreviousTxSatoshis: parent.Outputs[0].Satoshis,
		}
		err := input.PreviousTxIDAdd(ph)
		require.NoError(b, err)
		child.Inputs = append(child.Inputs, input)

		// Add an output so the tx passes minimal format checks (non-empty outputs).
		err = child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 1)
		require.NoError(b, err)

		// Mark as extended so the validator knows inputs are already decorated.
		child.SetExtended(true)

		children[i] = child
	}
	return children
}
