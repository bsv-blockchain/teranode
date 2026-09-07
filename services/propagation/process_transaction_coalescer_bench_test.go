//go:build aerospike

package propagation

// BenchmarkProcessTransaction_FlagOffVsOn_RealAerospike compares the
// throughput of ProcessTransaction with the coalescer disabled
// (today's direct-validator path) vs enabled (coalescer batching) when
// driven by N concurrent submitters. Uses real Aerospike via
// testcontainers, mirroring the v1 bench at
// services/validator/validate_batch_aerospike_bench_test.go.
//
// Run:
//
//	go test -tags aerospike \
//	  -bench=BenchmarkProcessTransaction_FlagOffVsOn_RealAerospike \
//	  -benchmem -benchtime=10x -run=NONE -timeout=20m \
//	  ./services/propagation -v

import (
	"context"
	crand "crypto/rand"
	"fmt"
	"net/url"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	aerostore "github.com/bsv-blockchain/teranode/stores/utxo/aerospike"
	"github.com/bsv-blockchain/teranode/services/propagation/propagation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/bsv-blockchain/teranode/util/tracing"
	aeroTest "github.com/bsv-blockchain/testcontainers-aerospike-go"
	"github.com/stretchr/testify/require"
)

func BenchmarkProcessTransaction_FlagOffVsOn_RealAerospike(b *testing.B) {
	for _, concurrency := range []int{32, 128, 512, 1024} {
		concurrency := concurrency
		for _, useBatch := range []bool{false, true} {
			useBatch := useBatch
			name := "direct"
			if useBatch {
				name = "coalescer"
			}
			b.Run(fmt.Sprintf("concurrency=%d/%s", concurrency, name), func(b *testing.B) {
				benchProcessTxOnAerospike(b, concurrency, useBatch)
			})
		}
	}
}

func benchProcessTxOnAerospike(b *testing.B, concurrency int, useBatch bool) {
	b.Helper()
	ctx := context.Background()
	ps, aeroStore, cleanup := newPropagationBackedByAerospike(b, useBatch)
	defer cleanup()

	// Pre-generate b.N rounds of `concurrency` fresh txs each so the
	// timed loop excludes generation overhead.
	rounds := make([][]*bt.Tx, b.N)
	for i := 0; i < b.N; i++ {
		parents := seedRandomParentsForCoalescerBench(b, ctx, aeroStore, concurrency)
		rounds[i] = buildChildrenForCoalescerBench(b, parents)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		for _, tx := range rounds[i] {
			tx := tx
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _ = ps.ProcessTransaction(ctx, &propagation_api.ProcessTransactionRequest{Tx: tx.Bytes()})
			}()
		}
		wg.Wait()
	}
}

// newPropagationBackedByAerospike spins up Aerospike via testcontainers,
// builds an Aerospike UTXO store, a real Validator wired to it, and a
// PropagationServer with Kafka producer set to nil and the flag set.
// Returns the server, the underlying aerospike store, plus a cleanup
// function. Mirrors validate_batch_aerospike_bench_test.go's
// newValidatorBackedByAerospike.
func newPropagationBackedByAerospike(b testing.TB, useBatch bool) (*PropagationServer, *aerostore.Store, func()) {
	b.Helper()
	tracing.SetupMockTracer()
	initPrometheusMetrics()

	ctx := context.Background()
	logger := ulogger.TestLogger{}
	tSettings := test.CreateBaseTestSettings(b)
	tSettings.BlockAssembly.Disabled = true
	tSettings.Validator.UseBatchValidation = useBatch
	tSettings.Validator.BatchCoalescerDrainMode = true

	// Prod-aligned per-op batcher settings — apply equally across both variants so
	// the comparison is apples-to-apples on the underlying store layer.
	tSettings.UtxoStore.GetBatcherSize = 512
	tSettings.UtxoStore.GetBatcherDurationMillis = 1
	tSettings.UtxoStore.GetBatcherDrainMode = true
	tSettings.UtxoStore.StoreBatcherSize = 512
	tSettings.UtxoStore.StoreBatcherDurationMillis = 1
	tSettings.Aerospike.StoreBatcherDuration = 1 * time.Millisecond
	tSettings.UtxoStore.StoreBatcherDrainMode = true
	tSettings.UtxoStore.SpendBatcherSize = 512
	tSettings.UtxoStore.SpendBatcherDurationMillis = 1
	tSettings.UtxoStore.SpendBatcherDrainMode = false
	tSettings.UtxoStore.SpendBatcherConcurrency = 256
	tSettings.UtxoStore.LockedBatcherSize = 512
	tSettings.UtxoStore.LockedBatcherDurationMillis = 1
	tSettings.UtxoStore.LockedBatcherDrainMode = false
	tSettings.UtxoStore.BatcherMaxConcurrent = 512

	container, err := aeroTest.RunContainer(ctx, aeroTest.WithTTLSupport("test"))
	if err != nil {
		b.Skipf("Aerospike testcontainer unavailable: %v", err)
	}

	host, err := container.Host(ctx)
	require.NoError(b, err)
	port, err := container.ServicePort(ctx)
	require.NoError(b, err)

	aerospikeContainerURL := fmt.Sprintf(
		"aerospike://%s:%d/test?set=test&block_retention=1&externalStore=file:///tmp/bench-coalescer-aero-external",
		host, port,
	)
	aeroURL, err := url.Parse(aerospikeContainerURL)
	require.NoError(b, err)

	aeroStore, err := aerostore.New(ctx, logger, tSettings, aeroURL)
	require.NoError(b, err)
	aeroStore.SetExternalStore(memory.New())

	vIface, err := validator.New(ctx, logger, tSettings, aeroStore, nil, nil, nil, nil)
	require.NoError(b, err)

	// NO test-only overrides — both paths (flag-off and flag-on) must run
	// exactly the same validation logic on exactly the same input. If a
	// tx is invalid, both paths must reject it for the same reason. If
	// they reject for DIFFERENT reasons, that's a correctness bug in
	// ValidateBatch as a drop-in for per-tx Validate, not a perf concern.

	// Build a PropagationServer wired to the aerospike-backed validator,
	// with Kafka producer explicitly nil (enables coalescer path when useBatch=true).
	txStore := memory.New()
	ps := New(logger, tSettings, txStore, vIface, nil, nil, nil)

	// Manually wire the coalescer — bypassing Start() which blocks on gRPC bind.
	if useBatch {
		ps.coalescer = NewTxCoalescer(
			ctx, logger, vIface,
			tSettings.Validator.BatchMaxSize,
			tSettings.Validator.BatchMaxWait,
			tSettings.Validator.BatchMaxConcurrent,
			tSettings.Validator.BatchCoalescerDrainMode,
		)
	}

	cleanup := func() {
		if ps.coalescer != nil {
			_ = ps.coalescer.Close(context.Background())
		}
		if termErr := container.Terminate(ctx); termErr != nil {
			b.Logf("warning: failed to terminate Aerospike container: %v", termErr)
		}
	}

	return ps, aeroStore, cleanup
}

// seedRandomParentsForCoalescerBench creates n parent txs directly in the Aerospike
// store via Create (bypassing the validator). Each parent is a regular (non-coinbase)
// extended tx with a random unique PreviousTxID so every parent gets a distinct tx
// hash, avoiding CREATE_ONLY collisions across bench iterations.
//
// Parents are stored as REGULAR txs (PreviousTxOutIndex=0, non-zero PreviousTxID)
// so the Aerospike store does NOT apply the coinbase maturity lock — children can
// spend these outputs immediately at blockHeight=0.
//
// PreviousTxSatoshis is set to 2000 so GetFees sees input(2000) >= output(1000) and
// accepts the tx. The OP_TRUE locking script gives children a trivially satisfiable
// spend condition.
func seedRandomParentsForCoalescerBench(b testing.TB, ctx context.Context, s *aerostore.Store, n int) []*bt.Tx {
	b.Helper()
	opTrue, err := bscript.NewFromHexString("51") // OP_1 / OP_TRUE (anyone-can-spend)
	require.NoError(b, err)

	parents := make([]*bt.Tx, n)
	emptyScript := bscript.Script{}

	for i := 0; i < n; i++ {
		tx := bt.NewTx()

		// Unique random 32-byte PreviousTxID so every parent has a distinct tx hash.
		// PreviousTxOutIndex=0 (not 0xFFFFFFFF) ensures IsCoinbase()==false, which
		// prevents the Aerospike Lua script from applying the coinbase maturity lock.
		var randBytes [32]byte
		_, randErr := crand.Read(randBytes[:])
		require.NoError(b, randErr)
		uniqueHash, hashErr := chainhash.NewHash(randBytes[:])
		require.NoError(b, hashErr)

		in := &bt.Input{
			PreviousTxOutIndex: 0,
			PreviousTxScript:   &emptyScript,
			PreviousTxSatoshis: 2000, // input > output so GetFees == 1000 (valid fee)
			UnlockingScript:    &emptyScript,
			SequenceNumber:     0xFFFFFFFF,
		}
		require.NoError(b, in.PreviousTxIDAdd(uniqueHash))
		tx.Inputs = append(tx.Inputs, in)

		tx.Outputs = append(tx.Outputs, &bt.Output{
			Satoshis:      1000,
			LockingScript: opTrue,
		})

		_, err = s.Create(ctx, tx, 0)
		require.NoError(b, err)
		parents[i] = tx
	}
	return parents
}

// buildChildrenForCoalescerBench builds one child tx per parent, each spending
// output 0 of its parent. Children are deliberately NON-EXTENDED: PreviousTxSatoshis
// and PreviousTxScript are left zero/nil, matching the wire-format shape of a tx
// arriving from the network. The validator must hydrate these fields at Phase A from
// the UTXO store — which is exactly the code path we want to exercise.
// UnlockingScript is set to a non-nil empty script so Store.Create can persist the
// child after validation succeeds (mirrors buildChildTxForParityTest).
func buildChildrenForCoalescerBench(b testing.TB, parents []*bt.Tx) []*bt.Tx {
	b.Helper()
	children := make([]*bt.Tx, len(parents))
	for i, parent := range parents {
		ph := parent.TxIDChainHash()
		child := bt.NewTx()

		emptyScript := bscript.Script{}
		in := &bt.Input{
			PreviousTxOutIndex: 0,
			// Deliberately leave PreviousTxSatoshis and PreviousTxScript unset
			// to simulate a non-extended (wire-format) transaction.
			UnlockingScript: &emptyScript,
		}
		require.NoError(b, in.PreviousTxIDAdd(ph))
		child.Inputs = append(child.Inputs, in)

		// Pay less than parent's 1000 satoshis so fee = 500 satoshis (valid once hydrated).
		require.NoError(b, child.PayToAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa", 500))

		children[i] = child
	}
	return children
}
