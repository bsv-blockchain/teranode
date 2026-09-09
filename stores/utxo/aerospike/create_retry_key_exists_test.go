package aerospike

import (
	"context"
	"fmt"
	"net/url"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/aerospike-client-go/v8"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// carriesCreateOnlyWriteFor reports whether a batch holds a CREATE_ONLY write
// for key, which is what sendStoreBatch issues for a transaction create.
func carriesCreateOnlyWriteFor(records []aerospike.BatchRecordIfc, key *aerospike.Key) bool {
	for _, rec := range records {
		w, ok := rec.(*aerospike.BatchWrite)
		if !ok || w.Policy == nil || w.Policy.RecordExistsAction != aerospike.CREATE_ONLY {
			continue
		}

		if w.Key != nil && w.Key.Equals(key) {
			return true
		}
	}

	return false
}

// TestCreateRetryKeyExistsDoesNotResurrectPrunedOutput replays a transaction the
// pruner removed end to end (parent included, so no marker survives anywhere)
// through the two phases the block paths run. In the resent_delivery case the
// create batch is delivered to the server twice, the way the Aerospike client
// re-sends a batch whose response was lost (aerospike_batchPolicy MaxRetries=5).
// The second delivery answers KEY_EXISTS for the record the first delivery
// wrote, and sendStoreBatch maps that to ErrTxExists, which is the only signal
// the block paths have for "did this attempt write the record".
//
// single_delivery is the control: the same fixture with the batch delivered
// once, proving the fixture and the end-state assertions are sound.
//
// The assertions are on the store, not on the calls: C:0 was consumed by the
// confirmed grandchild G, so whatever the store answered along the way, after
// the block-path sequence C:0 must not be presented as unspent and no new
// transaction may take it.
func TestCreateRetryKeyExistsDoesNotResurrectPrunedOutput(t *testing.T) {
	t.Run("single_delivery", func(t *testing.T) { testCreateRetryKeyExists(t, false) })
	t.Run("resent_delivery", func(t *testing.T) { testCreateRetryKeyExists(t, true) })
}

func testCreateRetryKeyExists(t *testing.T, resend bool) {
	t.Helper()
	InitPrometheusMetrics()

	logger := ulogger.New("create-retry-key-exists-test")
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)

	container, err := runAerospikeTestContainer(ctx)
	test.SkipIfContainerUnavailable(t, err)

	t.Cleanup(func() {
		require.NoError(t, container.Terminate(ctx))
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo&externalStore=file://./data/external", host, port))
	require.NoError(t, err)

	store, err := New(ctx, logger, tSettings, aeroURL)
	require.NoError(t, err)

	store.SetExternalStore(memory.New())

	const addr = "1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa"

	// P -> C -> G, all mined. Build the transactions first so the seam below can
	// be keyed on C's record before any store operation runs.
	p := bt.NewTx()
	require.NoError(t, p.From("1111111111111111111111111111111111111111111111111111111111111111", 0, "51", 30000))
	require.NoError(t, p.PayToAddress(addr, 4000))

	c := bt.NewTx()
	require.NoError(t, c.From(p.TxID(), 0, p.Outputs[0].LockingScript.String(), p.Outputs[0].Satoshis))
	require.NoError(t, c.PayToAddress(addr, 3000))

	g := bt.NewTx()
	require.NoError(t, g.From(c.TxID(), 0, c.Outputs[0].LockingScript.String(), c.Outputs[0].Satoshis))
	require.NoError(t, g.PayToAddress(addr, 2000))

	cKey, aErr := aerospike.NewKey(store.namespace, store.setName, c.TxIDChainHash().CloneBytes())
	require.NoError(t, aErr)

	// The seam stands in for the client's own retry: once armed, the first
	// CREATE_ONLY batch carrying C is delivered to the server twice, unchanged.
	// Installed before any store operation so no batcher goroutine reads the
	// field concurrently with this write.
	var (
		armed  atomic.Bool
		resent atomic.Int32
	)

	store.batchOperateFn = func(policy *aerospike.BatchPolicy, records []aerospike.BatchRecordIfc) aerospike.Error {
		batchErr := store.client.BatchOperate(policy, records)

		if resend && armed.Load() && carriesCreateOnlyWriteFor(records, cKey) && resent.CompareAndSwap(0, 1) {
			batchErr = store.client.BatchOperate(policy, records)
		}

		return batchErr
	}

	require.NoError(t, store.SetBlockHeight(1000))

	mined := func(blockHeight uint32, txs ...*bt.Tx) {
		t.Helper()

		hashes := make([]*chainhash.Hash, 0, len(txs))
		for _, tx := range txs {
			hashes = append(hashes, tx.TxIDChainHash())
		}

		_, err := store.SetMinedMulti(ctx, hashes, utxo.MinedBlockInfo{BlockID: blockHeight, BlockHeight: blockHeight, OnLongestChain: true})
		require.NoError(t, err)
	}

	_, err = store.Create(ctx, p, 1000)
	require.NoError(t, err)

	_, _, err = store.SpendAndCreate(ctx, c, 1000)
	require.NoError(t, err)
	mined(1000, p, c)

	_, _, err = store.SpendAndCreate(ctx, g, 1001)
	require.NoError(t, err)
	mined(1001, g)

	// The pruner removes the fully spent, mined C, then the fully spent, mined P.
	// C's marker lived on P's record and goes with it: nothing is left that says
	// C ever existed.
	require.NoError(t, store.DeleteComplete(ctx, c.TxIDChainHash()))
	require.NoError(t, store.DeleteComplete(ctx, p.TxIDChainHash()))

	_, err = store.Get(ctx, c.TxIDChainHash())
	require.ErrorIs(t, err, errors.ErrTxNotFound, "fixture: C is gone before the replay")

	// A block containing C is validated again. Phase 1 creates every transaction
	// (quick_validate.go createAndSpendUTXOsForBatch); in the resent case the
	// create batch for C is delivered twice by the seam.
	armed.Store(true)

	minedInfo := utxo.MinedBlockInfo{BlockID: 1400, BlockHeight: 1400}

	_, _, createErr := store.SpendAndCreate(ctx, c, 1400, utxo.WithCreateOnly(), utxo.WithMinedBlockInfo(minedInfo), utxo.WithLocked(true))

	// Derived exactly as the block paths derive it: a create that answered
	// ErrTxExists is still ours when the record is locked, because that is the
	// two-phase mark phase 1 writes and nothing else leaves on a mined record
	// (utxo.LeftoversAmong).
	createdHere := createErr == nil
	if !createdHere && errors.Is(createErr, errors.ErrTxExists) {
		leftovers, leftErr := utxo.LeftoversAmong(ctx, store, []*chainhash.Hash{c.TxIDChainHash()})
		require.NoError(t, leftErr)
		_, createdHere = leftovers[*c.TxIDChainHash()]
	}
	if createErr != nil {
		require.ErrorIs(t, createErr, errors.ErrTxExists, "fixture: any other create error fails the block before it reaches the spend phase")
	}

	t.Logf("phase 1 create of the replayed C returned %v (batch re-sent %d time(s)); the block path files it as createdHere=%v", createErr, resent.Load(), createdHere)

	// Phase 2 spends with the flag derived from phase 1 (spendBatchWithRetry), and
	// on a hard failure the block path removes the ghosts it identified.
	_, _, spendErr := store.SpendAndCreate(ctx, c, 1400, utxo.WithSpendOnly(), utxo.WithIgnoreLocked(true), utxo.WithSpenderCreatedByCaller(createdHere))

	t.Logf("phase 2 spend of the replayed C returned %v", spendErr)

	if spendErr != nil {
		var rejected []*chainhash.Hash
		if utxo.IsPrunedReplayRejection(spendErr, createdHere) {
			rejected = append(rejected, c.TxIDChainHash())
		}

		ghosts := utxo.PrunedReplayGhosts([]*bt.Tx{c}, rejected, func(*chainhash.Hash) bool { return createdHere })
		require.NoError(t, utxo.DeleteCreated(ctx, logger, store, ghosts, 8))
	}

	// End state. C:0 was consumed by the confirmed G. If a record for C exists at
	// all, C:0 must still name G as its spender.
	c0Hash, err := util.UTXOHashFromOutput(c.TxIDChainHash(), c.Outputs[0], 0)
	require.NoError(t, err)

	c0 := &utxo.Spend{TxID: c.TxIDChainHash(), Vout: 0, UTXOHash: c0Hash}

	after, err := store.GetSpend(ctx, c0)
	require.NoError(t, err)

	if after.Status != int(utxo.Status_NOT_FOUND) {
		require.NotNil(t, after.SpendingData, "C:0 was consumed by the confirmed G; after the replay it is presented as unspent (create answered %v, spend answered %v)", createErr, spendErr)
		require.Equal(t, *g.TxIDChainHash(), *after.SpendingData.TxID, "C:0 must still be spent by G")
	}

	// And no new transaction may take it.
	thief := bt.NewTx()
	require.NoError(t, thief.From(c.TxID(), 0, c.Outputs[0].LockingScript.String(), c.Outputs[0].Satoshis))
	require.NoError(t, thief.PayToAddress(addr, 2999))

	_, _, thiefErr := store.SpendAndCreate(ctx, thief, 1401, utxo.WithIgnoreLocked(true))
	require.Error(t, thiefErr, "a new transaction took C:0, an output the confirmed G already consumed")

	thiefKey, aErr := aerospike.NewKey(store.namespace, store.setName, thief.TxIDChainHash().CloneBytes())
	require.NoError(t, aErr)

	thiefExists, aErr := store.client.Exists(nil, thiefKey)
	require.NoError(t, aErr)
	require.False(t, thiefExists, "the thief must not have been created")
}
