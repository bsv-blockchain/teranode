package netsync

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo/nullstore"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// outpointOnlySpyStore wraps NullStore to count BatchPreviousOutputsDecorate calls
// and capture the CreateOptions passed to Create, so tests can assert which legacy
// path engaged without standing up a real SQL store.
type outpointOnlySpyStore struct {
	*nullstore.NullStore
	decorateCalls atomic.Int32
}

func (s *outpointOnlySpyStore) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	s.decorateCalls.Add(1)
	return s.NullStore.BatchPreviousOutputsDecorate(ctx, txs)
}

// SupportsOutpointOnlySpend overrides the embedded NullStore to model a store that
// honours the fast path, so legacyOutpointOnly can engage in these tests.
func (s *outpointOnlySpyStore) SupportsOutpointOnlySpend() bool { return true }

// newOutpointOnlySettings returns settings configured so legacyOutpointOnly can
// return true: the feature flag on, a SQL-backed (sqlitememory) UTXO store URL,
// and a single hard-coded checkpoint at checkpointHeight on the chain params.
func newOutpointOnlySettings(t *testing.T, enabled bool, sqlStore bool, checkpointHeight int32) (*settings.Settings, *chaincfg.Params) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OutpointOnlyBelowCheckpoint = enabled

	if sqlStore {
		u, err := url.Parse("sqlitememory://test")
		require.NoError(t, err)
		tSettings.UtxoStore.UtxoStore = u
	} else {
		// aerospike scheme = non-SQL
		u, err := url.Parse("aerospike://host:3000/ns/set")
		require.NoError(t, err)
		tSettings.UtxoStore.UtxoStore = u
	}

	params := chaincfg.RegressionNetParams
	params.Checkpoints = []chaincfg.Checkpoint{{Height: checkpointHeight}}
	tSettings.ChainCfgParams = &params

	return tSettings, &params
}

// TestSyncManager_legacyOutpointOnly is the full truth table for the gate helper.
// Every conjunct must hold (flag on AND SQL store AND at/below the highest hard-coded
// checkpoint) for the fast path to engage; any one missing keeps it OFF (fail-safe).
func TestSyncManager_legacyOutpointOnly(t *testing.T) {
	const checkpointHeight = int32(1000)
	const below = uint32(500)
	const atCheckpoint = uint32(1000)
	const above = uint32(1500)

	tests := []struct {
		name       string
		enabled    bool
		sqlStore   bool
		nilChain   bool
		noCheckpts bool
		height     uint32
		want       bool
	}{
		{name: "flag off, SQL, below", enabled: false, sqlStore: true, height: below, want: false},
		{name: "flag on, non-SQL (aerospike), below", enabled: true, sqlStore: false, height: below, want: false},
		{name: "flag on, SQL, below checkpoint", enabled: true, sqlStore: true, height: below, want: true},
		{name: "flag on, SQL, at checkpoint", enabled: true, sqlStore: true, height: atCheckpoint, want: true},
		{name: "flag on, SQL, above checkpoint", enabled: true, sqlStore: true, height: above, want: false},
		{name: "flag on, SQL, height 0", enabled: true, sqlStore: true, height: 0, want: true},
		{name: "flag on, SQL, nil chain params", enabled: true, sqlStore: true, nilChain: true, height: below, want: false},
		{name: "flag on, SQL, no checkpoints", enabled: true, sqlStore: true, noCheckpts: true, height: below, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tSettings, params := newOutpointOnlySettings(t, tt.enabled, tt.sqlStore, checkpointHeight)

			sm := &SyncManager{
				settings:    tSettings,
				chainParams: params,
				logger:      ulogger.TestLogger{},
			}

			// The gate now asks the store, not the settings URL: a supporting store
			// (spy over NullStore) for the SQL case, a plain NullStore (reports false)
			// otherwise.
			if tt.sqlStore {
				sm.utxoStore = &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}
			} else {
				sm.utxoStore = &nullstore.NullStore{}
			}

			if tt.nilChain {
				sm.chainParams = nil
				sm.settings.ChainCfgParams = nil
			}

			if tt.noCheckpts {
				noCp := chaincfg.RegressionNetParams
				noCp.Checkpoints = nil
				sm.chainParams = &noCp
				sm.settings.ChainCfgParams = &noCp
			}

			require.Equal(t, tt.want, sm.legacyOutpointOnly(tt.height),
				"legacyOutpointOnly(%d) enabled=%v sql=%v", tt.height, tt.enabled, tt.sqlStore)
		})
	}
}

// buildExtendedSubtreeBlock builds a 1-coinbase + n-regular-tx block, populates
// each regular tx's inputs with extended parent data, and returns the txMap +
// order ready for createSubtrees. Mirrors TestSyncManager_createSubtrees_*.
func buildExtendedSubtreeBlock(t *testing.T, height int32, n int) (*bsvutil.Block, *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper], []chainhash.Hash) {
	t.Helper()

	msgBlock := &wire.MsgBlock{
		Header: wire.BlockHeader{Version: 1, Timestamp: time.Now(), Bits: 0x1d00ffff},
	}

	coinbase := wire.NewMsgTx(1)
	coinbase.AddTxIn(&wire.TxIn{
		PreviousOutPoint: wire.OutPoint{Hash: chainhash.Hash{}, Index: 0xffffffff},
		SignatureScript:  []byte{0x00},
		Sequence:         0xffffffff,
	})
	coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100000000, PkScript: []byte{0x76, 0xa9, 0x14}})
	msgBlock.Transactions = append(msgBlock.Transactions, coinbase)

	parentHash := chainhash.Hash{0x01}
	for i := 0; i < n; i++ {
		reg := wire.NewMsgTx(1)
		reg.AddTxIn(&wire.TxIn{
			PreviousOutPoint: wire.OutPoint{Hash: parentHash, Index: uint32(i)},
			SignatureScript:  []byte{0x00, byte(i)},
			Sequence:         0xffffffff,
		})
		reg.AddTxOut(&wire.TxOut{Value: 1000 + int64(i), PkScript: []byte{0x76, 0xa9, 0x14, byte(i)}})
		msgBlock.Transactions = append(msgBlock.Transactions, reg)
	}

	block := bsvutil.NewBlock(msgBlock)
	block.SetHeight(height)

	sm := &SyncManager{logger: ulogger.TestLogger{}}
	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))
	txOrder, err := sm.createTxMap(context.Background(), block, txMap)
	require.NoError(t, err)

	// Populate extended parent data so the OFF path's calculateTransactionFee succeeds
	// (input 5000 > output ~1000, positive fee).
	for _, wrapper := range txMap.Range() {
		for _, in := range wrapper.Tx.Inputs {
			in.PreviousTxSatoshis = 5_000
			in.PreviousTxScript = &bscript.Script{0x76, 0xa9, 0x14}
		}
	}

	return block, txMap, txOrder
}

func makeSubtreeSlices(t *testing.T, txCount int) ([]*subtreepkg.Subtree, []*subtreepkg.Data, []*subtreepkg.Meta) {
	t.Helper()

	// maxItems must be a power of two; 4 mirrors TestSyncManager_createSubtrees_*.
	subtreeSize, numSubtrees, finalLeafCount, err := partitionLegacyBlock(txCount, 4)
	require.NoError(t, err)

	slices := make([]*subtreepkg.Subtree, numSubtrees)
	datas := make([]*subtreepkg.Data, numSubtrees)
	metas := make([]*subtreepkg.Meta, numSubtrees)

	for i := 0; i < numSubtrees; i++ {
		capacity := subtreeSize
		if i == numSubtrees-1 && finalLeafCount < subtreeSize {
			capacity = finalLeafCount
		}

		st, terr := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
		require.NoError(t, terr)

		if i == 0 {
			require.NoError(t, st.AddCoinbaseNode())
		}

		slices[i] = st
		datas[i] = subtreepkg.NewSubtreeData(st)
		metas[i] = subtreepkg.NewSubtreeMeta(st)
	}

	return slices, datas, metas
}

// TestSyncManager_createSubtrees_OutpointOnlyZeroFees is the paired ON/OFF fee test:
// with the gate ON every subtree node fee is 0 (calculateTransactionFee skipped);
// with the gate OFF the real fee (input-output) is computed as before.
func TestSyncManager_createSubtrees_OutpointOnlyZeroFees(t *testing.T) {
	initPrometheusMetrics()

	const checkpointHeight = int32(1000)
	const blockHeight = int32(500) // below checkpoint

	t.Run("gate ON => fees are 0", func(t *testing.T) {
		tSettings, params := newOutpointOnlySettings(t, true, true, checkpointHeight)
		block, txMap, txOrder := buildExtendedSubtreeBlock(t, blockHeight, 5)

		sm := &SyncManager{settings: tSettings, chainParams: params, logger: ulogger.TestLogger{},
			utxoStore: &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}}
		require.True(t, sm.legacyOutpointOnly(uint32(blockHeight)), "gate must be ON for this case")

		slices, datas, metas := makeSubtreeSlices(t, len(block.Transactions()))
		require.NoError(t, sm.createSubtrees(context.Background(), testBlockIdent(block), txOrder, txMap, slices, datas, metas, true))

		for _, st := range slices {
			for _, node := range st.Nodes {
				require.Equal(t, uint64(0), node.Fee, "outpoint-only path must stamp fee 0")
			}
		}
	})

	t.Run("gate OFF => real fees computed", func(t *testing.T) {
		// flag off keeps the gate closed
		tSettings, params := newOutpointOnlySettings(t, false, true, checkpointHeight)
		block, txMap, txOrder := buildExtendedSubtreeBlock(t, blockHeight, 5)

		sm := &SyncManager{settings: tSettings, chainParams: params, logger: ulogger.TestLogger{}}
		require.False(t, sm.legacyOutpointOnly(uint32(blockHeight)), "gate must be OFF for this case")

		slices, datas, metas := makeSubtreeSlices(t, len(block.Transactions()))
		require.NoError(t, sm.createSubtrees(context.Background(), testBlockIdent(block), txOrder, txMap, slices, datas, metas, false))

		var nonZeroFees int
		for _, st := range slices {
			for _, node := range st.Nodes {
				if node.Fee > 0 {
					nonZeroFees++
				}
			}
		}
		require.Positive(t, nonZeroFees, "default-off path must compute real (non-zero) fees")
	})
}

// TestSyncManager_extendTransactions_OutpointOnlySkipsDecorate is the paired ON/OFF
// decorate test: with the gate ON BatchPreviousOutputsDecorate is never called;
// with the gate OFF it is called exactly once (the bulk Phase-2 decorate).
func TestSyncManager_extendTransactions_OutpointOnlySkipsDecorate(t *testing.T) {
	initPrometheusMetrics()

	const checkpointHeight = int32(1000)
	const blockHeight = int32(500)

	run := func(t *testing.T, enabled bool) int32 {
		tSettings, params := newOutpointOnlySettings(t, enabled, true, checkpointHeight)

		spy := &outpointOnlySpyStore{NullStore: &nullstore.NullStore{}}

		sm := &SyncManager{
			settings:    tSettings,
			chainParams: params,
			utxoStore:   spy,
			logger:      ulogger.TestLogger{},
		}

		block, txMap, txOrder := buildExtendedSubtreeBlock(t, blockHeight, 5)
		require.Equal(t, enabled, sm.legacyOutpointOnly(uint32(blockHeight)))

		err := sm.extendTransactions(context.Background(), testBlockIdent(block), txOrder, txMap, enabled)
		require.NoError(t, err)

		return spy.decorateCalls.Load()
	}

	t.Run("gate ON => decorate skipped", func(t *testing.T) {
		require.Equal(t, int32(0), run(t, true), "outpoint-only path must NOT call BatchPreviousOutputsDecorate")
	})

	t.Run("gate OFF => decorate called", func(t *testing.T) {
		require.Equal(t, int32(1), run(t, false), "default-off path must call BatchPreviousOutputsDecorate once")
	})
}
