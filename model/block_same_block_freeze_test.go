package model

import (
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/require"
)

// TestCheckParentExistsOnChain_SameBlockParentFreeze pins the block-level freeze check
// for a parent that is itself in the block (issue #1422). Subtree validation blesses a
// known parent/child pair on existence without re-spending the child, and the chain
// check skips same-block parents, so this is the only place an alert-system freeze on
// such a parent's output is judged.
func TestCheckParentExistsOnChain_SameBlockParentFreeze(t *testing.T) {
	ctx := context.Background()
	store := createTestUTXOStore(t)

	const (
		windowStart = 500
		windowStop  = 600
	)

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	parentTx := newTx(4242)
	parentTx.Inputs[0].PreviousTxSatoshis = 10_000 // the store computes fees on create
	parentTx.Inputs[0].PreviousTxScript = script
	parentTx.Outputs = []*bt.Output{
		{Satoshis: 1000, LockingScript: script},
		{Satoshis: 1000, LockingScript: script},
	}
	parentHash := *parentTx.TxIDChainHash()

	seedParent(t, store, parentTx, 1)

	frozenHash, err := util.UTXOHashFromOutput(&parentHash, parentTx.Outputs[1], 1)
	require.NoError(t, err)

	require.NoError(t, store.FreezeUTXOs(ctx, []*utxo.Spend{{
		TxID: &parentHash, Vout: 1, UTXOHash: frozenHash, FreezeFrom: windowStart, FreezeUntil: windowStop,
	}}, settings.NewSettings()))

	childHash, _ := chainhash.NewHashFromStr("0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206")
	unknownHash, _ := chainhash.NewHashFromStr("000000006a625f06636b8bb6ac7b960a8d03705d1ace08b1a19da3fdcc99ddbd")

	check := func(t *testing.T, height uint32, parent chainhash.Hash, vouts []uint32) error {
		t.Helper()

		b := &Block{Height: height}

		_, err := b.checkParentExistsOnChain(ctx, ulogger.TestLogger{}, store, missingParentTx{
			parentTxHash: parent,
			txHash:       *childHash,
			vouts:        vouts,
			sameBlock:    true,
		}, map[uint32]struct{}{})

		return err
	}

	t.Run("spending the frozen output inside the window is block-invalid", func(t *testing.T) {
		err := check(t, windowStart, parentHash, []uint32{0, 1})
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrBlockInvalid)
		require.ErrorIs(t, err, errors.ErrTxInvalid)
		require.ErrorIs(t, err, errors.ErrUtxoConsensusFrozen)
	})

	t.Run("spending only the unfrozen output is fine", func(t *testing.T) {
		require.NoError(t, check(t, windowStart, parentHash, []uint32{0}))
	})

	t.Run("outside the window the frozen output may be spent", func(t *testing.T) {
		require.NoError(t, check(t, windowStart-1, parentHash, []uint32{1}))
		require.NoError(t, check(t, windowStop, parentHash, []uint32{1}))
	})

	t.Run("a same-block parent the store does not hold is passed over", func(t *testing.T) {
		require.NoError(t, check(t, windowStart, *unknownHash, []uint32{1}))
	})

	t.Run("the same parent as an out-of-block edge is still chain-checked", func(t *testing.T) {
		// Sanity: the flag is what selects the freeze-only path. Without it an unknown
		// parent is the ordinary "not in the store" incomplete verdict.
		b := &Block{Height: windowStart}

		_, err := b.checkParentExistsOnChain(ctx, ulogger.TestLogger{}, store, missingParentTx{
			parentTxHash: *unknownHash,
			txHash:       *childHash,
			vouts:        []uint32{1},
		}, map[uint32]struct{}{})
		require.Error(t, err)
		require.ErrorIs(t, err, errors.ErrBlockIncomplete)
	})
}

// countingGetStore counts the Gets that reach the wrapped store, per hash.
type countingGetStore struct {
	utxo.Store

	mu   sync.Mutex
	gets map[chainhash.Hash]int
}

func (s *countingGetStore) Get(ctx context.Context, hash *chainhash.Hash, f ...fields.FieldName) (*meta.Data, error) {
	s.mu.Lock()
	s.gets[*hash]++
	s.mu.Unlock()

	return s.Store.Get(ctx, hash, f...)
}

// freezeFixture stores a parent with two outputs, the second frozen for [500, 600).
func freezeFixture(t *testing.T) (utxo.Store, chainhash.Hash) {
	t.Helper()

	store := createTestUTXOStore(t)

	script, err := bscript.NewP2PKHFromAddress("1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa")
	require.NoError(t, err)

	parentTx := newTx(4343)
	parentTx.Inputs[0].PreviousTxSatoshis = 10_000
	parentTx.Inputs[0].PreviousTxScript = script
	parentTx.Outputs = []*bt.Output{
		{Satoshis: 1000, LockingScript: script},
		{Satoshis: 1000, LockingScript: script},
	}
	parentHash := *parentTx.TxIDChainHash()

	seedParent(t, store, parentTx, 1)

	frozenHash, err := util.UTXOHashFromOutput(&parentHash, parentTx.Outputs[1], 1)
	require.NoError(t, err)

	require.NoError(t, store.FreezeUTXOs(context.Background(), []*utxo.Spend{{
		TxID: &parentHash, Vout: 1, UTXOHash: frozenHash, FreezeFrom: 500, FreezeUntil: 600,
	}}, settings.NewSettings()))

	return store, parentHash
}

// A same-block parent is read once however many children of the block spend it, and the
// verdict still names the child that spends the frozen output.
func TestCheckParentsExistOnChain_SameBlockParentReadOnce(t *testing.T) {
	store, parentHash := freezeFixture(t)
	counting := &countingGetStore{Store: store, gets: map[chainhash.Hash]int{}}

	childA := chainhash.HashH([]byte("child-a"))
	childB := chainhash.HashH([]byte("child-b"))

	b := &Block{Height: 550}
	deps := &validationDependencies{txMetaStore: counting, oldBlockIDsMap: txmap.NewSyncedMap[chainhash.Hash, []uint32]()}
	validationCtx := &validationContext{currentBlockHeaderIDsMap: map[uint32]struct{}{}}

	edges := []missingParentTx{
		{parentTxHash: parentHash, txHash: childA, vouts: []uint32{0}, sameBlock: true},
		{parentTxHash: parentHash, txHash: childB, vouts: []uint32{1}, sameBlock: true},
	}

	err := b.checkParentsExistOnChain(context.Background(), ulogger.TestLogger{}, deps, validationCtx, edges)
	require.ErrorIs(t, err, errors.ErrUtxoConsensusFrozen)
	require.ErrorContains(t, err, childB.String())
	require.Equal(t, 1, counting.gets[parentHash], "one read for both edges")

	counting.gets = map[chainhash.Hash]int{}

	require.NoError(t, b.checkParentsExistOnChain(context.Background(), ulogger.TestLogger{}, deps, validationCtx, edges[:1]))
	require.Equal(t, 1, counting.gets[parentHash])
}

// The out-of-block call site judges the parent's freeze records on the read it already
// makes for the chain check (getParentTxMetaBlockIDs).
func TestCheckParentExistsOnChain_OutOfBlockParentFreeze(t *testing.T) {
	store, parentHash := freezeFixture(t)
	child := chainhash.HashH([]byte("child"))

	check := func(height uint32, vouts []uint32) error {
		b := &Block{Height: height}

		_, err := b.checkParentExistsOnChain(context.Background(), ulogger.TestLogger{}, store, missingParentTx{
			parentTxHash: parentHash,
			txHash:       child,
			vouts:        vouts,
		}, map[uint32]struct{}{1: {}})

		return err
	}

	err := check(550, []uint32{1})
	require.ErrorIs(t, err, errors.ErrBlockInvalid)
	require.ErrorIs(t, err, errors.ErrUtxoConsensusFrozen)

	require.NoError(t, check(550, []uint32{0}), "the unfrozen output may be spent")
	require.NoError(t, check(600, []uint32{1}), "the window has closed")
	require.NoError(t, check(550, nil), "no spent outputs, nothing to judge")
}

// At or below the hardcoded checkpoint the block is canonical, so validateTransaction hands
// on no spent outputs: no same-block parent is read and no out-of-block parent's records are
// judged.
func TestValidateTransaction_CheckpointSkipsFreezeCheck(t *testing.T) {
	vouts := [][]uint32{{0}}
	block, meta, txs, parent := inpointsBufferBlock(t, vouts)

	validationCtx := &validationContext{subtreeBase: []uint64{1, 2}}

	for _, skip := range []bool{false, true} {
		params := &transactionValidationParams{
			subtreeMetaSlice: meta,
			subtreeHash:      &parent,
			subtreeNode:      subtreepkg.Node{Hash: txs[0]},
			run:              &recordingRun{},
		}

		missing, err := block.validateTransaction(context.Background(), &validationDependencies{skipFreezeCheck: skip}, validationCtx, params)
		require.NoError(t, err)

		if skip {
			require.Empty(t, missing)
		} else {
			require.Len(t, missing, 1)
			require.True(t, missing[0].sameBlock)
		}
	}
}
