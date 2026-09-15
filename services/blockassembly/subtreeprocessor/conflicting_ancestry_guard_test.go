package subtreeprocessor

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blob_memory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Conflict resolution demotes a losing transaction: marks it conflicting,
// unspends it, and hands its outpoints to the winner. That is correct when the
// loser was mined on a branch being abandoned — the ordinary reorg case, where
// the loser's block is a SIBLING of the block being applied.
//
// It is never correct when the loser is confirmed in the applying block's own
// ancestry: that reverses a confirmed spend with no reorg, leaving the loser
// sitting in a block on the chain while the UTXO set says someone else spent the
// outpoint.
//
// These tests pin both directions. The refusal case is the security fix; the
// sibling case is the regression risk, because a guard that misfires there would
// break every legitimate double-spend reorg.

type ancestryGuardHarness struct {
	stp             *SubtreeProcessor
	mockUtxoStore   *utxo.MockUtxostore
	mockBlockchain  *blockchain.Mock
	block           *model.Block
	winner          chainhash.Hash
	loser           chainhash.Hash
	loserBlockIDs   []uint32
	ancestryAnswer  bool
	ancestryQueried bool
}

func newAncestryGuardHarness(t *testing.T, loserBlockIDs []uint32, loserIsOnAncestry bool) *ancestryGuardHarness {
	t.Helper()

	h := &ancestryGuardHarness{
		mockUtxoStore:  new(utxo.MockUtxostore),
		mockBlockchain: new(blockchain.Mock),
		winner:         chainhash.HashH([]byte("winner-tx")),
		loser:          chainhash.HashH([]byte("loser-tx")),
		loserBlockIDs:  loserBlockIDs,
		ancestryAnswer: loserIsOnAncestry,
	}

	settings := test.CreateBaseTestSettings(t)
	settings.BlockAssembly.InitialMerkleItemsPerSubtree = 4

	ctx := context.Background()

	stp, err := NewSubtreeProcessor(
		ctx,
		ulogger.TestLogger{},
		settings,
		blob_memory.New(),
		h.mockBlockchain,
		h.mockUtxoStore,
		make(chan NewSubtreeRequest, 10),
	)
	require.NoError(t, err)

	stp.Start(ctx)

	h.stp = stp

	h.block = &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  &chainhash.Hash{},
			HashMerkleRoot: &chainhash.Hash{},
			Timestamp:      1234567890,
			Bits:           model.NBit{},
			Nonce:          99,
		},
		Height:   10,
		Subtrees: []*chainhash.Hash{},
	}

	h.mockBlockchain.On("GetBlockIsMined", mock.Anything, mock.Anything).Return(true, nil)
	h.mockBlockchain.On("GetBlockHeader", mock.Anything, mock.Anything).
		Return(&model.BlockHeader{}, &model.BlockHeaderMeta{Height: 10}, nil)

	// The winner is conflicting and its counter-spender is the loser.
	h.mockUtxoStore.On("Get", mock.Anything, mock.Anything, mock.Anything).
		Return(&meta.Data{Conflicting: true}, nil)
	h.mockUtxoStore.On("GetCounterConflicting", mock.Anything, mock.Anything).
		Return([]chainhash.Hash{h.loser}, nil)

	// Supply the loser's block IDs, which is what the guard is asked about.
	h.mockUtxoStore.On("BatchDecorate", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			for _, u := range args.Get(1).([]*utxo.UnresolvedMetaData) {
				u.Data = &meta.Data{BlockIDs: loserBlockIDs}
			}
		}).Return(nil)

	h.mockBlockchain.On("CheckBlockIsAncestorOfBlock", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			h.ancestryQueried = true
		}).Return(loserIsOnAncestry, nil)

	// Only reached when a demotion is actually allowed to proceed.
	h.mockUtxoStore.On("SetConflicting", mock.Anything, mock.Anything, mock.Anything).
		Return([]*utxo.Spend{}, []chainhash.Hash{}, nil)
	h.mockUtxoStore.On("Unspend", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	h.mockUtxoStore.On("SpendAndCreate", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil, []*utxo.Spend{}, nil)
	h.mockUtxoStore.On("SetLocked", mock.Anything, mock.Anything, mock.Anything).Return(nil)

	return h
}

// TestAncestryGuardRefusesDemotionOfAncestryConfirmedTx is the security
// regression: a block whose conflict resolution would demote a transaction
// confirmed on that block's own ancestry must not have that demotion applied,
// and the block must be queued for invalidation.
func TestAncestryGuardRefusesDemotionOfAncestryConfirmedTx(t *testing.T) {
	h := newAncestryGuardHarness(t, []uint32{42}, true)

	losing, _, err := h.stp.processConflictingTransactions(
		context.Background(), h.block, []chainhash.Hash{h.winner}, map[chainhash.Hash]struct{}{})

	require.NoError(t, err, "a refused demotion must not fail block movement — that would wedge block assembly")
	require.True(t, h.ancestryQueried, "the guard must consult the chain before demoting")

	require.False(t, losing.Exists(h.loser),
		"the ancestry-confirmed transaction must not be reported as a loser")

	// No UTXO mutation may have happened for the refused loser.
	h.mockUtxoStore.AssertNotCalled(t, "SetConflicting", mock.Anything, mock.Anything, mock.Anything)
	h.mockUtxoStore.AssertNotCalled(t, "Unspend", mock.Anything, mock.Anything, mock.Anything)

	pending := h.stp.DrainPendingInvalidations()
	require.Len(t, pending, 1, "the offending block must be queued for invalidation")
	require.True(t, pending[0].IsEqual(h.block.Hash()))

	// Draining is destructive, so a second call reports nothing outstanding.
	require.Empty(t, h.stp.DrainPendingInvalidations())
}

// TestAncestryGuardAllowsDemotionOfSiblingForkTx is the reorg-safety case. In a
// legitimate reorg the losing transaction was mined on the branch being
// abandoned, whose blocks are siblings of — never ancestors of — the block being
// applied. The guard must stay out of the way, or every double-spend reorg
// breaks.
func TestAncestryGuardAllowsDemotionOfSiblingForkTx(t *testing.T) {
	h := newAncestryGuardHarness(t, []uint32{7}, false)

	losing, _, err := h.stp.processConflictingTransactions(
		context.Background(), h.block, []chainhash.Hash{h.winner}, map[chainhash.Hash]struct{}{})

	require.NoError(t, err)
	require.True(t, h.ancestryQueried)

	require.True(t, losing.Exists(h.loser),
		"a loser mined on a sibling fork must still be demoted")

	require.Empty(t, h.stp.DrainPendingInvalidations(),
		"a legitimate reorg must not queue any block for invalidation")
}

// TestAncestryGuardFailsOpenOnUnresolvableLoser pins the direction of failure.
// A loser whose record cannot be resolved is not evidence of an ancestor double
// spend, and refusing on missing data would stall legitimate reorgs whenever a
// record is pruned or a backend hiccups.
func TestAncestryGuardFailsOpenOnUnresolvableLoser(t *testing.T) {
	h := newAncestryGuardHarness(t, nil, true)

	losing, _, err := h.stp.processConflictingTransactions(
		context.Background(), h.block, []chainhash.Hash{h.winner}, map[chainhash.Hash]struct{}{})

	require.NoError(t, err)
	require.False(t, h.ancestryQueried,
		"with no block IDs to test there is nothing to ask the chain about")
	require.True(t, losing.Exists(h.loser), "an unresolvable loser must not be refused")
	require.Empty(t, h.stp.DrainPendingInvalidations())
}
