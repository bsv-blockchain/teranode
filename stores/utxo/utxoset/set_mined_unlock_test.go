package utxoset

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// A transaction still locked when a block records it, because the two-phase-commit release
// never ran (the validator crashed between delivery and unlock, or the create came from a path
// that never unlocks), is released by the mine, as both reference stores do: the sql store sets
// locked = false in setMined and the aerospike UDF clears the bin. Before this, the mine cleared
// only the unmined marker, so the transaction left unmined recovery, which is what would have
// unlocked it, and every child spending it kept getting TX_LOCKED.
func TestMiningReleasesTheLockSoChildrenCanSpend(t *testing.T) {
	for _, onLongestChain := range []bool{true, false} {
		name := "longest chain"
		if !onLongestChain {
			name = "fork block"
		}

		t.Run(name, func(t *testing.T) {
			s, ctx := newUncheckpointedStore(t)

			const height = 1000

			parent := mkTx(t, 2, 5_000)
			_, err := s.Create(ctx, parent, height, utxo.WithLocked(true))
			require.NoError(t, err)

			// Locked as created: the spend path refuses a child.
			early := payFrom(t, 1, []*bt.Tx{parent}, []uint32{1})
			_, _, err = s.SpendAndCreate(ctx, early, height)
			require.ErrorIs(t, err, errors.ErrTxLocked)

			_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash()},
				utxo.MinedBlockInfo{BlockID: 7, BlockHeight: height + 1, SubtreeIdx: 0, OnLongestChain: onLongestChain})
			require.NoError(t, err)

			child := payFrom(t, 2, []*bt.Tx{parent}, []uint32{0})
			_, _, err = s.SpendAndCreate(ctx, child, height+2)
			require.NoError(t, err, "a child of a mined transaction was refused")

			got, err := s.Get(ctx, parent.TxIDChainHash(), fields.Locked)
			require.NoError(t, err)
			require.False(t, got.Locked, "a mined transaction still reports itself locked")
		})
	}
}

// Below the checkpoint quick validation creates a block's transactions locked and with their
// block, so they have no identity row, and a retry of the block reaches the existing ones
// through SetMinedMulti. The lock lives on the containment row and the UTXOs only, and the mine
// releases it there too, as the aerospike UDF does for a record left locked by quick validation.
func TestMiningReleasesTheLockOfABlockPathCreateWithNoIdentityRow(t *testing.T) {
	s, ctx := newTestStore(t)

	const height = 700_100

	info := utxo.MinedBlockInfo{BlockID: 42, BlockHeight: height, OnLongestChain: true}

	parent := mkTx(t, 2, 5_000)
	_, err := s.Create(ctx, parent, height, utxo.WithMinedBlockInfo(info), utxo.WithLocked(true))
	require.NoError(t, err)
	require.False(t, identExists(t, s, ctx, parent), "a block-path create below the checkpoint has no identity row")

	_, err = s.SetMinedMulti(ctx, []*chainhash.Hash{parent.TxIDChainHash()}, info)
	require.NoError(t, err)

	child := payFrom(t, 2, []*bt.Tx{parent}, []uint32{0})
	_, _, err = s.SpendAndCreate(ctx, child, height+1)
	require.NoError(t, err, "a child of a mined transaction was refused")

	got, err := s.Get(ctx, parent.TxIDChainHash(), fields.Locked)
	require.NoError(t, err)
	require.False(t, got.Locked, "a mined transaction still reports itself locked")
}
