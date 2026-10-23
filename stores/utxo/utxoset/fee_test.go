package utxoset

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/stretchr/testify/require"
)

// extendedChild builds a transaction spending parent's output vout with a 1,000-satoshi fee,
// extended (its input carries the parent output's value and script), without storing it.
func extendedChild(t *testing.T, parent *bt.Tx, vout uint32) *bt.Tx {
	t.Helper()

	child := bt.NewTx()
	require.NoError(t, child.FromUTXOs(&bt.UTXO{
		TxIDHash:      parent.TxIDChainHash(),
		Vout:          vout,
		LockingScript: parent.Outputs[vout].LockingScript,
		Satoshis:      parent.Outputs[vout].Satoshis,
	}))
	child.AddOutput(&bt.Output{
		Satoshis:      parent.Outputs[vout].Satoshis - 1_000,
		LockingScript: parent.Outputs[vout].LockingScript,
	})

	return child
}

// TestCreateStoresTheFee pins the fee this store used to drop. It was written NULL on every
// create and read back as 0, and the 0 is not harmless: the validator hands the create's fee to
// block assembly, and subtree validation recomputes subtree fees from stored metadata before the
// block reward check (model.Block.checkBlockRewardAndFees), so above the highest checkpoint an
// honest block whose coinbase claims its fees would fail as claiming more than subsidy plus 0.
//
// Every route a transaction can be created by is covered: waiting to be mined, mined at or below
// the checkpoint (the containment-table claim), and mined above it (the identity claim with a
// block). Each must report the fee from Create and from a later read.
func TestCreateStoresTheFee(t *testing.T) {
	cases := []struct {
		name  string
		store func(*testing.T) (*Store, context.Context)
		opts  func(height uint32) []utxo.CreateOption
	}{
		{"waiting to be mined", newTestStore, func(uint32) []utxo.CreateOption { return nil }},
		{"mined at or below the checkpoint", newTestStore, func(h uint32) []utxo.CreateOption {
			return []utxo.CreateOption{utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 9, BlockHeight: h, OnLongestChain: true})}
		}},
		{"mined above the checkpoint", newUncheckpointedStore, func(h uint32) []utxo.CreateOption {
			return []utxo.CreateOption{utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: 9, BlockHeight: h, OnLongestChain: true})}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, ctx := tc.store(t)

			parent := mkTx(t, 1, 5_000)
			_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
				utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
			require.NoError(t, err)

			child := extendedChild(t, parent, 0)

			md, err := s.Create(ctx, child, 1_010, tc.opts(1_010)...)
			require.NoError(t, err)
			require.Equal(t, uint64(1_000), md.Fee, "Create reports the fee")

			got, err := s.Get(ctx, child.TxIDChainHash(), fields.Fee)
			require.NoError(t, err)
			require.Equal(t, uint64(1_000), got.Fee, "and it is stored")
		})
	}
}

// TestCreateOfAnUnextendedTransactionStoresNoFee pins the other half: without input values the
// fee cannot be known, which is the below-checkpoint fast path, so it is stored as unknown and
// never as a guessed number.
func TestCreateOfAnUnextendedTransactionStoresNoFee(t *testing.T) {
	s, ctx := newTestStore(t)

	parent := mkTx(t, 1, 5_000)
	_, err := s.Create(ctx, parent, 1_000, utxo.WithMinedBlockInfo(
		utxo.MinedBlockInfo{BlockID: 1, BlockHeight: 1_000, OnLongestChain: true}))
	require.NoError(t, err)

	child := extendedChild(t, parent, 0)
	child.Inputs[0].PreviousTxSatoshis = 0
	child.Inputs[0].PreviousTxScript = nil
	require.False(t, child.IsExtended())

	md, err := s.Create(ctx, child, 1_010)
	require.NoError(t, err)
	require.Zero(t, md.Fee)

	var fee *int64
	h := child.TxIDChainHash()
	require.NoError(t, s.pool.QueryRow(ctx, `SELECT fee FROM tx_ident WHERE leaf = $1 AND txid = $2`, LeafFor(h[:]), h[:]).Scan(&fee))
	require.Nil(t, fee, "an unknown fee is stored as unknown")
}

func TestTxFee(t *testing.T) {
	parent := mkTx(t, 1, 5_000)

	require.Equal(t, int64(1_000), *txFee(extendedChild(t, parent, 0)))

	unextended := extendedChild(t, parent, 0)
	unextended.Inputs[0].PreviousTxSatoshis = 0
	unextended.Inputs[0].PreviousTxScript = nil
	require.Nil(t, txFee(unextended))
}
