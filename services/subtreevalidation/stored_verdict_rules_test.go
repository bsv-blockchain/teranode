package subtreevalidation

import (
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// opTrueSpend returns a tx spending the OP_TRUE output of a coinbase mined at 50, with the
// given unlocking script, lock time and sequence number. It has no signature, so any lock
// time can be set.
func opTrueSpend(t *testing.T, f *conflictingFixture, unlocking []byte, lockTime, sequence uint32) *bt.Tx {
	t.Helper()

	opTrue := bscript.NewFromBytes([]byte{bscript.OpTRUE})

	parent := transactions.Create(t,
		transactions.WithCoinbaseData(50, "/stored verdict rules parent/"),
		transactions.WithOutput(100_000, opTrue),
	)
	_, err := f.store.Create(t.Context(), parent, 50, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 50, BlockHeight: 50, SubtreeIdx: 0}))
	require.NoError(t, err)

	tx := bt.NewTx()
	tx.LockTime = lockTime
	input := &bt.Input{
		PreviousTxOutIndex: 0,
		SequenceNumber:     sequence,
		UnlockingScript:    bscript.NewFromBytes(unlocking),
	}
	require.NoError(t, input.PreviousTxIDAdd(parent.TxIDChainHash()))
	tx.Inputs = append(tx.Inputs, input)
	tx.AddOutput(&bt.Output{Satoshis: 90_000, LockingScript: opTrue})

	return tx
}

// storedAt validates tx at height in block context, straight to the validator, past the
// recorder: the block path has not seen the tx.
func storedAt(t *testing.T, f *conflictingFixture, tx *bt.Tx, height uint32) {
	t.Helper()

	_, err := f.recorder.Interface.ValidateWithOptions(t.Context(), tx.Clone(), height,
		validator.ProcessOptions(validator.WithSkipPolicyChecks(true), validator.WithAddTXToBlockAssembly(false)))
	require.NoError(t, err)

	stored := &meta.Data{}
	require.NoError(t, f.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
	require.Equal(t, height, stored.UnminedSince, "precondition: the record names the height it was validated at")
}

// storedBelowGenesis returns a tx the node validated at 99, below regtest's Genesis at 100,
// whose unlocking script, OP_1 OP_1 OP_ADD, is not push-only: valid at 99, invalid at the
// height of the fixture's blocks, 101.
func storedBelowGenesis(t *testing.T, f *conflictingFixture) *bt.Tx {
	t.Helper()

	require.Equal(t, uint32(100), f.settings.ChainCfgParams.GenesisActivationHeight)

	tx := opTrueSpend(t, f, []byte{bscript.Op1, bscript.Op1, bscript.OpADD}, 0, 0xffffffff)
	storedAt(t, f, tx, 99)

	return tx
}

// TestBlockPathRechecksStoredVerdictUnderOtherRules pins that a block takes a stored tx's
// verdict only where it holds at the block's own height. The UTXO store keeps one verdict per
// txid, recording no script flags. A tx validated below an activation height, Genesis here,
// was accepted above it on that stored verdict alone, while a node that had not seen it
// validated it at the block's height and rejected the block: the same block, opposite
// verdicts. bitcoin-sv clears its script cache and mempool when the tip crosses Genesis or
// Chronicle (FinalizeEraCrossing) for the same reason.
func TestBlockPathRechecksStoredVerdictUnderOtherRules(t *testing.T) {
	entryPoints := blockEntryPoints()
	entryPoints["cached subtree"] = viaCachedSubtree

	rechecks := func() float64 {
		return testutil.ToFloat64(prometheusSubtreeValidationStoredVerdictRechecks)
	}

	for name, check := range entryPoints {
		t.Run(name+": a tx validated below Genesis is checked again above it", func(t *testing.T) {
			f := newConflictingFixture(t)
			tx := storedBelowGenesis(t, f)

			err := check(t, f, tx)
			require.True(t, errors.Is(err, errors.ErrTxInvalid), "the unlocking script is not push-only at the block's height: %v", err)
			require.False(t, f.recorder.validated(*tx.TxIDChainHash()), "the check writes nothing")
		})

		t.Run(name+": a verdict under the block's rules is taken", func(t *testing.T) {
			f := newConflictingFixture(t)

			stored := &meta.Data{}
			require.NoError(t, f.store.GetMeta(t.Context(), f.winner.TxIDChainHash(), stored))
			require.Equal(t, uint32(101), stored.UnminedSince, "precondition: validated at the block's height")

			before := rechecks()

			require.NoError(t, check(t, f, f.winner))
			require.False(t, f.recorder.validated(*f.winner.TxIDChainHash()))
			require.Equal(t, before, rechecks(), "a verdict that holds is not checked again")
		})

		// A reorg or an invalidation backdates unmined_since (utxo.BackdatedUnminedSince), which
		// then names no height the tx was validated at.
		t.Run(name+": a tx moved back by a reorg is checked again", func(t *testing.T) {
			f := newConflictingFixture(t)

			_, err := f.store.RawDB().ExecContext(t.Context(), "UPDATE transactions SET unmined_since = 1 WHERE hash = $1", f.winner.TxIDChainHash()[:])
			require.NoError(t, err)

			before := rechecks()

			require.NoError(t, check(t, f, f.winner), "still valid at the block's height")
			require.False(t, f.recorder.validated(*f.winner.TxIDChainHash()), "the check writes nothing")
			require.Greater(t, rechecks(), before, "checked again")
		})

		// A lock time that is a height is final at any height above one it was final at
		// (TestStoredCheckFor covers a time).
		t.Run(name+": a tx locked to a height takes its verdict", func(t *testing.T) {
			f := newConflictingFixture(t)

			tx := opTrueSpend(t, f, []byte{bscript.Op1}, 50, 0)
			storedAt(t, f, tx, 101)

			before := rechecks()

			require.NoError(t, check(t, f, tx))
			require.Equal(t, before, rechecks(), "not checked again")
		})

		// The parent, the fixture's winner, is unmined, so the check reads it at the
		// unconfirmed-parent sentinel. A tx checked again gets the verdict the entry point gives
		// a tx it has not seen: the block paths resolve the sentinel to the block's height, the
		// non-legacy checkSubtreeFromBlock does not.
		t.Run(name+": a tx with an unmined parent checked again gets a new tx's verdict", func(t *testing.T) {
			childOfWinner := func(f *conflictingFixture) *bt.Tx {
				return transactions.Create(t,
					transactions.WithPrivateKey(f.privateKey),
					transactions.WithInput(f.winner, 0, f.privateKey),
					transactions.WithP2PKHOutputs(1, 50_000, f.privateKey.PubKey()),
				)
			}

			// the cached-record entry point needs the tx stored; a new tx takes the same path
			// without the cache
			freshCheck := check
			if name == "checkSubtreeFromBlock cached record" {
				freshCheck = entryPoints["checkSubtreeFromBlock http"]
			}

			f := newConflictingFixture(t)
			fresh := freshCheck(t, f, childOfWinner(f))

			f = newConflictingFixture(t)
			child := childOfWinner(f)

			_, err := f.recorder.Interface.Validate(t.Context(), child.Clone(), 101)
			require.NoError(t, err)

			_, err = f.store.RawDB().ExecContext(t.Context(), "UPDATE transactions SET unmined_since = 1 WHERE hash = $1", child.TxIDChainHash()[:])
			require.NoError(t, err)

			before := rechecks()
			again := check(t, f, child)

			require.Greater(t, rechecks(), before, "checked again")
			require.Equal(t, fresh == nil, again == nil, "new tx: %v, checked again: %v", fresh, again)

			if !strings.HasPrefix(name, "checkSubtreeFromBlock http") && name != "checkSubtreeFromBlock cached record" {
				require.NoError(t, again, "the block paths resolve an unmined parent to the block's height")
			}
		})
	}
}

// TestStoredCheckFor pins what a block does with a stored verdict, from the record alone.
// Regtest: Genesis at 100, Chronicle at 200; the tip is 1000 and the unmined retention 144, so an unmined_since
// at or below 856 is backdated.
func TestStoredCheckFor(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.UtxoStore.UnminedTxRetention = 144

	server := &Server{settings: tSettings}
	tip := func() uint32 { return 1000 }

	tests := []struct {
		name        string
		data        meta.Data
		blockHeight uint32
		want        storedCheck
	}{
		{"validated at the block's height", meta.Data{UnminedSince: 900}, 900, storedTake},
		{"validated below the block, same rules", meta.Data{UnminedSince: 857}, 900, storedTake},
		{"mined below the block, same rules", meta.Data{BlockHeights: []uint32{870}}, 900, storedTake},
		{"validated below Genesis", meta.Data{BlockHeights: []uint32{99}}, 101, storedFull},
		{"mined below Chronicle, block above", meta.Data{BlockHeights: []uint32{150}}, 900, storedFull},
		{"validated above the block", meta.Data{UnminedSince: 950}, 900, storedFull},
		{"backdated by a reorg", meta.Data{UnminedSince: 856}, 900, storedFull},
		{"no height at all", meta.Data{}, 900, storedFull},
		{"locked to a height", meta.Data{UnminedSince: 900, LockTime: 899}, 900, storedTake},
		{"locked to a time", meta.Data{UnminedSince: 900, LockTime: 500_000_000}, 900, storedAtHeight},
		{"conflicting", meta.Data{UnminedSince: 900, Conflicting: true}, 900, storedAtHeight},
		{"conflicting, under other rules", meta.Data{BlockHeights: []uint32{99}, Conflicting: true}, 101, storedFull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, server.storedCheckFor(&tt.data, tt.blockHeight, tip))
		})
	}
}

// TestBlockPathValidatesACachedTxWhoseRecordIsGone pins that a tx the txmeta cache holds but
// the UTXO store no longer does is validated as a new tx, as on a node without the cache
// entry. Failing on the missing record instead would retry the block for as long as the
// cache entry lives.
func TestBlockPathValidatesACachedTxWhoseRecordIsGone(t *testing.T) {
	f := newConflictingFixture(t)
	server, _ := f.newServer(t)

	cache, ok := server.utxoStore.(*txmetacache.TxMetaCache)
	require.True(t, ok)

	stored := &meta.Data{}
	require.NoError(t, f.store.GetMeta(t.Context(), f.winner.TxIDChainHash(), stored))
	require.NoError(t, cache.SetCache(f.winner.TxIDChainHash(), stored))
	require.NoError(t, f.store.Delete(t.Context(), f.winner.TxIDChainHash()))

	err := server.processTransactionsInLevels(t.Context(), []*bt.Tx{f.winner.Clone(), f.other.Clone()}, chainhash.Hash{}, chainhash.Hash{}, 101, 0, 0, map[uint32]bool{}, false, nil)
	require.NoError(t, err)
	require.True(t, f.recorder.validated(*f.winner.TxIDChainHash()), "validated as a new tx")
}

// TestCachedSubtreeFullRecheckFailsWithoutAVerdictOnAPrunedParent pins that a block
// referencing a subtree this node already validated does not take a stored verdict it must
// check again when it cannot: the cached path has no tx body to validate from, so it fails
// without a verdict, as a retry, never as an invalid block.
func TestCachedSubtreeFullRecheckFailsWithoutAVerdictOnAPrunedParent(t *testing.T) {
	f := newConflictingFixture(t)
	tx := storedBelowGenesis(t, f)

	require.NoError(t, f.store.Delete(t.Context(), tx.Inputs[0].PreviousTxIDChainHash()))

	err := viaCachedSubtree(t, f, tx)
	require.Error(t, err)
	require.False(t, errors.Is(err, errors.ErrTxInvalid), "no verdict on the peer: %v", err)
}
