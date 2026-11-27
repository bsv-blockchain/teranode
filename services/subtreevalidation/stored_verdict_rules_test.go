package subtreevalidation

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/txrules"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
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

		// A competing block at the tip's height holds txs validated for the tip's next block,
		// above it, under the same rules: only coinbase maturity can differ, and is checked.
		t.Run(name+": a tx validated above the block spending a mature coinbase is taken", func(t *testing.T) {
			f := newConflictingFixture(t)

			tx := opTrueSpend(t, f, []byte{bscript.Op1}, 0, 0xffffffff)
			storedAt(t, f, tx, 102)

			before := rechecks()

			require.NoError(t, check(t, f, tx), "the coinbase, mined at 50, is mature at 101")
			require.False(t, f.recorder.validated(*tx.TxIDChainHash()), "the check writes nothing")
			require.Equal(t, before, rechecks(), "no full recheck")
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
		{"validated above the block, same rules", meta.Data{UnminedSince: 950}, 900, storedMaturity},
		{"validated above the block, locked to a height", meta.Data{UnminedSince: 950, LockTime: 880}, 900, storedAtHeight},
		{"validated above an activation, block below", meta.Data{BlockHeights: []uint32{200}}, 199, storedFull},
		{"backdated by a reorg", meta.Data{UnminedSince: 856}, 900, storedFull},
		{"no height at all", meta.Data{}, 900, storedFull},
		{"locked to a height", meta.Data{UnminedSince: 900, LockTime: 899}, 900, storedTake},
		{"locked to a time", meta.Data{UnminedSince: 900, LockTime: 500_000_000}, 900, storedAtHeight},
		{"conflicting", meta.Data{UnminedSince: 900, Conflicting: true}, 900, storedAtHeight},
		{"conflicting, under other rules", meta.Data{BlockHeights: []uint32{99}, Conflicting: true}, 101, storedFull},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			era := txrules.EraOf(tSettings.ChainCfgParams, tt.blockHeight)
			require.Equal(t, tt.want, server.storedCheckFor(&tt.data, era, tip))
		})
	}

	// A tip of 0, before the UTXO store knows its height, cannot tell a backdated
	// unmined_since from a real one.
	t.Run("unknown tip", func(t *testing.T) {
		era := txrules.EraOf(tSettings.ChainCfgParams, 900)
		require.Equal(t, storedFull, server.storedCheckFor(&meta.Data{UnminedSince: 900}, era, func() uint32 { return 0 }))
	})
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

	err := server.processTransactionsInLevels(t.Context(), []*bt.Tx{f.winner.Clone(), f.other.Clone()}, chainhash.Hash{}, chainhash.Hash{}, 101, 0, 0, map[uint32]bool{}, false)
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

// tipStore is a UTXO store at a fixed block height; nothing else is called on it.
type tipStore struct {
	utxostore.Store
	tip uint32
}

func (s *tipStore) GetBlockHeight() uint32 { return s.tip }

// TestCachedVerdictsNeedReading pins when the cached-subtree path reads every tx's record
// instead of taking the subtree's stored verdicts: within a retention window of the block's
// rules taking effect or ending, when the rules change between the block and the tip's next
// block, and always when the UTXO store keeps everything (retention 0). Regtest: Genesis at
// 100, Chronicle at 200, CSV at 576, BIP66 at 1251.
func TestCachedVerdictsNeedReading(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = 50
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0
	require.Equal(t, uint32(50), tSettings.GetUtxoStoreBlockHeightRetention())

	tests := []struct {
		name             string
		blockHeight, tip uint32
		want             bool
	}{
		{"just above Genesis", 101, 100, true},
		{"last block of the window after Genesis", 149, 148, true},
		{"just below Chronicle, within a window of it", 160, 159, true},
		{"side block just below CSV, tip above", 500, 600, true},
		{"between activations, past both windows", 700, 699, false},
		{"competing block at the tip's height, past both windows", 900, 900, false},
		{"far past every activation", 5000, 4999, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := &Server{settings: tSettings, utxoStore: &tipStore{tip: tt.tip}}
			require.Equal(t, tt.want, server.cachedVerdictsNeedReading(tt.blockHeight))
		})
	}
}

// TestCachedVerdictsNeedReading_NoRetention pins that a store keeping every record (retention
// 0) has no window: old verdicts never age out, so the cached path always reads.
func TestCachedVerdictsNeedReading_NoRetention(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.GlobalBlockHeightRetention = 0
	tSettings.UtxoStore.BlockHeightRetentionAdjustment = 0

	server := &Server{settings: tSettings, utxoStore: &tipStore{tip: 4999}}
	require.True(t, server.cachedVerdictsNeedReading(5000))
}

// TestBlockPathRejectsAStoredTxAlreadyMinedOnItsChain pins that a block including a stored tx
// already mined on the candidate's own chain is rejected before the tx is checked again: a
// check whose parent was pruned would otherwise give no verdict, and the block would be
// retried instead of rejected. It is the check blessMissingTransaction runs on a tx it
// validates.
func TestBlockPathRejectsAStoredTxAlreadyMinedOnItsChain(t *testing.T) {
	const minedBlockID = 77

	minedAndPruned := func(t *testing.T) *conflictingFixture {
		f := newConflictingFixture(t)

		_, err := f.store.SetMinedMulti(t.Context(), []*chainhash.Hash{f.winner.TxIDChainHash()},
			utxostore.MinedBlockInfo{BlockID: minedBlockID, BlockHeight: 101, SubtreeIdx: 0, OnLongestChain: true})
		require.NoError(t, err)
		require.NoError(t, f.store.Delete(t.Context(), f.parent.TxIDChainHash()))

		return f
	}

	onChain := map[uint32]bool{minedBlockID: true}

	t.Run("level path", func(t *testing.T) {
		f := minedAndPruned(t)

		err := f.server.processTransactionsInLevels(t.Context(), []*bt.Tx{f.winner.Clone()}, chainhash.Hash{}, chainhash.Hash{}, 102, 0, 0, onChain, false)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
		require.ErrorContains(t, err, "already mined on our chain")
	})

	t.Run("subtree from a block", func(t *testing.T) {
		f := minedAndPruned(t)
		server, _ := f.newServer(t)

		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
		require.NoError(t, err)
		require.NoError(t, subtree.AddNode(*f.winner.TxIDChainHash(), 0, 0))

		_, err = server.ValidateSubtreeInternal(t.Context(), ValidateSubtree{
			SubtreeHash: *subtree.RootHash(),
			BaseURL:     "http://peer.invalid",
			TxHashes:    []chainhash.Hash{*f.winner.TxIDChainHash()},
			FromBlock:   true,
		}, 102, onChain)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
		require.ErrorContains(t, err, "already mined on our chain")
	})

	t.Run("mined on another chain is not a duplicate", func(t *testing.T) {
		f := newConflictingFixture(t)

		_, err := f.store.SetMinedMulti(t.Context(), []*chainhash.Hash{f.winner.TxIDChainHash()},
			utxostore.MinedBlockInfo{BlockID: minedBlockID, BlockHeight: 101, SubtreeIdx: 0, OnLongestChain: false})
		require.NoError(t, err)

		err = f.server.processTransactionsInLevels(t.Context(), []*bt.Tx{f.winner.Clone()}, chainhash.Hash{}, chainhash.Hash{}, 102, 0, 0, map[uint32]bool{1: true}, false)
		require.NoError(t, err)
	})
}

// recordStore answers BatchDecorate from fixed records, by the fields asked for: a record
// listed in missing is not found. Nothing else is called on it.
type recordStore struct {
	utxostore.Store
	records map[chainhash.Hash]*meta.Data
	missing map[chainhash.Hash]bool
}

func (s *recordStore) BatchDecorate(_ context.Context, items []*utxostore.UnresolvedMetaData, readFields ...fields.FieldName) error {
	for _, item := range items {
		record, ok := s.records[item.Hash]
		if !ok || s.missing[item.Hash] {
			item.Err = errors.NewTxNotFoundError("transaction %s not found", item.Hash.String())

			continue
		}

		data := &meta.Data{}

		for _, field := range readFields {
			switch field {
			case fields.Tx:
				data.Tx = record.Tx
			case fields.TxInpoints:
				data.TxInpoints = record.TxInpoints
			case fields.IsCoinbase:
				data.IsCoinbase = record.IsCoinbase
			case fields.BlockHeights:
				data.BlockHeights = record.BlockHeights
			}
		}

		item.Data = data
	}

	return nil
}

// TestCheckCoinbaseMaturity pins the maturity check a verdict reached under the same rules
// above the block gets: a coinbase spend immature at the block is TX_INVALID, a record that
// lists no parents is read from its body, and a record missing for one candidate does not
// hide another candidate's immature spend. The test maturity is 1.
func TestCheckCoinbaseMaturity(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	require.EqualValues(t, 1, tSettings.ChainCfgParams.CoinbaseMaturity)

	coinbase := chainhash.HashH([]byte("coinbase mined at 101"))
	plainParent := chainhash.HashH([]byte("non-coinbase parent"))
	prunedParent := chainhash.HashH([]byte("pruned parent"))

	spending := func(parent chainhash.Hash) *bt.Tx {
		tx := bt.NewTx()
		input := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff, UnlockingScript: bscript.NewFromBytes([]byte{bscript.Op1})}
		require.NoError(t, input.PreviousTxIDAdd(&parent))
		tx.Inputs = append(tx.Inputs, input)
		tx.AddOutput(&bt.Output{Satoshis: 1, LockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE})})

		return tx
	}

	immatureSpend := spending(coinbase)
	plainSpend := spending(plainParent)
	prunedSpend := spending(prunedParent)

	newServer := func(records map[chainhash.Hash]*meta.Data) *Server {
		return &Server{logger: ulogger.TestLogger{}, settings: tSettings, utxoStore: &recordStore{records: records}}
	}

	parents := map[chainhash.Hash]*meta.Data{
		coinbase:    {IsCoinbase: true, BlockHeights: []uint32{101}},
		plainParent: {BlockHeights: []uint32{50}},
	}

	t.Run("immature at the block, mature above it", func(t *testing.T) {
		server := newServer(parents)
		candidates := []maturityCandidate{{hash: *immatureSpend.TxIDChainHash(), tx: immatureSpend}}

		err := server.checkCoinbaseMaturity(t.Context(), candidates, 101)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
		require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "%v", err)

		require.NoError(t, server.checkCoinbaseMaturity(t.Context(), candidates, 102))
	})

	t.Run("a record without inpoints is read from its body", func(t *testing.T) {
		records := map[chainhash.Hash]*meta.Data{*immatureSpend.TxIDChainHash(): {Tx: immatureSpend}}
		for hash, data := range parents {
			records[hash] = data
		}

		err := newServer(records).checkCoinbaseMaturity(t.Context(), []maturityCandidate{{hash: *immatureSpend.TxIDChainHash()}}, 101)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
	})

	t.Run("a record with neither inpoints nor a body is no verdict", func(t *testing.T) {
		records := map[chainhash.Hash]*meta.Data{*immatureSpend.TxIDChainHash(): {}}

		err := newServer(records).checkCoinbaseMaturity(t.Context(), []maturityCandidate{{hash: *immatureSpend.TxIDChainHash()}}, 101)
		require.Error(t, err)
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
	})

	t.Run("a missing parent of one candidate does not hide another's immature spend", func(t *testing.T) {
		candidates := []maturityCandidate{
			{hash: *prunedSpend.TxIDChainHash(), tx: prunedSpend},
			{hash: *plainSpend.TxIDChainHash(), tx: plainSpend},
			{hash: *immatureSpend.TxIDChainHash(), tx: immatureSpend},
		}

		err := newServer(parents).checkCoinbaseMaturity(t.Context(), candidates, 101)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
	})

	t.Run("a missing parent alone is no verdict", func(t *testing.T) {
		candidates := []maturityCandidate{{hash: *prunedSpend.TxIDChainHash(), tx: prunedSpend}, {hash: *plainSpend.TxIDChainHash(), tx: plainSpend}}

		err := newServer(parents).checkCoinbaseMaturity(t.Context(), candidates, 101)
		require.True(t, errors.Is(err, errors.ErrTxNotFound), "%v", err)
		require.False(t, errors.Is(err, errors.ErrTxInvalid), "%v", err)
	})
}
