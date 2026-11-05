//go:build bvgaps

package subtreevalidation

import (
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/stretchr/testify/require"
)

// gap047SpendableOutput mines a fresh coinbase at height 3 (mature from 4 with the test maturity
// of 1) into the fixture's store and returns it, so a tx spending it conflicts with nothing.
func gap047SpendableOutput(t *testing.T, f *conflictingFixture) *bt.Tx {
	t.Helper()

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(3, "/gap bv047 spendable/"),
		transactions.WithP2PKHOutputs(1, 100_000, f.privateKey.PubKey()),
	)
	_, err := f.store.Create(t.Context(), coinbase, 3, utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: 3, BlockHeight: 3, SubtreeIdx: 0}))
	require.NoError(t, err)

	return coinbase
}

// gap047LockedTx spends parent's output 0 with the given nLockTime and a non-final input
// sequence, so the lock time is enforced (util.IsTransactionFinal).
func gap047LockedTx(t *testing.T, f *conflictingFixture, parent *bt.Tx, lockTime uint32) *bt.Tx {
	t.Helper()

	tx := transactions.Create(t,
		transactions.WithPrivateKey(f.privateKey),
		transactions.WithInput(parent, 0, f.privateKey),
		transactions.WithP2PKHOutputs(1, 75_000, f.privateKey.PubKey()),
	)

	tx.LockTime = lockTime
	tx.Inputs[0].SequenceNumber = 0
	require.NoError(t, tx.FillAllInputs(t.Context(), &unlocker.Getter{PrivateKey: f.privateKey}))

	return tx
}

// gap047Store validates tx on the mempool path at validationHeight, as a node does while its tip
// is at validationHeight-1, and requires the stored record to be NOT conflicting: the case
// GAP-BV-047 is about. With createConflicting it instead requires a conflicting record.
func gap047Store(t *testing.T, f *conflictingFixture, tx *bt.Tx, validationHeight uint32, createConflicting bool) {
	t.Helper()

	if createConflicting {
		_, err := f.recorder.Interface.Validate(t.Context(), tx.Clone(), validationHeight, validator.WithCreateConflicting(true))
		require.True(t, errors.Is(err, errors.ErrTxConflicting), "precondition: a conflicting record: %v", err)
	} else {
		_, err := f.recorder.Interface.Validate(t.Context(), tx.Clone(), validationHeight)
		require.NoError(t, err, "precondition: the tx is valid in the context it is first validated in")
	}

	stored := &meta.Data{}
	require.NoError(t, f.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
	require.Equal(t, createConflicting, stored.Conflicting, "precondition: the stored record's conflicting flag")
}

// gap047EntryPoint runs a block at blockHeight, whose parent is genesis and whose header time is
// now, that holds tx and the fixture's unrelated tx, through one block-path entry point.
type gap047EntryPoint func(t *testing.T, f *conflictingFixture, tx *bt.Tx, blockHeight uint32) error

func gap047Header(f *conflictingFixture) *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  f.settings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
	}
}

// gap047ViaLevels is processTransactionsInLevels with the candidate times CheckBlockSubtrees
// passes below CSVHeight: the header's timestamp.
func gap047ViaLevels(t *testing.T, f *conflictingFixture, tx *bt.Tx, blockHeight uint32) error {
	return f.server.processTransactionsInLevels(t.Context(), []*bt.Tx{tx.Clone(), f.other.Clone()}, chainhash.Hash{}, chainhash.Hash{},
		blockHeight, gap047Header(f).Timestamp, 0, map[uint32]bool{}, false)
}

// gap047ViaBatched is processTransactionsBatched, the catch-up path, with the same candidate times.
func gap047ViaBatched(t *testing.T, f *conflictingFixture, tx *bt.Tx, blockHeight uint32) error {
	checker, ok := f.recorder.Interface.(validator.BlockBatchChecker)
	require.True(t, ok)

	return f.server.processTransactionsBatched(t.Context(), checker, []*bt.Tx{tx.Clone(), f.other.Clone()}, chainhash.Hash{},
		blockHeight, gap047Header(f).Timestamp, 0, map[uint32]bool{})
}

// gap047ViaCheckBlockSubtrees is the entry block validation calls for a block whose subtree this
// node does not hold yet.
func gap047ViaCheckBlockSubtrees(t *testing.T, f *conflictingFixture, tx *bt.Tx, blockHeight uint32) error {
	f.settings.AdaptiveFetch.BootstrapMode = "pessimistic"
	server, subtreeStore := f.newServer(t)
	server.adaptiveFetch.Arm()

	root := servePeerSubtree(t, subtreeStore, tx, f.other)

	block, err := model.NewBlock(gap047Header(f), &bt.Tx{Version: 1}, []*chainhash.Hash{&root}, 3, 1000, blockHeight, 0)
	require.NoError(t, err)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.CheckBlockSubtrees(t.Context(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes, BaseUrl: "http://peer.invalid"})
	if err == nil {
		require.True(t, resp.Blessed)
	}

	return err
}

// gap047ViaSubtreeFromBlock is checkSubtreeFromBlock (ValidateSubtreeInternal for a block's
// subtree), optionally with the record already in the txmeta cache.
func gap047ViaSubtreeFromBlock(cached bool) gap047EntryPoint {
	return func(t *testing.T, f *conflictingFixture, tx *bt.Tx, blockHeight uint32) error {
		server, subtreeStore := f.newServer(t)

		if cached {
			cache, ok := server.utxoStore.(*txmetacache.TxMetaCache)
			require.True(t, ok)

			stored := &meta.Data{}
			require.NoError(t, f.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
			require.NoError(t, cache.SetCache(tx.TxIDChainHash(), stored))

			_, found := cache.GetMetaCached(t.Context(), *tx.TxIDChainHash())
			require.True(t, found, "precondition: the block path finds the record in the cache")
		}

		root := servePeerSubtree(t, subtreeStore, tx, f.other)
		blockHash := chainhash.HashH([]byte("gap bv047 block"))

		_, err := server.checkSubtreeFromBlock(t.Context(), &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			Hash:              root[:],
			BaseUrl:           "http://peer.invalid",
			BlockHeight:       blockHeight,
			BlockHash:         blockHash[:],
			PreviousBlockHash: f.settings.ChainCfgParams.GenesisHash[:],
		})

		return err
	}
}

// TestGapBV047_StoredNonFinalTxRejectedInSiblingBlock covers GAP-BV-047 (BV-BODY-020,
// AC-BV-BODY-020.1).
//
// Spec: finality MUST be evaluated against the candidate block's own height, also for a tx
// validated earlier in another context; a prior validation MUST NOT be reused across a change of
// height. Given T with nLockTime = H validated while the tip was at H (so checked for block H+1,
// where it is final), a competing block at height H that includes T MUST be rejected as containing
// a non-final tx.
//
// Today: the block path finds T in the txmeta cache / UTXO store as a non-conflicting record and
// takes it as validated. The at-height re-check (validator.CheckStoredTxAtHeight) runs only on
// records flagged Conflicting: Server.checkStoredConflictingAtHeight in stored_conflicting.go skips
// every other hit, and processTransactionsInLevels / processTransactionsBatched in
// check_block_subtrees.go(_batched.go) mark the hit validated without it. The block is accepted.
//
// Controls on the same harness, passing today: the same shape of tx flagged Conflicting IS
// re-checked and rejected at H (the harness reaches the re-check with the block's height and
// candidate times), and the non-conflicting T is accepted in a block at H+1, where it is final.
//
// checkSubtreeFromBlock is not exercised: below CSVHeight it passes no candidate block time, so it
// skips finality for every tx, conflicting or not (the "no candidate block time" clause of
// GAP-BV-047, services/subtreevalidation/Server.go checkSubtreeFromBlock).
//
// Layer: this proof asserts the subtree-validation verdict (ErrTxInvalid with ErrNonFinal), where
// the gap sits. The block-level outcome is derived from this TX_INVALID under BV-BODY-016 and is
// not asserted here.
func TestGapBV047_StoredNonFinalTxRejectedInSiblingBlock(t *testing.T) {
	// H: T is validated at H+1 (tip at H), a sibling block sits at H
	const lockHeight = uint32(101)

	entryPoints := map[string]gap047EntryPoint{
		"processTransactionsInLevels": gap047ViaLevels,
		"processTransactionsBatched":  gap047ViaBatched,
		"CheckBlockSubtrees":          gap047ViaCheckBlockSubtrees,
	}

	for name, check := range entryPoints {
		t.Run(name+": control: a conflicting record of a non-final tx is re-checked at the block's height", func(t *testing.T) {
			f := newConflictingFixture(t)
			// double-spends the fixture's parent output, which the node's winner already spends
			tx := gap047LockedTx(t, f, f.parent, lockHeight)
			gap047Store(t, f, tx, lockHeight+1, true)

			err := check(t, f, tx, lockHeight)
			require.True(t, errors.Is(err, errors.ErrTxInvalid), "control: the conflicting record is non-final at %d: %v", lockHeight, err)
			require.True(t, errors.Is(err, errors.ErrNonFinal), "control: rejected as non-final: %v", err)
		})

		t.Run(name+": control: the stored tx is accepted at a height where it is final", func(t *testing.T) {
			f := newConflictingFixture(t)
			tx := gap047LockedTx(t, f, gap047SpendableOutput(t, f), lockHeight)
			gap047Store(t, f, tx, lockHeight+1, false)

			require.NoError(t, check(t, f, tx, lockHeight+1))
		})

		t.Run(name+": AC-BV-BODY-020.1: a sibling block at the lock height is rejected", func(t *testing.T) {
			f := newConflictingFixture(t)
			tx := gap047LockedTx(t, f, gap047SpendableOutput(t, f), lockHeight)
			gap047Store(t, f, tx, lockHeight+1, false)

			err := check(t, f, tx, lockHeight)
			require.True(t, errors.Is(err, errors.ErrTxInvalid),
				"BV-BODY-020: tx %s (nLockTime %d) was validated for height %d; a block at %d includes it and must be rejected as non-final, got: %v",
				tx.TxIDChainHash().String(), lockHeight, lockHeight+1, lockHeight, err)
			require.True(t, errors.Is(err, errors.ErrNonFinal), "AC-BV-BODY-020.1: rejected as containing a non-final tx: %v", err)
		})
	}
}

// TestGapBV047_StoredTxSpendingImmatureCoinbaseRejectedAtLowerForkHeight covers GAP-BV-047
// (BV-BODY-020, BV-BODY-015 coinbase maturity).
//
// Spec: coinbase maturity MUST be evaluated against the candidate block's own height, also for a
// tx validated earlier at another tip. A tx validated at 102, spending a coinbase mined at 101
// (mature from 102), MUST be rejected in a fork block at 101, where that coinbase is immature.
//
// Today: the non-conflicting stored record is taken as validated by every block entry point
// (Server.checkStoredConflictingAtHeight in stored_conflicting.go re-checks only conflicting
// records; processTransactionsInLevels / processTransactionsBatched in check_block_subtrees.go and
// ValidateSubtreeInternal's FromBlock lookups in SubtreeValidation.go do the same), so the block is
// accepted.
//
// Control on the same harness, passing today: the same spend stored as a conflicting record
// (storedConflictingAtLaterHeight) IS re-checked and rejected at 101 on every entry point.
func TestGapBV047_StoredTxSpendingImmatureCoinbaseRejectedAtLowerForkHeight(t *testing.T) {
	const forkHeight = uint32(101)

	entryPoints := map[string]gap047EntryPoint{
		"processTransactionsInLevels":         gap047ViaLevels,
		"processTransactionsBatched":          gap047ViaBatched,
		"CheckBlockSubtrees":                  gap047ViaCheckBlockSubtrees,
		"checkSubtreeFromBlock":               gap047ViaSubtreeFromBlock(false),
		"checkSubtreeFromBlock cached record": gap047ViaSubtreeFromBlock(true),
	}

	for name, check := range entryPoints {
		t.Run(name+": control: a conflicting record spending the coinbase is re-checked at the fork height", func(t *testing.T) {
			f := newConflictingFixture(t)
			tx := storedConflictingAtLaterHeight(t, f)

			err := check(t, f, tx, forkHeight)
			require.True(t, errors.Is(err, errors.ErrTxInvalid), "control: the coinbase is immature at %d: %v", forkHeight, err)
			require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "control: rejected as an immature coinbase spend: %v", err)
		})

		t.Run(name+": a non-conflicting record spending the coinbase is rejected at the fork height", func(t *testing.T) {
			f := newConflictingFixture(t)

			coinbase := transactions.Create(t,
				transactions.WithCoinbaseData(forkHeight, "/gap bv047 maturing coinbase/"),
				transactions.WithP2PKHOutputs(1, 100_000, f.privateKey.PubKey()),
			)
			_, err := f.store.Create(t.Context(), coinbase, forkHeight,
				utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: forkHeight, BlockHeight: forkHeight, SubtreeIdx: 0}))
			require.NoError(t, err)

			tx := transactions.Create(t,
				transactions.WithPrivateKey(f.privateKey),
				transactions.WithInput(coinbase, 0, f.privateKey),
				transactions.WithP2PKHOutputs(1, 90_000, f.privateKey.PubKey()),
			)
			gap047Store(t, f, tx, forkHeight+1, false)

			err = check(t, f, tx, forkHeight)
			require.True(t, errors.Is(err, errors.ErrTxInvalid),
				"BV-BODY-020: tx %s spends a coinbase mined at %d (mature from %d); it was validated at %d and a fork block at %d must be rejected, got: %v",
				tx.TxIDChainHash().String(), forkHeight, forkHeight+1, forkHeight+1, forkHeight, err)
			require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "BV-BODY-020: rejected as an immature coinbase spend: %v", err)
		})
	}
}

// TestGapBV047_CachedSubtreeReevaluatedAgainstLowerParentMTP covers GAP-BV-047 (BV-BODY-020).
//
// Spec: time-based finality MUST be evaluated against the candidate block's own parent
// median-time-past, also for a tx in a subtree this node validated earlier; a cached subtree's
// verdict MUST NOT be reused across a change of that context. A tx with a time nLockTime final
// against the tip's MTP (now) is not final in a block above CSVHeight whose parent is genesis, whose
// MTP is the genesis timestamp; that block MUST be rejected.
//
// Today: checkCachedSubtreesAgainstCandidateChain (check_block_subtrees.go) returns early when the
// store flags no tx of the cached subtrees Conflicting, and re-checks at the block's height and
// candidate MTP only the conflicting ones; its doc comment assumes the rest "does not change
// between blocks". The non-conflicting tx is never re-evaluated and the block is accepted.
//
// Control on the same harness, passing today: the same time-locked spend stored as a conflicting
// record in the cached subtree IS re-checked against the genesis parent MTP and rejected.
//
// The candidate's height (above CSVHeight) and its genesis parent are not a coherent chain
// position: the height is chosen only so the time basis is the parent's MTP, and
// checkCachedSubtreesAgainstCandidateChain reads nothing of the block but its height and parent.
func TestGapBV047_CachedSubtreeReevaluatedAgainstLowerParentMTP(t *testing.T) {
	check := func(t *testing.T, f *conflictingFixture, tx *bt.Tx) error {
		server, subtreeStore := f.newServer(t)

		subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
		require.NoError(t, err)
		require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, 0))
		require.NoError(t, subtree.AddNode(*f.other.TxIDChainHash(), 0, 0))

		subtreeBytes, err := subtree.Serialize()
		require.NoError(t, err)

		root := *subtree.RootHash()
		require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtree, subtreeBytes))

		// above CSVHeight, so finality is measured against the parent's (genesis') MTP
		blockHeight := uint32(f.settings.ChainCfgParams.CSVHeight) + 24 //nolint:gosec

		block, err := model.NewBlock(gap047Header(f), &bt.Tx{Version: 1}, []*chainhash.Hash{&root}, 3, 1000, blockHeight, 0)
		require.NoError(t, err)

		return server.checkCachedSubtreesAgainstCandidateChain(t.Context(), block, []chainhash.Hash{root})
	}

	// final against the store's MTP (now), not against the genesis parent's MTP
	lockTime := uint32(time.Now().Add(-time.Hour).Unix()) //nolint:gosec

	t.Run("control: a conflicting time-locked record in a cached subtree is re-checked against the parent MTP", func(t *testing.T) {
		f := newConflictingFixture(t)
		require.Greater(t, int64(lockTime), f.settings.ChainCfgParams.GenesisBlock.Header.Timestamp.Unix(), "precondition")

		tx := gap047LockedTx(t, f, f.parent, lockTime)
		gap047Store(t, f, tx, 101, true)

		err := check(t, f, tx)
		require.True(t, errors.Is(err, errors.ErrTxInvalid), "control: non-final against the genesis MTP: %v", err)
		require.True(t, errors.Is(err, errors.ErrNonFinal), "control: rejected as non-final: %v", err)
	})

	t.Run("a non-conflicting time-locked tx in a cached subtree is re-evaluated against the parent MTP", func(t *testing.T) {
		f := newConflictingFixture(t)

		tx := gap047LockedTx(t, f, gap047SpendableOutput(t, f), lockTime)
		gap047Store(t, f, tx, 101, false)

		err := check(t, f, tx)
		require.True(t, errors.Is(err, errors.ErrTxInvalid),
			"BV-BODY-020: tx %s (nLockTime %d) was final against the tip's MTP; a block whose parent MTP is the genesis timestamp reuses its cached subtree and must be rejected as non-final, got: %v",
			tx.TxIDChainHash().String(), lockTime, err)
		require.True(t, errors.Is(err, errors.ErrNonFinal), "BV-BODY-020: rejected as non-final: %v", err)
	})
}
