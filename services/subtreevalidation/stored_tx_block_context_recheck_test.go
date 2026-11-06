//go:build bvgaps

package subtreevalidation

import (
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-bt/v2/unlocker"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	utiltest "github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestKnownDefect_TxSeenBeforeIsNotRecheckedForFinalityInALowerBlock is the plain, step-by-step
// version of the GAP-BV-047 proof (bitcoin-sv/teranode#5000). Everything is set up inline; the only
// code called is production code.
//
// The rule (bitcoin-sv IsFinalTx): a tx with nLockTime = 101 and a non-final input sequence may go
// in a block above 101, never in block 101 itself.
//
// The story:
//   - The chain tip is at 100. A tx locked until 101 arrives. The node validates it for block 102,
//     where it is final, and stores it as valid.
//   - A competing block at 101 includes that tx. bitcoin-sv rejects the block (bad-txns-nonfinal).
//   - Teranode finds the tx already validated and does not check finality again, so it accepts it.
//
// The first subtest is the control: a tx the node has NOT seen before, with the same lock, is
// rejected by the very same block check. So the check works; it is skipped only for a tx the
// node validated earlier at another height.
func TestKnownDefect_TxSeenBeforeIsNotRecheckedForFinalityInALowerBlock(t *testing.T) {
	const (
		tipHeight      = uint32(100) // the node's chain tip
		lockHeight     = uint32(101) // the tx's nLockTime: final only in blocks ABOVE 101
		nextHeight     = uint32(102) // the height the node validates a new tx for
		competingBlock = uint32(101) // the sibling block that includes the tx
	)

	ctx := t.Context()
	now := uint32(time.Now().Unix()) //nolint:gosec

	// 1. A node: settings, a real UTXO store (in-memory SQLite) at height 100, a real validator.
	tSettings := utiltest.CreateBaseTestSettings(t)
	tSettings.BlockAssembly.Disabled = true

	storeURL, err := url.Parse("sqlitememory:///flat_finality_" + t.Name())
	require.NoError(t, err)

	utxoStore, err := sql.New(ctx, ulogger.TestLogger{}, tSettings, storeURL)
	require.NoError(t, err)
	require.NoError(t, utxoStore.SetBlockHeight(tipHeight))
	require.NoError(t, utxoStore.SetMedianBlockTime(now))

	txValidator, err := validator.New(ctx, ulogger.TestLogger{}, tSettings, utxoStore, nil, nil, nil, nil, nil)
	require.NoError(t, err)

	// The subtree validation server, the component that checks a block's transactions.
	server := &Server{
		logger:                ulogger.TestLogger{},
		settings:              tSettings,
		utxoStore:             utxoStore,
		validatorClient:       txValidator,
		policyRejectedTxCache: newTxPolicyRejectedCache(1024 * 1024),
	}

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)

	// newLockedTx stores an ordinary (non-coinbase) funding tx as mined in block 50, and returns a
	// tx spending it with nLockTime = 101 and input sequence 0, so the lock is enforced. seed makes
	// each funding tx, and so each locked tx, unique.
	newLockedTx := func(seed byte) *bt.Tx {
		const fundingBlock = uint32(50)

		// The funding tx's own input points at a made-up earlier output. WithSkipExtendedInputs
		// tells the store not to look that output up; only the funding tx's own output matters.
		funding := bt.NewTx()
		fundingInput := &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: bt.DefaultSequenceNumber, UnlockingScript: bscript.NewFromBytes([]byte{bscript.OpTRUE})}
		require.NoError(t, fundingInput.PreviousTxIDAdd(&chainhash.Hash{seed}))
		funding.Inputs = append(funding.Inputs, fundingInput)
		payToKey, err := bscript.NewP2PKHFromPubKeyBytes(privateKey.PubKey().Compressed())
		require.NoError(t, err)
		require.NoError(t, funding.PayTo(payToKey, 100_000))

		_, err = utxoStore.Create(ctx, funding, fundingBlock,
			utxostore.WithSkipExtendedInputs(true),
			utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: fundingBlock, BlockHeight: fundingBlock, SubtreeIdx: 0}))
		require.NoError(t, err)

		tx := transactions.Create(t,
			transactions.WithPrivateKey(privateKey),
			transactions.WithInput(funding, 0, privateKey),
			transactions.WithP2PKHOutputs(1, 75_000, privateKey.PubKey()),
		)
		tx.LockTime = lockHeight
		tx.Inputs[0].SequenceNumber = 0
		require.NoError(t, tx.FillAllInputs(ctx, &unlocker.Getter{PrivateKey: privateKey}))

		// The consensus rule itself, as bitcoin-sv applies it: not final at 101, final at 102.
		require.Error(t, util.IsTransactionFinal(tx, competingBlock, now), "rule: not final in block 101")
		require.NoError(t, util.IsTransactionFinal(tx, nextHeight, now), "rule: final in block 102")

		return tx
	}

	// checkBlock runs a block at blockHeight holding tx through the block path's transaction check
	// (the one CheckBlockSubtrees uses), and returns its verdict: nil means accepted.
	checkBlock := func(tx *bt.Tx, blockHeight uint32) error {
		return server.processTransactionsInLevels(ctx, []*bt.Tx{tx.Clone()}, chainhash.Hash{}, chainhash.Hash{},
			blockHeight, now, 0, map[uint32]bool{}, false)
	}

	t.Run("control: a tx the node has not seen is rejected in block 101", func(t *testing.T) {
		unseenTx := newLockedTx(1)

		err := checkBlock(unseenTx, competingBlock)

		require.True(t, errors.Is(err, errors.ErrTxInvalid), "want TX_INVALID, got: %v", err)
		require.True(t, errors.Is(err, errors.ErrNonFinal), "want NON_FINAL, got: %v", err)
	})

	t.Run("a tx the node validated for block 102 is rejected in block 101", func(t *testing.T) {
		seenTx := newLockedTx(2)

		// The tx arrives while the tip is at 100: the node validates it for block 102, where it
		// is final, and stores it as valid (not conflicting).
		_, err := txValidator.Validate(ctx, seenTx.Clone(), nextHeight)
		require.NoError(t, err)

		stored := &meta.Data{}
		require.NoError(t, utxoStore.GetMeta(ctx, seenTx.TxIDChainHash(), stored))
		require.False(t, stored.Conflicting, "stored as an ordinary valid tx")

		// A competing block at 101 includes it.
		err = checkBlock(seenTx, competingBlock)

		// It is not final at 101, so the block must be rejected, as bitcoin-sv does.
		require.True(t, errors.Is(err, errors.ErrTxInvalid),
			"tx %s is locked until %d and was validated for block %d; block %d includes it and must reject it as non-final, got: %v",
			seenTx.TxIDChainHash(), lockHeight, nextHeight, competingBlock, err)
		require.True(t, errors.Is(err, errors.ErrNonFinal), "want NON_FINAL, got: %v", err)
	})
}

// TestKnownDefect_SiblingBlockAtLockHeight_RejectsTxAcceptedForNextHeight proves that a tx the
// node already accepted for one height is not re-checked for finality when a sibling block at a
// lower height includes it.
//
// Spec: BV-BODY-020 and AC-BV-BODY-020.1 (tracked as GAP-BV-047).
//
// Scenario: Given a node whose UTXO store tip is at 100, and a tx locked until height 101
// (nLockTime 101, non-final input sequence) that the node accepted into its mempool for height
// 102, where it is final. When a sibling block at height 101 includes that tx. Then the block
// must be rejected as containing a non-final tx: TX_INVALID caused by NON_FINAL.
//
// Today: the block path finds the tx in the txmeta cache / UTXO store as a non-conflicting record
// and takes it as validated. The at-height re-check (validator.CheckStoredTxAtHeight) runs only on
// records flagged Conflicting: Server.checkStoredConflictingAtHeight in stored_conflicting.go skips
// every other hit, and processTransactionsInLevels / processTransactionsBatched in
// check_block_subtrees.go and check_block_subtrees_batched.go mark the hit validated without it.
// The block is accepted.
//
// Controls (on every entry point, passing today): the same shape of tx stored as a conflicting
// record IS re-checked and rejected at height 101, which proves the harness reaches the re-check
// with the block's height and candidate times; and the accepted tx is valid in a block at 102,
// where it is final.
//
// Not covered: checkSubtreeFromBlock. Below CSVHeight it passes no candidate block time, so it
// skips finality for every tx, conflicting or not (the "no candidate block time" clause of the
// defect, Server.checkSubtreeFromBlock in Server.go).
//
// Layer: this asserts the subtree-validation verdict (TX_INVALID caused by NON_FINAL), where the
// defect sits. The block-level outcome follows from that verdict under BV-BODY-016 and is not
// asserted here.
func TestKnownDefect_SiblingBlockAtLockHeight_RejectsTxAcceptedForNextHeight(t *testing.T) {
	const (
		tipHeight          = uint32(100) // the UTXO store height newConflictingFixture sets
		lockedUntilHeight  = uint32(101) // tx.nLockTime: final only in blocks above 101
		validatedForHeight = uint32(102) // the height the mempool validation is run for
		siblingBlockHeight = uint32(101) // the competing block that includes the tx
		finalBlockHeight   = uint32(102) // a block where the tx is final
	)

	entryPoints := []blockContextEntryPoint{
		viaProcessTransactionsInLevels,
		viaProcessTransactionsBatched,
		viaCheckBlockSubtrees,
	}

	for _, entryPoint := range entryPoints {
		t.Run("control: a conflicting record of a time-locked tx is rejected as non-final at the lock height, via "+entryPoint.name, func(t *testing.T) {
			// Given a tx locked until 101 that double-spends the output the node's winner already
			// spends, so it is stored as a conflicting record when validated for 102
			node := newRealSubtreeValidationNode(t)
			tx := node.txLockedUntilHeight(t, node.parent, lockedUntilHeight)
			node.storeAsConflictingRecord(t, tx, validatedForHeight)

			// When a block at 101 includes it
			err := entryPoint.validateBlockContaining(node, t, tx, siblingBlockHeight)

			// Then the conflicting record is re-checked at 101 and rejected
			requireBlockRejectsTxAsNonFinal(t, err,
				"a conflicting record locked until %d must be rejected as non-final in a block at %d",
				lockedUntilHeight, siblingBlockHeight)
		})

		t.Run("control: the accepted tx is valid in a block above its lock height, via "+entryPoint.name, func(t *testing.T) {
			// Given a tx locked until 101, accepted into the mempool for 102
			node := newRealSubtreeValidationNode(t)
			funding := node.mineSpendableCoinbase(t)
			tx := node.txLockedUntilHeight(t, funding, lockedUntilHeight)
			node.acceptIntoMempool(t, tx, validatedForHeight)

			// When a block at 102 includes it
			err := entryPoint.validateBlockContaining(node, t, tx, finalBlockHeight)

			// Then the block accepts it
			require.NoError(t, err)
		})

		t.Run("a sibling block at the lock height rejects the tx accepted for the next height, via "+entryPoint.name, func(t *testing.T) {
			// Given a tx locked until 101, accepted into the mempool for 102
			node := newRealSubtreeValidationNode(t)
			funding := node.mineSpendableCoinbase(t)
			tx := node.txLockedUntilHeight(t, funding, lockedUntilHeight)
			node.acceptIntoMempool(t, tx, validatedForHeight)

			// When a sibling block at 101 includes it
			err := entryPoint.validateBlockContaining(node, t, tx, siblingBlockHeight)

			// Then the block rejects it as non-final
			requireBlockRejectsTxAsNonFinal(t, err,
				"tx %s is locked until height %d and was accepted (store tip %d) for height %d; a sibling block at %d includes it and must reject it as non-final",
				tx.TxIDChainHash().String(), lockedUntilHeight, tipHeight, validatedForHeight, siblingBlockHeight)
		})
	}
}

// TestKnownDefect_ForkBlockAtCoinbaseHeight_RejectsAcceptedSpendOfImmatureCoinbase proves that a
// tx the node already accepted at one height is not re-checked for coinbase maturity when a fork
// block at a lower height includes it.
//
// Spec: BV-BODY-020 and BV-BODY-015 coinbase maturity (tracked as GAP-BV-047).
//
// Scenario: Given a coinbase mined at height 101 (spendable from 102 with the test maturity of 1)
// and a tx spending it that the node validated for height 102. When a fork block at height 101
// includes that tx. Then the block must be rejected: TX_INVALID caused by TX_COINBASE_IMMATURE.
//
// Today: every block entry point takes the non-conflicting stored record as validated.
// Server.checkStoredConflictingAtHeight in stored_conflicting.go re-checks only conflicting
// records; processTransactionsInLevels / processTransactionsBatched in check_block_subtrees.go and
// ValidateSubtreeInternal's FromBlock lookups in SubtreeValidation.go do the same. The block is
// accepted.
//
// Controls (on every entry point, passing today): the same spend stored as a conflicting record
// (storedConflictingAtLaterHeight) IS re-checked and rejected at 101.
func TestKnownDefect_ForkBlockAtCoinbaseHeight_RejectsAcceptedSpendOfImmatureCoinbase(t *testing.T) {
	const (
		coinbaseHeight     = uint32(101) // the coinbase is spendable from 102
		validatedForHeight = uint32(102) // the first height the coinbase is mature
		forkBlockHeight    = uint32(101) // the fork block that includes the spend
	)

	entryPoints := []blockContextEntryPoint{
		viaCheckSubtreeFromBlock,
		viaCheckSubtreeFromBlockWithCachedRecord,
		viaProcessTransactionsInLevels,
		viaProcessTransactionsBatched,
		viaCheckBlockSubtrees,
	}

	for _, entryPoint := range entryPoints {
		t.Run("control: a conflicting record spending the young coinbase is rejected as immature at the fork height, via "+entryPoint.name, func(t *testing.T) {
			// Given a conflicting record, created for 102, that spends a coinbase mined at 101
			node := newRealSubtreeValidationNode(t)
			tx := storedConflictingAtLaterHeight(t, node.conflictingFixture)

			// When a fork block at 101 includes it
			err := entryPoint.validateBlockContaining(node, t, tx, forkBlockHeight)

			// Then the record is re-checked at 101 and rejected
			requireBlockRejectsTxAsImmatureCoinbaseSpend(t, err,
				"a conflicting record spending a coinbase that is immature at %d must be rejected in a block at %d",
				forkBlockHeight, forkBlockHeight)
		})

		t.Run("a fork block at the coinbase's height rejects the accepted spend as immature, via "+entryPoint.name, func(t *testing.T) {
			// Given a coinbase mined at 101 and a spend of it the node validated for 102
			node := newRealSubtreeValidationNode(t)
			coinbase := node.mineCoinbase(t, coinbaseHeight, "/gap bv047 maturing coinbase/")
			tx := node.txSpendingCoinbase(t, coinbase)
			node.acceptIntoMempool(t, tx, validatedForHeight)

			// When a fork block at 101 includes it
			err := entryPoint.validateBlockContaining(node, t, tx, forkBlockHeight)

			// Then the block rejects it as an immature coinbase spend
			requireBlockRejectsTxAsImmatureCoinbaseSpend(t, err,
				"tx %s spends a coinbase mined at %d (mature from %d); it was validated for %d and a fork block at %d must reject it",
				tx.TxIDChainHash().String(), coinbaseHeight, validatedForHeight, validatedForHeight, forkBlockHeight)
		})
	}
}

// TestKnownDefect_CachedSubtreeUnderOlderParent_RejectsTimeLockedTx proves that a subtree this
// node validated earlier is not re-checked for time-based finality when a candidate block with an
// older parent median-time-past reuses it.
//
// Spec: BV-BODY-020 (tracked as GAP-BV-047).
//
// Scenario: Given a tx with a time nLockTime of one hour ago, final against the node's tip MTP
// (now), validated for height 101 and held in a cached subtree. When a candidate block above
// CSVHeight, whose parent is genesis and so whose parent MTP is the genesis timestamp, reuses that
// subtree. Then the block must be rejected: TX_INVALID caused by NON_FINAL.
//
// Today: checkCachedSubtreesAgainstCandidateChain (check_block_subtrees.go) returns early when the
// store flags no tx of the cached subtrees Conflicting, and re-checks at the block's height and
// candidate MTP only the conflicting ones; its doc comment assumes the rest "does not change
// between blocks". The non-conflicting tx is never re-evaluated and the block is accepted.
//
// Controls (passing today): the same time-locked spend stored as a conflicting record in the
// cached subtree IS re-checked against the genesis parent MTP and rejected.
//
// Note: the candidate's height (above CSVHeight) and its genesis parent are not a coherent chain
// position. The height is chosen only so the time basis is the parent's MTP, and
// checkCachedSubtreesAgainstCandidateChain reads nothing of the block but its height and parent.
func TestKnownDefect_CachedSubtreeUnderOlderParent_RejectsTimeLockedTx(t *testing.T) {
	const validatedForHeight = uint32(101)

	// final against the store's MTP (now), not against the genesis parent's MTP
	lockedUntilTime := uint32(time.Now().Add(-time.Hour).Unix()) //nolint:gosec

	t.Run("control: a conflicting time-locked record in a cached subtree is rejected against the genesis parent's MTP", func(t *testing.T) {
		// Given a conflicting record of a tx locked until an hour ago, in a cached subtree
		node := newRealSubtreeValidationNode(t)
		require.Greater(t, int64(lockedUntilTime), node.settings.ChainCfgParams.GenesisBlock.Header.Timestamp.Unix(), "precondition")

		tx := node.txLockedUntilTime(t, node.parent, lockedUntilTime)
		node.storeAsConflictingRecord(t, tx, validatedForHeight)

		// When a candidate block whose parent is genesis reuses the cached subtree
		err := node.validateBlockReusingCachedSubtree(t, tx)

		// Then the record is re-checked against the genesis MTP and rejected
		requireBlockRejectsTxAsNonFinal(t, err,
			"a conflicting record locked until time %d must be rejected as non-final against the genesis parent's MTP", lockedUntilTime)
	})

	t.Run("a time-locked tx in a cached subtree is rejected against the genesis parent's MTP", func(t *testing.T) {
		// Given a tx locked until an hour ago, accepted for 101 and held in a cached subtree
		node := newRealSubtreeValidationNode(t)
		funding := node.mineSpendableCoinbase(t)
		tx := node.txLockedUntilTime(t, funding, lockedUntilTime)
		node.acceptIntoMempool(t, tx, validatedForHeight)

		// When a candidate block whose parent is genesis reuses the cached subtree
		err := node.validateBlockReusingCachedSubtree(t, tx)

		// Then the tx is re-evaluated against the genesis MTP and rejected
		requireBlockRejectsTxAsNonFinal(t, err,
			"tx %s (nLockTime %d) was final against the tip's MTP; a block whose parent MTP is the genesis timestamp reuses its cached subtree and must reject it as non-final",
			tx.TxIDChainHash().String(), lockedUntilTime)
	})
}

// realSubtreeValidationNode is a subtree validation node over a real validator and an in-memory
// SQLite UTXO store at height 100 (newConflictingFixture). Its parent output is already spent by
// the node's winner, and its other tx conflicts with nothing.
type realSubtreeValidationNode struct {
	*conflictingFixture
}

func newRealSubtreeValidationNode(t *testing.T) *realSubtreeValidationNode {
	t.Helper()

	return &realSubtreeValidationNode{conflictingFixture: newConflictingFixture(t)}
}

// mineCoinbase stores a coinbase mined at height, paying 100,000 satoshis to the node's key.
func (node *realSubtreeValidationNode) mineCoinbase(t *testing.T, height uint32, coinbaseTag string) *bt.Tx {
	t.Helper()

	coinbase := transactions.Create(t,
		transactions.WithCoinbaseData(height, coinbaseTag),
		transactions.WithP2PKHOutputs(1, 100_000, node.privateKey.PubKey()),
	)
	_, err := node.store.Create(t.Context(), coinbase, height,
		utxostore.WithMinedBlockInfo(utxostore.MinedBlockInfo{BlockID: height, BlockHeight: height, SubtreeIdx: 0}))
	require.NoError(t, err)

	return coinbase
}

// mineSpendableCoinbase stores a coinbase mined at height 3 (mature from 4 with the test maturity
// of 1), so a tx spending it conflicts with nothing.
func (node *realSubtreeValidationNode) mineSpendableCoinbase(t *testing.T) *bt.Tx {
	t.Helper()

	const spendableCoinbaseHeight = uint32(3)

	return node.mineCoinbase(t, spendableCoinbaseHeight, "/gap bv047 spendable/")
}

// txSpendingCoinbase spends output 0 of coinbase into a 90,000 satoshi output, with no lock time.
func (node *realSubtreeValidationNode) txSpendingCoinbase(t *testing.T, coinbase *bt.Tx) *bt.Tx {
	t.Helper()

	return transactions.Create(t,
		transactions.WithPrivateKey(node.privateKey),
		transactions.WithInput(coinbase, 0, node.privateKey),
		transactions.WithP2PKHOutputs(1, 90_000, node.privateKey.PubKey()),
	)
}

// txLockedUntilHeight spends funding's output 0 with nLockTime set to a block height.
func (node *realSubtreeValidationNode) txLockedUntilHeight(t *testing.T, funding *bt.Tx, lockedUntilHeight uint32) *bt.Tx {
	t.Helper()

	return node.txWithEnforcedLockTime(t, funding, lockedUntilHeight)
}

// txLockedUntilTime spends funding's output 0 with nLockTime set to a unix timestamp.
func (node *realSubtreeValidationNode) txLockedUntilTime(t *testing.T, funding *bt.Tx, lockedUntilTime uint32) *bt.Tx {
	t.Helper()

	return node.txWithEnforcedLockTime(t, funding, lockedUntilTime)
}

// txWithEnforcedLockTime spends funding's output 0 into a 75,000 satoshi output with the given
// nLockTime and a non-final input sequence, so the lock time is enforced
// (util.IsTransactionFinal).
func (node *realSubtreeValidationNode) txWithEnforcedLockTime(t *testing.T, funding *bt.Tx, lockTime uint32) *bt.Tx {
	t.Helper()

	tx := transactions.Create(t,
		transactions.WithPrivateKey(node.privateKey),
		transactions.WithInput(funding, 0, node.privateKey),
		transactions.WithP2PKHOutputs(1, 75_000, node.privateKey.PubKey()),
	)

	tx.LockTime = lockTime
	tx.Inputs[0].SequenceNumber = 0
	require.NoError(t, tx.FillAllInputs(t.Context(), &unlocker.Getter{PrivateKey: node.privateKey}))

	return tx
}

// acceptIntoMempool validates tx on the mempool path for validatedForHeight and requires the stored
// record to be NOT conflicting: the record the defect is about.
func (node *realSubtreeValidationNode) acceptIntoMempool(t *testing.T, tx *bt.Tx, validatedForHeight uint32) {
	t.Helper()

	_, err := node.recorder.Interface.Validate(t.Context(), tx.Clone(), validatedForHeight)
	require.NoError(t, err, "precondition: the tx is valid in the context it is first validated in")

	node.requireStoredConflictingFlag(t, tx, false)
}

// storeAsConflictingRecord validates tx for validatedForHeight the way the block path creates a
// conflicting tx, and requires the stored record to be flagged conflicting.
func (node *realSubtreeValidationNode) storeAsConflictingRecord(t *testing.T, tx *bt.Tx, validatedForHeight uint32) {
	t.Helper()

	_, err := node.recorder.Interface.Validate(t.Context(), tx.Clone(), validatedForHeight, validator.WithCreateConflicting(true))
	require.True(t, errors.Is(err, errors.ErrTxConflicting), "precondition: a conflicting record: %v", err)

	node.requireStoredConflictingFlag(t, tx, true)
}

func (node *realSubtreeValidationNode) requireStoredConflictingFlag(t *testing.T, tx *bt.Tx, wantConflicting bool) {
	t.Helper()

	stored := &meta.Data{}
	require.NoError(t, node.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
	require.Equal(t, wantConflicting, stored.Conflicting, "precondition: the stored record's conflicting flag")
}

// candidateBlockHeader is the header of a candidate block whose parent is genesis and whose
// timestamp is now.
func (node *realSubtreeValidationNode) candidateBlockHeader() *model.BlockHeader {
	return &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  node.settings.ChainCfgParams.GenesisHash,
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
	}
}

// blockContextEntryPoint is one block-path entry point. validateBlockContaining runs a block at
// blockHeight, whose parent is genesis and whose header time is now, holding tx and the node's
// unrelated other tx, and returns the subtree-validation verdict.
type blockContextEntryPoint struct {
	name                    string
	validateBlockContaining func(node *realSubtreeValidationNode, t *testing.T, tx *bt.Tx, blockHeight uint32) error
}

var (
	viaProcessTransactionsInLevels = blockContextEntryPoint{
		name:                    "processTransactionsInLevels",
		validateBlockContaining: (*realSubtreeValidationNode).validateViaProcessTransactionsInLevels,
	}
	viaProcessTransactionsBatched = blockContextEntryPoint{
		name:                    "processTransactionsBatched",
		validateBlockContaining: (*realSubtreeValidationNode).validateViaProcessTransactionsBatched,
	}
	viaCheckBlockSubtrees = blockContextEntryPoint{
		name:                    "CheckBlockSubtrees",
		validateBlockContaining: (*realSubtreeValidationNode).validateViaCheckBlockSubtrees,
	}
	viaCheckSubtreeFromBlock = blockContextEntryPoint{
		name:                    "checkSubtreeFromBlock",
		validateBlockContaining: (*realSubtreeValidationNode).validateViaCheckSubtreeFromBlock,
	}
	viaCheckSubtreeFromBlockWithCachedRecord = blockContextEntryPoint{
		name:                    "checkSubtreeFromBlock with the record in the txmeta cache",
		validateBlockContaining: (*realSubtreeValidationNode).validateViaCheckSubtreeFromBlockWithCachedRecord,
	}
)

// validateViaProcessTransactionsInLevels is processTransactionsInLevels with the candidate times
// CheckBlockSubtrees passes below CSVHeight: the header's timestamp and no parent MTP.
func (node *realSubtreeValidationNode) validateViaProcessTransactionsInLevels(t *testing.T, tx *bt.Tx, blockHeight uint32) error {
	const (
		noParentMedianTime   = uint32(0)
		addTxToBlockAssembly = false
	)

	return node.server.processTransactionsInLevels(t.Context(), []*bt.Tx{tx.Clone(), node.other.Clone()}, chainhash.Hash{}, chainhash.Hash{},
		blockHeight, node.candidateBlockHeader().Timestamp, noParentMedianTime, map[uint32]bool{}, addTxToBlockAssembly)
}

// validateViaProcessTransactionsBatched is processTransactionsBatched, the catch-up path, with the
// same candidate times.
func (node *realSubtreeValidationNode) validateViaProcessTransactionsBatched(t *testing.T, tx *bt.Tx, blockHeight uint32) error {
	const noParentMedianTime = uint32(0)

	checker, ok := node.recorder.Interface.(validator.BlockBatchChecker)
	require.True(t, ok)

	return node.server.processTransactionsBatched(t.Context(), checker, []*bt.Tx{tx.Clone(), node.other.Clone()}, chainhash.Hash{},
		blockHeight, node.candidateBlockHeader().Timestamp, noParentMedianTime, map[uint32]bool{})
}

// validateViaCheckBlockSubtrees is the entry block validation calls for a block whose subtree this
// node does not hold yet; a peer serves it.
func (node *realSubtreeValidationNode) validateViaCheckBlockSubtrees(t *testing.T, tx *bt.Tx, blockHeight uint32) error {
	node.settings.AdaptiveFetch.BootstrapMode = "pessimistic"
	server, subtreeStore := node.newServer(t)
	server.adaptiveFetch.Arm()

	root := servePeerSubtree(t, subtreeStore, tx, node.other)

	block, err := model.NewBlock(node.candidateBlockHeader(), &bt.Tx{Version: 1}, []*chainhash.Hash{&root}, 3, 1000, blockHeight, 0)
	require.NoError(t, err)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	resp, err := server.CheckBlockSubtrees(t.Context(), &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes, BaseUrl: "http://peer.invalid"})
	if err == nil {
		require.True(t, resp.Blessed)
	}

	return err
}

// validateViaCheckSubtreeFromBlock is checkSubtreeFromBlock (ValidateSubtreeInternal for a block's
// subtree), finding the record in the UTXO store.
func (node *realSubtreeValidationNode) validateViaCheckSubtreeFromBlock(t *testing.T, tx *bt.Tx, blockHeight uint32) error {
	server, subtreeStore := node.newServer(t)

	return node.checkPeerSubtreeFromBlock(t, server, subtreeStore, tx, blockHeight)
}

// validateViaCheckSubtreeFromBlockWithCachedRecord is checkSubtreeFromBlock with the stored record
// already in the txmeta cache.
func (node *realSubtreeValidationNode) validateViaCheckSubtreeFromBlockWithCachedRecord(t *testing.T, tx *bt.Tx, blockHeight uint32) error {
	server, subtreeStore := node.newServer(t)

	cache, ok := server.utxoStore.(*txmetacache.TxMetaCache)
	require.True(t, ok)

	stored := &meta.Data{}
	require.NoError(t, node.store.GetMeta(t.Context(), tx.TxIDChainHash(), stored))
	require.NoError(t, cache.SetCache(tx.TxIDChainHash(), stored))

	_, found := cache.GetMetaCached(t.Context(), *tx.TxIDChainHash())
	require.True(t, found, "precondition: the block path finds the record in the cache")

	return node.checkPeerSubtreeFromBlock(t, server, subtreeStore, tx, blockHeight)
}

func (node *realSubtreeValidationNode) checkPeerSubtreeFromBlock(t *testing.T, server *Server, subtreeStore blob.Store, tx *bt.Tx, blockHeight uint32) error {
	root := servePeerSubtree(t, subtreeStore, tx, node.other)
	blockHash := chainhash.HashH([]byte("gap bv047 block"))

	_, err := server.checkSubtreeFromBlock(t.Context(), &subtreevalidation_api.CheckSubtreeFromBlockRequest{
		Hash:              root[:],
		BaseUrl:           "http://peer.invalid",
		BlockHeight:       blockHeight,
		BlockHash:         blockHash[:],
		PreviousBlockHash: node.settings.ChainCfgParams.GenesisHash[:],
	})

	return err
}

// validateBlockReusingCachedSubtree stores a subtree of tx and the node's other tx as already
// validated, then runs checkCachedSubtreesAgainstCandidateChain for a block above CSVHeight whose
// parent is genesis, so finality is measured against the genesis MTP.
func (node *realSubtreeValidationNode) validateBlockReusingCachedSubtree(t *testing.T, tx *bt.Tx) error {
	const heightAboveCSVHeight = 24

	server, subtreeStore := node.newServer(t)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, 0))
	require.NoError(t, subtree.AddNode(*node.other.TxIDChainHash(), 0, 0))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	root := *subtree.RootHash()
	require.NoError(t, subtreeStore.Set(t.Context(), root[:], fileformat.FileTypeSubtree, subtreeBytes))

	blockHeight := uint32(node.settings.ChainCfgParams.CSVHeight) + heightAboveCSVHeight //nolint:gosec

	block, err := model.NewBlock(node.candidateBlockHeader(), &bt.Tx{Version: 1}, []*chainhash.Hash{&root}, 3, 1000, blockHeight, 0)
	require.NoError(t, err)

	return server.checkCachedSubtreesAgainstCandidateChain(t.Context(), block, []chainhash.Hash{root})
}

// requireBlockRejectsTxAsNonFinal requires a TX_INVALID verdict caused by NON_FINAL.
func requireBlockRejectsTxAsNonFinal(t *testing.T, err error, format string, args ...any) {
	t.Helper()

	expectation := fmt.Sprintf(format, args...)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "%s; want TX_INVALID, got: %v", expectation, err)
	require.True(t, errors.Is(err, errors.ErrNonFinal), "%s; want it caused by NON_FINAL, got: %v", expectation, err)
}

// requireBlockRejectsTxAsImmatureCoinbaseSpend requires a TX_INVALID verdict caused by
// TX_COINBASE_IMMATURE.
func requireBlockRejectsTxAsImmatureCoinbaseSpend(t *testing.T, err error, format string, args ...any) {
	t.Helper()

	expectation := fmt.Sprintf(format, args...)
	require.True(t, errors.Is(err, errors.ErrTxInvalid), "%s; want TX_INVALID, got: %v", expectation, err)
	require.True(t, errors.Is(err, errors.ErrTxCoinbaseImmature), "%s; want it caused by TX_COINBASE_IMMATURE, got: %v", expectation, err)
}
