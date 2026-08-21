package netsync

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	safeconversion "github.com/bsv-blockchain/go-safe-conversion"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/blockassemblyutil"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
)

const (
	// txNotFoundInTxMapMsg is the error message used when a transaction hash
	// cannot be located in the block's txMap.
	txNotFoundInTxMapMsg = "transaction %s not found in txMap"
)

func (sm *SyncManager) HandleBlockDirect(ctx context.Context, peer *peer.Peer, blockHash chainhash.Hash, msgBlock *wire.MsgBlock) (err error) {
	sm.logger.Debugf("[HandleBlockDirect][%s] starting handling block", blockHash.String())

	// Make sure we have the correct height for this block before continuing
	var (
		blockHeight             uint32
		previousBlockHeaderMeta *model.BlockHeaderMeta
	)

	// check whether this block already exists
	blockExists, err := sm.blockchainClient.GetBlockExists(ctx, &blockHash)
	if err != nil {
		sm.logger.Errorf("[HandleBlockDirect][%s] failed to check if block exists: %s", blockHash.String(), err)
		return errors.NewProcessingError("failed to check if block exists", err)
	}

	if blockExists {
		sm.logger.Warnf("[HandleBlockDirect][%s] block already exists", blockHash.String())
		return nil
	}

	// The sync peer's association just delivered a full block. Refresh its
	// last-block time now, at receipt, so the minutes-long validation that
	// follows (extend/createUtxos/validate/subtree writes for a multi-GB block)
	// is not mistaken for a stall — which would rotate the sync peer
	// mid-processing. Association-aware: the block arrives on the DATA1 stream,
	// a different Peer from the GENERAL sync peer.
	if sps, ok := sm.syncPeerStateFor(peer); ok {
		sps.updateLastBlockTime()
	}

	block := bsvutil.NewBlock(msgBlock)

	// Lookup previous block height from blockchain
	_, previousBlockHeaderMeta, err = sm.blockchainClient.GetBlockHeader(ctx, &block.MsgBlock().Header.PrevBlock)
	if err != nil {
		// A missing parent is an expected, recoverable condition: a normal orphan /
		// out-of-order tip announce, or a descendant of a block that was rejected
		// upstream. The caller (handleBlockMsg) answers with a getblocks and, for a
		// known-failed ancestor, short-circuits the descendant cascade (#1333) — so
		// logging it at ERROR here only produces misleading "previous block
		// NOT_FOUND" spam. Genuine lookup failures (e.g. storage errors) still ERROR.
		if errors.Is(err, errors.ErrBlockNotFound) {
			sm.logger.Debugf("[HandleBlockDirect][%s] previous block %s not found (orphan/out-of-order; caller will request missing blocks): %v", blockHash.String(), block.MsgBlock().Header.PrevBlock, err)
		} else {
			sm.logger.Errorf("[HandleBlockDirect][%s] failed to get block header for previous block %s: %s", blockHash.String(), block.MsgBlock().Header.PrevBlock, err)
		}

		return errors.NewProcessingError("failed to get block header for previous block %s", block.MsgBlock().Header.PrevBlock, err)
	}

	if block.Height() <= 0 {
		// block height was not set in the msgBlock, set it from our lookup
		blockHeight = previousBlockHeaderMeta.Height + 1

		blockHeightInt32, err := safeconversion.Uint32ToInt32(blockHeight)
		if err != nil {
			return errors.NewProcessingError("failed to convert block height to int32", err)
		}

		block.SetHeight(blockHeightInt32)
	} else {
		// check whether the block height being reported is the correct block height
		previousBlockHeightInt32, err := safeconversion.Uint32ToInt32(previousBlockHeaderMeta.Height + 1)
		if err != nil {
			return errors.NewProcessingError("failed to convert block height to int32", err)
		}

		if block.Height() != previousBlockHeightInt32 {
			return errors.NewBlockInvalidError("block height %d is not the correct height for block %s, expected %d", block.Height(), blockHash, previousBlockHeaderMeta.Height+1)
		}

		blockHeight, err = safeconversion.Int32ToUint32(block.Height())
		if err != nil {
			return errors.NewProcessingError("failed to convert block height to uint32", err)
		}
	}

	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "HandleBlockDirect",
		tracing.WithLogMessage(
			sm.logger,
			"[HandleBlockDirect][%s %d] %d txs, peer %s",
			block.Hash().String(),
			blockHeight,
			len(block.Transactions()),
			peer.String(),
		),
		tracing.WithTag("blockHash", block.Hash().String()),
		tracing.WithTag("peer", peer.String()),
		tracing.WithHistogram(prometheusLegacyNetsyncHandleBlockDirect),
	)
	defer func() {
		// set the block height gauge in the prometheus metrics
		prometheusLegacyNetsyncBlockHeight.Set(float64(blockHeight))

		deferFn(err)
	}()

	// Wait for block assembly to be ready
	if err = blockassemblyutil.WaitForBlockAssemblyReady(ctx, sm.logger, sm.blockAssembly, blockHeight, sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly); err != nil {
		// block-assembly is still behind, so we cannot process this block
		return err
	}

	// Wait for the previous block's setTxMined to complete before validating
	// this block's transactions. Ensures BIP68 sequence lock validation can
	// correctly look up parent transaction BlockHeights in the UTXO store.
	// Skipped on the below-checkpoint outpoint-only fast path — see
	// needsParentMinedWait for the redundancy argument.
	if sm.needsParentMinedWait(blockHeight) {
		if err = sm.waitForPreviousBlockMined(ctx, &block.MsgBlock().Header.PrevBlock, blockHeight); err != nil {
			return err
		}
	}

	// 3. Create a block message with (block hash, coinbase tx and slice if 1 subtree)
	var headerBytes bytes.Buffer
	if err = block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
		return errors.NewProcessingError("failed to serialize header", err)
	}

	// create the Teranode compatible block header
	header, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return errors.NewProcessingError("failed to create block header from bytes", err)
	}

	var coinbase bytes.Buffer
	if err = block.Transactions()[0].MsgTx().Serialize(&coinbase); err != nil {
		return errors.NewProcessingError("failed to serialize coinbase", err)
	}

	// Single coinbase decode per block, retained into the teranodeBlock model
	// for downstream use; stays on the standard heap path. The arena variant
	// would require Put before return, at which point coinbaseTx's scripts
	// would alias soon-to-be-reused memory. Per-block tx loops in legacy
	// ingestion live in bsvutil/subtree-assembly, which works with
	// bsvutil.Tx (not bt.Tx) and never round-trips through go-bt decode.
	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return errors.NewProcessingError("failed to create bt.Tx for coinbase", err)
	}

	// Pre-extract everything the post-pipeline code needs from the decoded
	// block BEFORE prepareSubtrees. Once createTxMap (inside prepareSubtrees)
	// has cloned the scripts out, nothing references block/msgBlock any more,
	// so the multi-GB wire block and its decode arena can be collected while
	// the heavy extend/subtree/utxo/validate phases are still running.
	blockSize := block.MsgBlock().SerializeSize()

	blockSizeUint64, err := safeconversion.IntToUint64(blockSize)
	if err != nil {
		return err
	}

	txCount := uint64(len(block.Transactions()))

	// Pre-extract the tx hashes for the orphan-processing goroutine below.
	// Copy the hash *values* (not the *chainhash.Hash pointers returned by
	// tx.Hash(), which alias into the bsvutil.Tx wrapper and would pin it).
	wireTxs := block.Transactions()
	txHashes := make([]chainhash.Hash, len(wireTxs))
	for i, tx := range wireTxs {
		txHashes[i] = *tx.Hash()
	}
	blockHashStr := block.Hash().String()

	// validate all subtrees and store all subtree data
	// this also should spend and create all utxos
	// NOTE: last use of `block` — from here down only scalars and tx hash
	// copies are referenced, so the decode arena is collectable as soon as
	// createTxMap inside prepareSubtrees returns.
	subtrees, preparedSubtreeSlices, blockID, err := sm.prepareSubtrees(ctx, block)
	if err != nil {
		return err
	}

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, txCount, blockSizeUint64, blockHeight, blockID)
	if err != nil {
		return errors.NewProcessingError("failed to create model.NewBlock", err)
	}

	// pre-check that there is enough proof of work on the block, before we do any other processing
	headerValid, _, err := teranodeBlock.Header.HasMetTargetDifficulty()
	if !headerValid {
		return errors.NewBlockInvalidError("invalid block header: %s", teranodeBlock.Header.Hash().String(), err)
	}

	// Unified route integrity floor: block.Valid (and its CheckMerkleRoot) no
	// longer runs server-side on this route, and unlike catchup — whose subtree
	// lists arrive bound to checkpoint-verified headers — legacy builds these
	// subtrees itself from the wire block. Verify the merkle root here so a
	// corrupt or tampered wire block can never reach the UTXO store. PoW was
	// checked above; header linkage was checked on receipt.
	//
	// CheckMerkleRoot alone is NOT sufficient: a CVE-2012-2459 duplicate-tx
	// mutation preserves the merkle root via the duplicate-last-when-odd rule,
	// so it passes the merkle check while still containing a repeated hash.
	// The CVE-2012-2459 dedup floor now runs unconditionally inside prepareSubtrees
	// (on the locally-built slices, for every route), so it is already enforced
	// before we get here — the second call below is a defensive belt-and-braces on
	// the unified route's returned slices and is kept in sync with unified_dedup_test.go.
	// It must run while SubtreeSlices is still populated, before the nil-out below.
	if preparedSubtreeSlices != nil {
		teranodeBlock.SubtreeSlices = preparedSubtreeSlices
		if err = teranodeBlock.CheckMerkleRoot(ctx); err != nil {
			return errors.NewBlockInvalidError("[HandleBlockDirect][%s %d] merkle root mismatch on unified route", blockHashStr, blockHeight, err)
		}
		if err = model.CheckSubtreeSlicesForDuplicateTxs(preparedSubtreeSlices); err != nil {
			return errors.NewBlockInvalidError("[HandleBlockDirect][%s %d] duplicate transaction on unified route", blockHashStr, blockHeight, err)
		}
		teranodeBlock.SubtreeSlices = nil
	}

	// call the process block wrapper, which will add tracing and logging
	err = sm.ProcessBlock(ctx, teranodeBlock)
	if err != nil {
		return err
	}

	// process any orphan transactions that are now valid in background
	// this will also remove the transactions from the orphan pool.
	// txHashes was pre-extracted above (before prepareSubtrees) so this
	// goroutine holds no reference into the decoded block.
	go func() {
		acceptedTxs := make([]*TxHashAndFee, 0)
		for i := range txHashes {
			sm.processOrphanTransactions(ctx, &txHashes[i], &acceptedTxs)
		}

		if len(acceptedTxs) > 0 {
			sm.logger.Infof("[HandleBlockDirect][%s %d] accepted %d orphan transactions", blockHashStr, blockHeight, len(acceptedTxs))
			sm.peerNotifier.AnnounceNewTransactions(acceptedTxs)
		}
	}()

	return nil
}

// waitForPreviousBlockMined waits for the previous block to have mined_set=true.
// This ensures setTxMined has completed for the previous block before we validate
// the next block's transactions, which is critical for BIP68 sequence lock validation
// that needs correct BlockHeights from parent transactions in the UTXO store.
func (sm *SyncManager) waitForPreviousBlockMined(ctx context.Context, prevBlockHash *chainhash.Hash, blockHeight uint32) error {
	_, err := retry.Retry(ctx, sm.logger, func() (bool, error) {
		isMined, err := sm.blockchainClient.GetBlockIsMined(ctx, prevBlockHash)
		if err != nil {
			return false, errors.NewServiceError(
				"[waitForPreviousBlockMined][height:%d] parent %s mined status not available yet",
				blockHeight, prevBlockHash.String(), err)
		}
		if !isMined {
			return false, errors.NewBlockParentNotMinedError(
				"[waitForPreviousBlockMined][height:%d] parent %s not mined yet",
				blockHeight, prevBlockHash.String())
		}
		return true, nil
	},
		retry.WithBackoffDurationType(sm.settings.BlockValidation.IsParentMinedRetryBackoffDuration),
		retry.WithBackoffMultiplier(sm.settings.BlockValidation.IsParentMinedRetryBackoffMultiplier),
		retry.WithRetryCount(sm.settings.BlockValidation.IsParentMinedRetryMaxRetry),
		retry.WithMessage("waitForPreviousBlockMined: legacy sync waiting for parent mined_set"),
	)
	return err
}

func (sm *SyncManager) ProcessBlock(ctx context.Context, teranodeBlock *model.Block) (err error) {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "SyncManager:processBlock",
		tracing.WithLogMessage(
			sm.logger,
			"[SyncManager:processBlock][%s %d] processing block",
			teranodeBlock.Hash().String(),
			teranodeBlock.Height,
		),
		tracing.WithHistogram(prometheusLegacyNetsyncProcessBlock),
	)
	defer func() {
		deferFn(err)
	}()

	// send the block to the blockValidation for processing and validation
	// all the block subtrees should have been validated in processSubtrees
	// teranodeBlock.ID was set by model.NewBlock from the pre-assigned ID returned by prepareSubtrees.
	// Read it from the struct here — avoids duplicating it as a parameter. It still has to travel as
	// a separate proto field in the gRPC request because block.Bytes() does not serialize ID.
	if err = sm.blockValidation.ProcessBlock(ctx, teranodeBlock, teranodeBlock.Height, "", "legacy", teranodeBlock.ID); err != nil {
		if errors.Is(err, errors.ErrBlockExists) {
			sm.logger.Infof("[SyncManager:processBlock][%s %d] block already exists", teranodeBlock.Hash().String(), teranodeBlock.Height)
			return nil
		}

		return errors.NewProcessingError("failed to process block", err)
	}

	return nil
}

type TxMapWrapper struct {
	Tx                 *bt.Tx
	SomeParentsInBlock bool
	ChildLevelInBlock  uint32
}

// blockIdent carries the scalar identity of a block through the pipeline
// stages below createTxMap. Passing these values (instead of *bsvutil.Block)
// is what lets the decoded wire block — and its multi-GB go-wire decode
// arena — be collected as soon as createTxMap has cloned the scripts out,
// while the heavy extend/subtree/utxo/validate phases are still running.
// This includes tracing's deferred log args, which retain whatever they are
// given until the span closes: a *chainhash.Hash from block.Hash() would pin
// the whole wrapper for the duration of the stage.
type blockIdent struct {
	hash      chainhash.Hash
	prevBlock chainhash.Hash
	height    uint32
	timestamp time.Time
}

func (sm *SyncManager) prepareSubtrees(ctx context.Context, block *bsvutil.Block) (subtrees []*chainhash.Hash, subtreeSlices []*subtreepkg.Subtree, blockID uint32, err error) {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "prepareSubtrees",
		tracing.WithLogMessage(
			sm.logger,
			"[prepareSubtrees][%s] processing subtree for block height %d, tx count %d",
			block.Hash().String(),
			block.Height(),
			len(block.Transactions()),
		),
		tracing.WithHistogram(prometheusLegacyNetsyncPrepareSubtrees),
	)
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("[prepareSubtrees] recovered in prepareSubtrees: %v", r, err)
		}

		deferFn(err)
	}()

	subtrees = make([]*chainhash.Hash, 0)

	txCount := len(block.Transactions())
	if txCount <= 1 {
		return subtrees, nil, blockID, nil
	}

	// Partition the block's transactions into K subtrees so each non-final subtree
	// is exactly subtreeSize leaves and the final subtree's leaf count is in
	// [1, subtreeSize] — matching model.Block.CheckMerkleRoot's Length-based lift
	// rules. The final subtree does not need to be a power of two: the
	// duplicate-when-odd rule applied inside BuildMerkleTreeStoreFromBytes already
	// pads its natural root to height ceil(log2(length)), which is what the lift
	// in CheckMerkleRoot expects. For blocks where txCount ≤
	// MaximumMerkleItemsPerSubtree the partition is the unchanged single-subtree
	// case.
	maxItems := sm.settings.BlockAssembly.MaximumMerkleItemsPerSubtree

	subtreeSize, numSubtrees, finalLeafCount, err := partitionLegacyBlock(txCount, maxItems)
	if err != nil {
		return nil, nil, 0, errors.NewProcessingError("[prepareSubtrees] failed to partition block", err)
	}

	slices := make([]*subtreepkg.Subtree, numSubtrees)
	subtreeDatas := make([]*subtreepkg.Data, numSubtrees)
	subtreeMetas := make([]*subtreepkg.Meta, numSubtrees)

	for i := 0; i < numSubtrees; i++ {
		capacity := subtreeSize
		if i == numSubtrees-1 && numSubtrees > 1 && finalLeafCount < subtreeSize {
			capacity = finalLeafCount
		}

		st, terr := subtreepkg.NewIncompleteTreeByLeafCount(capacity)
		if terr != nil {
			return nil, nil, 0, errors.NewSubtreeError("[prepareSubtrees] failed to create subtree %d", i, terr)
		}

		if i == 0 {
			if err = st.AddCoinbaseNode(); err != nil {
				return nil, nil, 0, errors.NewSubtreeError("[prepareSubtrees] failed to add coinbase placeholder", err)
			}
		}

		slices[i] = st
		subtreeDatas[i] = subtreepkg.NewSubtreeData(st)
		subtreeMetas[i] = subtreepkg.NewSubtreeMeta(st)
	}

	blockHeight32, convErr := safeconversion.Int32ToUint32(block.Height())
	if convErr != nil {
		return nil, nil, 0, errors.NewProcessingError("[prepareSubtrees] failed to convert block height", convErr)
	}

	// Scalar identity for every stage below createTxMap — see blockIdent.
	bi := blockIdent{
		hash:      *block.Hash(),
		prevBlock: block.MsgBlock().Header.PrevBlock,
		height:    blockHeight32,
		timestamp: block.MsgBlock().Header.Timestamp,
	}

	txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](txCount)

	// Last use of `block`: createTxMap clones every script into txMap and
	// returns the block-order tx hash list. From here on the decoded wire
	// block is unreferenced and its decode arena can be GC'd.
	txOrder, err := sm.createTxMap(ctx, block, txMap)
	if err != nil {
		return nil, nil, 0, err
	}

	// createTxMap is the only writer of txMap and it runs single-threaded; every
	// stage below only reads it (concurrently, from many goroutines in extend /
	// createUtxos / pre-validate). Freeze it so those reads skip the RWMutex —
	// the per-read reader-counter atomic becomes a cache-line contention point
	// across cores on a large block. After Freeze any txMap write panics, which
	// is the intended guard since nothing should mutate it past this point.
	txMap.Freeze()

	// Compute the below-checkpoint fast-path mode ONCE for this block and thread it into
	// the phases (extend, create-subtrees) so decorate-skip and fee=0 cannot disagree.
	outpointOnly := sm.legacyOutpointOnly(bi.height)

	if err = sm.extendTransactions(ctx, bi, txOrder, txMap, outpointOnly); err != nil {
		return nil, nil, 0, err
	}

	if err = sm.createSubtrees(ctx, bi, txOrder, txMap, slices, subtreeDatas, subtreeMetas, outpointOnly); err != nil {
		return nil, nil, 0, err
	}

	// CVE-2012-2459 dedup floor for EVERY route (inline, unified, non-quick).
	// createTxMap uses txMap.Set, which silently overwrites a duplicated txid, so
	// the map cannot catch it; but createSubtrees replays the block-order txOrder
	// (which retains both copies) and AddNodes the duplicate into `slices` twice.
	// Run the explicit dedup scan here, on the locally-built slices, before any
	// UTXO create/spend or subtree write — regardless of which slices are later
	// returned to the caller. The gated call in HandleBlockDirect fires only when
	// preparedSubtreeSlices != nil (the unified route), so the inline route never
	// ran the check; this single call closes that gap for all paths. A
	// CVE-2012-2459 duplicate-tx mutation preserves the merkle root via the
	// duplicate-last-when-odd rule, so CheckMerkleRoot alone would pass — this is
	// the only thing that catches it.
	if err = model.CheckSubtreeSlicesForDuplicateTxs(slices); err != nil {
		return nil, nil, 0, errors.NewBlockInvalidError("[prepareSubtrees][%s %d] duplicate transaction in block (CVE-2012-2459)", bi.hash.String(), bi.height, err)
	}

	// Quick validation is safe whenever the block sits at/below the highest hard-coded
	// checkpoint for the active network. POW (verified upstream by HasMetTargetDifficulty)
	// plus checkpoint-anchored chain linkage make the block canonical regardless of which
	// FSM state drove the catch-up. The checkpoint list is owned by go-chaincfg — see PR
	// #844 for the matching FSM-RUN gate that relies on the same invariant.
	quickValidationMode := sm.quickValidationAllowed(bi.height)

	if quickValidationMode {
		if sm.legacyUnified(bi.height) {
			// Unified route: block ID assignment and UTXO create+spend happen
			// server-side in quickValidateBlock (blockID 0 = "assign server-side",
			// Server.ProcessBlock skips pre-assignment when the wire field is 0).
			sm.logger.Debugf("[prepareSubtrees][%s] unified route: deferring blockID and UTXO ops to quick validation", bi.hash.String())
		} else {
			// Fetch block ID upfront so UTXOs carry mined info from creation. This ID is
			// threaded through to blockvalidation via ProcessBlock so it can call
			// AddBlock(WithID, WithMinedSet(true)) and cause the setMinedChan worker to
			// skip setTxMinedStatus (MinedSet guard in BlockValidation.go).
			//
			// Restart-safety + cross-path consistency: if this block's transactions were
			// already created in a prior attempt (or by the blockvalidation catchup path),
			// reuse the block id recorded in the UTXO store so the committed block matches
			// the existing UTXO mined-info. Otherwise fall back to the idempotent per-hash
			// AssignBlockID. Both paths converging on one id is what prevents orphaned
			// (phantom) block ids that wedge sync in checkOldBlockIDs.
			if reused, ok := sm.reuseBlockIDFromUTXO(ctx, bi, txOrder); ok {
				blockID = reused
			} else {
				id, idErr := sm.blockchainClient.AssignBlockID(ctx, &bi.hash)
				if idErr != nil {
					return nil, nil, 0, errors.NewProcessingError("[prepareSubtrees] failed to assign block ID", idErr)
				}
				blockID, idErr = safeconversion.Uint64ToUint32(id)
				if idErr != nil {
					return nil, nil, 0, errors.NewProcessingError("[prepareSubtrees] assigned block id %d exceeds uint32", id, idErr)
				}
			}

			// in quickValidationMode, we can process transactions in a block in parallel, but in reverse order
			// first we create all the utxos, then we spend them
			if err = sm.ValidateTransactionsLegacyMode(ctx, txMap, bi, blockID); err != nil {
				return nil, nil, 0, err
			}
		}
	}

	for i := 0; i < numSubtrees; i++ {
		if err = sm.writeSubtree(ctx, bi, slices[i], subtreeDatas[i], subtreeMetas[i], quickValidationMode); err != nil {
			return nil, nil, 0, err
		}
	}

	// In quickValidationMode the transactions and subtree files have already been
	// produced locally, so we can skip the round-trip through subtreeValidation.
	if !quickValidationMode {
		for i := 0; i < numSubtrees; i++ {
			if err = sm.checkSubtreeFromBlock(ctx, bi, slices[i]); err != nil {
				return nil, nil, 0, err
			}
		}
	}

	for i := 0; i < numSubtrees; i++ {
		subtrees = append(subtrees, slices[i].RootHash())
	}

	// Unified route: return the in-memory slices for the merkle check in HandleBlockDirect.
	// The caller sets them on the model.Block, verifies CheckMerkleRoot, then nils the field
	// so the block heads to gRPC without pinning the slices.
	if sm.legacyUnified(bi.height) {
		subtreeSlices = slices
	}

	return subtrees, subtreeSlices, blockID, nil
}

// quickValidationAllowed reports whether the given block height is covered by a
// hard-coded checkpoint for the active network. Checkpoint-anchored chain linkage
// combined with the upstream PoW check makes the block canonical, so we can skip
// subtree re-validation and the per-UTXO setTxMined cross-check.
//
// Returns false when the network defines no checkpoints (regtest) or when the
// block height is above the highest checkpoint — those blocks must follow the
// regular validation path.
//
// Boundary and eligibility live in model.BelowCheckpoint / model.OutpointOnlyEligible — one definition for every path.
func (sm *SyncManager) quickValidationAllowed(blockHeight uint32) bool {
	if sm.chainParams == nil {
		return false
	}

	return model.BelowCheckpoint(sm.chainParams.Checkpoints, blockHeight)
}

// legacyOutpointOnly reports whether this block may use the below-checkpoint
// outpoint-only fast path on the legacy netsync route: skip the bulk decorate,
// stamp subtree fees as 0, do a minimal (inputs-only) UTXO create, and spend
// using the validator's outpoint-only mode. Default OFF — every conjunct must
// hold for the path to engage, so when the setting is off, the store does not
// support the fast path, or the block is above the highest hard-coded checkpoint,
// the legacy path behaves exactly as before (byte-identical, invariant I2).
//
// Boundary and eligibility live in model.BelowCheckpoint / model.OutpointOnlyEligible — one definition for every path.
func (sm *SyncManager) legacyOutpointOnly(height uint32) bool {
	if sm.settings == nil {
		return false
	}

	return model.OutpointOnlyEligible(sm.settings, sm.utxoStore, sm.chainParams, height)
}

// legacyUnified reports whether this block takes the unified below-checkpoint
// route: the operator enabled blockvalidation_legacy_unified_below_checkpoint
// AND the full outpoint-only gate holds. When true, prepareSubtrees becomes a
// protocol adapter — it still partitions the wire block, builds the txMap,
// extends in-block parents, creates and writes the subtree files, and
// HandleBlockDirect verifies the merkle root — but UTXO create+spend and block
// ID assignment move server-side into quickValidateBlock (the same machinery
// native catchup uses). Default off; with the flag off the inline pipeline is
// byte-identical.
func (sm *SyncManager) legacyUnified(height uint32) bool {
	return sm.settings != nil &&
		sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint &&
		sm.legacyOutpointOnly(height)
}

// legacyFailClosed reports whether this block takes the fail-closed variant of the
// non-unified legacy below-checkpoint inline path: the operator enabled
// blockvalidation_legacy_below_checkpoint_fail_closed AND the outpoint-only gate
// holds AND the unified route is NOT engaged (the unified route is already
// fail-closed end-to-end, so this inline lever only applies when it is off). When
// true, PreValidateTransactions drops validator.WithCreateConflicting so a genuine
// conflict hard-fails the block (no conflicting subtree node is written) instead of
// being written-and-reconciled later. Default off; with the flag off the inline
// pipeline is byte-identical to today.
func (sm *SyncManager) legacyFailClosed(height uint32) bool {
	return sm.settings != nil &&
		sm.settings.BlockValidation.LegacyBelowCheckpointFailClosed &&
		sm.legacyOutpointOnly(height) &&
		!sm.legacyUnified(height)
}

// needsParentMinedWait reports whether HandleBlockDirect must block on the
// parent's mined_set before processing a block at this height. Heights 0 and 1
// never wait (pre-existing behaviour). On the below-checkpoint outpoint-only
// fast path the wait is redundant three ways: (1) its documented purpose is
// BIP68 parent-height lookup, and BIP68 is skipped below the checkpoint;
// (2) block dispatch is serial — blockHandler in manager.go is the single
// goroutine consuming blockQueue, so the parent's UTXOs are committed before
// this block starts; (3) the legacy path calls AddBlock with WithMinedSet(true)
// (see buildAddBlockOpts in services/blockvalidation/BlockValidation.go), so
// GetBlockIsMined is always instantly true and only costs a gRPC round-trip
// per block.
func (sm *SyncManager) needsParentMinedWait(height uint32) bool {
	return height > 1 && !sm.legacyOutpointOnly(height)
}

func (sm *SyncManager) checkSubtreeFromBlock(ctx context.Context, bi blockIdent, subtree *subtreepkg.Subtree) error {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "checkSubtreeFromBlock",
		tracing.WithLogMessage(sm.logger, "[checkSubtreeFromBlock][%s] checking subtree for block %s height %d", subtree.RootHash().String(), bi.hash.String(), bi.height),
	)

	defer deferFn()

	if err := sm.subtreeValidation.CheckSubtreeFromBlock(ctx, *subtree.RootHash(), "legacy", bi.height, &bi.hash, &bi.prevBlock); err != nil {
		return errors.NewSubtreeError("failed to check subtree", err)
	}

	return nil
}

func (sm *SyncManager) writeSubtree(ctx context.Context, bi blockIdent, subtree *subtreepkg.Subtree,
	subtreeData *subtreepkg.Data, subtreeMetaData *subtreepkg.Meta, quickValidationMode bool) error {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "writeSubtree",
		tracing.WithLogMessage(sm.logger, "[writeSubtree][%s] writing subtree for block %s height %d", subtree.RootHash().String(), bi.hash.String(), bi.height),
	)

	subtreeFileExtension := fileformat.FileTypeSubtreeToCheck
	if quickValidationMode {
		subtreeFileExtension = fileformat.FileTypeSubtree
	}

	defer deferFn()

	g, gCtx := errgroup.WithContext(ctx)
	// Limit to 3 concurrent writes (subtree, subtreeData, subtreeMeta)
	util.SafeSetLimit(sm.logger, g, 3)

	g.Go(func() error {
		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			return errors.NewStorageError("[writeSubtree][%s] failed to serialize subtree", subtree.RootHash().String(), err)
		}

		// Subtree files use the subtree-validation retention (global + adjustment),
		// matching quick_validate.go and get_blocks.go — one retention source for
		// subtree files on every path.
		dah := bi.height + sm.settings.GetSubtreeValidationBlockHeightRetention()

		storer, err := filestorer.NewFileStorer(
			gCtx,
			sm.logger,
			sm.settings,
			sm.subtreeStore,
			subtree.RootHash()[:],
			subtreeFileExtension,
			options.WithDeleteAt(dah),
		)
		if err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				return nil
			}

			return errors.NewStorageError("[writeSubtree][%s] failed to create subtree file", subtree.RootHash().String(), err)
		}

		// Track whether write succeeded to determine whether to close or abort
		var writeSucceeded bool
		defer func() {
			if !writeSucceeded {
				storer.Abort(errors.NewProcessingError("[writeSubtree] write failed for subtree %s", subtree.RootHash().String()))
			}
		}()

		// TODO Write header extra - *subtree.RootHash(), uint32(block.Height())

		if _, err = storer.Write(subtreeBytes); err != nil {
			return errors.NewStorageError("error writing subtree to disk", err)
		}

		if err = storer.Close(ctx); err != nil {
			return errors.NewStorageError("error closing subtree file", err)
		}

		writeSucceeded = true

		return nil
	})

	g.Go(func() error {
		// Subtree files use the subtree-validation retention (global + adjustment),
		// matching quick_validate.go and get_blocks.go — one retention source for
		// subtree files on every path.
		dah := bi.height + sm.settings.GetSubtreeValidationBlockHeightRetention()

		storer, err := filestorer.NewFileStorer(
			gCtx,
			sm.logger,
			sm.settings,
			sm.subtreeStore,
			subtreeData.RootHash()[:],
			fileformat.FileTypeSubtreeData,
			options.WithDeleteAt(dah),
		)
		if err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				return nil
			}

			return errors.NewStorageError("[writeSubtree][%s] failed to create subtree data file", subtree.RootHash().String(), err)
		}

		// Track whether write succeeded to determine whether to close or abort
		var writeSucceeded bool
		defer func() {
			if !writeSucceeded {
				storer.Abort(errors.NewProcessingError("[writeSubtree] write failed for subtree data %s", subtree.RootHash().String()))
			}
		}()

		// TODO Write header extra - , *subtreeData.RootHash(), uint32(block.Height())

		// Stream transactions directly to the file storer instead of serializing
		// into a single large buffer. This eliminates the ~10.9 GB intermediate
		// allocation that Serialize() creates for large blocks.
		if err := subtreeData.WriteTransactionsToWriter(storer, 0, subtreeData.Subtree.Length()); err != nil {
			return errors.NewStorageError("error streaming subtree data to disk", err)
		}

		if err = storer.Close(ctx); err != nil {
			return errors.NewStorageError("error closing subtree data file", err)
		}

		writeSucceeded = true

		return nil
	})

	// Always store subtree meta data - even when not in quickValidationMode, we need to ensure
	// metadata exists because checkSubtreeFromBlock may return early if the subtree already exists
	// (e.g., created by block assembly) without creating the metadata
	g.Go(func() error {
		// Check if metadata already exists (e.g., came in via P2P) to avoid unnecessary work
		if exists, _ := sm.subtreeStore.Exists(gCtx, subtreeData.RootHash()[:], fileformat.FileTypeSubtreeMeta); exists {
			return nil
		}

		subtreeBytes, err := subtreeMetaData.Serialize()
		if err != nil {
			return errors.NewStorageError("[writeSubtree][%s] failed to serialize subtree data", subtree.RootHash().String(), err)
		}

		// Subtree files use the subtree-validation retention (global + adjustment),
		// matching quick_validate.go and get_blocks.go — one retention source for
		// subtree files on every path.
		dah := bi.height + sm.settings.GetSubtreeValidationBlockHeightRetention()

		storer, err := filestorer.NewFileStorer(
			gCtx,
			sm.logger,
			sm.settings,
			sm.subtreeStore,
			subtreeData.RootHash()[:],
			fileformat.FileTypeSubtreeMeta,
			options.WithDeleteAt(dah),
		)
		if err != nil {
			if errors.Is(err, errors.ErrBlobAlreadyExists) {
				return nil
			}

			return errors.NewStorageError("[writeSubtree][%s] failed to store subtree meta data", subtree.RootHash().String(), err)
		}

		// Track whether write succeeded to determine whether to close or abort
		var writeSucceeded bool
		defer func() {
			if !writeSucceeded {
				storer.Abort(errors.NewProcessingError("[writeSubtree] write failed for subtree meta %s", subtree.RootHash().String()))
			}
		}()

		// TODO Write header extra - , *subtree.RootHash(), uint32(block.Height())

		if _, err = storer.Write(subtreeBytes); err != nil {
			return errors.NewStorageError("error writing subtree meta to disk", err)
		}

		if err = storer.Close(gCtx); err != nil {
			return errors.NewStorageError("error closing subtree meta file", err)
		}

		writeSucceeded = true

		return nil
	})

	return g.Wait()
}

func (sm *SyncManager) ValidateTransactionsLegacyMode(ctx context.Context, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	bi blockIdent, blockID uint32) (err error) {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "validateTransactionsLegacyMode",
		tracing.WithHistogram(prometheusLegacyNetsyncValidateTransactionsLegacyMode),
		tracing.WithLogMessage(sm.logger, "[validateTransactionsLegacyMode] called for block %s, height %d", bi.hash, bi.height),
	)

	defer func() {
		deferFn(err)
	}()

	// Compute the below-checkpoint fast-path mode ONCE for this block and thread it into
	// the phases (minimal create, outpoint-only pre-validate) so they cannot disagree.
	outpointOnly := sm.legacyOutpointOnly(bi.height)

	// failClosed is the requested A/B lever: when engaged, the inline spend drops
	// WithCreateConflicting so a genuine conflict hard-fails rather than writing a
	// conflicting subtree node. Computed once and threaded in for the same reason.
	failClosed := sm.legacyFailClosed(bi.height)

	if err = sm.createUtxos(ctx, txMap, bi, blockID, outpointOnly); err != nil {
		return err
	}

	sm.logger.Infof("[validateTransactionsLegacyMode] created utxos with %d items", txMap.Length())

	candidateBlockTime, candidateParentMedianTime, err := sm.candidateFinalityTimesForBlock(ctx, bi)
	if err != nil {
		return errors.NewProcessingError("[validateTransactionsLegacyMode] failed to select finality time sources", err)
	}

	if err = sm.PreValidateTransactions(ctx, txMap, bi.hash, bi.height, candidateBlockTime, candidateParentMedianTime, outpointOnly, failClosed); err != nil {
		return errors.NewProcessingError("[validateTransactionsLegacyMode] failed to pre-validate transactions", err)
	}

	return nil
}

// candidateFinalityTimesForBlock picks the validator finality-time options
// for the given block based on its CSV era. Exactly one return value is
// non-zero on success:
//
//   - Pre-CSV (blockHeight < CSVHeight): returns (block header timestamp, 0).
//     The validator consumes Options.CandidateBlockTime in this era.
//   - Post-CSV (blockHeight >= CSVHeight): returns (0, candidate-parent MTP).
//     The validator consumes Options.CandidateParentMedianTime in this era,
//     and the parent-chain-walk sourcing rule + chain re-anchor + walk
//     fallback live inside candidateParentMedianTimeForBlock.
//
// The other field stays zero so candidateBlockTimePtr /
// candidateParentMedianTimePtr in services/validator can drop it from the
// proto wire. Extracted as a separate method so the era-selection branch
// can be table-tested at the package level without standing up the full
// SyncManager pipeline.
func (sm *SyncManager) candidateFinalityTimesForBlock(ctx context.Context, bi blockIdent) (candidateBlockTime uint32, candidateParentMedianTime uint32, err error) {
	if bi.height < uint32(sm.chainParams.CSVHeight) {
		candidateBlockTime, err = safeconversion.Int64ToUint32(bi.timestamp.Unix())
		if err != nil {
			return 0, 0, err
		}

		return candidateBlockTime, 0, nil
	}

	candidateParentMedianTime, err = sm.candidateParentMedianTimeForBlock(ctx, &bi.prevBlock)
	if err != nil {
		return 0, 0, err
	}

	return 0, candidateParentMedianTime, nil
}

// candidateParentMedianTimeForBlock returns the candidate-parent MTP for the
// post-CSV consensus path, i.e. the equivalent of bitcoin-sv's
// pindexPrev->GetMedianTimePast() for a candidate whose parent is parentHash.
//
// The MTP is computed by fetching 11 block headers walking back from
// parentHash via the blockchain's GetBlockHeaders API and taking the median of
// their timestamps. GetBlockHeaders is fork-aware: its SQL fallback path
// recursively walks parent_id when the start hash is not on the main chain,
// so a candidate building on a side-chain parent receives the MTP of THAT
// parent chain — not the main-chain MTP at the same height (which is what a
// height-based lookup like GetMedianTimePastForHeights would return).
//
// The value is computed and returned unconditionally because the validator's
// post-CSV consensus path now hard-errors on a missing
// Options.CandidateParentMedianTime (no tip-MTP soft-fall). The earlier
// parent==tip optimisation was unsound — the validator reads
// blockState.MedianTime later than the caller's tip check, and the utxo
// store updates that field asynchronously from blockchain notifications, so
// a tip advance / reorg between the two reads would silently swap the
// comparison time source.
func (sm *SyncManager) candidateParentMedianTimeForBlock(ctx context.Context, parentHash *chainhash.Hash) (uint32, error) {
	if parentHash == nil {
		return 0, errors.NewProcessingError("nil parent hash")
	}

	// Try the batched API first — it is cache-friendly and resolves in a
	// single round-trip on the steady-state path. The SQL implementation
	// runs an on_main_chain probe and a SELECT in two statements; if a reorg
	// lands between them, the returned headers may not anchor to parentHash.
	// candidateParentMedianTimeFromHeaders re-anchors the result and returns
	// an error in that case.
	headers, _, err := sm.blockchainClient.GetBlockHeaders(ctx, parentHash, blockchain.MedianTimeBlocks)
	if err != nil {
		return 0, errors.NewProcessingError("parent hash %s: failed to fetch parent-chain headers", parentHash.String(), err)
	}

	mtp, anchorErr := candidateParentMedianTimeFromHeaders(parentHash, headers)
	if anchorErr == nil {
		return mtp, nil
	}

	// Re-anchor failure on the batched path. Retrying the batched API does
	// not help because GetBlockHeaders caches the (parentHash, 11) result —
	// the next call replays the same headers from cache. Fall back to a
	// hash-keyed parent-chain walk: GetBlockHeader's cache is keyed by hash,
	// so each header is uniquely identified and the same race cannot poison
	// this path. Cost on a cold cache is N round-trips (N=11) instead of 1,
	// taken only on the rare reorg-race event.
	walked, walkErr := sm.walkParentChain(ctx, parentHash, blockchain.MedianTimeBlocks)
	if walkErr != nil {
		return 0, errors.NewProcessingError("parent hash %s: batched-API re-anchor failed (%v); fallback walk failed", parentHash.String(), anchorErr, walkErr)
	}

	mtp, err = candidateParentMedianTimeFromHeaders(parentHash, walked)
	if err != nil {
		return 0, errors.NewProcessingError("parent hash %s: re-anchor failed on both batched fetch (%v) and hash-walk fallback", parentHash.String(), anchorErr, err)
	}

	return mtp, nil
}

// walkParentChain fetches exactly depth block headers starting at startHash and
// walking backwards via HashPrevBlock. Each hop uses blockchainClient.GetBlockHeader
// which is keyed by hash in the in-memory cache — so its results are
// deterministic regardless of which block is canonical at any given height
// (block contents are immutable once stored). This makes the walk
// race-safe under reorg, at the cost of N round-trips on a cold cache.
//
// Returned headers are ordered newest-first, matching the contract of
// blockchainClient.GetBlockHeaders' return order so candidateParentMedianTimeFromHeaders
// can re-anchor them with the same logic.
//
// A nil pointer (cur == nil) or a nil header response (header == nil) is
// treated as a hard error. Walking off the BEGINNING of the chain is not one of
// those cases: the loop breaks at genesis, because on teratestnet, tstn and stn
// CSVHeight is 0, so a candidate can legitimately sit below the first `depth`
// blocks and a genesis-terminated run is the correct short window there.
// Tolerating other short returns would silently produce an incomplete MTP on a
// transient cache miss mid-chain; raising loudly instead forces the caller to
// surface the underlying issue.
func (sm *SyncManager) walkParentChain(ctx context.Context, startHash *chainhash.Hash, depth uint64) ([]*model.BlockHeader, error) {
	headers := make([]*model.BlockHeader, 0, depth)
	cur := startHash

	for i := uint64(0); i < depth; i++ {
		if cur == nil {
			return nil, errors.NewProcessingError("walkParentChain: nil prev-block link at depth %d (walked off the chain)", i)
		}

		header, _, err := sm.blockchainClient.GetBlockHeader(ctx, cur)
		if err != nil {
			return nil, errors.NewProcessingError("walkParentChain: failed at depth %d (hash %s)", i, cur.String(), err)
		}

		if header == nil {
			return nil, errors.NewProcessingError("walkParentChain: nil header at depth %d (hash %s) — possible transient cache miss", i, cur.String())
		}

		headers = append(headers, header)

		// Stop at genesis. Its HashPrevBlock is the all-zero hash, not nil, so the nil guard
		// above never fires — without this the walk asks GetBlockHeader for the zero hash and
		// fails with "failed at depth N". A run that ends at genesis is a complete window; the
		// caller's genesis carve-out decides whether it is long enough.
		if header.HashPrevBlock == nil || header.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
			break
		}

		cur = header.HashPrevBlock
	}

	return headers, nil
}

// candidateParentMedianTimeFromHeaders verifies that the supplied headers form
// a contiguous chain ending at parentHash, then returns the median of their
// timestamps.
//
// The verification closes a concurrency gap in blockchainClient.GetBlockHeaders:
// its main-chain fast path probes the start hash's on_main_chain status in one
// SQL statement and then runs the SELECT that returns the headers in a second
// statement. A reorg fired between the two statements (READ COMMITTED isolation)
// would return main-chain headers at the same height range that no longer
// correspond to parentHash — silently swapping the timestamp set we compute
// MTP over. Re-anchoring the result locally is O(11) and bulletproof: we check
// that the newest returned header equals parentHash and that each consecutive
// pair is linked via HashPrevBlock → Hash().
//
// Empty input and any verification failure surface as a hard error: silently
// returning 0 would let the caller pass Options.CandidateParentMedianTime=0
// to the validator, which now rejects post-CSV consensus requests with a
// missing parent MTP (no tip-MTP soft-fall) — but the error here gives a
// more precise diagnostic at the source rather than waiting for the
// validator's downstream rejection.
//
// Mirrors bitcoin-sv's CBlockIndex::GetMedianTimePast() for the median
// computation itself: sorts the gathered timestamps and returns
// `pbegin[(pend - pbegin) / 2]` — the upper-middle on even counts.
func candidateParentMedianTimeFromHeaders(parentHash *chainhash.Hash, headers []*model.BlockHeader) (uint32, error) {
	if len(headers) == 0 {
		return 0, errors.NewProcessingError("cannot compute median timestamp from zero headers")
	}

	if parentHash == nil {
		return 0, errors.NewProcessingError("nil parent hash")
	}

	// headers are returned newest-first (ORDER BY height DESC). headers[0] must
	// equal parentHash; each subsequent header must be the parent of the one
	// before it. Each element is guarded against nil — production paths
	// (SQL store, gRPC client) do not emit nil entries, but the helper is
	// meant to hard-fail on bad header data rather than panic.
	if headers[0] == nil {
		return 0, errors.NewProcessingError("nil header at depth 0")
	}

	headHash := headers[0].Hash()
	if headHash == nil || !headHash.IsEqual(parentHash) {
		return 0, errors.NewProcessingError("returned chain head does not match requested parent hash (possible reorg between header probe and fetch)")
	}

	// A run LONGER than the window is as wrong as one shorter than it, and just as invisible to
	// the anchor and linkage checks: an over-long run is still anchored at the parent and still
	// correctly linked, but its median covers blocks the consensus rule excludes. Unreachable
	// while the only caller requests exactly MedianTimeBlocks, which the store cannot exceed;
	// enforced so that all three copies of this helper — here, subtreevalidation, and block
	// assembly's verifyParentChainRun — agree, and a later change to the request size fails
	// loudly rather than silently widening the window in some of them.
	if uint64(len(headers)) > blockchain.MedianTimeBlocks {
		return 0, errors.NewProcessingError("run below parent %s holds %d headers, more than the %d the median window covers", parentHash.String(), len(headers), blockchain.MedianTimeBlocks)
	}

	for i := 1; i < len(headers); i++ {
		if headers[i] == nil {
			return 0, errors.NewProcessingError("nil header at depth %d", i)
		}

		prev := headers[i-1].HashPrevBlock
		cur := headers[i].Hash()
		if prev == nil || cur == nil || !prev.IsEqual(cur) {
			return 0, errors.NewProcessingError("parent-chain link broken at depth %d (possible reorg between header probe and fetch)", i)
		}
	}

	// The anchor and link checks above cannot see a run truncated at its OLDEST
	// end: such a run is still anchored at parentHash and still correctly
	// linked, but the median comes out of a narrower window and stops being the
	// consensus one. svnode cannot express that state — GetMedianTimePast walks
	// pprev pointers, so its window is shorter than MedianTimeBlocks only when
	// the chain itself ends at genesis. Re-establish that here, matching
	// model.Block CheckHeaderContextual: a short run is legitimate only when its
	// oldest header is genesis. On the batched path the error sends the caller to
	// walkParentChain, which walks by hash — immune to the batched query's race — and
	// stops at genesis, so it returns either the full window or a genuinely
	// genesis-terminated one that this same carve-out then accepts.
	if uint64(len(headers)) < blockchain.MedianTimeBlocks {
		oldest := headers[len(headers)-1]
		if oldest.HashPrevBlock == nil || !oldest.HashPrevBlock.IsEqual(&chainhash.Hash{}) {
			return 0, errors.NewProcessingError("parent-chain run holds only %d of %d headers and does not reach genesis, so its median is not the consensus median-time-past", len(headers), blockchain.MedianTimeBlocks)
		}
	}

	timestamps := make([]uint32, len(headers))
	for i, h := range headers {
		timestamps[i] = h.Timestamp
	}

	sort.Slice(timestamps, func(i, j int) bool { return timestamps[i] < timestamps[j] })

	return timestamps[len(timestamps)/2], nil
}

// createUtxos creates all the utxos for the transactions in the block in parallel
// before any spending is done. This only occurs in legacy mode when we assume the
// block is valid.
func (sm *SyncManager) createUtxos(ctx context.Context, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper], bi blockIdent, blockID uint32, outpointOnly bool) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "createUtxos",
		tracing.WithLogMessage(sm.logger, "[createUtxos] called for block %s / height %d", bi.hash, bi.height),
		tracing.WithHistogram(prometheusLegacyNetsyncCreateUtxos),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in createUtxos: %v", r, err)
		}

		deferFn(err)
	}()

	storeBatcherSize := sm.settings.Legacy.StoreBatcherSize
	storeBatcherConcurrency := sm.settings.Legacy.StoreBatcherConcurrency

	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(sm.logger, g, storeBatcherSize*storeBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

	blockHeightUint32 := bi.height

	// Below-checkpoint outpoint-only fast path: do a minimal create (compute meta
	// with fee=0, persist per-input parent script/satoshis as empty/zero) while
	// still retaining every output and per-input outpoint. Inputs were never
	// decorated on the gated path, so a full create would store empty parent data
	// anyway; WithSkipExtendedInputs makes that explicit and skips the fee/GetFees
	// work. Default OFF: when closed, baseOpts is empty and create is unchanged.
	var baseOpts []utxo.CreateOption
	if outpointOnly {
		baseOpts = append(baseOpts, utxo.WithSkipExtendedInputs(true))
	}

	// Track txs that already exist in the store so we can merge our blockID into their
	// BlockIDs after the Create pass. The quickValidation fast path skips the async
	// setTxMinedStatus step entirely (AddBlock with MinedSet=true), so any tx that
	// pre-existed without our blockID (propagation, prior crashed attempt, or the
	// pre-fast-path subtreeValidation route) would otherwise stay with empty/wrong
	// BlockIDs and fail descendant blocks with "has no block IDs".
	var (
		existingTxsMu    sync.Mutex
		existingTxHashes []*chainhash.Hash
	)

	// create all the utxos first
	for _, txHash := range txMap.Keys() {
		txHash := txHash

		g.Go(func() error {
			txWrapper, ok := txMap.Get(txHash)
			if !ok {
				return errors.NewProcessingError(txNotFoundInTxMapMsg, txHash.String())
			}

			createOpts := append(baseOpts[:len(baseOpts):len(baseOpts)],
				utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{
					BlockID:     blockID,
					BlockHeight: blockHeightUint32,
					SubtreeIdx:  0, // legacy path produces a single subtree at index 0
				}),
				utxo.WithCreateOnly(),
			)

			if _, _, err := sm.utxoStore.SpendAndCreate(gCtx, txWrapper.Tx, blockHeightUint32, createOpts...); err != nil {
				if errors.Is(err, errors.ErrTxExists) {
					existingTxsMu.Lock()
					existingTxHashes = append(existingTxHashes, &txHash)
					existingTxsMu.Unlock()
					return nil
				}
				return err
			}

			return nil
		})
	}

	// wait for all utxos to be created
	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("failed to create utxos", err)
	}

	// Merge our blockID into any tx that already existed. Without this, those txs
	// keep their stale (or empty) BlockIDs and the next block's validOrderAndBlessed
	// check fails in model/Block.go getParentTxMetaBlockIDs.
	// Chunked via the shared helper (issue 936): a single SetMinedMulti call with every
	// existing tx in the block overruns the aerospike client connection pool on fat
	// blocks (e.g. mainnet 755880 = 2.87M txs).
	if len(existingTxHashes) > 0 {
		minedBlockInfo := utxo.MinedBlockInfo{
			BlockID:        blockID,
			BlockHeight:    blockHeightUint32,
			SubtreeIdx:     0,
			OnLongestChain: true,
		}

		if err = utxo.SetMinedMultiChunked(ctx, sm.logger, sm.utxoStore, existingTxHashes, minedBlockInfo,
			sm.settings.UtxoStore.MaxMinedBatchSize, sm.settings.UtxoStore.MaxMinedRoutines); err != nil {
			return err
		}
	}

	return nil
}

// reuseBlockIDFromUTXO returns an already-recorded block id for this block by
// reading the BlockIDs of its first non-coinbase transaction from the UTXO
// store. This recovers the id after a restart (when the blockchain service's
// in-memory reservation is gone) or when another ingestion path created the
// UTXOs first, keeping the committed block id consistent with the UTXO
// mined-info. Returns ok=false when nothing is recorded yet.
func (sm *SyncManager) reuseBlockIDFromUTXO(ctx context.Context, bi blockIdent, txOrder []chainhash.Hash) (uint32, bool) {
	if len(txOrder) < 2 { // index 0 is the coinbase; need a real tx
		return 0, false
	}
	// Use the same tx-hash key shape the UTXO store is keyed by. createUtxos
	// creates entries from bt.Tx via its TxIDChainHash; the txOrder hashes and
	// bt.Tx.TxIDChainHash() both resolve to the same 32-byte txid as
	// chainhash.Hash from github.com/bsv-blockchain/go-bt/v2/chainhash
	// — the type utxoStore.Get expects.
	// We trust BlockIDs[0] as this block's id: the first non-coinbase tx of a
	// block is created by that block during sync, so the first recorded mined-in
	// id is this block's. (blockvalidation's quick path keys the same recovery on
	// its first non-coinbase tx — keep the two in sync if this assumption changes.)
	meta, err := sm.utxoStore.Get(ctx, &txOrder[1], fields.BlockIDs)
	if err != nil || meta == nil || len(meta.BlockIDs) == 0 {
		return 0, false
	}
	if len(meta.BlockIDs) > 1 {
		// Normal sync records exactly one mined-in block per fresh tx. More than
		// one means this tx is referenced by multiple blocks (reorg / re-mine), so
		// BlockIDs[0] may not be THIS block — surface it loudly rather than risk a
		// silent mis-assignment that could re-create the phantom-id wedge.
		sm.logger.Warnf("[reuseBlockIDFromUTXO] tx %s has %d mined-in block ids %v; reusing [0] for block %s — verify if sync stalls",
			txOrder[1].String(), len(meta.BlockIDs), meta.BlockIDs, bi.hash.String())
	}
	return meta.BlockIDs[0], true
}

// PreValidateTransactions pre-validates all the transactions in the block before
// sending them to subtree validation.
//
// candidateBlockTime and candidateParentMedianTime are paired finality-time
// sources for the consensus path inside the validator (SkipPolicyChecks=true):
// the former is consumed only when blockHeight < CSVHeight (bitcoin-sv's pre-
// BIP113 ContextualCheckBlock at src/validation.cpp:6020-6022), the latter
// only when blockHeight >= CSVHeight (bitcoin-sv's post-BIP113 path at
// src/validation.cpp:6001). The caller passes the one matching this block's
// era and zeroes the other.
func (sm *SyncManager) PreValidateTransactions(ctx context.Context, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	blockHash chainhash.Hash, blockHeight uint32, candidateBlockTime uint32, candidateParentMedianTime uint32, outpointOnly bool, failClosed bool) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "PreValidateTransactions",
		tracing.WithLogMessage(sm.logger, "[PreValidateTransactions] called for block %s / height %d", blockHash, blockHeight),
		tracing.WithHistogram(prometheusLegacyNetsyncPreValidateTransactions),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in PreValidateTransactions: %v", r, err)
		}

		deferFn(err)
	}()

	spendBatcherSize := sm.settings.Legacy.SpendBatcherSize
	spendBatcherConcurrency := sm.settings.Legacy.SpendBatcherConcurrency
	concurrencyLimit := spendBatcherSize * spendBatcherConcurrency

	// Pre-warm the MTP store once before spawning per-transaction goroutines, so each goroutine
	// can read mtpStore[h] without locking and without making gRPC calls.
	if err = sm.validationClient.EnsureMTPLoaded(ctx, blockHeight); err != nil {
		return err
	}

	// Below-checkpoint outpoint-only fast path: validate spends by outpoint only,
	// without decorated inputs. WithOutpointOnlySpend requires WithSkipScriptValidation
	// (already set unconditionally below for this checkpoint-covered path), satisfying
	// the validator's C1 precondition. Default OFF: when closed the spend path is
	// unchanged (full UTXO-hash-checked spend with whatever decoration was applied).
	// outpointOnly is computed once per block by the caller and threaded in.

	// These transactions arrive as part of a block, so they should be treated as valid
	// transactions that all need to be processed. If one fails (e.g. transient Aerospike
	// DEVICE_OVERLOAD), rolling back or cancelling all other independent transactions
	// in the block makes no sense. We retry failed transactions with backoff to adapt
	// to whatever throughput the storage backend can handle.
	const maxRetries = 10
	const retryBackoff = 2 * time.Second

	pendingTxHashes := txMap.Keys()
	totalTxCount := txMap.Length()

	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return errors.NewProcessingError("[PreValidateTransactions] context cancelled")
		}

		if attempt > 0 {
			sm.logger.Infof("[PreValidateTransactions] retry %d/%d: %d of %d transactions remaining",
				attempt, maxRetries, len(pendingTxHashes), totalTxCount)
			time.Sleep(retryBackoff)
		}

		g, _ := errgroup.WithContext(ctx)
		util.SafeSetLimit(sm.logger, g, concurrencyLimit)

		var (
			mu           sync.Mutex
			retryableTxs []chainhash.Hash
			lastErr      error
			hardFail     error
		)

		for _, txHash := range pendingTxHashes {
			txHash := txHash

			g.Go(func() (err error) {
				timeStart := time.Now()
				defer func() {
					prometheusLegacyNetsyncBlockTxValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
				}()

				txWrapper, ok := txMap.Get(txHash)
				if !ok {
					// Not found in txMap — non-recoverable, fail immediately
					mu.Lock()
					hardFail = errors.NewProcessingError(txNotFoundInTxMapMsg, txHash.String())
					mu.Unlock()
					return nil
				}

				validateOpts := []validator.Option{
					validator.WithSkipUtxoCreation(true),
					validator.WithAddTXToBlockAssembly(false),
					validator.WithSkipPolicyChecks(true),
					validator.WithInBlock(true),
					validator.WithSkipTxMetaPublishing(true),
					// PreValidateTransactions is only reached via the quickValidationMode
					// path (see prepareSubtrees → ValidateTransactionsLegacyMode), which
					// runs only when the block height is at or below the highest
					// hard-coded checkpoint. PoW + checkpoint linkage establish the chain
					// as canonical, so re-running BDK scripts is pure overhead.
					validator.WithSkipScriptValidation(true),
					validator.WithCandidateBlockTime(candidateBlockTime),
					validator.WithCandidateParentMedianTime(candidateParentMedianTime),
				}
				if !failClosed {
					// Default (flag OFF): the validator may create a conflicting txMeta on a
					// spend clash, which is later written as a conflicting subtree node and
					// reconciled by block assembly. On the fail-closed path this option is
					// dropped so the validator instead returns a non-retryable ErrSpent/
					// ErrTxConflicting (joined into a ProcessingError wrapping ErrUtxoError),
					// which hard-fails the block before AddBlock — no conflicting node is ever
					// written below the checkpoint.
					validateOpts = append(validateOpts, validator.WithCreateConflicting(true))
				}
				if outpointOnly {
					validateOpts = append(validateOpts, validator.WithOutpointOnlySpend(true))
				}

				if _, validateErr := sm.validationClient.Validate(ctx,
					txWrapper.Tx,
					blockHeight,
					validateOpts...,
				); validateErr != nil {
					// ErrTxConflicting is expected during legacy catchup when the UTXO store
					// has stale spending data. The block is confirmed, so its transactions
					// take precedence — the conflict will be resolved by ProcessConflicting
					// during block acceptance.
					//
					// On the fail-closed path this swallow is unreachable: dropping
					// WithCreateConflicting above means the validator never takes the
					// conflicting-create branch, so a clash surfaces as a non-retryable
					// ErrSpent/ErrTxConflicting that must reach hardFail below. The swallow is
					// therefore gated on !failClosed both to preserve today's behaviour when
					// the flag is off AND so a future refactor that re-introduces a conflict
					// path cannot silently swallow a genuine below-checkpoint conflict.
					if !failClosed && errors.Is(validateErr, errors.ErrTxConflicting) {
						return nil
					}

					if errors.IsRetryableError(validateErr) {
						mu.Lock()
						retryableTxs = append(retryableTxs, txHash)
						lastErr = validateErr
						mu.Unlock()
					} else {
						mu.Lock()
						hardFail = validateErr
						mu.Unlock()
					}
				}

				return nil
			})
		}

		_ = g.Wait()

		if hardFail != nil {
			return errors.NewProcessingError("[PreValidateTransactions] non-retryable error", hardFail)
		}

		if len(retryableTxs) == 0 {
			if attempt > 0 {
				sm.logger.Infof("[PreValidateTransactions] all transactions succeeded after %d retries", attempt)
			}
			return nil
		}

		// No progress since last attempt — stop retrying
		if attempt > 0 && len(retryableTxs) >= len(pendingTxHashes) {
			return errors.NewProcessingError("[PreValidateTransactions] %d of %d transactions failed with no progress, giving up",
				len(retryableTxs), totalTxCount, lastErr)
		}

		pendingTxHashes = retryableTxs
	}

	return errors.NewProcessingError("[PreValidateTransactions] %d of %d transactions still failing after %d retries",
		len(pendingTxHashes), totalTxCount, maxRetries)
}

// classifyAndCountPrewarmError routes a validator error from the pre-warm path
// (validateTransactions) into the prometheusLegacyNetsyncPrewarmErrors counter
// and emits a log line at the level appropriate for the class. Pre-warm errors
// are intentionally dropped — real subtree validation runs later and catches
// consensus violations on its own — so this helper exists purely to give ops
// observability into a path that previously silently swallowed every error
// (see issue #4590).
func classifyAndCountPrewarmError(logger ulogger.Logger, err error) {
	switch {
	case errors.Is(err, errors.ErrTxInvalid):
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("tx_invalid").Inc()
		logger.Errorf("[validateTransactions][prewarm] critical: tx invalid: %v", err)
	case errors.Is(err, errors.ErrServiceError):
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("service").Inc()
		logger.Warnf("[validateTransactions][prewarm] service error (transient): %v", err)
	case errors.Is(err, errors.ErrTxConflicting), errors.Is(err, errors.ErrTxExists):
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("policy").Inc()
		logger.Debugf("[validateTransactions][prewarm] expected: %v", err)
	case errors.Is(err, errors.ErrProcessing):
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("processing").Inc()
		logger.Warnf("[validateTransactions][prewarm] processing error: %v", err)
	default:
		prometheusLegacyNetsyncPrewarmErrors.WithLabelValues("other").Inc()
		logger.Warnf("[validateTransactions][prewarm] unclassified: %v", err)
	}
}

// validateTransactions validates all the transactions in the block in parallel
// per level. This is done to speed up subtree validation later on.
// The levels indicate the number of parents in the block.
func (sm *SyncManager) validateTransactions(ctx context.Context, maxLevel uint32, blockTxsPerLevel map[uint32][]*bt.Tx, bi blockIdent) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "validateTransactions",
		tracing.WithLogMessage(sm.logger, "[validateTransactions] called for block %s / height %d", bi.hash, bi.height),
		tracing.WithHistogram(prometheusLegacyNetsyncValidateTransactions),
	)

	blockHeightUint32 := bi.height

	candidateBlockTime, candidateParentMedianTime, err := sm.candidateFinalityTimesForBlock(ctx, bi)
	if err != nil {
		return errors.NewProcessingError("[validateTransactions] failed to select finality time sources", err)
	}

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in validateTransactions: %v", r, err)
		}

		deferFn(err)
	}()

	spendBatcherSize := sm.settings.Legacy.SpendBatcherSize
	spendBatcherConcurrency := sm.settings.Legacy.SpendBatcherConcurrency

	var timeStart time.Time

	if err = sm.validationClient.EnsureMTPLoaded(ctx, blockHeightUint32); err != nil {
		return err
	}

	// try to pre-validate the transactions through the validation, to speed up subtree validation later on.
	// This allows us to process all the transactions in parallel. The levels indicate the number of parents in the block.
	for i := uint32(0); i <= maxLevel; i++ {
		_, _, deferLevelFn := tracing.Tracer("netsync").Start(ctx, fmt.Sprintf("validateTransactions:level:%d", i))

		if len(blockTxsPerLevel[i]) < 10 {
			// if we have less than 10 transactions on a certain level, we can process them immediately by triggering the batcher
			for txIdx := range blockTxsPerLevel[i] {
				timeStart = time.Now()

				if _, validateErr := sm.validationClient.Validate(ctx, blockTxsPerLevel[i][txIdx], blockHeightUint32, validator.WithSkipPolicyChecks(true), validator.WithInBlock(true), validator.WithCandidateBlockTime(candidateBlockTime), validator.WithCandidateParentMedianTime(candidateParentMedianTime)); validateErr != nil {
					classifyAndCountPrewarmError(sm.logger, validateErr)
				}

				prometheusLegacyNetsyncBlockTxValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
			}

			sm.validationClient.TriggerBatcher()
		} else {
			// process all the transactions on a certain level in parallel
			g, gCtx := errgroup.WithContext(ctx)
			util.SafeSetLimit(sm.logger, g, spendBatcherSize*spendBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

			for txIdx := range blockTxsPerLevel[i] {
				txIdx := txIdx

				g.Go(func() error {
					timeStart := time.Now()
					defer func() {
						prometheusLegacyNetsyncBlockTxValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
					}()

					// send to validation, but only if the parent is not in the same block
					if _, validateErr := sm.validationClient.Validate(gCtx, blockTxsPerLevel[i][txIdx], blockHeightUint32, validator.WithSkipPolicyChecks(true), validator.WithInBlock(true), validator.WithCandidateBlockTime(candidateBlockTime), validator.WithCandidateParentMedianTime(candidateParentMedianTime)); validateErr != nil {
						classifyAndCountPrewarmError(sm.logger, validateErr)
					}

					return nil
				})
			}

			// we don't care about errors here, we are just pre-warming caches for a quicker subtree validation
			_ = g.Wait()

			deferLevelFn()
		}
	}

	return nil
}

func (sm *SyncManager) extendTransactions(ctx context.Context, bi blockIdent, txOrder []chainhash.Hash, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper], outpointOnly bool) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "extendTransactions",
		tracing.WithLogMessage(sm.logger, "[extendTransactions] called for block %s / height %d", bi.hash, bi.height),
		tracing.WithHistogram(prometheusLegacyNetsyncExtendTransactions),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in extendTransactions: %v", r, err)
		}

		deferFn(err)
	}()

	outpointBatcherSize := sm.settings.Legacy.OutpointBatcherSize

	// Phase 1: populate inputs whose parents are same-block transactions. These are
	// served directly from the in-memory txMap, so no DB work is needed here. We run
	// per-tx goroutines (bounded by OutpointBatcherSize) because each tx's own inputs
	// are populated independently; this phase reads same-block parent outputs
	// immediately and does not wait for the parent transaction to be extended first.
	g, gCtx := errgroup.WithContext(ctx)
	util.SafeSetLimit(sm.logger, g, outpointBatcherSize)

	// Blocks always include a coinbase, but guard against 0-tx edge cases
	// (malformed/test blocks) where len-1 would produce a negative capacity.
	txCount := len(txOrder)
	txCapacity := 0
	if txCount > 0 {
		txCapacity = txCount - 1
	}
	txs := make([]*bt.Tx, 0, txCapacity)

	for idx, txHash := range txOrder {
		if idx == 0 {
			// skip the coinbase transaction, as it cannot be extended
			continue
		}

		// the coinbase transaction is not part of the txMap
		txWrapper, found := txMap.Get(txHash)
		if !found {
			return errors.NewTxError(txNotFoundInTxMapMsg, txHash.String())
		}

		tx := txWrapper.Tx
		txs = append(txs, tx)

		g.Go(func() error {
			if err := sm.extendFromTxMap(gCtx, tx, txMap); err != nil {
				return errors.NewTxError("failed to extend transaction from txMap", err)
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("failed to extend transactions from txMap", err)
	}

	// Phase 2: for inputs whose parents are NOT same-block, batch the decoration
	// using the store's internal chunking instead of issuing one DB lookup per tx.
	// For a 20k-tx block this collapses ~20k round-trips into roughly O(N / chunkSize).
	//
	// BatchPreviousOutputsDecorate skips inputs that already have PreviousTxScript set,
	// so Phase 1's work is preserved. If it returns a processing/not-found error the
	// most likely cause is a parent that's been pruned (DAH'd) because the child
	// already had a prior processing pass. Fall back to per-tx decoration so the
	// existing recovery path (utxoStore.Get on the child itself) can still kick in.
	//
	// Below-checkpoint outpoint-only fast path: the dominant DB cost of legacy IBD
	// is this bulk decorate (one chunked round-trip set per block to fetch parent
	// scripts/satoshis). On the gated path the spend uses outpoint-only validation
	// and the create is minimal (inputs-only), so parent scripts/satoshis are never
	// needed — skip the decorate and its per-tx fallback entirely. Phase 1's in-block
	// extension above still runs (no DB reads); out-of-block inputs are simply left
	// undecorated. Default OFF: when the gate is closed this is the unchanged path.
	if outpointOnly {
		return nil
	}

	if batchErr := sm.utxoStore.BatchPreviousOutputsDecorate(ctx, txs); batchErr != nil {
		if errors.Is(batchErr, errors.ErrProcessing) || errors.Is(batchErr, errors.ErrTxNotFound) {
			return sm.extendPerTxFallback(ctx, txs)
		}
		return errors.NewProcessingError("failed to batch-decorate previous outputs", batchErr)
	}

	return nil
}

// extendFromTxMap populates a transaction's inputs whose parents are in the same
// block (available via txMap). Parent Outputs are populated at wire-parse time
// and never mutated afterwards, so they can be read immediately without waiting
// for the parent's own inputs to be extended.
//
// Inputs whose parents are not in txMap are left for a later bulk DB lookup (see
// extendTransactions phase 2).
func (sm *SyncManager) extendFromTxMap(ctx context.Context, tx *bt.Tx, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) error {
	defer func() {
		prometheusLegacyNetsyncBlockTxSize.Observe(float64(tx.Size()))
		prometheusLegacyNetsyncBlockTxNrInputs.Observe(float64(len(tx.Inputs)))
		prometheusLegacyNetsyncBlockTxNrOutputs.Observe(float64(len(tx.Outputs)))
		// NOTE: no per-tx extend-duration metric is observed here. This function is
		// phase 1 only (same-block parents from txMap); phase 2 (bulk DB decoration
		// via BatchPreviousOutputsDecorate) runs block-wide in extendTransactions.
		// Observing a tx-level duration here would under-report end-to-end extend
		// cost versus the old per-tx DB path. We could revisit by adding a
		// block-level phase-2 histogram if dashboards need it.
	}()

	txWrapper, found := txMap.Get(*tx.TxIDChainHash())
	if !found {
		return errors.NewProcessingError("tx %s not found in txMap", tx.TxIDChainHash())
	}

	// The per-input work here is trivial (bounds check + two field assignments),
	// and extendTransactions already parallelises across transactions up to
	// Legacy.OutpointBatcherSize (default 1024). Spawning another goroutine per
	// input would multiply concurrency into the tens of thousands for large
	// blocks with negligible wall-clock benefit. Process inputs synchronously.
	for i, input := range tx.Inputs {
		// Honour caller-initiated cancellation between inputs.
		if err := ctx.Err(); err != nil {
			return err
		}

		prevTxHash := *input.PreviousTxIDChainHash()

		prevTxWrapper, found := txMap.Get(prevTxHash)
		if !found {
			// Parent lives outside this block — phase 2 will decorate it via the batch DB call.
			continue
		}

		// Flag the child tx as having at least one in-block parent (used by
		// downstream bookkeeping). Safe to set repeatedly from this single
		// goroutine.
		txWrapper.SomeParentsInBlock = true

		// A malformed/hostile block could carry a wrapper without a parsed
		// parent transaction; fail with a TxInvalidError instead of panicking
		// on the dereferences below.
		if prevTxWrapper.Tx == nil {
			return errors.NewTxInvalidError("tx %s input %d references missing previous transaction %s",
				tx.TxIDChainHash(), i, prevTxHash)
		}

		if input.PreviousTxOutIndex >= uint32(len(prevTxWrapper.Tx.Outputs)) {
			return errors.NewTxInvalidError("tx %s input %d references out-of-range output %d on parent %s (has %d outputs)",
				tx.TxIDChainHash(), i, input.PreviousTxOutIndex, prevTxHash, len(prevTxWrapper.Tx.Outputs))
		}

		prevOutput := prevTxWrapper.Tx.Outputs[input.PreviousTxOutIndex]
		if prevOutput == nil || prevOutput.LockingScript == nil {
			return errors.NewTxInvalidError("tx %s input %d previous output %d is nil or has nil locking script (parent %s)",
				tx.TxIDChainHash(), i, input.PreviousTxOutIndex, prevTxHash)
		}

		// Parent's Outputs are populated at wire-parse time and never mutated
		// afterwards, so we can read them immediately without waiting for the
		// parent tx itself to finish being extended. The old implementation
		// polled on prevTxWrapper.Tx.IsExtended(); that was unnecessary
		// (IsExtended checks the parent's *inputs*, not its outputs) and caused
		// a deadlock under the two-phase flow in extendTransactions, where a
		// pure-non-local-parent tx only becomes "extended" after phase 2 runs.
		tx.Inputs[i].PreviousTxSatoshis = prevOutput.Satoshis
		tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(*prevOutput.LockingScript)
	}

	return nil
}

// extendPerTxFallback runs the original per-tx decoration path. It is invoked only
// when BatchPreviousOutputsDecorate fails with a missing-parent / processing error;
// the per-tx path additionally tries `utxoStore.Get(txHash, fields.Tx)` to recover
// from DAH'd parents that the child itself has already been processed with.
func (sm *SyncManager) extendPerTxFallback(ctx context.Context, txs []*bt.Tx) error {
	for _, tx := range txs {
		if err := sm.utxoStore.PreviousOutputsDecorate(ctx, tx); err != nil {
			if errors.Is(err, errors.ErrProcessing) || errors.Is(err, errors.ErrTxNotFound) {
				txMeta, metaErr := sm.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
				if metaErr == nil && txMeta != nil && txMeta.Tx != nil {
					if len(txMeta.Tx.Inputs) != len(tx.Inputs) {
						return errors.NewProcessingError("recovered tx %s has %d inputs but expected %d",
							tx.TxIDChainHash(), len(txMeta.Tx.Inputs), len(tx.Inputs))
					}
					for i, input := range txMeta.Tx.Inputs {
						tx.Inputs[i].PreviousTxSatoshis = input.PreviousTxSatoshis
						tx.Inputs[i].PreviousTxScript = input.PreviousTxScript
					}
					continue
				}
			}
			return errors.NewProcessingError("failed to decorate previous outputs for tx %s", tx.TxIDChainHash(), err)
		}
	}
	return nil
}

// createSubtrees fills the supplied subtree slices in order with the block's
// non-coinbase transactions, advancing to the next slice whenever the current
// one is complete. Subtree 0's first node is the coinbase placeholder (added
// by prepareSubtrees) so the per-slice fill count is subtreeSize-1 leaves for
// subtree 0 and subtreeSize for subsequent subtrees (subject to the final
// subtree's smaller capacity).
func (sm *SyncManager) createSubtrees(ctx context.Context, bi blockIdent, txOrder []chainhash.Hash, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	subtreeSlices []*subtreepkg.Subtree, subtreeDatas []*subtreepkg.Data, subtreeMetas []*subtreepkg.Meta, outpointOnly bool) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "createSubtrees",
		tracing.WithLogMessage(sm.logger, "[createSubtrees] called for block %s / height %d", bi.hash, bi.height),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in createSubtrees: %v", r, err)
		}

		deferFn(err)
	}()

	currentSubtreeIdx := 0

	// Below-checkpoint outpoint-only fast path: inputs are never decorated on the
	// gated path, so calculateTransactionFee (which requires extended inputs) would
	// fail. Subtree fees are not consensus-checked below the highest hard-coded
	// checkpoint, so stamp 0 and skip the fee calculation entirely. Default OFF:
	// when the gate is closed the real fee is computed exactly as before.
	// outpointOnly is computed once per block by the caller and threaded in.

	for _, txHash := range txOrder {
		// the coinbase transaction is not part of the txMap
		txWrapper, found := txMap.Get(txHash)
		if !found {
			continue
		}

		tx := txWrapper.Tx

		// Advance to the next subtree slot if the current one is full.
		for currentSubtreeIdx < len(subtreeSlices) && subtreeSlices[currentSubtreeIdx].IsComplete() {
			currentSubtreeIdx++
		}

		if currentSubtreeIdx >= len(subtreeSlices) {
			return errors.NewSubtreeError("[createSubtrees] no subtree slot remaining for tx %s", txHash.String())
		}

		subtree := subtreeSlices[currentSubtreeIdx]
		subtreeData := subtreeDatas[currentSubtreeIdx]
		subtreeMeta := subtreeMetas[currentSubtreeIdx]

		txSize, err := safeconversion.IntToUint64(tx.Size())
		if err != nil {
			return err
		}

		var fee uint64
		if !outpointOnly {
			fee, err = calculateTransactionFee(tx)
			if err != nil {
				return err
			}
		}

		if err = subtree.AddNode(txHash, fee, txSize); err != nil {
			return errors.NewTxError("failed to add node (%s) to subtree", txHash, err)
		}

		nodeIdx := subtree.Length() - 1

		if err = subtreeData.AddTx(tx, nodeIdx); err != nil {
			return errors.NewTxError("failed to add tx to subtree data", err)
		}

		if err = subtreeMeta.SetTxInpointsFromTx(tx); err != nil {
			return errors.NewTxError("failed to add tx to subtree meta data", err)
		}
	}

	sm.logger.Infof("[createSubtrees] created %d subtrees for block %s / height %d", len(subtreeSlices), bi.hash, bi.height)

	return nil
}

func calculateTransactionFee(tx *bt.Tx) (uint64, error) {
	// Calculate the fees of this transaction
	// we do this with a signed int, to prevent overflow in case of invalid fees
	inputValue := uint64(0)
	outputValue := uint64(0)

	if tx == nil {
		return 0, errors.NewTxError("transaction is nil")
	}

	// can only calculate fees for extended transactions
	if !tx.IsExtended() { // block height not used
		return 0, errors.NewTxError("transaction %s is not extended", tx.TxIDChainHash())
	}

	// We don't need to check for coinbase transactions, as they have no inputs
	if !tx.IsCoinbase() {
		// Calculate the fees of this transaction
		// We don't need to check for coinbase transactions, as they have no inputs
		for _, input := range tx.Inputs {
			inputValue += input.PreviousTxSatoshis
		}

		for _, output := range tx.Outputs {
			outputValue += output.Satoshis
		}

		if inputValue < outputValue {
			return 0, errors.NewTxError("transaction %s has invalid fees: %d (input: %d, output: %d)", tx.TxIDChainHash(), inputValue-outputValue, inputValue, outputValue)
		}
	}

	return inputValue - outputValue, nil
}

// createTxMap converts every transaction in the block to an arena-independent
// bt.Tx in txMap (coinbase excluded) and returns the block-order tx hash list
// (coinbase included at index 0). The returned order is what the downstream
// stages iterate instead of block.Transactions(), so this is the last function
// that needs the decoded wire block.
func (sm *SyncManager) createTxMap(ctx context.Context, block *bsvutil.Block, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) ([]chainhash.Hash, error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "createTxMap",
		tracing.WithDebugLogMessage(
			sm.logger,
			"[createTxMap][%s %d] processing transactions into map for block",
			block.Hash().String(),
			block.Height(),
		),
	)
	defer deferFn()

	txOrder := make([]chainhash.Hash, 0, len(block.Transactions()))

	for _, wireTx := range block.Transactions() {
		// Copy the hash value out of the bsvutil.Tx wrapper. bt.Tx.SetTxHash
		// stores the pointer, so passing wireTx.Hash() directly would keep
		// the wrapping wire.MsgTx (and its decode arena) alive through this
		// bt.Tx and the TxMapWrapper it lands in.
		hashCopy := *wireTx.Hash()
		txOrder = append(txOrder, hashCopy)

		tx := &bt.Tx{}

		if err := WireTxToGoBtTx(wireTx, tx); err != nil {
			return nil, errors.NewProcessingError("failed to convert wire.Tx to bt.Tx", err)
		}

		// don't add the coinbase to the txMap, we cannot process it anyway
		if !tx.IsCoinbase() {
			tx.SetTxHash(&hashCopy)
			txMap.Set(hashCopy, &TxMapWrapper{Tx: tx})
		}
	}

	return txOrder, nil
}

// WireTxToGoBtTx converts a wire.Tx to a bt.Tx.
//
// Script bytes are *copied* (not aliased) so the resulting bt.Tx is fully
// independent of the source wire.MsgBlock's decode arena. This is the
// load-bearing fix for legacy-sync GC pressure: aliasing kept the arena's
// 4 MiB chunks reachable for the entire downstream pipeline lifetime
// (subtree prep, validation, persistence, the orphan goroutine below), so
// arenas piled up across all in-flight blocks and dominated the live heap.
// Copying lets the arena and its containing MsgBlock be reclaimed as soon
// as this function (and any other consumers in HandleBlockDirect) returns.
func WireTxToGoBtTx(wireTx *bsvutil.Tx, tx *bt.Tx) error {
	wTx := wireTx.MsgTx()

	tx.Version = uint32(wTx.Version) //nolint:gosec
	tx.LockTime = wTx.LockTime

	tx.Inputs = make([]*bt.Input, len(wTx.TxIn))
	for i, in := range wTx.TxIn {
		tx.Inputs[i] = &bt.Input{
			UnlockingScript:    &bscript.Script{},
			PreviousTxOutIndex: in.PreviousOutPoint.Index,
			SequenceNumber:     in.Sequence,
		}
		_ = tx.Inputs[i].PreviousTxIDAdd(&in.PreviousOutPoint.Hash)
		*tx.Inputs[i].UnlockingScript = bytes.Clone(in.SignatureScript)
	}

	tx.Outputs = make([]*bt.Output, len(wTx.TxOut))
	for i, out := range wTx.TxOut {
		tx.Outputs[i] = &bt.Output{
			Satoshis:      uint64(out.Value),
			LockingScript: &bscript.Script{},
		}
		*tx.Outputs[i].LockingScript = bytes.Clone(out.PkScript)
	}

	return nil
}
