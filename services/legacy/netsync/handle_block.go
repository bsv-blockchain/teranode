package netsync

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockvalidation"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/blockassemblyutil"
	"github.com/bsv-blockchain/teranode/util/retry"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// legacyCorruptPeerID is the serving-peer identity threaded into
// blockValidation.ProcessBlock for the block-validation corrupt cap
// (bitcoin-sv/teranode#4692).
//
// The two caps' keys intentionally differ only by the LegacyPeerIDPrefix namespace:
// netsync's own cap (recordCorruptBlockAttempt) keys on the bare peer.Addr(), while
// this value carries the prefix — both are still derived from the identical
// peer.Addr() call, so they still bound the same serving connection. The divergence
// exists solely so nothing downstream of blockvalidation can mistake this value for a
// libp2p peer ID: isLegacyPeerID (services/blockvalidation/peer_metrics_helpers.go)
// makes isPeerMalicious, penalizeCorruptBlockPeer and the invalid-block Kafka producer
// treat any LegacyPeerIDPrefix-prefixed value the same as an empty peerID, so it never
// reaches p2pClient.AddBanScore/IsPeerMalicious.
//
// Peer.Addr() dereferences the peer with no nil-receiver guard, so a nil peer degrades
// to the empty-peerID no-cap defence rather than panicking. That is also what a block
// recovered from disk gets: the park has no peer to charge, and charging the wrong one
// is worse than charging nobody.
func legacyCorruptPeerID(peer *peer.Peer) string {
	if peer == nil {
		return ""
	}

	return blockvalidation.LegacyPeerIDPrefix + peer.Addr()
}

// commitPreparedBlock checks a converted block's proof of work and commits it
// through ProcessBlock.
//
// The merkle root and the CVE-2012-2459 duplicate check are not repeated here,
// and that is not skipping them. The pipeline sink verified the merkle root
// against the header before the record was ever written (pipelineBlockSink),
// and the stream builder's duplicate map refuses a repeated transaction, so
// neither failure can reach a written record. Re-running either would need the
// transactions themselves, which is what this route exists not to read.
func (sm *SyncManager) commitPreparedBlock(ctx context.Context, teranodeBlock *model.Block, peerID string) error {
	// pre-check that there is enough proof of work on the block, before we do any other processing
	headerValid, _, err := teranodeBlock.Header.HasMetTargetDifficulty()
	if !headerValid {
		return errors.NewBlockInvalidError("invalid block header: %s", teranodeBlock.Header.Hash().String(), err)
	}

	// call the process block wrapper, which will add tracing and logging
	return sm.ProcessBlock(ctx, teranodeBlock, peerID)
}

// HandleConvertedBlock commits a block that was converted from wire bytes into
// a model.Block as it streamed off the socket (pipelineBlockSink), instead of
// being decoded from a whole block read back off disk. blk is that record,
// already carrying its header, coinbase, counts and subtree root hashes — see
// (*blockPark).ReadConverted.
//
// It is reached only from the two places that commit a block already sitting in
// the park: drainParkedDescendants (block_park_drain.go) and the dispatcher's
// parked worker (block_dispatcher.go). Neither ever has an in-flight parent to
// hand it: by the time anything commits a parked block, its parent is already in
// the chain, so the ordinary blockchain-store lookup below is always the right
// way to resolve its height. That is why this function takes no parent parameter
// at all, rather than a parameter callers must remember to pass as nil.
func (sm *SyncManager) HandleConvertedBlock(ctx context.Context, peer *peer.Peer, blockHash chainhash.Hash, blk *model.Block) (err error) {
	sm.logger.Debugf("[HandleConvertedBlock][%s] starting handling converted block", blockHash.String())

	// check whether this block already exists
	blockExists, err := sm.blockchainClient.GetBlockExists(ctx, &blockHash)
	if err != nil {
		sm.logger.Errorf("[HandleConvertedBlock][%s] failed to check if block exists: %s", blockHash.String(), err)
		return errors.NewProcessingError("failed to check if block exists", err)
	}

	if blockExists {
		sm.logger.Warnf("[HandleConvertedBlock][%s] block already exists", blockHash.String())
		return nil
	}

	// The validation this block is about to go through can run long, and without
	// this the sync peer looks stalled and gets rotated mid-commit.
	if sps, ok := sm.syncPeerStateFor(peer); ok {
		sps.updateLastBlockTime()
	}

	// pipelineBlockSink no longer refuses to convert a block above the final
	// checkpoint, or one whose parent height would not resolve at conversion
	// time — see its own doc comment. So this can no longer assert
	// legacyUnified(blk.Height): an above-checkpoint record reaching here is
	// now the ordinary case this task exists to make work, not a bug, and
	// blockID 0 is correct on both routes regardless of legacyUnified (Step 1
	// of this task's report: the ordinary route already passes zero above the
	// checkpoint, and zero is the universal "assign server-side" convention
	// full validation reads too).
	//
	// What still has to hold is the same "parent resolved" test the sink
	// itself runs — a record whose height was never resolved (the sentinel 0
	// pipelineParentHeight's fallback writes) needs correcting before
	// anything below trusts blk.Height. That test is answered by THIS lookup,
	// not a separate pipelineParentHeight call: a first version of this fix
	// called pipelineParentHeight here too, purely to evaluate the gate,
	// before making the identical GetBlockHeader call below to re-derive the
	// height — two store round trips for the same parent, on the single
	// goroutine that commits every block in order, which starving is this
	// node's own known stall mode. GetBlockHeader failing with
	// ErrBlockNotFound below IS "not resolved": by the time a commit is
	// attempted the parent is already required to be committed
	// (parentIsInChain, streaming_install.go, gates the drain that gets
	// here), so pipelineParentHeight's extra header-cache path (needed at
	// conversion time, when the parent may still only be in flight) answers
	// nothing here that this store call does not already answer.
	//
	// A ServiceError, not anything else, for the not-found case: parkCommitFailure
	// reads a ServiceError as parkDispositionRetryLater (keep the blob, no
	// rewind, no blame) rather than parkDispositionBlockRejected (delete the
	// only copy, rewind the cursor, blame the peer, and fail the block
	// forever at that height) — see pipeline_sink.go's own gate comment for
	// the rule this protects: never let a converted record reach a committer
	// that can refuse it on a condition the re-download would only
	// reproduce. (parkCommitFailure also has its own dedicated
	// errors.ErrBlockNotFound case, parkDispositionParentGone, which keeps
	// the blob just as this does; this still classifies explicitly rather
	// than relying on that fallback matching, so the classification here is
	// not a silent side effect of what error type happens to wrap what.)
	_, previousBlockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(ctx, blk.Header.HashPrevBlock)
	if err != nil {
		if errors.Is(err, errors.ErrBlockNotFound) {
			sm.logger.Debugf("[HandleConvertedBlock][%s] previous block %s not found (orphan/out-of-order; caller will request missing blocks): %v", blockHash.String(), blk.Header.HashPrevBlock, err)

			return errors.NewServiceError("[HandleConvertedBlock][%s] parent %s is not yet resolvable; retrying once it is", blockHash.String(), blk.Header.HashPrevBlock, err)
		}

		sm.logger.Errorf("[HandleConvertedBlock][%s] failed to get block header for previous block %s: %s", blockHash.String(), blk.Header.HashPrevBlock, err)

		return errors.NewProcessingError("failed to get block header for previous block %s", blk.Header.HashPrevBlock, err)
	}

	// derivedHeight is the store's own current, authoritative answer.
	//
	// blk.Height == 0 is pipelineParentHeight's own "unresolved" sentinel (see
	// its doc comment): it means this record's height was never really
	// resolved at conversion time, not that the block is genuinely at height
	// 0 (genesis is never received over the wire). Correcting it here, now
	// that the parent is required to be committed, is the re-derivation the
	// sink's own comment promises ("the committer re-derives the height from
	// the store before committing anything, so a record carrying zero is
	// corrected there") — without it, a record stuck at 0 would fail the
	// mismatch check below forever, since nothing else ever rewrites the
	// stored record.
	//
	// A ServiceError, not a BlockInvalidError, and deliberately so, for every
	// OTHER mismatch: this is a disagreement between two things THIS node
	// computed about its own chain view, not a claim the peer made. blk.Height
	// was resolved once already, at conversion time (pipelineParentHeight),
	// from whichever of the header cache or the store answered first; this
	// re-derives it from the store's CURRENT view of the same parent. A
	// mismatch means that view moved between conversion and commit — a
	// reorg, or the record simply going stale while it sat parked — not that
	// the block's own header chain or merkle root lied about anything, both
	// of which are checked elsewhere. parkCommitFailure has no case for a
	// plain ProcessingError-shaped BlockInvalidError here either, but the
	// eligibility assertion above already established the pattern this must
	// match: a ServiceError is IsTransientLocalError, which parkCommitFailure
	// reads as parkDispositionRetryLater (keep the blob, no rewind, no blame)
	// instead of parkDispositionBlockRejected (delete the only copy, rewind
	// the cursor, blame the peer, and fail the block at that height forever).
	// A converted record has no whole-block fallback to re-derive from, so
	// treating a local staleness as a bad block would destroy it for a
	// condition a retry, once this node's own view catches up, resolves
	// cleanly.
	derivedHeight := previousBlockHeaderMeta.Height + 1

	switch {
	case blk.Height == 0:
		blk.Height = derivedHeight
	case blk.Height != derivedHeight:
		return errors.NewServiceError("[HandleConvertedBlock][%s] block height %d is not the correct height for block %s, expected %d", blockHash.String(), blk.Height, blockHash, derivedHeight)
	}

	blockHeight := blk.Height

	// A block committed from the park after a restart, or drained by a worker
	// whose parked entry carries a nil peer, has no delivering peer at all.
	peerLabel := "recovered-from-disk"
	if peer != nil {
		peerLabel = peer.String()
	}

	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "HandleConvertedBlock",
		tracing.WithLogMessage(
			sm.logger,
			"[HandleConvertedBlock][%s %d] %d txs, peer %s",
			blk.Hash().String(),
			blockHeight,
			blk.TransactionCount,
			peerLabel,
		),
		tracing.WithTag("blockHash", blk.Hash().String()),
		tracing.WithTag("peer", peerLabel),
		tracing.WithHistogram(prometheusLegacyNetsyncHandleBlockDirect),
	)
	defer func() {
		prometheusLegacyNetsyncBlockHeight.Set(float64(blockHeight))

		deferFn(err)
	}()

	// Wait for block assembly to be ready.
	if err = blockassemblyutil.WaitForBlockAssemblyReady(ctx, sm.logger, sm.blockAssembly, blockHeight, sm.settings.BlockValidation.MaxBlocksBehindBlockAssembly); err != nil {
		if sm.windowRoute(blockHeight) {
			return errors.NewServiceError("[HandleConvertedBlock][%s] block assembly not ready for height %d on the window route", blockHash.String(), blockHeight, err)
		}

		return err
	}

	// Wait for the previous block's setTxMined to complete — see
	// needsParentMinedWait for the redundancy argument. Below the checkpoint,
	// on the outpoint-only fast path, that wait is skipped as redundant. Above
	// the checkpoint outpoint-only is not active, so the wait runs.
	//
	// The origin is re-read from the header cache rather than remembered from the
	// delivery that converted this record: a converted record can be committed
	// after a restart, where nothing about that delivery survives. An unprovable
	// record simply takes the wait, which is the safe direction — the wait costs
	// latency, skipping it wrongly costs ordering.
	if sm.needsParentMinedWait(sm.blockOrigin(blockHash), blockHeight) {
		if err = sm.waitForPreviousBlockMined(ctx, blk.Header.HashPrevBlock, blockHeight); err != nil {
			return err
		}
	}

	return sm.commitPreparedBlock(ctx, blk, legacyCorruptPeerID(peer))
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

func (sm *SyncManager) ProcessBlock(ctx context.Context, teranodeBlock *model.Block, peerID string) (err error) {
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
	// teranodeBlock.ID travels as a separate proto field in the gRPC request because
	// block.Bytes() does not serialize ID.
	if err = sm.blockValidation.ProcessBlock(ctx, teranodeBlock, teranodeBlock.Height, peerID, "legacy", teranodeBlock.ID); err != nil {
		if errors.Is(err, errors.ErrBlockExists) {
			sm.logger.Infof("[SyncManager:processBlock][%s %d] block already exists", teranodeBlock.Hash().String(), teranodeBlock.Height)
			return nil
		}

		return errors.NewProcessingError("failed to process block", err)
	}

	return nil
}

// quickValidationAllowed reports whether this block may skip script validation,
// subtree re-validation and the per-UTXO setTxMined cross-check because it
// belongs to the checkpoint-certified prefix of the chain.
//
// It requires TWO things, and height is the weaker of them:
//
//  1. the height is inside the certified prefix (model.BelowCheckpoint), and
//  2. the block was requested from a header run proven to terminate at a pinned
//     checkpoint hash (origin.headerProven).
//
// Conjunct 2 is the fix for GHSA-gggq-8f59-4jm9. The hardcoded checkpoints
// certify ONE CHAIN, not a height range, so height alone cannot establish that a
// block belongs to it: before this, a peer could advertise a fabricated block
// claiming any height in 1..945000, have it requested (the only
// unsolicited-block check on the path), and be granted checkpoint trust — script
// validation skipped, its transactions spending real UTXOs in the shared store.
//
// The proof is provenance rather than a chain query on purpose. handleHeadersMsg
// already verifies each header run links back to a block we trust and forward to
// a pinned checkpoint hash. fetchHeaderBlocks grants provenance only through the
// last matched checkpoint height; an appended, unverified tail carries no proof,
// even after the pending checkpoint advances. Consulting this proof needs no
// store lookup. Asking the blockchain store "is this block's parent on the main
// chain" cannot establish this proof: the block is not stored
// yet, and GetBlockHeadersFromHeight is deliberately fork-inclusive with no
// tie-break among equal heights, so it cannot answer the question being asked.
//
// Fail-closed: no chain params (nothing certified) or no provenance denies the
// fast path. A false negative costs slower but fully correct validation; a false
// positive lets an unauthenticated peer write forged spends.
func (sm *SyncManager) quickValidationAllowed(origin blockRequestOrigin, blockHeight uint32) bool {
	if sm.chainParams == nil {
		return false
	}

	if !origin.headerProven {
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
func (sm *SyncManager) legacyOutpointOnly(origin blockRequestOrigin, height uint32) bool {
	if sm.settings == nil {
		return false
	}

	// Same requirement as quickValidationAllowed: this path skips the decorate,
	// stamps fees as 0 and spends by outpoint alone, all of which are only sound
	// for a block proven to be in the certified prefix. model.OutpointOnlyEligible
	// supplies the settings/store/height conjuncts; provenance is the one it
	// cannot know about.
	if !origin.headerProven {
		return false
	}

	return model.OutpointOnlyEligible(sm.settings, sm.utxoStore, sm.chainParams, height)
}

// windowRouteEnabled is every conjunct of windowRoute that does not depend on the
// block's height: the settings, the store's outpoint-only support and the chain
// params. The dispatcher asks it before spending an RPC to resolve a block's height,
// because with any of these off the answer to windowRoute could only be false.
func (sm *SyncManager) windowRouteEnabled() bool {
	return sm.settings != nil &&
		sm.settings.BlockValidation.QuickWindowBlocks >= 1 &&
		sm.settings.BlockValidation.LegacyUnifiedBelowCheckpoint &&
		sm.settings.BlockValidation.OutpointOnlyBelowCheckpoint &&
		sm.utxoStore != nil && sm.utxoStore.SupportsOutpointOnlySpend() &&
		sm.chainParams != nil
}

// windowRoute reports whether blocks at height take the quick window: the unified
// below-checkpoint route with the window setting at 1 or more.
func (sm *SyncManager) windowRoute(height uint32) bool {
	return sm.windowRouteEnabled() && model.BelowCheckpoint(sm.chainParams.Checkpoints, height)
}

// needsParentMinedWait reports whether HandleConvertedBlock must block on the
// parent's mined_set before processing a block at this height. Heights 0 and 1
// never wait (pre-existing behaviour). On the below-checkpoint outpoint-only
// fast path the wait is redundant three ways: (1) its documented purpose is
// BIP68 parent-height lookup, and BIP68 is skipped below the checkpoint;
// (2) the quick window gates every spend on the commit of the create that made
// its coin, so a parent's outputs are never spent before the parent's create
// has committed, see services/blockvalidation/quick_window.go; (3) the legacy
// path calls AddBlock with WithMinedSet(true)
// (see buildAddBlockOpts in services/blockvalidation/BlockValidation.go), so
// GetBlockIsMined is always instantly true and only costs a gRPC round-trip
// per block.
func (sm *SyncManager) needsParentMinedWait(origin blockRequestOrigin, height uint32) bool {
	return height > 1 && !sm.legacyOutpointOnly(origin, height)
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
