package netsync

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sync/atomic"
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
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/legacy/bsvutil"
	"github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/services/utxopersister/filestorer"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/blockassemblyutil"
	"github.com/bsv-blockchain/teranode/util/tracing"
	"golang.org/x/sync/errgroup"
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

	block := bsvutil.NewBlock(msgBlock)

	// Lookup previous block height from blockchain
	_, previousBlockHeaderMeta, err = sm.blockchainClient.GetBlockHeader(ctx, &block.MsgBlock().Header.PrevBlock)
	if err != nil {
		sm.logger.Errorf("[HandleBlockDirect][%s] failed to get block header for previous block %s: %s", blockHash.String(), block.MsgBlock().Header.PrevBlock, err)
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

	coinbaseTx, err := bt.NewTxFromBytes(coinbase.Bytes())
	if err != nil {
		return errors.NewProcessingError("failed to create bt.Tx for coinbase", err)
	}

	// validate all subtrees and store all subtree data
	// this also should spend and create all utxos
	subtrees, err := sm.prepareSubtrees(ctx, block)
	if err != nil {
		return err
	}

	// create valid teranode block, with the subtree hash
	blockSize := block.MsgBlock().SerializeSize()

	blockSizeUint64, err := safeconversion.IntToUint64(blockSize)
	if err != nil {
		return err
	}

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(block.Transactions())), blockSizeUint64, blockHeight, 0)
	if err != nil {
		return errors.NewProcessingError("failed to create model.NewBlock", err)
	}

	// pre-check that there is enough proof of work on the block, before we do any other processing
	headerValid, _, err := teranodeBlock.Header.HasMetTargetDifficulty()
	if !headerValid {
		return errors.NewBlockInvalidError("invalid block header: %s", teranodeBlock.Header.Hash().String(), err)
	}

	// call the process block wrapper, which will add tracing and logging
	err = sm.ProcessBlock(ctx, teranodeBlock)
	if err != nil {
		return err
	}

	// process any orphan transactions that are now valid in background
	// this will also remove the transactions from the orphan pool
	go func() {
		acceptedTxs := make([]*TxHashAndFee, 0)
		for _, tx := range block.Transactions() {
			sm.processOrphanTransactions(ctx, tx.Hash(), &acceptedTxs)
		}

		if len(acceptedTxs) > 0 {
			sm.logger.Infof("[HandleBlockDirect][%s %d] accepted %d orphan transactions", block.Hash().String(), blockHeight, len(acceptedTxs))
			sm.peerNotifier.AnnounceNewTransactions(acceptedTxs)
		}
	}()

	return nil
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
	if err = sm.blockValidation.ProcessBlock(ctx, teranodeBlock, teranodeBlock.Height, "", "legacy"); err != nil {
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

func (sm *SyncManager) prepareSubtrees(ctx context.Context, block *bsvutil.Block) (subtrees []*chainhash.Hash, err error) {
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

	var (
		subtree    *subtreepkg.Subtree
		legacyMode bool
	)

	// create 1 subtree + subtree.subtreeData
	// then validate the subtree through the subtreeValidation service
	if len(block.Transactions()) > 1 {
		if subtree, err = subtreepkg.NewIncompleteTreeByLeafCount(len(block.Transactions())); err != nil {
			return nil, errors.NewSubtreeError("[prepareSubtrees] failed to create subtree", err)
		}

		if err = subtree.AddCoinbaseNode(); err != nil {
			return nil, errors.NewSubtreeError("[prepareSubtrees] failed to add coinbase placeholder", err)
		}

		// subtreeData contains the extended tx bytes of all transactions references in the subtree
		// except the coinbase transaction
		subtreeData := subtreepkg.NewSubtreeData(subtree)
		subtreeMetaData := subtreepkg.NewSubtreeMeta(subtree)

		// Create a map of all transactions in the block
		txMap := txmap.NewSyncedMap[chainhash.Hash, *TxMapWrapper](len(block.Transactions()))

		if err = sm.createTxMap(ctx, block, txMap); err != nil {
			return nil, err
		}

		// extend all the transactions in the block
		if err = sm.extendTransactions(ctx, block, txMap); err != nil {
			return nil, err
		}

		// create the subtree and subtreeData for the block
		if err = sm.createSubtree(ctx, block, txMap, subtree, subtreeData, subtreeMetaData); err != nil {
			return nil, err
		}

		if legacyMode, err = sm.blockchainClient.IsFSMCurrentState(sm.ctx, blockchain_api.FSMStateType_LEGACYSYNCING); err != nil {
			sm.logger.Errorf("[prepareSubtrees] Failed to get current state: %s", err)
		}

		// quick validation mode is used when we are in legacy mode
		// we can skip some of the processing since we assume the block is valid
		quickValidationMode := legacyMode

		if quickValidationMode {
			// in quickValidationMode, we can process transactions in a block in parallel, but in reverse order
			// first we create all the utxos, then we spend them
			if err = sm.ValidateTransactionsLegacyMode(ctx, txMap, block); err != nil {
				return nil, err
			}
		}

		// write all the subtree data to the subtree store
		if err = sm.writeSubtree(ctx, block, subtree, subtreeData, subtreeMetaData, quickValidationMode); err != nil {
			return nil, err
		}

		// we don't need to check the subtree in the subtree validation in legacy or catching blocks mode,
		// since we already validated the transactions and created all the subtree files needed
		if !quickValidationMode {
			if err = sm.checkSubtreeFromBlock(ctx, block, subtree); err != nil {
				return nil, err
			}
		}

		subtrees = append(subtrees, subtree.RootHash())
	}

	return subtrees, nil
}

func (sm *SyncManager) checkSubtreeFromBlock(ctx context.Context, block *bsvutil.Block, subtree *subtreepkg.Subtree) error {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "checkSubtreeFromBlock",
		tracing.WithLogMessage(sm.logger, "[checkSubtreeFromBlock][%s] checking subtree for block %s height %d", subtree.RootHash().String(), block.Hash().String(), block.Height()),
	)

	defer deferFn()

	blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
	if err != nil {
		return err
	}

	if err := sm.subtreeValidation.CheckSubtreeFromBlock(ctx, *subtree.RootHash(), "legacy", blockHeightUint32, block.Hash(), &block.MsgBlock().Header.PrevBlock); err != nil {
		return errors.NewSubtreeError("failed to check subtree", err)
	}

	return nil
}

func (sm *SyncManager) writeSubtree(ctx context.Context, block *bsvutil.Block, subtree *subtreepkg.Subtree,
	subtreeData *subtreepkg.Data, subtreeMetaData *subtreepkg.Meta, quickValidationMode bool) error {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "writeSubtree",
		tracing.WithLogMessage(sm.logger, "[writeSubtree][%s] writing subtree for block %s height %d", subtree.RootHash().String(), block.Hash().String(), block.Height()),
	)

	subtreeFileExtension := fileformat.FileTypeSubtreeToCheck
	if quickValidationMode {
		subtreeFileExtension = fileformat.FileTypeSubtree
	}

	defer deferFn()

	g, gCtx := errgroup.WithContext(ctx)
	// Limit to 3 concurrent writes (subtree, subtreeData, subtreeMeta)
	util.SafeSetLimit(g, 3)

	g.Go(func() error {
		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			return errors.NewStorageError("[writeSubtree][%s] failed to serialize subtree", subtree.RootHash().String(), err)
		}

		dah := uint32(block.Height()) + sm.settings.GlobalBlockHeightRetention // nolint: gosec

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
		subtreeDataBytes, err := subtreeData.Serialize()
		if err != nil {
			return errors.NewStorageError("[writeSubtree][%s] failed to serialize subtree data", subtree.RootHash().String(), err)
		}

		dah := uint32(block.Height()) + sm.settings.GlobalBlockHeightRetention // nolint: gosec

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

		if _, err := storer.Write(subtreeDataBytes); err != nil {
			return errors.NewStorageError("error writing subtree data to disk", err)
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

		dah := uint32(block.Height()) + sm.settings.GlobalBlockHeightRetention // nolint: gosec

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
	block *bsvutil.Block) (err error) {
	ctx, _, deferFn := tracing.Tracer("netsync").Start(ctx, "validateTransactionsLegacyMode",
		tracing.WithHistogram(prometheusLegacyNetsyncValidateTransactionsLegacyMode),
		tracing.WithLogMessage(sm.logger, "[validateTransactionsLegacyMode] called for block %s, height %d", block.Hash(), block.Height()),
	)

	defer func() {
		deferFn(err)
	}()

	if err = sm.createUtxos(ctx, txMap, block); err != nil {
		return err
	}

	sm.logger.Infof("[validateTransactionsLegacyMode] created utxos with %d items", txMap.Length())

	blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
	if err != nil {
		// already wrapped in a processing error
		return err
	}

	if err = sm.PreValidateTransactions(ctx, txMap, *block.Hash(), blockHeightUint32); err != nil {
		return errors.NewProcessingError("[validateTransactionsLegacyMode] failed to pre-validate transactions", err)
	}

	return nil
}

// createUtxos creates all the utxos for the transactions in the block in parallel
// before any spending is done. This only occurs in legacy mode when we assume the
// block is valid.
func (sm *SyncManager) createUtxos(ctx context.Context, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper], block *bsvutil.Block) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "createUtxos",
		tracing.WithLogMessage(sm.logger, "[createUtxos] called for block %s / height %d", block.Hash(), block.Height()),
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

	g, gCtx := errgroup.WithContext(context.Background())          // we don't want the tracing to be linked to these calls
	util.SafeSetLimit(g, storeBatcherSize*storeBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

	blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
	if err != nil {
		return errors.NewProcessingError("failed to convert block height to uint32", err)
	}

	// create all the utxos first
	for _, txHash := range txMap.Keys() {
		txHash := txHash

		g.Go(func() error {
			txWrapper, ok := txMap.Get(txHash)
			if !ok {
				return errors.NewProcessingError("transaction %s not found in txMap", txHash.String())
			}

			if _, err := sm.utxoStore.Create(gCtx, txWrapper.Tx, blockHeightUint32); err != nil {
				if errors.Is(err, errors.ErrTxExists) {
					sm.logger.Debugf("failed to create utxo for tx %s: %s", txHash.String(), err)
				} else {
					return err
				}
			}

			return nil
		})
	}

	// wait for all utxos to be created
	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("failed to create utxos", err)
	}

	return nil
}

// PreValidateTransactions pre-validates all the transactions in the block before
// sending them to subtree validation.
func (sm *SyncManager) PreValidateTransactions(ctx context.Context, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	blockHash chainhash.Hash, blockHeight uint32) (err error) {
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

	// validate all the transactions in parallel
	g, gCtx := errgroup.WithContext(context.Background())          // we don't want the tracing to be linked to these calls
	util.SafeSetLimit(g, spendBatcherSize*spendBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

	// validate all the transactions in parallel
	for _, txHash := range txMap.Keys() {
		txHash := txHash

		g.Go(func() (err error) {
			timeStart := time.Now()
			defer func() {
				prometheusLegacyNetsyncBlockTxValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
			}()

			txWrapper, ok := txMap.Get(txHash)
			if !ok {
				return errors.NewProcessingError("transaction %s not found in txMap", txHash.String())
			}

			// call the validator to validate the transaction, but skip the utxo creation
			_, err = sm.validationClient.Validate(gCtx,
				txWrapper.Tx,
				blockHeight,
				validator.WithSkipUtxoCreation(true),
				validator.WithAddTXToBlockAssembly(false),
				validator.WithSkipPolicyChecks(true),
			)

			return err
		})
	}

	// wait for all the transactions to be validated
	return g.Wait()
}

// validateTransactions validates all the transactions in the block in parallel
// per level. This is done to speed up subtree validation later on.
// The levels indicate the number of parents in the block.
func (sm *SyncManager) validateTransactions(ctx context.Context, maxLevel uint32, blockTxsPerLevel map[uint32][]*bt.Tx, block *bsvutil.Block) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "validateTransactions",
		tracing.WithLogMessage(sm.logger, "[validateTransactions] called for block %s / height %d", block.Hash(), block.Height()),
		tracing.WithHistogram(prometheusLegacyNetsyncValidateTransactions),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in validateTransactions: %v", r, err)
		}

		deferFn(err)
	}()

	spendBatcherSize := sm.settings.Legacy.SpendBatcherSize
	spendBatcherConcurrency := sm.settings.Legacy.SpendBatcherConcurrency

	var timeStart time.Time

	// try to pre-validate the transactions through the validation, to speed up subtree validation later on.
	// This allows us to process all the transactions in parallel. The levels indicate the number of parents in the block.
	for i := uint32(0); i <= maxLevel; i++ {
		_, _, deferLevelFn := tracing.Tracer("netsync").Start(ctx, fmt.Sprintf("validateTransactions:level:%d", i))

		if len(blockTxsPerLevel[i]) < 10 {
			// if we have less than 10 transactions on a certain level, we can process them immediately by triggering the batcher
			for txIdx := range blockTxsPerLevel[i] {
				blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
				if err != nil {
					return err
				}

				timeStart = time.Now()

				_, _ = sm.validationClient.Validate(ctx, blockTxsPerLevel[i][txIdx], blockHeightUint32, validator.WithSkipPolicyChecks(true))

				prometheusLegacyNetsyncBlockTxValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
			}

			sm.validationClient.TriggerBatcher()
		} else {
			// process all the transactions on a certain level in parallel
			g, gCtx := errgroup.WithContext(context.Background())          // we don't want the tracing to be linked to these calls
			util.SafeSetLimit(g, spendBatcherSize*spendBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

			for txIdx := range blockTxsPerLevel[i] {
				txIdx := txIdx

				g.Go(func() error {
					timeStart := time.Now()
					defer func() {
						prometheusLegacyNetsyncBlockTxValidate.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
					}()

					blockHeightUint32, err := safeconversion.Int32ToUint32(block.Height())
					if err != nil {
						return err
					}

					// send to validation, but only if the parent is not in the same block
					_, _ = sm.validationClient.Validate(gCtx, blockTxsPerLevel[i][txIdx], blockHeightUint32, validator.WithSkipPolicyChecks(true))

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

func (sm *SyncManager) extendTransactions(ctx context.Context, block *bsvutil.Block, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "extendTransactions",
		tracing.WithLogMessage(sm.logger, "[extendTransactions] called for block %s / height %d", block.Hash(), block.Height()),
		tracing.WithHistogram(prometheusLegacyNetsyncExtendTransactions),
	)

	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in extendTransactions: %v", r, err)
		}

		deferFn(err)
	}()

	outpointBatcherSize := sm.settings.Legacy.OutpointBatcherSize

	g, gCtx := errgroup.WithContext(ctx)      // we don't want the tracing to be linked to these calls
	util.SafeSetLimit(g, outpointBatcherSize) // we limit the number of concurrent requests, to not overload Aerospike

	for idx, wireTx := range block.Transactions() {
		if idx == 0 {
			// skip the coinbase transaction, as it cannot be extended
			continue
		}

		txHash := *wireTx.Hash()

		// the coinbase transaction is not part of the txMap
		if txWrapper, found := txMap.Get(txHash); found {
			tx := txWrapper.Tx

			g.Go(func() error {
				if err := sm.ExtendTransaction(gCtx, tx, txMap); err != nil {
					return errors.NewTxError("failed to extend transaction", err)
				}

				return nil
			})
		} else {
			// we don't have the transaction in the txMap, so we cannot extend it
			return errors.NewTxError("transaction %s not found in txMap", txHash.String())
		}
	}

	// wait for all tx to be processed - we don't need to process errors here
	if err = g.Wait(); err != nil {
		return errors.NewProcessingError("failed to process transactions", err)
	}

	return nil
}

func (sm *SyncManager) createSubtree(ctx context.Context, block *bsvutil.Block, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper],
	subtree *subtreepkg.Subtree, subtreeData *subtreepkg.Data, subtreeMetaData *subtreepkg.Meta) (err error) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "createSubtree",
		tracing.WithLogMessage(sm.logger, "[createSubtree] called for block %s / height %d", block.Hash(), block.Height()),
	)

	// Add a defer recover to catch any panics and log them
	defer func() {
		if r := recover(); r != nil {
			err = errors.NewProcessingError("recovered in createSubtree: %v", r, err)
		}

		deferFn(err)
	}()

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()

		// the coinbase transaction is not part of the txMap
		if txWrapper, found := txMap.Get(txHash); found {
			tx := txWrapper.Tx

			txSize, err := safeconversion.IntToUint64(tx.Size())
			if err != nil {
				return err
			}

			fee, err := calculateTransactionFee(tx)
			if err != nil {
				return err
			}

			if err = subtree.AddNode(txHash, fee, txSize); err != nil {
				return errors.NewTxError("failed to add node (%s) to subtree", txHash, err)
			}
			// we need to match the indexes of the subtree and the tx data in subtreeData
			currentIdx := subtree.Length() - 1

			// store the extended transaction in our subtree tx data file
			if err = subtreeData.AddTx(tx, currentIdx); err != nil {
				return errors.NewTxError("failed to add tx to subtree data", err)
			}

			if err = subtreeMetaData.SetTxInpointsFromTx(tx); err != nil {
				return errors.NewTxError("failed to add tx to subtree meta data", err)
			}
		}
	}

	sm.logger.Infof("[createSubtree] created subtree %s for block %s / height %d", subtree.RootHash(), block.Hash(), block.Height())

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

func (sm *SyncManager) createTxMap(ctx context.Context, block *bsvutil.Block, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) error {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "createTxMap",
		tracing.WithDebugLogMessage(
			sm.logger,
			"[createTxMap][%s %d] processing transactions into map for block",
			block.Hash().String(),
			block.Height(),
		),
	)
	defer deferFn()

	for _, wireTx := range block.Transactions() {
		tx := &bt.Tx{}

		if err := WireTxToGoBtTx(wireTx, tx); err != nil {
			return errors.NewProcessingError("failed to convert wire.Tx to bt.Tx", err)
		}

		// don't add the coinbase to the txMap, we cannot process it anyway
		if !tx.IsCoinbase() {
			tx.SetTxHash(wireTx.Hash())
			txMap.Set(*tx.TxIDChainHash(), &TxMapWrapper{Tx: tx})
		}
	}

	return nil
}

// prepareTxsPerLevel prepares the transactions per level for processing
// levels are determined by the number of parents in the block
func (sm *SyncManager) prepareTxsPerLevel(ctx context.Context, block *bsvutil.Block, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) (uint32, [][]*bt.Tx) {
	_, _, deferFn := tracing.Tracer("netsync").Start(ctx, "prepareTxsPerLevel")
	defer deferFn()

	maxLevel := uint32(0)
	sizePerLevel := make(map[uint32]uint64)

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()
		if txWrapper, found := txMap.Get(txHash); found {
			if txWrapper.SomeParentsInBlock {
				for _, input := range txWrapper.Tx.Inputs {
					parentTxHash := *input.PreviousTxIDChainHash()
					if parentTxWrapper, found := txMap.Get(parentTxHash); found {
						// if the parent from this input is at the same level or higher,
						// we need to increase the child level of this transaction
						if parentTxWrapper.ChildLevelInBlock >= txWrapper.ChildLevelInBlock {
							txWrapper.ChildLevelInBlock = parentTxWrapper.ChildLevelInBlock + 1
						}

						if txWrapper.ChildLevelInBlock > maxLevel {
							maxLevel = txWrapper.ChildLevelInBlock
						}
					}
				}
			}

			sizePerLevel[txWrapper.ChildLevelInBlock] += 1
		}
	}

	blockTxsPerLevel := make([][]*bt.Tx, maxLevel+1)

	// pre-allocation of the blockTxsPerLevel map
	for i := uint32(0); i <= maxLevel; i++ {
		blockTxsPerLevel[i] = make([]*bt.Tx, 0, sizePerLevel[i])
	}

	// put all transactions in a map per level for processing
	for _, txWrapper := range txMap.Range() {
		blockTxsPerLevel[txWrapper.ChildLevelInBlock] = append(blockTxsPerLevel[txWrapper.ChildLevelInBlock], txWrapper.Tx)
	}

	return maxLevel, blockTxsPerLevel
}

func (sm *SyncManager) ExtendTransaction(ctx context.Context, tx *bt.Tx, txMap *txmap.SyncedMap[chainhash.Hash, *TxMapWrapper]) error {
	timeStart := time.Now()
	defer func() {
		prometheusLegacyNetsyncBlockTxSize.Observe(float64(tx.Size()))
		prometheusLegacyNetsyncBlockTxNrInputs.Observe(float64(len(tx.Inputs)))
		prometheusLegacyNetsyncBlockTxNrOutputs.Observe(float64(len(tx.Outputs)))
		prometheusLegacyNetsyncBlockTxExtend.Observe(float64(time.Since(timeStart).Microseconds()) / 1_000_000)
	}()

	txWrapper, found := txMap.Get(*tx.TxIDChainHash())
	if !found {
		return errors.NewProcessingError("tx %s not found in txMap", tx.TxIDChainHash())
	}

	inputLen := len(tx.Inputs)
	populatedInputs := atomic.Int32{}

	g := errgroup.Group{}
	// Limit goroutines to number of CPU cores to prevent scheduler thrashing
	// This prevents spawning thousands of goroutines for transactions with many inputs
	util.SafeSetLimit(&g, runtime.NumCPU()*2)

	for i, input := range tx.Inputs {
		i := i         // capture the loop variable
		input := input // capture the input variable
		prevTxHash := *input.PreviousTxIDChainHash()

		if prevTxWrapper, found := txMap.Get(prevTxHash); found {
			g.Go(func() error {
				txWrapper.SomeParentsInBlock = true

				// we do have a parent, but since everything is happening in parallel, we need to check if the parent has
				// already been extended
				timeOut := time.After(120 * time.Second)

			WaitForParent:
				for {
					select {
					case <-timeOut:
						return errors.NewProcessingError("timed out waiting for parent transaction %s to be extended", prevTxHash.String())
					default:
						if prevTxWrapper.Tx.IsExtended() {
							break WaitForParent
						}

						time.Sleep(10 * time.Millisecond) // wait for the parent transaction to be extended
					}
				}

				// No lock needed - each goroutine writes to a unique index
				tx.Inputs[i].PreviousTxSatoshis = prevTxWrapper.Tx.Outputs[input.PreviousTxOutIndex].Satoshis
				tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(*prevTxWrapper.Tx.Outputs[input.PreviousTxOutIndex].LockingScript)

				populatedInputs.Add(1)

				return nil
			})
		}
	}

	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("failed to extend transaction %s", tx.TxIDChainHash(), err)
	}

	if int(populatedInputs.Load()) == inputLen {
		// all inputs were populated, we can return early
		return nil
	}

	if err := sm.utxoStore.PreviousOutputsDecorate(ctx, tx); err != nil {
		if errors.Is(err, errors.ErrProcessing) || errors.Is(err, errors.ErrTxNotFound) {
			// we could not decorate the transaction. This could be because the parent transaction has been DAH'd, which
			// can only happen if this transaction has been processed. In that case, we can try getting the transaction
			// itself.
			txMeta, err := sm.utxoStore.Get(ctx, tx.TxIDChainHash(), fields.Tx)
			if err == nil && txMeta != nil {
				if txMeta.Tx != nil {
					for i, input := range txMeta.Tx.Inputs {
						tx.Inputs[i].PreviousTxSatoshis = input.PreviousTxSatoshis
						tx.Inputs[i].PreviousTxScript = input.PreviousTxScript
					}

					return nil
				}
			}
		}

		return errors.NewProcessingError("failed to decorate previous outputs for tx %s", tx.TxIDChainHash(), err)
	}

	return nil
}

// WireTxToGoBtTx converts a wire.Tx to a bt.Tx
// This does not use the bytes methods, but directly uses the fields of the wire.Tx
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
		*tx.Inputs[i].UnlockingScript = in.SignatureScript
	}

	tx.Outputs = make([]*bt.Output, len(wTx.TxOut))
	for i, out := range wTx.TxOut {
		tx.Outputs[i] = &bt.Output{
			Satoshis:      uint64(out.Value),
			LockingScript: &bscript.Script{},
		}
		*tx.Outputs[i].LockingScript = out.PkScript
	}

	return nil
}
