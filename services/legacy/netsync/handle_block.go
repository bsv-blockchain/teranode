package netsync

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockchain/blockchain_api"
	"github.com/bitcoin-sv/ubsv/services/legacy/bsvutil"
	"github.com/bitcoin-sv/ubsv/services/legacy/peer"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/bitcoin-sv/ubsv/tracing"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/bscript"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

func (sm *SyncManager) HandleBlockDirect(ctx context.Context, peer *peer.Peer, block *bsvutil.Block) error {
	// Make sure we have the correct height for this block before continuing
	var blockHeight uint32

	if block.Height() <= 0 {
		// Lookup block height from blockchain
		_, previousBlockHeaderMeta, err := sm.blockchainClient.GetBlockHeader(ctx, &block.MsgBlock().Header.PrevBlock)
		if err != nil {
			return errors.NewProcessingError("failed to get block header for previous block %s", block.MsgBlock().Header.PrevBlock, err)
		}

		blockHeight = previousBlockHeaderMeta.Height + 1
		// nolint: gosec
		block.SetHeight(int32(blockHeight))
	} else {
		blockHeight = uint32(block.Height())
	}

	ctx, _, deferFn := tracing.StartTracing(ctx, "HandleBlockDirect",
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
	)
	defer deferFn()

	// 3. Create a block message with (block hash, coinbase tx and slice if 1 subtree)
	var headerBytes bytes.Buffer
	if err := block.MsgBlock().Header.Serialize(&headerBytes); err != nil {
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

	teranodeBlock, err := model.NewBlock(header, coinbaseTx, subtrees, uint64(len(block.Transactions())), uint64(blockSize), blockHeight)
	if err != nil {
		return errors.NewProcessingError("failed to create model.NewBlock", err)
	}

	// call the process block wrapper, which will add tracing and logging
	err = sm.ProcessBlock(ctx, teranodeBlock)
	if err != nil {
		return err
	}

	return nil
}

func (sm *SyncManager) ProcessBlock(ctx context.Context, teranodeBlock *model.Block) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "SyncManager:processBlock",
		tracing.WithLogMessage(
			sm.logger,
			"[SyncManager:processBlock][%s %d] processing block",
			teranodeBlock.Hash().String(),
			teranodeBlock.Height,
		),
	)
	defer deferFn()

	// send the block to the blockValidation for processing and validation
	// all the block subtrees should have been validated in processSubtrees
	if err := sm.blockValidation.ProcessBlock(ctx, teranodeBlock, teranodeBlock.Height); err != nil {
		return errors.NewProcessingError("failed to process block", err)
	}

	return nil
}

type txMapWrapper struct {
	tx                 *bt.Tx
	someParentsInBlock bool
	childLevelInBlock  uint32
}

func (sm *SyncManager) prepareSubtrees(ctx context.Context, block *bsvutil.Block) ([]*chainhash.Hash, error) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "prepareSubtrees",
		tracing.WithLogMessage(
			sm.logger,
			"[prepareSubtrees][%s] processing subtree for block height %d, tx count %d",
			block.Hash().String(),
			block.Height(),
			len(block.Transactions()),
		),
	)
	defer deferFn()

	subtrees := make([]*chainhash.Hash, 0)
	blockHeight := uint32(block.Height())

	// create 1 subtree + subtree.subtreeData
	// then validate the subtree through the subtreeValidation service
	if len(block.Transactions()) > 1 {
		subtree, err := util.NewIncompleteTreeByLeafCount(len(block.Transactions()))
		if err != nil {
			return nil, errors.NewSubtreeError("failed to create subtree", err)
		}

		if err := subtree.AddNode(model.CoinbasePlaceholder, 0, 0); err != nil {
			return nil, errors.NewSubtreeError("failed to add coinbase placeholder", err)
		}

		// subtreeData contains the extended tx bytes of all transactions references in the subtree
		// except the coinbase transaction
		subtreeData := util.NewSubtreeData(subtree)

		txMap, err := sm.createTxMap(ctx, block)
		if err != nil {
			return nil, err
		}

		err = sm.extendTransactions(ctx, block, txMap, subtree, subtreeData)
		if err != nil {
			return nil, err
		}

		// TODO move this into a convenience function in the blockchain client
		currentState, err := sm.blockchainClient.GetFSMCurrentState(sm.ctx)
		if err != nil {
			// TODO: how to handle it gracefully?
			sm.logger.Errorf("[BlockAssembly] Failed to get current state: %s", err)
		}

		legacyMode := currentState != nil && *currentState == blockchain_api.FSMStateType_LEGACYSYNCING

		if legacyMode {
			// in legacy sync mode, we can process transactions in a block in parallel, but in reverse order
			// first we create all the utxos, then we spend them
			if err = sm.validateTransactionsLegacyMode(ctx, txMap, blockHeight); err != nil {
				return nil, err
			}
		} else {
			maxLevel, blockTxsPerLevel := sm.prepareTxsPerLevel(ctx, block, txMap)
			sm.validateTransactions(ctx, maxLevel, blockTxsPerLevel, blockHeight)
		}

		subtreeBytes, err := subtree.Serialize()
		if err != nil {
			return nil, errors.NewStorageError("failed to serialize subtree", err)
		}

		if err = sm.subtreeStore.Set(ctx,
			subtree.RootHash()[:],
			subtreeBytes,
			options.WithFileExtension("subtree"),
			options.WithTTL(2*time.Minute),
		); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
			return nil, errors.NewStorageError("failed to store subtree", err)
		}

		subtreeDataBytes, err := subtreeData.Serialize()
		if err != nil {
			return nil, errors.NewStorageError("failed to serialize subtree data", err)
		}

		if err = sm.subtreeStore.Set(ctx,
			subtreeData.RootHash()[:],
			subtreeDataBytes,
			options.WithFileExtension("subtreeData"),
			options.WithTTL(2*time.Minute),
		); err != nil && !errors.Is(err, errors.ErrBlobAlreadyExists) {
			return nil, errors.NewStorageError("failed to store subtree data", err)
		}

		if err = sm.subtreeValidation.CheckSubtree(ctx, *subtree.RootHash(), "legacy", blockHeight); err != nil {
			return nil, errors.NewSubtreeError("failed to check subtree", err)
		}

		subtrees = append(subtrees, subtree.RootHash())
	}

	return subtrees, nil
}

func (sm *SyncManager) validateTransactionsLegacyMode(ctx context.Context, txMap map[chainhash.Hash]*txMapWrapper, blockHeight uint32) error {
	ctx, _, deferFn := tracing.StartTracing(ctx, "validateTransactionsLegacyMode")
	defer deferFn()

	if err := sm.createUtxos(ctx, txMap, blockHeight); err != nil {
		return err
	}

	sm.preValidateTransactions(ctx, txMap, blockHeight)

	return nil
}

// createUtxos creates all the utxos for the transactions in the block in parallel
// before any spending is done. This only occurs in legacy mode when we assume the
// block is valid.
func (sm *SyncManager) createUtxos(ctx context.Context, txMap map[chainhash.Hash]*txMapWrapper, blockHeight uint32) error {
	_, _, deferFn := tracing.StartTracing(ctx, "createUtxos")
	defer deferFn()

	storeBatcherSize, _ := gocore.Config().GetInt("utxostore_storeBatcherSize", 1024)
	storeBatcherConcurrency, _ := gocore.Config().GetInt("utxostore_storeBatcherConcurrency", 32)

	g, gCtx := errgroup.WithContext(context.Background())  // we don't want the tracing to be linked to these calls
	g.SetLimit(storeBatcherSize * storeBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

	// create all the utxos first
	for txHash := range txMap {
		txHash := txHash

		g.Go(func() error {
			if _, err := sm.utxoStore.Create(gCtx, txMap[txHash].tx, blockHeight); err != nil {
				if errors.Is(err, errors.ErrTxAlreadyExists) {
					sm.logger.Debugf("failed to create utxo for tx %s: %s", txHash.String(), err)
				} else {
					return err
				}
			}

			return nil
		})
	}

	// wait for all utxos to be created
	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("failed to create utxos", err)
	}

	return nil
}

// preValidateTransactions pre-validates all the transactions in the block before
// sending them to subtree validation.
func (sm *SyncManager) preValidateTransactions(ctx context.Context, txMap map[chainhash.Hash]*txMapWrapper, blockHeight uint32) {
	_, _, deferFn := tracing.StartTracing(ctx, "preValidateTransactions")
	defer deferFn()

	spendBatcherSize, _ := gocore.Config().GetInt("utxostore_spendBatcherSize", 1024)

	spendBatcherConcurrency, _ := gocore.Config().GetInt("utxostore_spendBatcherConcurrency", 32)

	// validate all the transactions in parallel
	g, gCtx := errgroup.WithContext(context.Background())  // we don't want the tracing to be linked to these calls
	g.SetLimit(spendBatcherSize * spendBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

	// validate all the transactions in parallel
	for txHash := range txMap {
		txHash := txHash

		g.Go(func() error {
			// call the validator to validate the transaction, but skip the utxo creation
			_ = sm.validationClient.Validate(gCtx, txMap[txHash].tx, blockHeight, validator.WithSkipUtxoCreation(true))

			return nil
		})
	}

	// wait for all the transactions to be validated
	_ = g.Wait()
}

// validateTransactions validates all the transactions in the block in parallel
// per level. This is done to speed up subtree validation later on.
// The levels indicate the number of parents in the block.
func (sm *SyncManager) validateTransactions(ctx context.Context, maxLevel uint32, blockTxsPerLevel map[uint32][]*bt.Tx, blockHeight uint32) {
	ctx, _, deferFn := tracing.StartTracing(ctx, "validateTransactions")
	defer deferFn()

	spendBatcherSize, _ := gocore.Config().GetInt("utxostore_spendBatcherSize", 1024)
	spendBatcherConcurrency, _ := gocore.Config().GetInt("utxostore_spendBatcherConcurrency", 32)

	// try to pre-validate the transactions through the validation, to speed up subtree validation later on.
	// This allows us to process all the transactions in parallel. The levels indicate the number of parents in the block.
	for i := uint32(0); i <= maxLevel; i++ {
		_, _, deferLevelFn := tracing.StartTracing(ctx, fmt.Sprintf("validateTransactions:level:%d", i))

		if len(blockTxsPerLevel[i]) < 10 {
			// if we have less than 10 transactions on a certain level, we can process them immediately by triggering the batcher
			for txIdx := range blockTxsPerLevel[i] {
				_ = sm.validationClient.Validate(ctx, blockTxsPerLevel[i][txIdx], blockHeight)
			}

			sm.validationClient.TriggerBatcher()
		} else {
			// process all the transactions on a certain level in parallel
			g, gCtx := errgroup.WithContext(context.Background())  // we don't want the tracing to be linked to these calls
			g.SetLimit(spendBatcherSize * spendBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

			for txIdx := range blockTxsPerLevel[i] {
				txIdx := txIdx

				g.Go(func() error {
					// send to validation, but only if the parent is not in the same block
					_ = sm.validationClient.Validate(gCtx, blockTxsPerLevel[i][txIdx], blockHeight)

					return nil
				})
			}

			// we don't care about errors here, we are just pre-warming caches for a quicker subtree validation
			_ = g.Wait()

			deferLevelFn()
		}
	}
}

func (sm *SyncManager) extendTransactions(ctx context.Context, block *bsvutil.Block, txMap map[chainhash.Hash]*txMapWrapper, subtree *util.Subtree, subtreeData *util.SubtreeData) error {
	_, _, deferFn := tracing.StartTracing(ctx, "extendTransactions")
	defer deferFn()

	outpointBatcherSize, _ := gocore.Config().GetInt("utxostore_outpointBatcherSize", 1024)
	outpointBatcherConcurrency, _ := gocore.Config().GetInt("utxostore_outpointBatcherConcurrency", 32)

	g, gCtx := errgroup.WithContext(ctx)                         // we don't want the tracing to be linked to these calls
	g.SetLimit(outpointBatcherSize * outpointBatcherConcurrency) // we limit the number of concurrent requests, to not overload Aerospike

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()

		// the coinbase transaction is not part of the txMap
		if txWrapper, found := txMap[txHash]; found {
			tx := txWrapper.tx
			txSize := uint64(tx.Size())
			tx.Size()

			// Calculate the fees of this transaction
			// We don't need to check for coinbase transactions, as they have no inputs
			fee := uint64(0)

			if !tx.IsCoinbase() {
				// Calculate the fees of this transaction
				// We don't need to check for coinbase transactions, as they have no inputs
				for _, input := range tx.Inputs {
					fee += input.PreviousTxSatoshis
				}

				for _, output := range tx.Outputs {
					fee -= output.Satoshis
				}
			}

			if err := subtree.AddNode(txHash, fee, txSize); err != nil {
				return errors.NewTxError("failed to add node (%s) to subtree", txHash, err)
			}
			// we need to match the indexes of the subtree and the tx data in subtreeData
			currentIdx := subtree.Length() - 1

			g.Go(func() error {
				if err := sm.extendTransaction(gCtx, tx, txMap); err != nil {
					return errors.NewTxError("failed to extend transaction", err)
				}

				// store the extended transaction in our subtree tx data file
				if err := subtreeData.AddTx(tx, currentIdx); err != nil {
					return errors.NewTxError("failed to add tx to subtree data", err)
				}

				return nil
			})
		}
	}

	// wait for all tx to be processed - we don't need to process errors here
	if err := g.Wait(); err != nil {
		return errors.NewProcessingError("failed to process transactions", err)
	}

	return nil
}

func (sm *SyncManager) createTxMap(ctx context.Context, block *bsvutil.Block) (map[chainhash.Hash]*txMapWrapper, error) {
	_, _, deferFn := tracing.StartTracing(ctx, "createTxMap",
		tracing.WithDebugLogMessage(
			sm.logger,
			"[createTxMap][%s %d] processing transactions into map for block",
			block.Hash().String(),
			block.Height(),
		),
	)
	defer deferFn()

	blockTransactions := block.Transactions()

	// Create a map of all transactions in the block
	txMap := make(map[chainhash.Hash]*txMapWrapper, len(blockTransactions))

	for _, wireTx := range blockTransactions {
		txHash := *wireTx.Hash()

		tx, err := WireTxToGoBtTx(wireTx)
		if err != nil {
			return nil, errors.NewProcessingError("failed to convert wire.Tx to bt.Tx", err)
		}

		// don't add the coinbase to the txMap, we cannot process it anyway
		if !tx.IsCoinbase() {
			tx.SetTxHash(&txHash)
			txMap[txHash] = &txMapWrapper{tx: tx}
		}
	}

	return txMap, nil
}

// prepareTxsPerLevel prepares the transactions per level for processing
// levels are determined by the number of parents in the block
func (sm *SyncManager) prepareTxsPerLevel(ctx context.Context, block *bsvutil.Block, txMap map[chainhash.Hash]*txMapWrapper) (uint32, map[uint32][]*bt.Tx) {
	_, _, deferFn := tracing.StartTracing(ctx, "prepareTxsPerLevel")
	defer deferFn()

	maxLevel := uint32(0)
	sizePerLevel := make(map[uint32]uint64)
	blockTxsPerLevel := make(map[uint32][]*bt.Tx)

	for _, wireTx := range block.Transactions() {
		txHash := *wireTx.Hash()
		if _, found := txMap[txHash]; found {
			if txMap[txHash].someParentsInBlock {
				for _, input := range txMap[txHash].tx.Inputs {
					parentTxHash := *input.PreviousTxIDChainHash()
					if parentTxWrapper, found := txMap[parentTxHash]; found {
						// if the parent from this input is at the same level or higher,
						// we need to increase the child level of this transaction
						if parentTxWrapper.childLevelInBlock >= txMap[txHash].childLevelInBlock {
							txMap[txHash].childLevelInBlock = parentTxWrapper.childLevelInBlock + 1
						}

						if txMap[txHash].childLevelInBlock > maxLevel {
							maxLevel = txMap[txHash].childLevelInBlock
						}
					}
				}
			}

			sizePerLevel[txMap[txHash].childLevelInBlock] += 1
		}
	}

	// pre-allocation of the blockTxsPerLevel map
	for i := uint32(0); i <= maxLevel; i++ {
		blockTxsPerLevel[i] = make([]*bt.Tx, 0, sizePerLevel[i])
	}

	// put all transactions in a map per level for processing
	for _, txWrapper := range txMap {
		blockTxsPerLevel[txWrapper.childLevelInBlock] = append(blockTxsPerLevel[txWrapper.childLevelInBlock], txWrapper.tx)
	}

	return maxLevel, blockTxsPerLevel
}

func (sm *SyncManager) extendTransaction(ctx context.Context, tx *bt.Tx, txMap map[chainhash.Hash]*txMapWrapper) error {
	previousOutputs := make([]*meta.PreviousOutput, 0, len(tx.Inputs))

	txWrapper, found := txMap[*tx.TxIDChainHash()]
	if !found {
		return errors.NewProcessingError("tx %s not found in txMap", tx.TxIDChainHash())
	}

	for i, input := range tx.Inputs {
		prevTxHash := *input.PreviousTxIDChainHash()
		if prevTxWrapper, found := txMap[prevTxHash]; found {
			txWrapper.someParentsInBlock = true
			tx.Inputs[i].PreviousTxSatoshis = prevTxWrapper.tx.Outputs[input.PreviousTxOutIndex].Satoshis
			tx.Inputs[i].PreviousTxScript = bscript.NewFromBytes(*prevTxWrapper.tx.Outputs[input.PreviousTxOutIndex].LockingScript)
		} else {
			previousOutputs = append(previousOutputs, &meta.PreviousOutput{
				PreviousTxID: prevTxHash,
				Vout:         input.PreviousTxOutIndex,
				Idx:          i,
			})
		}
	}

	if err := sm.utxoStore.PreviousOutputsDecorate(ctx, previousOutputs); err != nil {
		return errors.NewProcessingError("failed to decorate previous outputs for tx %s", tx.TxIDChainHash(), err)
	}

	// run through the previous outputs and extend the transaction
	for _, po := range previousOutputs {
		if po.LockingScript == nil {
			return errors.NewProcessingError("previous output script is empty for %s:%d", po.PreviousTxID, po.Vout)
		}

		tx.Inputs[po.Idx].PreviousTxSatoshis = po.Satoshis
		tx.Inputs[po.Idx].PreviousTxScript = bscript.NewFromBytes(po.LockingScript)
	}

	return nil
}

// WireTxToGoBtTx converts a wire.Tx to a bt.Tx
// This does not use the bytes methods, but directly uses the fields of the wire.Tx
func WireTxToGoBtTx(wireTx *bsvutil.Tx) (*bt.Tx, error) {
	wTx := wireTx.MsgTx()

	tx := &bt.Tx{
		Version:  uint32(wTx.Version),
		LockTime: wTx.LockTime,
	}

	tx.Inputs = make([]*bt.Input, len(wTx.TxIn))
	for i, in := range wTx.TxIn {
		tx.Inputs[i] = &bt.Input{
			UnlockingScript:    bscript.NewFromBytes(in.SignatureScript),
			PreviousTxOutIndex: in.PreviousOutPoint.Index,
			SequenceNumber:     in.Sequence,
		}
		_ = tx.Inputs[i].PreviousTxIDAdd(&in.PreviousOutPoint.Hash)
	}

	tx.Outputs = make([]*bt.Output, len(wTx.TxOut))
	for i, out := range wTx.TxOut {
		tx.Outputs[i] = &bt.Output{
			Satoshis:      uint64(out.Value),
			LockingScript: bscript.NewFromBytes(out.PkScript),
		}
	}

	return tx, nil
}
