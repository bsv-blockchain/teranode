package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/opentracing/opentracing-go"
	"github.com/ordishs/go-utils"
	"github.com/ordishs/gocore"
	"golang.org/x/sync/errgroup"
)

type BlockValidation struct {
	logger           utils.Logger
	blockchainClient blockchain.ClientI
	subtreeStore     blob.Store
	subtreeTTL       time.Duration
	txStore          blob.Store
	txMetaStore      txmeta.Store
	validatorClient  validator.Interface
}

func NewBlockValidation(logger utils.Logger, blockchainClient blockchain.ClientI, subtreeStore blob.Store,
	txStore blob.Store, txMetaStore txmeta.Store, validatorClient validator.Interface) *BlockValidation {

	subtreeTTLMinutes, _ := gocore.Config().GetInt("blockvalidation_subtreeTTL", 120)
	subtreeTTL := time.Duration(subtreeTTLMinutes) * time.Minute

	bv := &BlockValidation{
		logger:           logger,
		blockchainClient: blockchainClient,
		subtreeStore:     subtreeStore,
		subtreeTTL:       subtreeTTL,
		txStore:          txStore,
		txMetaStore:      txMetaStore,
		validatorClient:  validatorClient,
	}

	return bv
}

func (u *BlockValidation) ValidateBlock(cntxt context.Context, block *model.Block, baseUrl string) error {
	span, spanCtx := opentracing.StartSpanFromContext(cntxt, "BlockValidation:ValidateBlock")
	timeStart, stat, ctx := util.NewStatFromContext(spanCtx, "ValidateBlock", stats)
	span.LogKV("block", block.Hash().String())
	defer func() {
		span.Finish()
		stat.AddTime(timeStart)
		prometheusBlockValidationValidateBlock.Inc()
	}()

	u.logger.Infof("[ValidateBlock][%s] called", block.Header.Hash().String())

	err := u.validateBLockSubtrees(ctx, block, baseUrl)
	if err != nil {
		return err
	}

	blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, 100)
	if err != nil {
		return err
	}

	// Add the coinbase transaction to the metaTxStore
	// TODO - we need to consider if we can do this differently
	if _, err = u.txMetaStore.Create(ctx, block.CoinbaseTx); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("[ValidateBlock][%s] failed to create coinbase transaction in txMetaStore [%s]", block.Hash().String(), err.Error())
		}
	}

	// validate the block
	// TODO do we pass in the subtreeStore here or the list of loaded subtrees?
	u.logger.Infof("[ValidateBlock][%s] validating block", block.Hash().String())
	if ok, err := block.Valid(ctx, u.subtreeStore, u.txMetaStore, blockHeaders); !ok {
		return fmt.Errorf("[ValidateBlock][%s] block is not valid: %v", block.String(), err)
	}

	// if valid, store the block
	u.logger.Infof("[ValidateBlock][%s] adding block to blockchain", block.Hash().String())
	if err = u.blockchainClient.AddBlock(ctx, block, true); err != nil {
		return fmt.Errorf("[ValidateBlock][%s] failed to store block [%w]", block.Hash().String(), err)
	}

	u.logger.Infof("[ValidateBlock][%s] storing coinbase tx: %s", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())
	if err = u.txStore.Set(ctx, block.CoinbaseTx.TxIDChainHash()[:], block.CoinbaseTx.Bytes()); err != nil {
		u.logger.Errorf("[ValidateBlock][%s] failed to store coinbase transaction [%w]", block.Hash().String(), err)
	}

	go func() {
		// this happens in the background, since we have already added the block to the blockchain
		// TODO should we recover this somehow if it fails?
		err = u.finalizeBlockValidation(ctx, block)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to finalize block validation [%w]", block.Hash().String(), err)
		}
	}()

	prometheusBlockValidationValidateBlockDuration.Observe(util.TimeSince(timeStart))

	u.logger.Infof("[ValidateBlock][%s] DONE", block.Hash().String())

	return nil
}

func (u *BlockValidation) finalizeBlockValidation(ctx context.Context, block *model.Block) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:finalizeBlockValidation")
	defer func() {
		span.Finish()
	}()

	// get all the subtrees from the block. This should have been loaded during validation, so should be instant
	u.logger.Infof("[ValidateBlock][%s] get subtrees", block.Hash().String())
	subtrees, err := block.GetSubtrees(u.subtreeStore)
	if err != nil {
		return fmt.Errorf("[ValidateBlock][%s] failed to get subtrees from block [%w]", block.Hash().String(), err)
	}

	g, gCtx := errgroup.WithContext(spanCtx)

	g.Go(func() error {
		u.logger.Infof("[ValidateBlock][%s] updating subtrees TTL", block.Hash().String())
		err = u.validateBlockUpdateSubtreesTTL(gCtx, block)
		if err != nil {
			u.logger.Errorf("[ValidateBlock][%s] failed to update subtrees TTL [%w]", block.Hash().String(), err)
		}

		return nil
	})

	g.Go(func() error {
		// add the transactions in this block to the txMeta block hashes
		u.logger.Infof("[ValidateBlock][%s] update tx mined", block.Hash().String())
		if err = blockassembly.UpdateTxMinedStatus(gCtx, u.txMetaStore, subtrees, block.Header); err != nil {
			// TODO this should be a fatal error, but for now we just log it
			//return nil, fmt.Errorf("[BlockAssembly] error updating tx mined status: %w", err)
			u.logger.Errorf("[ValidateBlock][%s] error updating tx mined status: %w", block.Hash().String(), err)
		}

		return nil
	})

	if err = g.Wait(); err != nil {
		return fmt.Errorf("[ValidateBlock][%s] failed to finalize block validation [%w]", block.Hash().String(), err)
	}

	return nil
}

func (u *BlockValidation) validateBlockUpdateSubtreesTTL(ctx context.Context, block *model.Block) (err error) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:validateBlockUpdateSubtreesTTL")
	defer func() {
		span.Finish()
	}()

	// update the subtree TTLs
	g, gCtx := errgroup.WithContext(spanCtx)
	for _, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash
		g.Go(func() error {
			err = u.subtreeStore.SetTTL(gCtx, subtreeHash[:], 0)
			if err != nil {
				return errors.Join(errors.New("failed to update subtree TTL"), err)
			}
			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.Join(fmt.Errorf("[ValidateBlock][%s] failed to update subtree TTLs", block.Hash().String()), err)
	}

	return nil
}

func (u *BlockValidation) validateBLockSubtrees(ctx context.Context, block *model.Block, baseUrl string) error {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:ValidateBlock")
	start, stat, _ := util.StartStatFromContext(spanCtx, "ValidateBlockSubtrees")
	defer func() {
		span.Finish()
		stat.AddTime(start)
	}()

	start1 := gocore.CurrentTime()
	g, gCtx := errgroup.WithContext(spanCtx)

	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees", block.Hash().String(), len(block.Subtrees))
	missingSubtrees := make([]*chainhash.Hash, len(block.Subtrees))
	missingSubtreesMu := sync.Mutex{}
	for idx, subtreeHash := range block.Subtrees {
		subtreeHash := subtreeHash
		idx := idx
		// first check all the subtrees exist or not in our store, in parallel, and gather what is missing
		g.Go(func() error {
			// get subtree from store
			subtreeExists, err := u.subtreeStore.Exists(gCtx, subtreeHash[:])
			if err != nil {
				return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to check if subtree exists in store", subtreeHash.String()), err)
			}
			if !subtreeExists {
				// subtree already exists in store, which means it's valid
				missingSubtreesMu.Lock()
				missingSubtrees[idx] = subtreeHash
				missingSubtreesMu.Unlock()
			}

			return nil
		})
	}
	err := g.Wait()
	stat.NewStat("1. missingSubtrees").AddTime(start1)
	if err != nil {
		return err
	}

	start2 := gocore.CurrentTime()
	stat2 := stat.NewStat("2. validateSubtrees")
	// validate the missing subtrees in series, transactions might rely on each other
	for _, subtreeHash := range missingSubtrees {
		if subtreeHash != nil {
			ctx1 := util.ContextWithStat(spanCtx, stat2)
			err := u.validateSubtree(ctx1, subtreeHash, baseUrl)
			if err != nil {
				return errors.Join(fmt.Errorf("[ValidateBlock][%s] invalid subtree found [%s]", block.Hash().String(), subtreeHash.String()), err)
			}
		}
	}
	stat2.AddTime(start2)

	return nil
}

func (u *BlockValidation) validateSubtree(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string) error {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "validateSubtree")
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:ValidateBlock")
	span.LogKV("subtree", subtreeHash.String())
	defer func() {
		span.Finish()
		stat.AddTime(startTotal)
		prometheusBlockValidationValidateSubtree.Inc()
	}()

	u.logger.Infof("[validateSubtree][%s] called", subtreeHash.String())

	start := gocore.CurrentTime()
	// get subtree from store
	subtreeExists, err := u.subtreeStore.Exists(spanCtx, subtreeHash[:])
	stat.NewStat("1. subtreeExists").AddTime(start)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to check if subtree exists in store", subtreeHash.String()), err)
	}
	if subtreeExists {
		// subtree already exists in store, which means it's valid
		// TODO is this true?
		return nil
	}

	// get subtree from network over http using the baseUrl
	if baseUrl == "" {
		return fmt.Errorf("[validateSubtree][%s] baseUrl for subtree is empty", subtreeHash.String())
	}

	start = gocore.CurrentTime()
	// do http request to baseUrl + subtreeHash.String()
	u.logger.Infof("[validateSubtree][%s] getting subtree from %s", subtreeHash.String(), baseUrl)
	url := fmt.Sprintf("%s/subtree/%s", baseUrl, subtreeHash.String())
	subtreeBytes, err := util.DoHTTPRequest(spanCtx, url)
	stat.NewStat("2. http fetch subtree").AddTime(start)
	if err != nil {
		return errors.Join(fmt.Errorf("failed to do http request"), err)
	}

	start = gocore.CurrentTime()
	// the subtree bytes we got from our competing miner only contain the transaction hashes
	// it's basically just a list of 32 byte transaction hashes
	txHashes := make([]*chainhash.Hash, len(subtreeBytes)/32)
	for i := 0; i < len(subtreeBytes); i += 32 {
		txHashes[i/32], err = chainhash.NewHash(subtreeBytes[i : i+32])
		if err != nil {
			return errors.Join(errors.New("failed to create transaction hash from bytes"), err)
		}
	}
	stat.NewStat("3. createTxHashes").AddTime(start)

	nrTransactions := len(txHashes)
	if !util.IsPowerOfTwo(nrTransactions) {
		//u.logger.Warnf("subtree is not a power of two [%d], mining on incomplete tree", nrTransactions)
		height := math.Ceil(math.Log2(float64(nrTransactions)))
		nrTransactions = int(math.Pow(2, height)) // 1024 * 1024
	}

	// create the empty subtree
	subtree := util.NewTreeByLeafCount(nrTransactions)

	start = gocore.CurrentTime()
	// validate the subtree
	txMetaMap := sync.Map{}
	g, gCtx := errgroup.WithContext(spanCtx)
	g.SetLimit(1024) // max 1024 concurrent requests
	u.logger.Infof("[validateSubtree][%s] processing %d txs from subtree", subtreeHash.String(), len(txHashes))
	missingTxHashes := make([]*chainhash.Hash, len(txHashes))
	var missingTxHashesMu sync.Mutex
	for idx, txHash := range txHashes {
		txHash := txHash
		idx := idx
		g.Go(func() error {
			var txMeta *txmeta.Data
			var err error
			if txHash.IsEqual(model.CoinbasePlaceholderHash) {
				txMeta = &txmeta.Data{
					Fee:         0,
					SizeInBytes: 0,
				}
			} else {
				// is the txid in the store?
				// no - get it from the network
				// yes - is the txid blessed?
				// if all txs in tree are blessed, then bless the tree
				txMeta, err = u.txMetaStore.GetMeta(gCtx, txHash)
				if err != nil {
					if strings.Contains(err.Error(), "not found") {
						// collect all missing transactions for processing in order
						missingTxHashesMu.Lock()
						missingTxHashes[idx] = txHash
						missingTxHashesMu.Unlock()
						return nil
					} else {
						return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to get tx meta", subtreeHash.String()), err)
					}
				}
			}

			if txMeta == nil {
				return fmt.Errorf("[validateSubtree][%s] tx meta is nil [%s]", subtreeHash.String(), txHash.String())
			}

			txMetaMap.Store(txHash, txMeta)

			return nil
		})
	}

	err = g.Wait()
	stat.NewStat("4. checkTxs").AddTime(start)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to bless all transactions in subtree", subtreeHash.String()), err)
	}

	if len(missingTxHashes) > 0 {
		start, stat5, ctx5 := util.StartStatFromContext(spanCtx, "5. processMissingTransactions")
		err = u.processMissingTransactions(ctx5, subtreeHash, missingTxHashes, baseUrl, &txMetaMap)
		if err != nil {
			return err
		}
		stat5.AddTime(start)
	}

	start = gocore.CurrentTime()
	var ok bool
	var txMeta *txmeta.Data
	u.logger.Infof("[validateSubtree][%s] adding %d nodes to subtree instance", subtreeHash.String(), len(txHashes))
	for _, txHash := range txHashes {
		// finally add the transaction hash and fee to the subtree
		_txMeta, _ := txMetaMap.Load(txHash)
		txMeta, ok = _txMeta.(*txmeta.Data)
		if !ok {
			return fmt.Errorf("[validateSubtree][%s] tx meta is not of type *txmeta.Data [%s]", subtreeHash.String(), txHash.String())
		}

		err = subtree.AddNode(txHash, txMeta.Fee, txMeta.SizeInBytes)
		if err != nil {
			return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to add node to subtree", subtreeHash.String()), err)
		}
	}
	stat.NewStat("6. addAllTxHashFeeSizesToSubtree").AddTime(start)

	// does the merkle tree give the correct root?
	merkleRoot := subtree.RootHash()
	if !merkleRoot.IsEqual(subtreeHash) {
		return fmt.Errorf("[validateSubtree][%s] subtree root hash does not match [%s]", subtreeHash.String(), merkleRoot.String())
	}

	u.logger.Infof("[validateSubtree][%s] serialize subtree", subtreeHash.String())
	completeSubtreeBytes, err := subtree.Serialize()
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to serialize subtree", subtreeHash.String()), err)
	}

	start = gocore.CurrentTime()
	// store subtree in store
	u.logger.Infof("[validateSubtree][%s] store subtree", subtreeHash.String())
	err = u.subtreeStore.Set(spanCtx, merkleRoot[:], completeSubtreeBytes, options.WithTTL(u.subtreeTTL))
	stat.NewStat("7. storeSubtree").AddTime(start)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to store subtree", subtreeHash.String()), err)
	}

	// only set this on no errors
	prometheusBlockValidationValidateSubtreeDuration.Observe(util.TimeSince(startTotal))

	return nil
}

func (u *BlockValidation) processMissingTransactions(ctx context.Context, subtreeHash *chainhash.Hash, missingTxHashes []*chainhash.Hash, baseUrl string, txMetaMap *sync.Map) (err error) {
	span, spanCtx := opentracing.StartSpanFromContext(ctx, "BlockValidation:processMissingTransactions")
	defer func() {
		span.Finish()
	}()

	var txMeta *txmeta.Data
	// process all the missing transactions in order, there might be parent / child dependencies
	// TODO get these in batches
	for _, txHash := range missingTxHashes {
		if txHash != nil {
			tx, err := u.getMissingTransaction(ctx, txHash, baseUrl)
			if err != nil {
				return errors.Join(fmt.Errorf("[blessMissingTransaction][%s] failed to get transaction", txHash.String()), err)
			}

			txMeta, err = u.blessMissingTransaction(spanCtx, tx)
			if err != nil {
				return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to bless missing transaction: %s", subtreeHash.String(), txHash.String()), err)
			}
			txMetaMap.Store(txHash, txMeta)
		}
	}

	return nil
}

func (u *BlockValidation) getMissingTransaction(ctx context.Context, txHash *chainhash.Hash, baseUrl string) (*bt.Tx, error) {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "getMissingTransaction")
	defer func() {
		stat.AddTime(startTotal)
	}()

	// get transaction from network over http using the baseUrl
	if baseUrl == "" {
		return nil, fmt.Errorf("[getMissingTransaction][%s] baseUrl for transaction is empty", txHash.String())
	}

	start := gocore.CurrentTime()
	alreadyHaveTransaction := true
	txBytes, err := u.txStore.Get(ctx, txHash[:])
	stat.NewStat("getTxFromStore").AddTime(start)
	if txBytes == nil || err != nil {
		alreadyHaveTransaction = false

		// do http request to baseUrl + txHash.String()
		u.logger.Infof("[getMissingTransaction][%s] getting tx from other miner", txHash.String(), baseUrl)
		url := fmt.Sprintf("%s/tx/%s", baseUrl, txHash.String())
		startM := gocore.CurrentTime()
		statM := stat.NewStat("http fetch missing tx")
		txBytes, err = util.DoHTTPRequest(ctx, url)
		statM.AddTime(startM)
		if err != nil {
			return nil, errors.Join(fmt.Errorf("[getMissingTransaction][%s] failed to do http request", txHash.String()), err)
		}
	}

	// validate the transaction by creating a transaction object
	tx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return nil, fmt.Errorf("[getMissingTransaction][%s] failed to create transaction from bytes [%s]", txHash.String(), err.Error())
	}

	if !alreadyHaveTransaction {
		start = gocore.CurrentTime()
		// store the transaction, we did not get it via propagation
		err = u.txStore.Set(ctx, txHash[:], txBytes)
		stat.NewStat("storeTx").AddTime(start)
		if err != nil {
			return nil, fmt.Errorf("[getMissingTransaction][%s] failed to store transaction [%s]", txHash.String(), err.Error())
		}
	}

	return tx, nil
}

func (u *BlockValidation) blessMissingTransaction(ctx context.Context, tx *bt.Tx) (txMeta *txmeta.Data, err error) {
	startTotal, stat, ctx := util.StartStatFromContext(ctx, "getMissingTransaction")
	defer func() {
		stat.AddTime(startTotal)
		prometheusBlockValidationBlessMissingTransaction.Inc()
		prometheusBlockValidationBlessMissingTransactionDuration.Observe(util.TimeSince(startTotal))
	}()

	u.logger.Infof("[blessMissingTransaction][%s] called", tx.TxID())

	if tx.IsCoinbase() {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] transaction is coinbase", tx.TxID())
	}

	// validate the transaction in the validation service
	// this should spend utxos, create the tx meta and create new utxos
	// todo return tx meta data
	err = u.validatorClient.Validate(ctx, tx)
	if err != nil {
		// TODO what to do here? This could be a double spend and the transaction needs to be marked as conflicting
		return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to validate transaction [%s]", tx.TxID(), err.Error())
	}

	start := gocore.CurrentTime()
	txMeta, err = u.txMetaStore.GetMeta(ctx, tx.TxIDChainHash())
	stat.NewStat("getTxMeta").AddTime(start)
	if err != nil {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to get tx meta [%s]", tx.TxID(), err.Error())
	}

	return txMeta, nil
}
