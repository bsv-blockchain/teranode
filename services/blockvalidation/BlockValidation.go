package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/services/blockassembly"
	"github.com/bitcoin-sv/ubsv/services/blockchain"
	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/txmeta"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/ordishs/go-utils"
	"golang.org/x/sync/errgroup"
)

type BlockValidation struct {
	logger           utils.Logger
	blockchainClient blockchain.ClientI
	subtreeStore     blob.Store
	txStore          blob.Store
	txMetaStore      txmeta.Store
	validatorClient  validator.Interface
	httpClient       *http.Client
}

func NewBlockValidation(logger utils.Logger, blockchainClient blockchain.ClientI, subtreeStore blob.Store,
	txStore blob.Store, txMetaStore txmeta.Store, validatorClient validator.Interface) *BlockValidation {

	bv := &BlockValidation{
		logger:           logger,
		blockchainClient: blockchainClient,
		subtreeStore:     subtreeStore,
		txStore:          txStore,
		txMetaStore:      txMetaStore,
		validatorClient:  validatorClient,
		httpClient:       &http.Client{},
	}

	return bv
}

func (u *BlockValidation) ValidateBlock(ctx context.Context, block *model.Block, baseUrl string) error {
	timeStart := time.Now()
	prometheusBlockValidationValidateBlock.Inc()
	u.logger.Infof("[ValidateBlock][%s] called", block.Header.Hash().String())

	g, gCtx := errgroup.WithContext(ctx)

	u.logger.Infof("[ValidateBlock][%s] validating %d subtrees", block.Hash().String(), len(block.Subtrees))
	for _, subtreeHash := range block.Subtrees {
		st := subtreeHash

		g.Go(func() error {
			err := u.validateSubtree(gCtx, st, baseUrl)
			if err != nil {
				return errors.Join(fmt.Errorf("[ValidateBlock][%s] invalid subtree found [%s]", block.Hash().String(), st.String()), err)
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
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

	// get all the subtrees from the block. This should have been loaded during validation, so should be instant
	u.logger.Infof("[ValidateBlock][%s] get subtrees", block.Hash().String())
	subtrees, err := block.GetSubtrees(u.subtreeStore)
	if err != nil {
		return fmt.Errorf("[ValidateBlock][%s] failed to get subtrees from block [%w]", block.Hash().String(), err)
	}

	// add the transactions in this block to the txMeta block hashes
	u.logger.Infof("[ValidateBlock][%s] update tx mined", block.Hash().String())
	if err = blockassembly.UpdateTxMinedStatus(ctx, u.txMetaStore, subtrees, block.Header); err != nil {
		// TODO this should be a fatal error, but for now we just log it
		//return nil, fmt.Errorf("[BlockAssembly] error updating tx mined status: %w", err)
		u.logger.Errorf("[ValidateBlock][%s] error updating tx mined status: %w", block.Hash().String(), err)
	}

	u.logger.Infof("[ValidateBlock][%s] storing coinbase tx: %s", block.Hash().String(), block.CoinbaseTx.TxIDChainHash().String())
	if err = u.txStore.Set(ctx, block.CoinbaseTx.TxIDChainHash()[:], block.CoinbaseTx.Bytes()); err != nil {
		u.logger.Errorf("[ValidateBlock][%s] failed to store coinbase transaction [%w]", block.Hash().String(), err)
	}

	prometheusBlockValidationValidateBlockDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return nil
}

func (u *BlockValidation) validateSubtree(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string) error {
	timeStart := time.Now()
	prometheusBlockValidationValidateSubtree.Inc()
	u.logger.Infof("[validateSubtree][%s] called", subtreeHash.String())

	// get subtree from store
	subtreeExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:])
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to check if subtree exists in store [%s]", subtreeHash.String(), err))
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

	// do http request to baseUrl + subtreeHash.String()
	u.logger.Infof("[validateSubtree][%s] getting subtree from %s", subtreeHash.String(), baseUrl)
	url := fmt.Sprintf("%s/subtree/%s", baseUrl, subtreeHash.String())
	subtreeBytes, err := util.DoHTTPRequest(ctx, url)
	if err != nil {
		return errors.Join(fmt.Errorf("failed to do http request"), err)
	}

	// the subtree bytes we got from our competing miner only contain the transaction hashes
	// it's basically just a list of 32 byte transaction hashes
	txHashes := make([]*chainhash.Hash, len(subtreeBytes)/32)
	for i := 0; i < len(subtreeBytes); i += 32 {
		txHashes[i/32], err = chainhash.NewHash(subtreeBytes[i : i+32])
		if err != nil {
			return errors.Join(fmt.Errorf("failed to create transaction hash from bytes [%s]", err))
		}
	}

	nrTransactions := len(txHashes)
	if !util.IsPowerOfTwo(nrTransactions) {
		//u.logger.Warnf("subtree is not a power of two [%d], mining on incomplete tree", nrTransactions)
		height := math.Ceil(math.Log2(float64(nrTransactions)))
		nrTransactions = int(math.Pow(2, height)) // 1024 * 1024
	}

	// create the empty subtree
	subtree := util.NewTreeByLeafCount(nrTransactions)

	// validate the subtree
	txMetaMap := sync.Map{}
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(100) // max 100 concurrent requests
	u.logger.Infof("[validateSubtree][%s] processing %d txs from subtree", subtreeHash.String(), len(txHashes))
	for _, txHash := range txHashes {
		txHash := txHash
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
				txMeta, err = u.txMetaStore.Get(gCtx, txHash)
				if err != nil {
					if strings.Contains(err.Error(), "not found") {
						txMeta, err = u.blessMissingTransaction(gCtx, txHash, baseUrl)
						if err != nil {
							return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to bless missing transaction: %s", subtreeHash.String(), txHash.String()), err)
						}
						// there was no error, so the transaction has been blessed
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

	if err = g.Wait(); err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to bless all transactions in subtree", subtreeHash.String()), err)
	}

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

	// store subtree in store
	u.logger.Infof("[validateSubtree][%s] store subtree", subtreeHash.String())
	err = u.subtreeStore.Set(ctx, merkleRoot[:], completeSubtreeBytes)
	if err != nil {
		return errors.Join(fmt.Errorf("[validateSubtree][%s] failed to store subtree", subtreeHash.String()), err)
	}

	prometheusBlockValidationValidateSubtreeDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return nil
}

func (u *BlockValidation) blessMissingTransaction(ctx context.Context, txHash *chainhash.Hash, baseUrl string) (*txmeta.Data, error) {
	// get transaction from network over http using the baseUrl
	if baseUrl == "" {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] baseUrl for transaction is empty", txHash.String())
	}
	u.logger.Infof("[blessMissingTransaction][%s] called", txHash.String())

	startTime := time.Now()
	prometheusBlockValidationBlessMissingTransaction.Inc()

	alreadyHaveTransaction := true
	txBytes, err := u.txStore.Get(ctx, txHash[:])
	if txBytes == nil || err != nil {
		alreadyHaveTransaction = false

		// do http request to baseUrl + txHash.String()
		u.logger.Infof("[blessMissingTransaction][%s] getting tx from other miner", txHash.String(), baseUrl)
		url := fmt.Sprintf("%s/tx/%s", baseUrl, txHash.String())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to create http request [%s]", txHash.String(), err.Error())
		}

		resp, err := u.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to do http request [%s]", txHash.String(), err.Error())
		}
		defer resp.Body.Close()

		txBytes, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to read http response body [%s]", txHash.String(), err.Error())
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("[blessMissingTransaction][%s] http response status code [%d]: %s", txHash.String(), resp.StatusCode, resp.Status)
		}
	}

	// validate the transaction by creating a transaction object
	tx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to create transaction from bytes [%s]", txHash.String(), err.Error())
	}

	if tx.IsCoinbase() {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] transaction is coinbase", txHash.String())
	}

	if !alreadyHaveTransaction {
		// store the transaction, we did not get it via propagation
		err = u.txStore.Set(ctx, txHash[:], txBytes)
		if err != nil {
			return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to store transaction [%s]", txHash.String(), err.Error())
		}
	}

	// validate the transaction in the validation service
	// this should spend utxos, create the tx meta and create new utxos
	err = u.validatorClient.Validate(ctx, tx)
	if err != nil {
		// TODO what to do here? This could be a double spend and the transaction needs to be marked as conflicting
		return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to validate transaction [%s]", txHash.String(), err.Error())
	}

	txMeta, err := u.txMetaStore.Get(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("[blessMissingTransaction][%s] failed to get tx meta [%s]", txHash.String(), err.Error())
	}

	prometheusBlockValidationBlessMissingTransactionDuration.Observe(time.Since(startTime).Seconds())

	return txMeta, nil
}
