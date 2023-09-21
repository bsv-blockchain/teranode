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

	g, gCtx := errgroup.WithContext(ctx)

	var sizeInBytes uint64
	sizeInBytesCh := make(chan uint64)

	go func() {
		for s := range sizeInBytesCh {
			sizeInBytes += s
		}
	}()

	for _, subtreeHash := range block.Subtrees {
		st := subtreeHash

		g.Go(func() error {
			err := u.validateSubtree(gCtx, st, baseUrl)
			if err != nil {
				return errors.Join(fmt.Errorf("invalid subtree found [%s]", st.String()), err)
			}

			sizeInBytesCh <- 0 // TODO get the size all the transactions of the subtree
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	close(sizeInBytesCh)

	blockHeaders, _, err := u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, 100)
	if err != nil {
		return err
	}

	// Add the coinbase transaction to the metaTxStore
	// TODO - we need to consider if we can do this differently
	if err = u.txMetaStore.Create(ctx, block.CoinbaseTx.TxIDChainHash(), 0, 0, nil, nil, 0); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			return fmt.Errorf("failed to create coinbase transaction in txMetaStore [%s]", err.Error())
		}
	}

	sizeInBytes += uint64(block.CoinbaseTx.Size())

	// validate the block
	// TODO do we pass in the subtreeStore here or the list of loaded subtrees?
	if ok, err := block.Valid(ctx, u.subtreeStore, u.txMetaStore, blockHeaders); !ok {
		return fmt.Errorf("block is not valid: %s - %v", block.String(), err)
	}

	// Store the size of the block
	block.SizeInBytes = 80 + util.VarintSize(block.TransactionCount) + sizeInBytes

	// if valid, store the block
	if err = u.blockchainClient.AddBlock(ctx, block); err != nil {
		return fmt.Errorf("failed to store block [%w]", err)
	}

	if err = u.txStore.Set(ctx, block.CoinbaseTx.TxIDChainHash()[:], block.CoinbaseTx.Bytes()); err != nil {
		u.logger.Errorf("failed to store coinbase transaction [%w]", err)
	}

	prometheusBlockValidationValidateBlockDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return nil
}

func (u *BlockValidation) validateSubtree(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string) error {
	timeStart := time.Now()
	prometheusBlockValidationValidateSubtree.Inc()
	u.logger.Infof("validating subtree [%s]", subtreeHash.String())

	// get subtree from store
	subtreeExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:])
	if err != nil {
		return errors.Join(fmt.Errorf("failed to check if subtree exists in store [%s]", err))
	}
	if subtreeExists {
		// subtree already exists in store, which means it's valid
		// TODO is this true?
		return nil
	}

	// get subtree from network over http using the baseUrl
	if baseUrl == "" {
		return fmt.Errorf("baseUrl for subtree is empty [%s]", subtreeHash.String())
	}

	// do http request to baseUrl + subtreeHash.String()
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
							return errors.Join(fmt.Errorf("failed to bless missing transaction: %s", txHash.String()), err)
						}
						// there was no error, so the transaction has been blessed
					} else {
						return errors.Join(fmt.Errorf("failed to get tx meta"), err)
					}
				}
			}

			if txMeta == nil {
				return fmt.Errorf("tx meta is nil [%s]", txHash.String())
			}

			txMetaMap.Store(txHash, txMeta)

			return nil
		})
	}

	if err = g.Wait(); err != nil {
		return errors.Join(fmt.Errorf("failed to bless all transactions in subtree %s", subtreeHash.String()), err)
	}

	var ok bool
	var txMeta *txmeta.Data
	for _, txHash := range txHashes {
		// finally add the transaction hash and fee to the subtree
		_txMeta, _ := txMetaMap.Load(txHash)
		txMeta, ok = _txMeta.(*txmeta.Data)
		if !ok {
			return fmt.Errorf("tx meta is not of type *txmeta.Data [%s]", txHash.String())
		}

		err = subtree.AddNode(txHash, txMeta.Fee, txMeta.SizeInBytes)
		if err != nil {
			return errors.Join(fmt.Errorf("failed to add node to subtree"), err)
		}
	}

	// does the merkle tree give the correct root?
	merkleRoot := subtree.RootHash()
	if !merkleRoot.IsEqual(subtreeHash) {
		return fmt.Errorf("subtree root hash does not match [%s] [%s]", merkleRoot.String(), subtreeHash.String())
	}

	completeSubtreeBytes, err := subtree.Serialize()
	if err != nil {
		return errors.Join(fmt.Errorf("failed to serialize subtree"), err)
	}

	// store subtree in store
	err = u.subtreeStore.Set(ctx, merkleRoot[:], completeSubtreeBytes)
	if err != nil {
		return errors.Join(fmt.Errorf("failed to store subtree"), err)
	}

	prometheusBlockValidationValidateSubtreeDuration.Observe(float64(time.Since(timeStart).Microseconds()))

	return nil
}

func (u *BlockValidation) blessMissingTransaction(ctx context.Context, txHash *chainhash.Hash, baseUrl string) (*txmeta.Data, error) {
	// get transaction from network over http using the baseUrl
	if baseUrl == "" {
		return nil, fmt.Errorf("baseUrl for transaction is empty [%s]", txHash.String())
	}

	startTime := time.Now()
	prometheusBlockValidationBlessMissingTransaction.Inc()

	alreadyHaveTransaction := true
	txBytes, err := u.txStore.Get(ctx, txHash[:])
	if txBytes == nil || err != nil {
		alreadyHaveTransaction = false

		// do http request to baseUrl + txHash.String()
		url := fmt.Sprintf("%s/tx/%s", baseUrl, txHash.String())
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create http request [%s]", err.Error())
		}

		resp, err := u.httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to do http request [%s]", err.Error())
		}
		defer resp.Body.Close()

		txBytes, err = io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("failed to read http response body [%s]", err.Error())
		}

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("http response status code [%d]: %s", resp.StatusCode, resp.Status)
		}
	}

	// validate the transaction by creating a transaction object
	tx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction from bytes [%s]", err.Error())
	}

	if tx.IsCoinbase() {
		return nil, fmt.Errorf("transaction is coinbase [%s]", txHash.String())
	}

	if !alreadyHaveTransaction {
		// store the transaction, we did not get it via propagation
		err = u.txStore.Set(ctx, txHash[:], txBytes)
		if err != nil {
			return nil, fmt.Errorf("failed to store transaction [%s]", err.Error())
		}
	}

	// validate the transaction in the validation service
	err = u.validatorClient.Validate(ctx, tx)
	if err != nil {
		// TODO what to do here? This could be a double spend and the transaction needs to be marked as conflicting
		return nil, fmt.Errorf("failed to validate transaction [%s]", err.Error())
	}

	txMeta, err := u.txMetaStore.Get(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get tx meta [%s]", err.Error())
	}

	prometheusBlockValidationBlessMissingTransactionDuration.Observe(float64(time.Since(startTime).Microseconds()))

	return txMeta, nil
}
