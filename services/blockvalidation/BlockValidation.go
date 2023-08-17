package blockvalidation

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"

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
	txMetaStore      txmeta.Store
	validatorClient  validator.Interface
	httpClient       *http.Client
}

func NewBlockValidation(logger utils.Logger, blockchainClient blockchain.ClientI, subtreeStore blob.Store,
	txMetaStore txmeta.Store, validatorClient validator.Interface) *BlockValidation {

	bv := &BlockValidation{
		logger:           logger,
		blockchainClient: blockchainClient,
		subtreeStore:     subtreeStore,
		txMetaStore:      txMetaStore,
		validatorClient:  validatorClient,
		httpClient:       &http.Client{},
	}

	return bv
}

func (u *BlockValidation) BlockFound(ctx context.Context, block *model.Block, baseUrl string) error {
	g, _ := errgroup.WithContext(ctx)

	for _, subtreeHash := range block.Subtrees {
		st := subtreeHash

		g.Go(func() error {
			isValid := u.validateSubtree(ctx, st, baseUrl)
			if !isValid {
				return fmt.Errorf("invalid subtree found [%s]", st.String())
			}

			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	blockHeaders, err := u.blockchainClient.GetBlockHeaders(ctx, block.Header.HashPrevBlock, 100)
	if err != nil {
		return err
	}

	// validate the block
	// TODO do we pass in the subtreeStore here or the list of loaded subtrees?
	if ok, err := block.Valid(ctx, u.subtreeStore, u.txMetaStore, blockHeaders); !ok {
		return fmt.Errorf("block is not valid: %s - %v", block.String(), err)
	}

	// if valid, store the block
	if err := u.blockchainClient.AddBlock(ctx, block); err != nil {
		return fmt.Errorf("failed to store block [%w]", err)
	}

	return nil
}

func (u *BlockValidation) validateSubtree(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string) bool {
	// get subtree from store
	subtreeExists, err := u.subtreeStore.Exists(ctx, subtreeHash[:])
	if err != nil {
		u.logger.Errorf("failed to check if subtree exists in store [%s]", err.Error())
		return false
	}
	if subtreeExists {
		// subtree already exists in store, which means it's valid
		// TODO is this true?
		return true
	}

	// get subtree from network over http using the baseUrl
	if baseUrl == "" {
		u.logger.Errorf("baseUrl for subtree is empty [%s]", subtreeHash.String())
		return false
	}

	// do http request to baseUrl + subtreeHash.String()
	url := fmt.Sprintf("%s/subtree/%s", baseUrl, subtreeHash.String())
	subtreeBytes, err := util.DoHTTPRequest(ctx, url)
	if err != nil {
		u.logger.Errorf("failed to do http request [%s]", err.Error())
		return false
	}

	// the subtree bytes we got from our competing miner only contain the transaction hashes
	// it's basically just a list of 32 byte transaction hashes
	txHashes := make([]*chainhash.Hash, len(subtreeBytes)/32)
	for i := 0; i < len(subtreeBytes); i += 32 {
		txHashes[i/32], err = chainhash.NewHash(subtreeBytes[i : i+32])
		if err != nil {
			u.logger.Errorf("failed to create transaction hash from bytes [%s]", err.Error())
			return false
		}
	}

	nrTransactions := len(txHashes)
	if !util.IsPowerOfTwo(nrTransactions) {
		u.logger.Warnf("subtree is not a power of two [%d], mining on incomplete tree", nrTransactions)
		height := math.Ceil(math.Log2(float64(nrTransactions)))
		nrTransactions = int(math.Pow(2, height)) // 1024 * 1024
	}

	// create the empty subtree
	subtree := util.NewTreeByLeafCount(nrTransactions)

	// validate the subtree
	var txMeta *txmeta.Data
	for _, txHash := range txHashes {
		// is the txid in the store?
		// no - get it from the network
		// yes - is the txid blessed?
		// if all txs in tree are blessed, then bless the tree
		txMeta, err = u.txMetaStore.Get(ctx, txHash)
		if err != nil {
			if errors.Is(err, txmeta.ErrNotFound) {
				txMeta, err = u.blessMissingTransaction(ctx, txHash, baseUrl)
				if err != nil {
					u.logger.Errorf("failed to bless missing transaction [%s]", err.Error())
					return false
				}
				// there was no error, so the transaction has been blessed
			} else {
				u.logger.Errorf("failed to get tx meta [%s]", err.Error())
				return false
			}
		}
		if txMeta == nil {
			u.logger.Errorf("tx meta is nil [%s]", txHash.String())
			return false
		}

		// finally add the transaction hash and fee to the subtree
		err = subtree.AddNode(txHash, txMeta.Fee)
		if err != nil {
			u.logger.Errorf("failed to add node to subtree [%s]", err.Error())
			return false
		}
	}

	// does the merkle tree give the correct root?
	merkleRoot := subtree.RootHash()
	if !merkleRoot.IsEqual(subtreeHash) {
		u.logger.Errorf("subtree root hash does not match [%s] [%s]", merkleRoot.String(), subtreeHash.String())
		return false
	}

	completeSubtreeBytes, err := subtree.Serialize()
	if err != nil {
		u.logger.Errorf("failed to serialize subtree [%s]", err.Error())
		return false
	}

	// store subtree in store
	err = u.subtreeStore.Set(ctx, merkleRoot[:], completeSubtreeBytes)
	if err != nil {
		u.logger.Errorf("failed to store subtree [%s]", err.Error())
		return false
	}

	return true
}

func (u *BlockValidation) blessMissingTransaction(ctx context.Context, txHash *chainhash.Hash, baseUrl string) (*txmeta.Data, error) {
	// get transaction from network over http using the baseUrl
	if baseUrl == "" {
		return nil, fmt.Errorf("baseUrl for transaction is empty [%s]", txHash.String())
	}

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

	txBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read http response body [%s]", err.Error())
	}

	// validate the transaction by creating a transaction object
	tx, err := bt.NewTxFromBytes(txBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction from bytes [%s]", err.Error())
	}

	if tx.IsCoinbase() {
		return nil, fmt.Errorf("transaction is coinbase [%s]", txHash.String())
	}

	// validate the transaction in the validation service
	// TODO should this request over network, whereby it will be added to block assembly?
	err = u.validatorClient.Validate(ctx, tx)
	if err != nil {
		// TODO what to do here? This could be a double spend and the transaction needs to be marked as conflicting
		return nil, fmt.Errorf("failed to validate transaction [%s]", err.Error())
	}

	txMeta, err := u.txMetaStore.Get(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to get tx meta [%s]", err.Error())
	}

	return txMeta, nil
}
