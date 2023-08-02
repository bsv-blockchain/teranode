package coinbasetracker

import (
	"context"

	"github.com/TAAL-GmbH/ubsv/db/model"
	networkModel "github.com/TAAL-GmbH/ubsv/model"
	"github.com/TAAL-GmbH/ubsv/services/blockchain"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-p2p/chaincfg/chainhash"
	"github.com/ordishs/go-utils"
)

type CoinbaseTracker struct {
	logger           utils.Logger
	blockchainClient blockchain.ClientI
}

func NewCoinbaseTracker(logger utils.Logger, blockchainClient blockchain.ClientI) *CoinbaseTracker {

	ct := &CoinbaseTracker{
		logger:           logger,
		blockchainClient: blockchainClient,
	}

	return ct
}

func (u *CoinbaseTracker) GetBestBlockFromDb(ctx context.Context) (*model.Block, error) {
	return nil, nil
}
func (u *CoinbaseTracker) GetBestBlockFromNetwork(ctx context.Context) (*model.Block, error) {
	return nil, nil
}
func (u *CoinbaseTracker) GetBlockFromNetwork(ctx context.Context, hash string) (*networkModel.Block, error) {
	// get block from network
	// unmarshal block
	// 	b, err := networkModel.NewBlockFromBytes()
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	coinbaseTx := b.CoinbaseTx
	// for _, utxo := range coinbaseTx.Outputs{
	// 	// store coinbase
	// }

	return nil, nil
}

// AddBlock will add a block to the database
func (u *CoinbaseTracker) AddBlock(ctx context.Context, block *model.Block) error {
	return nil
}

// AddUtxo will add a utxo to the database
func (u *CoinbaseTracker) AddUtxo(ctx context.Context, block *model.UTXO) error {
	return nil
}

func (u *CoinbaseTracker) GetUtxos(ctx context.Context, publickey *chainhash.Hash, amount uint64) ([]*bt.UTXO, error) {
	return nil, nil
}

func (u *CoinbaseTracker) SubmitTransaction(ctx context.Context, transaction []byte) error {
	return nil
}
