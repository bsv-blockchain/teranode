package blockvalidation

import (
	"context"
	"time"

	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/libsv/go-bt/v2/chainhash"
)

type Interface interface {
	Health(ctx context.Context) (bool, error)
	BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseUrl string, waitToComplete bool) error
	ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32) error
	SubtreeFound(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string) error
	Get(ctx context.Context, subtreeHash []byte) ([]byte, error)
	Exists(ctx context.Context, subtreeHash []byte) (bool, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration) error
	SetTxMeta(ctx context.Context, txMetaData []*meta.Data) error
	DelTxMeta(ctx context.Context, hash *chainhash.Hash) error
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
}

var _ Interface = &MockBlockValidation{}

type MockBlockValidation struct{}

func (mv *MockBlockValidation) Health(ctx context.Context) (bool, error) {
	return true, nil
}

func (mv *MockBlockValidation) BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseUrl string, waitToComplete bool) error {
	return nil
}

func (mv *MockBlockValidation) ProcessBlock(ctx context.Context, block *model.Block, blockHeight uint32) error {
	return nil
}

func (mv *MockBlockValidation) SubtreeFound(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string) error {
	return nil
}

func (mv *MockBlockValidation) Get(ctx context.Context, subtreeHash []byte) ([]byte, error) {
	return nil, nil
}

func (mv *MockBlockValidation) Exists(ctx context.Context, subtreeHash []byte) (bool, error) {
	return false, nil
}

func (mv *MockBlockValidation) Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error {
	return nil
}

func (mv *MockBlockValidation) SetTTL(ctx context.Context, key []byte, ttl time.Duration) error {
	return nil
}

func (mv *MockBlockValidation) SetTxMeta(ctx context.Context, txMetaData []*meta.Data) error {
	return nil
}

func (mv *MockBlockValidation) DelTxMeta(ctx context.Context, hash *chainhash.Hash) error {
	return nil
}

func (mv *MockBlockValidation) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	return nil
}
