package blockvalidation

import (
	"context"
	"github.com/bitcoin-sv/ubsv/model"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"
	"github.com/libsv/go-bt/v2/chainhash"
	"time"
)

type Interface interface {
	Health(ctx context.Context) (bool, error)
	BlockFound(ctx context.Context, blockHash *chainhash.Hash, baseUrl string, waitToComplete bool) error
	ProcessBlock(ctx context.Context, block *model.Block) error
	SubtreeFound(ctx context.Context, subtreeHash *chainhash.Hash, baseUrl string) error
	Get(ctx context.Context, subtreeHash []byte) ([]byte, error)
	Exists(ctx context.Context, subtreeHash []byte) (bool, error)
	Set(ctx context.Context, key []byte, value []byte, opts ...options.Options) error
	SetTTL(ctx context.Context, key []byte, ttl time.Duration) error
	SetTxMeta(ctx context.Context, txMetaData []*meta.Data) error
	DelTxMeta(ctx context.Context, hash *chainhash.Hash) error
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
}
