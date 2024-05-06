package txmeta

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Error functions
func NewErrTxmetaNotFound(key *chainhash.Hash) error {
	return errors.New(errors.ERR_NOT_FOUND, fmt.Sprintf("txmeta key %q", key.String()))
}

func NewErrTxmetaAlreadyExists(key *chainhash.Hash) error {
	return errors.New(errors.ERR_TXMETA_ALREADY_EXISTS, fmt.Sprintf("txmeta key %q", key.String()))
}

type MissingTxHash struct {
	Hash   chainhash.Hash
	Idx    int
	Data   *Data // This is nil until it has been fetched
	Fields []string
	Err    error
}

type Store interface {
	Get(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	// This function is not pure as it will update the Data object in the MissingTxHash with the fetched data
	MetaBatchDecorate(ctx context.Context, hashes []*MissingTxHash, fields ...string) error
	GetMeta(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	Create(ctx context.Context, tx *bt.Tx, lockTime ...uint32) (*Data, error)
	SetMined(ctx context.Context, hash *chainhash.Hash, blockID uint32) error
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
	Delete(ctx context.Context, hash *chainhash.Hash) error
}

type TxMetaAerospikeRecord struct {
	Tx             []byte                      `as:"tx" json:"tx"`
	Fee            uint64                      `as:"fee" json:"fee"`
	SizeInBytes    uint64                      `as:"size" json:"size"`
	LockTime       uint32                      `as:"locktime" json:"locktime"`
	Utxos          map[interface{}]interface{} `as:"utxos" json:"utxos"`
	ParentTxHashes []byte                      `as:"parentTxHashes" json:"parent_tx_hashes"`
	BlockIDs       []uint32                    `as:"blockIDs" json:"block_ids"`
	IsCoinbase     bool                        `as:"isCoinbase" json:"is_coinbase"`
}
