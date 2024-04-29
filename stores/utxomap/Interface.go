package utxomap

import (
	"context"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Spend is a struct that holds the txid and vout of the output being spent, the hash of the tx
// that being spent and the spending txid.
type Spend struct {
	TxID         *chainhash.Hash `json:"txId"`
	Vout         uint32          `json:"vout"`
	Hash         *chainhash.Hash `json:"hash"`
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`
}

// SpendResponse is a struct that holds the response from the GetSpend function
type SpendResponse struct {
	Status       int             `json:"status"`
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`
	LockTime     uint32          `json:"lockTime,omitempty"`
}

// UnresolvedMetaData is a struct that holds the hash of a tx and the index in the original list
// of hashes that was passed to the MetaBatchDecorate function. It also holds the optional fields
// that should be fetched and the error that was returned when fetching the data.
type UnresolvedMetaData struct {
	Hash   chainhash.Hash // hash of the tx
	Idx    int            // index in the original list
	Data   *Data          // This is nil until it has been fetched
	Fields []string       // optional fields to fetch
	Err    error          // returned error
}

// Interface is the interface for the UTXO map store
type Interface interface {
	Health(ctx context.Context) (int, string, error)
	Create(ctx context.Context, tx *bt.Tx, lockTime ...uint32) (*Data, error)
	Get(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error)
	GetMeta(ctx context.Context, hash *chainhash.Hash) (*Data, error)
	// This function is not pure as it will update the Data object in the MissingTxHash with the fetched data
	MetaBatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...string) error
	Spend(ctx context.Context, spends []*Spend) error
	UnSpend(ctx context.Context, spends []*Spend) error
	SetMined(ctx context.Context, hash *chainhash.Hash, blockID uint32) error
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error
	Delete(ctx context.Context, hash *chainhash.Hash) error

	// internal state functions
	DeleteSpends(deleteSpends bool)
	SetBlockHeight(height uint32) error
	GetBlockHeight() (uint32, error)
}
