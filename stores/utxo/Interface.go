package utxo

import (
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"

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
	Data   *meta.Data     // This is nil until it has been fetched
	Fields []string       // optional fields to fetch
	Err    error          // returned error
}

// Store is the interface for the UTXO map store
type Store interface {
	// CRUD functions
	Health(ctx context.Context) (int, string, error)
	Create(ctx context.Context, tx *bt.Tx, blockIDs ...uint32) (*meta.Data, error)
	Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error)
	GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error)    // Remove? Only used in tests
	GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) // Remove?
	Delete(ctx context.Context, hash *chainhash.Hash) error

	// Blockchain specific functions
	Spend(ctx context.Context, spends []*Spend) error
	UnSpend(ctx context.Context, spends []*Spend) error
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error

	// these functions are not pure as they will update the data object in place
	MetaBatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...string) error
	PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error

	// internal state functions
	SetBlockHeight(height uint32) error
	GetBlockHeight() (uint32, error)
}
