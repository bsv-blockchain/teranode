package utxo

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

// Spend is a struct that holds the txid and vout of the output being spent, the hash of the utxohash
// that being spent and the spending txid.
type Spend struct {
	TxID         *chainhash.Hash `json:"txId"`
	Vout         uint32          `json:"vout"`
	UTXOHash     *chainhash.Hash `json:"utxoHash"`
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`
}

// SpendResponse is a struct that holds the response from the GetSpend function
type SpendResponse struct {
	Status       int             `json:"status"`
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`
	LockTime     uint32          `json:"lockTime,omitempty"`
}

var (
	MetaFields       = []string{"locktime", "fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase"}
	MetaFieldsWithTx = []string{"tx", "locktime", "fee", "sizeInBytes", "parentTxHashes", "blockIDs", "isCoinbase"}
)

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

	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, blockIDs ...uint32) (*meta.Data, error)
	Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error)
	Delete(ctx context.Context, hash *chainhash.Hash) error

	GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error)    // Remove? Only used in tests
	GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) // Remove?

	// Blockchain specific functions
	Spend(ctx context.Context, spends []*Spend, blockHeight uint32) error
	UnSpend(ctx context.Context, spends []*Spend) error
	SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error

	// these functions are not pure as they will update the data object in place
	BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...string) error
	PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error

	// internal state functions
	SetBlockHeight(height uint32) error
	GetBlockHeight() (uint32, error)
}

var _ Store = &MockUtxostore{}

type MockUtxostore struct{}

func (mu *MockUtxostore) Health(ctx context.Context) (int, string, error) {
	return 0, "Validator test", nil
}

func (mu *MockUtxostore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, blockIDs ...uint32) (*meta.Data, error) {
	return nil, nil
}

func (mu *MockUtxostore) Get(ctx context.Context, hash *chainhash.Hash, fields ...[]string) (*meta.Data, error) {
	return nil, nil

}
func (mu *MockUtxostore) Delete(ctx context.Context, hash *chainhash.Hash) error {
	return nil

}

func (mu *MockUtxostore) GetSpend(ctx context.Context, spend *Spend) (*SpendResponse, error) {
	return nil, nil
}
func (mu *MockUtxostore) GetMeta(ctx context.Context, hash *chainhash.Hash) (*meta.Data, error) {
	return nil, nil
}

func (mu *MockUtxostore) Spend(ctx context.Context, spends []*Spend, blockHeight uint32) error {
	return nil

}
func (mu *MockUtxostore) UnSpend(ctx context.Context, spends []*Spend) error {
	return nil

}
func (mu *MockUtxostore) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, blockID uint32) error {
	return nil

}

func (mu *MockUtxostore) BatchDecorate(ctx context.Context, unresolvedMetaDataSlice []*UnresolvedMetaData, fields ...string) error {
	return nil

}
func (mu *MockUtxostore) PreviousOutputsDecorate(ctx context.Context, outpoints []*meta.PreviousOutput) error {
	return nil
}

func (mu *MockUtxostore) SetBlockHeight(height uint32) error {
	return nil
}
func (mu *MockUtxostore) GetBlockHeight() (uint32, error) {
	return 0, nil
}
