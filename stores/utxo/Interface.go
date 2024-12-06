package utxo

import (
	"context"

	"github.com/bitcoin-sv/ubsv/stores/utxo/meta"

	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
)

const ReAssignedUtxoSpendableAfterBlocks = 1_000

// Spend is a struct that holds the txid and vout of the output being spent, the hash of the utxohash
// that being spent and the spending txid.
type Spend struct {
	TxID         *chainhash.Hash `json:"txId"`
	Vout         uint32          `json:"vout"`
	UTXOHash     *chainhash.Hash `json:"utxoHash"`
	SpendingTxID *chainhash.Hash `json:"spendingTxId,omitempty"`
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

type CreateOption func(*CreateOptions)

type CreateOptions struct {
	BlockIDs   []uint32
	TxID       *chainhash.Hash
	IsCoinbase *bool
}

func WithBlockIDs(blockIDs ...uint32) CreateOption {
	return func(o *CreateOptions) {
		o.BlockIDs = blockIDs
	}
}

func WithTXID(txID *chainhash.Hash) CreateOption {
	return func(o *CreateOptions) {
		o.TxID = txID
	}
}

func WithSetCoinbase(b bool) CreateOption {
	return func(o *CreateOptions) {
		o.IsCoinbase = &b
	}
}

// Store is the interface for the UTXO map store
type Store interface {
	// CRUD functions
	Health(ctx context.Context, checkLiveness bool) (int, string, error)

	Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error)
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

	// functions related to Alert System
	FreezeUTXOs(ctx context.Context, spends []*Spend) error
	UnFreezeUTXOs(ctx context.Context, spends []*Spend) error
	ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend) error

	// internal state functions
	SetBlockHeight(height uint32) error
	GetBlockHeight() uint32
	SetMedianBlockTime(height uint32) error
	GetMedianBlockTime() uint32
}

var _ Store = &MockUtxostore{}

type MockUtxostore struct{}

func (mu *MockUtxostore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	return 0, "Validator test", nil
}
func (mu *MockUtxostore) Create(ctx context.Context, tx *bt.Tx, blockHeight uint32, opts ...CreateOption) (*meta.Data, error) {
	options := &CreateOptions{}
	for _, opt := range opts {
		opt(options)
	}

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
	for _, outpoint := range outpoints {
		outpoint.LockingScript = []byte{}
		outpoint.Satoshis = 32_280_613_550
	}

	return nil
}

func (mu *MockUtxostore) FreezeUTXOs(ctx context.Context, spends []*Spend) error {
	return nil
}

func (mu *MockUtxostore) UnFreezeUTXOs(ctx context.Context, spends []*Spend) error {
	return nil
}

func (mu *MockUtxostore) ReAssignUTXO(ctx context.Context, utxo *Spend, newUtxo *Spend) error {
	return nil
}

func (mu *MockUtxostore) SetBlockHeight(_ uint32) error {
	return nil
}
func (mu *MockUtxostore) GetBlockHeight() uint32 {
	return 0
}
func (mu *MockUtxostore) SetMedianBlockTime(_ uint32) error {
	return nil
}
func (mu *MockUtxostore) GetMedianBlockTime() uint32 {
	return 0
}
