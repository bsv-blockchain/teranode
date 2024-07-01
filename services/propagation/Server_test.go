package propagation

import (
	"context"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"

	"github.com/bitcoin-sv/ubsv/services/validator"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/require"
)

func TestValidatorErrors(t *testing.T) {
	tx := bt.NewTx()

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}))
	require.NoError(t, err)
	err = v.Validate(context.Background(), tx, util.GenesisActivationHeight)
	require.Error(t, err)
	// TODO - SAO - I am disabling this as it will be changed when the new error system is applied
	// assert.True(t, errors.Is(err, errors.ErrProcessing))
}

type NullStore struct{}

func (ns *NullStore) SetBlockHeight(height uint32) error {
	return nil
}

func (ns *NullStore) GetBlockHeight() (uint32, error) {
	return 0, nil
}

func (ns *NullStore) Health(ctx context.Context) (int, string, error) {
	return 0, "Validator test Null Store", nil
}

func (ns *NullStore) DeleteSpends(deleteSpends bool) {
	// No nothing
}

func (ns *NullStore) Get(ctx context.Context, spend *utxostore.Spend) (*utxostore.SpendResponse, error) {
	// fmt.Printf("Get(%s)\n", hash.String())
	return nil, nil
}

func (ns *NullStore) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	// fmt.Printf("Store(%s)\n", hash.String())
	return nil
}

func (ns *NullStore) StoreFromHashes(ctx context.Context, txID chainhash.Hash, utxoHashes []chainhash.Hash, lockTime uint32) error {
	return nil
}

func (ns *NullStore) Spend(ctx context.Context, spends []*utxostore.Spend, blockHeight uint32) error {
	// fmt.Printf("Spend(%s, %s)\n", hash.String(), txID.String())
	return nil
}

func (ns *NullStore) UnSpend(ctx context.Context, spends []*utxostore.Spend) error {
	// fmt.Printf("MoveUpBlock(%s)\n", hash.String())
	return nil
}

func (ns *NullStore) Delete(ctx context.Context, tx *bt.Tx) error {
	// fmt.Printf("MoveUpBlock(%s)\n", hash.String())
	return nil
}
