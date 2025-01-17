package propagation

import (
	"context"
	"testing"

	"github.com/bitcoin-sv/teranode/errors"
	"github.com/bitcoin-sv/teranode/services/validator"
	utxostore "github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/memory"
	"github.com/bitcoin-sv/teranode/tracing"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidatorErrors(t *testing.T) {
	tracing.SetGlobalMockTracer()

	tx := bt.NewTx()

	tSettings := test.CreateBaseTestSettings()

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, tSettings, memory.New(ulogger.TestLogger{}), nil, nil)
	require.NoError(t, err)

	// _, err = v.Validate(context.Background(), tx, chaincfg.GenesisActivationHeight)
	_, err = v.Validate(context.Background(), tx, 0)
	require.Error(t, err)

	assert.True(t, errors.Is(err, errors.ErrProcessing))
	assert.True(t, errors.Is(err, errors.ErrTxInvalid))
}

type NullStore struct{}

func (ns *NullStore) SetBlockHeight(height uint32) error {
	return nil
}

func (ns *NullStore) GetBlockHeight() (uint32, error) {
	return 0, nil
}

func (ns *NullStore) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
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
	// fmt.Printf("MoveForwardBlock(%s)\n", hash.String())
	return nil
}

func (ns *NullStore) Delete(ctx context.Context, tx *bt.Tx) error {
	// fmt.Printf("MoveForwardBlock(%s)\n", hash.String())
	return nil
}
