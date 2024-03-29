package propagation

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"

	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestErr1(t *testing.T) {
	err := fmt.Errorf("test error: %w", ErrBadRequest)
	require.True(t, errors.Is(err, ErrBadRequest))
	require.False(t, errors.Is(err, ErrInternal))
	assert.Equal(t, "test error: PROPAGATION_BAD_REQUEST", err.Error())
}

func TestErr2(t *testing.T) {
	err := fmt.Errorf("test error: %w: %w", ErrBadRequest, ErrInternal)
	require.True(t, errors.Is(err, ErrBadRequest))
	require.True(t, errors.Is(err, ErrInternal))
	assert.Equal(t, "test error: PROPAGATION_BAD_REQUEST: PROPAGATION_INTERNAL", err.Error())
}

func TestErr3(t *testing.T) {
	err := fmt.Errorf("test error: %v: %w", ErrBadRequest, ErrInternal)
	require.False(t, errors.Is(err, ErrBadRequest))
	require.True(t, errors.Is(err, ErrInternal))
	assert.Equal(t, "test error: PROPAGATION_BAD_REQUEST: PROPAGATION_INTERNAL", err.Error())
}

func TestValidatorErrors(t *testing.T) {
	tx := bt.NewTx()

	ns := &NullStore{}

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, ns, memory.New(ulogger.TestLogger{}))
	require.NoError(t, err)
	err = v.Validate(context.Background(), tx, validator.GenesisActivationHeight)
	require.Error(t, err)
	assert.True(t, errors.Is(err, validator.ErrBadRequest))
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

func (ns *NullStore) Get(ctx context.Context, spend *utxostore.Spend) (*utxostore.Response, error) {
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

func (ns *NullStore) Spend(ctx context.Context, spends []*utxostore.Spend) error {
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
