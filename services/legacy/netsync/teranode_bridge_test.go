package netsync

import (
	"context"
	"github.com/ordishs/gocore"
	"testing"

	"github.com/bitcoin-sv/ubsv/services/validator"
	"github.com/bitcoin-sv/ubsv/stores/txmeta/memory"
	"github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
}

func (ns *NullStore) Get(ctx context.Context, spend *utxo.Spend) (*utxo.Response, error) {
	return nil, nil
}

func (ns *NullStore) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	return nil
}

func (ns *NullStore) StoreFromHashes(ctx context.Context, txID chainhash.Hash, utxoHashes []chainhash.Hash, lockTime uint32) error {
	return nil
}

func (ns *NullStore) Spend(ctx context.Context, spends []*utxo.Spend) error {
	return nil
}

func (ns *NullStore) UnSpend(ctx context.Context, spends []*utxo.Spend) error {
	return nil
}

func (ns *NullStore) Delete(ctx context.Context, tx *bt.Tx) error {
	return nil
}

func TestTXc99c49da4c38af669dea436d3e73780dfdb6c1ecf9958baa52960e8baee30e73(t *testing.T) {
	gocore.Config().Set("blockassembly_disabled", "true")

	tx, err := bt.NewTxFromString("010000000000000000ef010276b76b07f4935c70acf54fbf1f438a4c397a9fb7e633873c4dd3bc062b6b40000000008c493046022100d23459d03ed7e9511a47d13292d3430a04627de6235b6e51a40f9cd386f2abe3022100e7d25b080f0bb8d8d5f878bba7d54ad2fda650ea8d158a33ee3cbd11768191fd004104b0e2c879e4daf7b9ab68350228c159766676a14f5815084ba166432aab46198d4cca98fa3e9981d0a90b2effc514b76279476550ba3663fdcaff94c38420e9d500000000404b4c00000000001976a914dc44b1164188067c3a32d4780f5996fa14a4f2d988ac0100093d00000000001976a9149a7b0f3b80c6baaeedce0a0842553800f832ba1f88ac00000000")
	require.NoError(t, err)

	ns := &NullStore{}

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, ns, memory.New(ulogger.TestLogger{}))
	require.NoError(t, err)

	ctx := context.Background()

	err = v.Validate(ctx, tx, 110300)
	require.NoError(t, err)
}

func TestTXfb0a1d8d34fa5537e461ac384bac761125e1bfa7fec286fa72511240fa66864d(t *testing.T) {
	tx, err := bt.NewTxFromString("010000000000000000ef012316aac445c13ff31af5f3d1e2cebcada83e54ba10d15e01f49ec28bddc285aa000000008e4b3048022200002b83d59c1d23c08efd82ee0662fec23309c3adbcbd1f0b8695378db4b14e736602220000334a96676e58b1bb01784cb7c556dd8ce1c220171904da22e18fe1e7d1510db5014104d0fe07ff74c9ef5b00fed1104fad43ecf72dbab9e60733e4f56eacf24b20cf3b8cd945bcabcc73ba0158bf9ce769d43e94bd58c5c7e331a188922b3fe9ca1f5affffffffc0c62d00000000001976a9147a2a3b481ca80c4ba7939c54d9278e50189d94f988ac01c0c62d00000000001976a9147a2a3b481ca80c4ba7939c54d9278e50189d94f988ac00000000")
	require.NoError(t, err)

	assert.Equal(t, "fb0a1d8d34fa5537e461ac384bac761125e1bfa7fec286fa72511240fa66864d", tx.TxIDChainHash().String())

	ns := &NullStore{}

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, ns, memory.New(ulogger.TestLogger{}))
	require.NoError(t, err)

	ctx := context.Background()

	err = v.Validate(ctx, tx, 124276)
	require.NoError(t, err)
}
