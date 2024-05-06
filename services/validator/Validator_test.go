package validator_test

import (
	"context"
	"fmt"
	"log"
	"testing"

	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bitcoin-sv/ubsv/services/validator"
	utxostore "github.com/bitcoin-sv/ubsv/stores/utxo"
	"github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	utxoMemorystore "github.com/bitcoin-sv/ubsv/stores/utxo/memory"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/bitcoin-sv/ubsv/util"
	"github.com/libsv/go-bt/v2"
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

func (ns *NullStore) Get(ctx context.Context, spend *utxostore.Spend) (*utxostore.SpendResponse, error) {
	return nil, nil
}

func (ns *NullStore) Store(ctx context.Context, tx *bt.Tx, lockTime ...uint32) error {
	return nil
}

func (ns *NullStore) StoreFromHashes(ctx context.Context, txID chainhash.Hash, utxoHashes []chainhash.Hash, lockTime uint32) error {
	return nil
}

func (ns *NullStore) Spend(ctx context.Context, spends []*utxostore.Spend) error {
	return nil
}

func (ns *NullStore) UnSpend(ctx context.Context, spends []*utxostore.Spend) error {
	return nil
}

func (ns *NullStore) Delete(ctx context.Context, tx *bt.Tx) error {
	return nil
}

func BenchmarkValidator(b *testing.B) {
	tx, err := bt.NewTxFromString("010000000000000000ef01f3f0d33a5c5afd524043762f8b812999caa5a225e6e20ecdb71a7e0e1c207b43530000006a473044022049e20908f21bdcb901b5c5a9a93b238446606267e19db4e662df1a7c4a5bae08022036960a340515e2cfee79b9c194093f24f253d4243bf9d0baa97352983e2263fa412102a98c1a3be041da2591761fbef4b2ab0f147aef36c308aee66df0b9825218de23ffffffff10000000000000001976a914a8d6bd6648139d95dac35d411c592b05bc0973aa88ac01000000000000000070006a0963657274696861736822314c6d763150594d70387339594a556e374d3948565473446b64626155386b514e4a403263333934306361313334353331373035326334346630613861636362323162323165633131386465646330396538643764393064323166333935663063613000000000")
	if err != nil {
		panic(err)
	}

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, memory.New(ulogger.TestLogger{}))
	if err != nil {
		panic(err)
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := v.Validate(context.Background(), tx, util.GenesisActivationHeight); err != nil {
			log.Printf("ERROR: %v\n", err)
		} else {
			fmt.Println("asd")
		}
	}
}

func TestValidate_CoinbaseTransaction(t *testing.T) {
	coinbaseHex := "01000000010000000000000000000000000000000000000000000000000000000000000000ffffffff1703fb03002f6d322d75732f0cb6d7d459fb411ef3ac6d65ffffffff03ac505763000000001976a914c362d5af234dd4e1f2a1bfbcab90036d38b0aa9f88acaa505763000000001976a9143c22b6d9ba7b50b6d6e615c69d11ecb2ba3db14588acaa505763000000001976a914b7177c7deb43f3869eabc25cfd9f618215f34d5588ac00000000"
	coinbase, err := bt.NewTxFromString(coinbaseHex)
	require.NoError(t, err)

	// need to add spendable utxo to utxo store

	// delete spends set to false
	utxoStore := utxoMemorystore.New(ulogger.TestLogger{})

	v, err := validator.New(context.Background(), ulogger.TestLogger{}, utxoStore)
	if err != nil {
		panic(err)
	}

	height, err := util.ExtractCoinbaseHeight(coinbase)
	require.NoError(t, err)

	err = v.Validate(context.Background(), coinbase, height)
	require.Error(t, err)
}

func TestValidate_ValidTransaction(t *testing.T) {
}

func TestValidate_InValidDoubleSpendTx(t *testing.T) {
}

func TestValidate_TxMetaStoreError(t *testing.T) {
}

func TestValidate_BlockAssemblyError(t *testing.T) {
}
