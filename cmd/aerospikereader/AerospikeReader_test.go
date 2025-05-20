package aerospikereader

import (
	"context"
	"net/url"
	"testing"

	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/aerospike"
	"github.com/bitcoin-sv/teranode/test/utils/transactions"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/libsv/go-bk/bec"
	"github.com/libsv/go-bt/v2/chainhash"
	"github.com/stretchr/testify/require"
)

func TestAerospikeReader(t *testing.T) {
	t.SkipNow()

	logger := ulogger.NewVerboseTestLogger(t)
	tSettings := test.CreateBaseTestSettings()
	ctx := context.Background()

	privKey, err := bec.NewPrivateKey(bec.S256())
	require.NoError(t, err)

	tx := transactions.Create(t,
		transactions.WithCoinbaseData(100, "/test miner/"),
		transactions.WithP2PKHOutputs(1, 100000, privKey.PubKey()),
	)

	aeroURL, err := url.Parse("aerospike://localhost:3000/test?set=utxo&externalStore=file://./data/external")
	require.NoError(t, err)

	store, err := aerospike.New(ctx, logger, tSettings, aeroURL)
	require.NoError(t, err)

	_, err = store.Create(ctx, tx, 0)
	require.NoError(t, err)

	err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: 0,
	})
	require.NoError(t, err)

	err = store.SetMinedMulti(ctx, []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID: 1,
	})
	require.NoError(t, err)

	t.Logf("txid: %s", tx.TxIDChainHash().String())
}
