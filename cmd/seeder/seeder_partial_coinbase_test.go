package seeder

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestProcessUTXOs_TruncatedHeaders_StillRestoresCoinbaseReadBeforeTruncation
// covers the partial-map guard in processUTXOs, which nothing else reached.
//
// loadCoinbaseTxs hands back the coinbases it decoded before failing, and
// processUTXOs is supposed to keep them rather than degrade to an empty map.
// Every existing partial-map test stops at loadCoinbaseTxs and asserts on its
// return value, so replacing the guard with an unconditional
// `coinbaseTxs = map[chainhash.Hash]*bt.Tx{}` left the whole package green
// (ctnguyen's ChiR5 predicted exactly this gap).
//
// This asserts the end result instead: with a headers file truncated after
// block 1 and a utxo-set holding block 1's coinbase, the coinbase must reach
// the store with its input restored. Drop the guard and the stored tx is the
// output-only reconstruction, with no inputs.
func TestProcessUTXOs_TruncatedHeaders_StillRestoresCoinbaseReadBeforeTruncation(t *testing.T) {
	ctx := context.Background()

	tSettings := forceSeederSettings(t)

	// A file-backed sqlite DB under the settings' temp data folder, not
	// sqlitememory: InitSQLiteDB gives every sqlitememory handle a randomly
	// named database, so the store processUTXOs opens could never be reopened
	// here to read the result back.
	utxoStoreURL, err := url.Parse("sqlite:///partial_coinbase")
	require.NoError(t, err)

	tSettings.UtxoStore.UtxoStore = utxoStoreURL

	blockchainStore := newTestBlockchainStore(t)

	genesis, err := blockchainStore.GetBlockByID(ctx, 0)
	require.NoError(t, err)

	genesisRecord, err := blockToIndexBytes(t, genesis, genesis.Header.Hash())
	require.NoError(t, err)

	coinbase1 := makeCoinbaseTx(t, 1, 1)
	block1 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000001,
			Nonce:          1,
			HashPrevBlock:  genesis.Header.Hash(),
			HashMerkleRoot: coinbase1.TxIDChainHash(),
			Bits:           genesis.Header.Bits,
		},
		CoinbaseTx:       coinbase1,
		TransactionCount: 1,
		Height:           1,
	}

	block1Record, err := blockToIndexBytes(t, block1, block1.Header.Hash())
	require.NoError(t, err)

	coinbase2 := makeCoinbaseTx(t, 2, 2)
	block2 := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			Timestamp:      1700000002,
			Nonce:          2,
			HashPrevBlock:  block1.Header.Hash(),
			HashMerkleRoot: coinbase2.TxIDChainHash(),
			Bits:           genesis.Header.Bits,
		},
		CoinbaseTx:       coinbase2,
		TransactionCount: 1,
		Height:           2,
	}

	block2Record, err := blockToIndexBytes(t, block2, block2.Header.Hash())
	require.NoError(t, err)

	// Claims a tip height of 2, but block2's record is cut 10 bytes in, so
	// block 1's coinbase is fully decoded and block 2's is not.
	headersFile := writeUtxoHeadersFile(t, 2, genesisRecord, block1Record, block2Record[:10])

	txid := coinbase1.TxIDChainHash()
	utxoFile := writeCompleteSnapshotFile(t, []*utxopersister.UTXOWrapper{
		wrapperFromCoinbase(coinbase1, txid, 1, []int{0, 1, 2}),
	})

	// The truncated headers file is a warning, not a failure: coinbase
	// restoration is best-effort and must not abort the import.
	_, err = processUTXOs(ctx, ulogger.TestLogger{}, tSettings, blockchainStore, utxoFile, headersFile, false, false)
	require.NoError(t, err)

	store, err := utxosql.New(ctx, ulogger.TestLogger{}, tSettings, utxoStoreURL)
	require.NoError(t, err)

	got, err := store.Get(ctx, txid)
	require.NoError(t, err)
	require.NotNil(t, got.Tx)

	require.Len(t, got.Tx.Inputs, 1,
		"block 1's coinbase was decoded before the truncation, so processUTXOs must have kept the partial map and restored its input")
	require.True(t, got.Tx.IsCoinbase())
	require.Equal(t, txid.String(), got.Tx.TxIDChainHash().String(),
		"the restored coinbase must hash back to the txid the block commits to")
}
