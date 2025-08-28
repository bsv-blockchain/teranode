// Package aerospike provides an Aerospike-based implementation of the UTXO store interface.
// It offers high performance, distributed storage capabilities with support for large-scale
// UTXO sets and complex operations like freezing, reassignment, and batch processing.
//
// # Architecture
//
// The implementation uses a combination of Aerospike Key-Value store and Lua scripts
// for atomic operations. Transactions are stored with the following structure:
//   - Main Record: Contains transaction metadata and up to 20,000 UTXOs
//   - Pagination Records: Additional records for transactions with >20,000 outputs
//   - External Storage: Optional blob storage for large transactions
//
// # Features
//
//   - Efficient UTXO lifecycle management (create, spend, unspend)
//   - Support for batched operations with LUA scripting
//   - Automatic cleanup of spent UTXOs through DAH
//   - Alert system integration for freezing/unfreezing UTXOs
//   - Metrics tracking via Prometheus
//   - Support for large transactions through external blob storage
//
// # Usage
//
//	store, err := aerospike.New(ctx, logger, settings, &url.URL{
//	    Scheme: "aerospike",
//	    Host:   "localhost:3000",
//	    Path:   "/test/utxos",
//	    RawQuery: "expiration=3600&set=txmeta",
//	})
//
// # Database Structure
//
// Normal Transaction:
//   - inputs: Transaction input data
//   - outputs: Transaction output data
//   - utxos: List of UTXO hashes
//   - totalUtxos: Total number of UTXOs
//   - spentUtxos: Number of spent UTXOs
//   - blockIDs: Block references
//   - isCoinbase: Coinbase flag
//   - spendingHeight: Coinbase maturity height
//   - frozen: Frozen status
//
// Large Transaction with External Storage:
//   - Same as normal but with external=true
//   - Transaction data stored in blob storage
//   - Multiple records for >20k outputs
//
// # Thread Safety
//
// The implementation is fully thread-safe and supports concurrent access through:
//   - Atomic operations via Lua scripts
//   - Batched operations for better performance
//   - Lock-free reads with optimistic concurrency
package aerospike

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/url"
	"os"
	"testing"

	"github.com/aerospike/aerospike-client-go/v8"
	"github.com/bitcoin-sv/teranode/stores/utxo"
	"github.com/bitcoin-sv/teranode/stores/utxo/fields"
	"github.com/bitcoin-sv/teranode/ulogger"
	"github.com/bitcoin-sv/teranode/util/test"
	"github.com/bitcoin-sv/teranode/util/uaerospike"
	aeroTest "github.com/bitcoin-sv/testcontainers-aerospike-go"
	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCalculateKeySource(t *testing.T) {
	hash := chainhash.HashH([]byte("test"))

	h := uaerospike.CalculateKeySource(&hash, 0)
	assert.Equal(t, hash[:], h)

	h = uaerospike.CalculateKeySource(&hash, 1)
	extra := make([]byte, 4)
	binary.LittleEndian.PutUint32(extra, uint32(1))
	assert.Equal(t, append(hash[:], extra...), h)

	h = uaerospike.CalculateKeySource(&hash, 2)
	extra = make([]byte, 4)
	binary.LittleEndian.PutUint32(extra, uint32(2))
	assert.Equal(t, append(hash[:], extra...), h)
}

func TestCalculateOffsetOutput(t *testing.T) {
	db := &Store{utxoBatchSize: 20_000}

	offset := db.calculateOffsetForOutput(0)
	assert.Equal(t, uint32(0), offset)

	offset = db.calculateOffsetForOutput(30_000)
	assert.Equal(t, uint32(10_000), offset)

	offset = db.calculateOffsetForOutput(40_000)
	assert.Equal(t, uint32(0), offset)
}

func TestUnmined(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	client, err := uaerospike.NewClient(host, port)
	require.NoError(t, err)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo&externalStore=file://./data/external", host, port))
	require.NoError(t, err)

	store, err := New(ctx, logger, tSettings, aeroURL)
	require.NoError(t, err)

	t.Run("check_empty_store", func(t *testing.T) {
		exists, err := store.indexExists("unminedSinceIndex")
		require.NoError(t, err)
		assert.True(t, exists)

		stmt := aerospike.NewStatement(store.namespace, store.setName)

		err = stmt.SetFilter(aerospike.NewRangeFilter(fields.UnminedSince.String(), 1, int64(4294967295)))
		require.NoError(t, err)

		recordset, err := client.Query(nil, stmt)
		require.NoError(t, err)

		count := 0

		for range recordset.Records() {
			count++
		}

		assert.Equal(t, count, 0)
	})

	t.Run("check_not_mined_tx", func(t *testing.T) {
		currentBlockHeight := uint32(1)

		txUnMined, err := bt.NewTxFromString("010000000000000000ef011c044c4db32b3da68aa54e3f30c71300db250e0b48ea740bd3897a8ea1a2cc9a020000006b483045022100c6177fa406ecb95817d3cdd3e951696439b23f8e888ef993295aa73046504029022052e75e7bfd060541be406ec64f4fc55e708e55c3871963e95bf9bd34df747ee041210245c6e32afad67f6177b02cfc2878fce2a28e77ad9ecbc6356960c020c592d867ffffffffd4c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac0301000000000000001976a914a4429da7462800dedc7b03a4fc77c363b8de40f588ac000000000000000024006a4c2042535620466175636574207c20707573682d7468652d627574746f6e2e617070d2c7a70c000000001976a914296b03a4dd56b3b0fe5706c845f2edff22e84d7388ac00000000")
		require.NoError(t, err)

		t.Logf("Unmined txid: %v", txUnMined.TxIDChainHash())

		_, err = store.Create(store.ctx, txUnMined, currentBlockHeight)
		require.NoError(t, err)

		txMined := txUnMined.Clone()
		txMined.Version++

		t.Logf("Mined txid: %v", txMined.TxIDChainHash())

		_, err = store.Create(store.ctx, txMined, currentBlockHeight, utxo.WithMinedBlockInfo(
			utxo.MinedBlockInfo{
				BlockID:     1,
				BlockHeight: currentBlockHeight,
				SubtreeIdx:  1,
			},
		))
		require.NoError(t, err)

		exists, err := store.indexExists("unminedSinceIndex")
		require.NoError(t, err)
		assert.True(t, exists)

		stmt := aerospike.NewStatement(store.namespace, store.setName)

		err = stmt.SetFilter(aerospike.NewRangeFilter(fields.UnminedSince.String(), int64(currentBlockHeight), int64(4294967295)))
		require.NoError(t, err)

		recordset, err := client.Query(nil, stmt)
		require.NoError(t, err)

		count := 0

		for rec := range recordset.Records() {
			assert.NotNil(t, rec.Bins[fields.UnminedSince.String()])
			assert.Equal(t, int(currentBlockHeight), rec.Bins[fields.UnminedSince.String()])
			assert.Equal(t, txUnMined.TxIDChainHash().CloneBytes(), rec.Bins[fields.TxID.String()])

			count++
		}

		assert.Equal(t, 1, count)

		// set the tx as mined
		err = store.SetMinedMulti(store.ctx, []*chainhash.Hash{txUnMined.TxIDChainHash()}, utxo.MinedBlockInfo{
			BlockID:     1,
			BlockHeight: currentBlockHeight,
			SubtreeIdx:  1,
		})
		require.NoError(t, err)

		recordset, err = client.Query(nil, stmt)
		require.NoError(t, err)

		count = 0

		for rec := range recordset.Records() {
			assert.NotNil(t, rec.Bins[fields.UnminedSince.String()])
			assert.Equal(t, int(currentBlockHeight), rec.Bins[fields.UnminedSince.String()])
			assert.Equal(t, txUnMined.TxIDChainHash().CloneBytes(), rec.Bins[fields.TxID.String()])

			count++
		}

		assert.Equal(t, 0, count)
	})
}

func TestLargeTxStoresExternally(t *testing.T) {
	logger := ulogger.NewErrorTestLogger(t)
	ctx := context.Background()

	tSettings := test.CreateBaseTestSettings(t)

	container, err := aeroTest.RunContainer(ctx)
	require.NoError(t, err)

	t.Cleanup(func() {
		err = container.Terminate(ctx)
		require.NoError(t, err)
	})

	host, err := container.Host(ctx)
	require.NoError(t, err)

	port, err := container.ServicePort(ctx)
	require.NoError(t, err)

	aeroURL, err := url.Parse(fmt.Sprintf("aerospike://%s:%d/test?set=utxo&externalStore=file://./data/external?hashPrefix=2", host, port))
	require.NoError(t, err)

	store, err := New(ctx, logger, tSettings, aeroURL)
	require.NoError(t, err)

	b, err := os.ReadFile("test/01d29b3fd5f2629c3b6586790312ee4a16039d8033e35a6ad0dcfa0235a39400")
	require.NoError(t, err)

	tx, err := bt.NewTxFromBytes(b)
	require.NoError(t, err)

	// Remove the external store
	err = os.RemoveAll("./data/external")
	require.NoError(t, err)

	_, err = store.Create(context.Background(), tx, 1)
	require.NoError(t, err)

	// check that the tx is stored externally
	_, err = os.Stat("./data/external/01/01d29b3fd5f2629c3b6586790312ee4a16039d8033e35a6ad0dcfa0235a39400.tx")
	require.NoError(t, err)

	// Create a transaction that spends the 2 outputs of tx
	spendTx := bt.NewTx()

	err = spendTx.FromUTXOs(&bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          0,
		Satoshis:      tx.Outputs[0].Satoshis,
		LockingScript: tx.Outputs[0].LockingScript,
	}, &bt.UTXO{
		TxIDHash:      tx.TxIDChainHash(),
		Vout:          1,
		Satoshis:      tx.Outputs[1].Satoshis,
		LockingScript: tx.Outputs[1].LockingScript,
	})
	require.NoError(t, err)

	// Now let's spend the outputs
	_, err = store.Spend(context.Background(), spendTx)
	require.NoError(t, err)

	// check that the tx is stored externally
	_, err = os.Stat("./data/external/01/01d29b3fd5f2629c3b6586790312ee4a16039d8033e35a6ad0dcfa0235a39400.tx.dah")
	require.Error(t, err) // DAH should not exist

	err = store.SetBlockHeight(100_000)
	require.NoError(t, err)

	// Now mark as mined
	err = store.SetMinedMulti(context.Background(), []*chainhash.Hash{tx.TxIDChainHash()}, utxo.MinedBlockInfo{
		BlockID:     1,
		BlockHeight: 1,
		SubtreeIdx:  1,
	})
	require.NoError(t, err)

	dah, err := os.ReadFile("./data/external/01/01d29b3fd5f2629c3b6586790312ee4a16039d8033e35a6ad0dcfa0235a39400.tx.dah")
	require.NoError(t, err)

	require.Equal(t, "100011", string(dah))
}
