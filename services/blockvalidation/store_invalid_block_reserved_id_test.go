package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Quick validation reserves a block id, stamps the block's transactions with it
// in the UTXO store, and only then writes the blocks row. When the block turns
// out invalid, storeInvalidBlock writes that row. It used to ask the store for a
// fresh id, so the row landed under a new id and every transaction stamped with
// the reserved one pointed at an id with no blocks row. The row must carry the
// reserved id.
func TestStoreInvalidBlock_KeepsTheReservedBlockID(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	coinbaseTx := shortScriptSigCoinbaseForService(t)
	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, &chainhash.Hash{0x01})

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 1, 0) //nolint:gosec
	require.NoError(t, err)

	reserved, err := blockchainClient.AssignBlockID(ctx, block.Hash())
	require.NoError(t, err)

	// A second reservation for an unrelated hash moves the id sequence on, so an
	// auto-increment INSERT could not land on the reserved id by coincidence.
	otherHash := chainhash.HashH([]byte("unrelated block"))
	_, err = blockchainClient.AssignBlockID(ctx, &otherHash)
	require.NoError(t, err)

	block.ID = uint32(reserved) //nolint:gosec

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, &subtreevalidation.MockSubtreeValidation{})

	bv.storeInvalidBlock(ctx, block, "", "", "test: block failed validation after its id was reserved")

	_, meta, err := blockchainClient.GetBlockHeader(ctx, block.Header.Hash())
	require.NoError(t, err, "storeInvalidBlock must have written the row")
	require.True(t, meta.Invalid, "the row must carry the invalid verdict")
	require.Equal(t, uint32(reserved), meta.ID, "the row must carry the id the block's transactions were stamped with") //nolint:gosec
}

// A block that never had an id reserved (block.ID == 0) must still be stored
// invalid, under whatever id the store allocates.
func TestStoreInvalidBlock_WithoutReservedIDStillStores(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	coinbaseTx := shortScriptSigCoinbaseForService(t)
	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, &chainhash.Hash{0x02})

	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 1, 0) //nolint:gosec
	require.NoError(t, err)
	require.Zero(t, block.ID)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, &subtreevalidation.MockSubtreeValidation{})

	bv.storeInvalidBlock(ctx, block, "", "", "test: block failed validation before any id was reserved")

	_, meta, err := blockchainClient.GetBlockHeader(ctx, block.Header.Hash())
	require.NoError(t, err)
	require.True(t, meta.Invalid)
	require.NotZero(t, meta.ID)
}
