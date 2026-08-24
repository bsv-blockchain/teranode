package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// shortScriptSigCoinbaseForService builds a well-formed coinbase input whose unlocking script is a
// single byte, below the bad-coinbase-length floor of 2 (errors/Error_types.go, model/Block.go
// step 4b).
func shortScriptSigCoinbaseForService(t *testing.T) *bt.Tx {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x01})
	require.True(t, coinbaseTx.IsCoinbase(), "fixture must still be a coinbase")

	return coinbaseTx
}

// TestValidateBlock_CoinbaseLengthBinding_Service is the bitcoin-sv/teranode#4692 SERVICE-LEVEL
// coverage for removing the outer, pre-binding bad-coinbase-length precheck from
// BlockValidation.ValidateBlock: with the precheck gone, every body reaches block.Valid, whose
// step 4b already classifies a bad coinbase length by whether the body was merkle-bound. This
// proves the consequence at the service boundary — a merkle-bound bad-length body is now
// condemned invalid and persisted, rather than looping forever as corrupt under the per-peer
// corrupt-attempt cap.
//
// CheckBlockSubtrees is mocked to succeed so the only failure comes from block.Valid's
// coinbase-length gate; the merkle binding itself runs for real against the stored subtree.
func TestValidateBlock_CoinbaseLengthBinding_Service(t *testing.T) {
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
	defer deferFunc()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockValidation.OptimisticMining = false // synchronous block.Valid path

	blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)
	blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
	require.NoError(t, err)

	subtreeVal := &subtreevalidation.MockSubtreeValidation{}
	subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

	coinbaseTx := shortScriptSigCoinbaseForService(t)

	// A merkle-bound subtree: coinbase placeholder + one node. The node need not be a real tx —
	// the merkle binding is structural and validOrderAndBlessed is never reached (the
	// coinbase-length check fails first, well before the fee/order checks).
	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(chainhash.Hash{0xAB}, 100, 0))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtree, subtreeBytes))

	replicated := subtree.Duplicate()
	replicated.ReplaceRootNode(coinbaseTx.TxIDChainHash(), 0, uint64(coinbaseTx.Size())) //nolint:gosec
	merkleRoot := replicated.RootHash()

	hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, merkleRoot)
	block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{subtree.RootHash()},
		uint64(subtree.Length()), uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
	require.NoError(t, err)

	bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeVal)

	err = bv.ValidateBlock(ctx, block, "http://localhost")
	require.Error(t, err)
	require.Contains(t, err.Error(), "bad coinbase length")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid),
		"a merkle-bound bad coinbase length must be condemned invalid, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "must NOT be corrupt")

	// The invalid verdict is REMEMBERED: storeInvalidBlock writes the block with invalid=true.
	exists, existsErr := blockchainClient.GetBlockExists(ctx, block.Header.Hash())
	require.NoError(t, existsErr)
	require.True(t, exists, "a condemned bad-coinbase-length block must be persisted so the verdict is remembered")

	_, meta, metaErr := blockchainClient.GetBlockHeader(ctx, block.Header.Hash())
	require.NoError(t, metaErr)
	require.NotNil(t, meta)
	require.True(t, meta.Invalid, "the stored row must carry the invalid verdict, not just exist")
}
