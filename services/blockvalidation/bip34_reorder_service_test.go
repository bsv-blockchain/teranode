package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	bec "github.com/bsv-blockchain/go-sdk/primitives/ec"
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

// minedBIP34Header builds a header of the given version carrying merkleRoot, mined to the regtest
// 207fffff target so the ValidateBlock difficulty gate accepts it. Version 4 is used so the block
// clears the BIP34/66/65 version floors (mandatory once BIP34 activates, which it does at height 1
// in these tests).
func minedBIP34Header(t *testing.T, version uint32, prev, merkleRoot *chainhash.Hash) *model.BlockHeader {
	t.Helper()

	nBits, err := model.NewNBitFromString("207fffff")
	require.NoError(t, err)

	hdr := &model.BlockHeader{
		Version:        version,
		HashPrevBlock:  prev,
		HashMerkleRoot: merkleRoot,
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           *nBits,
		Nonce:          0,
	}

	for {
		if ok, _, _ := hdr.HasMetTargetDifficulty(); ok {
			break
		}
		hdr.Nonce++
	}

	return hdr
}

// bip34Coinbase builds a coinbase whose BIP34 scriptSig encodes height 99 (so a height-100 block is
// a deliberate BIP34 mismatch), with one P2PKH output.
func bip34Coinbase(t *testing.T) *bt.Tx {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	// 0x03 = push 3 bytes; 0x63 0x00 0x00 = height 99 (little-endian); rest is arbitrary tag data.
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, 0x63, 0x00, 0x00, 0x00, '/', 'T', 'e', 's', 't'})
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))

	return coinbaseTx
}

// TestValidateBlock_BIP34Reorder_Service is the freemans13 item 3 SERVICE-LEVEL coverage
// (bitcoin-sv/teranode#4692, C2/C3), driving ValidateBlock (OptimisticMining off → synchronous
// block.Valid). It proves the reorder's poison/no-poison consequence at the service boundary, so it
// FAILS if the reorder is reverted (block.Valid would then return corrupt for the merkle-bound
// block, and the service would strike + re-download instead of poisoning):
//   - a merkle-BOUND block with a wrong BIP34 height is condemned invalid AND persisted (poison);
//   - a coinbase-only (unbound) block with a wrong BIP34 height is corrupt and NOT persisted.
//
// CheckBlockSubtrees is mocked to succeed so the only failure comes from block.Valid's BIP34 gate;
// the merkle binding itself runs for real against the stored subtree.
func TestValidateBlock_BIP34Reorder_Service(t *testing.T) {
	initPrometheusMetrics()

	// Custom regtest: activate BIP34 at height 1 so a height-100 block runs the coinbase-height check.
	params := chaincfg.RegressionNetParams
	params.BIP0034Height = 1

	t.Run("merkle-bound bad-BIP34 -> service condemns AND persists invalid (poison)", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
		defer deferFunc()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false // synchronous block.Valid path
		tSettings.ChainCfgParams = &params

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
		require.NoError(t, err)

		// Subtree validation passes; the only failure must be BIP34 inside block.Valid.
		subtreeVal := &subtreevalidation.MockSubtreeValidation{}
		subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		coinbaseTx := bip34Coinbase(t)

		// A merkle-bound subtree: coinbase placeholder + one node. The node need not be a real tx —
		// the merkle binding is structural and validOrderAndBlessed is never reached (BIP34 fails
		// first). Store it so block.Valid's GetAndValidateSubtrees + CheckMerkleRoot can load it.
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
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a merkle-bound wrong BIP34 height must be condemned invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err),
			"must NOT be corrupt — it would be if the merkle-first reorder were reverted")
		require.ErrorContains(t, err, "does not match block height")

		exists, existsErr := blockchainClient.GetBlockExists(ctx, block.Header.Hash())
		require.NoError(t, existsErr)
		require.True(t, exists, "the service must PERSIST a merkle-bound bad-BIP34 block as invalid (poison)")
	})

	t.Run("coinbase-only bad-BIP34 -> corrupt, NOT persisted (never poison an unbound body)", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		utxoStore, _, _, txStore, subtreeStore, deferFunc := setup(t)
		defer deferFunc()

		tSettings := test.CreateBaseTestSettings(t)
		tSettings.BlockValidation.OptimisticMining = false
		tSettings.ChainCfgParams = &params

		blockChainStore, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
		require.NoError(t, err)
		blockchainClient, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, blockChainStore, nil, nil)
		require.NoError(t, err)

		subtreeVal := &subtreevalidation.MockSubtreeValidation{}
		subtreeVal.Mock.On("CheckBlockSubtrees", mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil)

		coinbaseTx := bip34Coinbase(t)

		// No subtrees: block.Valid's merkle block is skipped, merkleRootChecked stays false, so BIP34
		// runs on an UNBOUND body and must classify corrupt — never invalid=true.
		hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash())
		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
		require.NoError(t, err)

		bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeVal)

		err = bv.ValidateBlock(ctx, block, "http://localhost")
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err),
			"an unbound (coinbase-only) wrong BIP34 height must be corrupt (re-download), got: %v", err)
		require.False(t, errors.Is(err, errors.ErrBlockInvalid), "an unbound body must never be poisoned on a BIP34 failure")
		require.ErrorContains(t, err, "does not match block height")

		exists, existsErr := blockchainClient.GetBlockExists(ctx, block.Header.Hash())
		require.NoError(t, existsErr)
		require.False(t, exists, "an unbound bad-BIP34 body must NOT be persisted (no poison)")
	})
}
