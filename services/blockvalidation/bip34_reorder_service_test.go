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
	"github.com/bsv-blockchain/teranode/util"
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
// a deliberate BIP34 mismatch), with one P2PKH output. The height is pushed with production's own
// canonical minimal encoder (util.EncodeCoinbaseHeightPush) so ExtractCoinbaseHeight accepts it and
// the block reaches the height-COMPARISON check — a hand-rolled non-minimal push (e.g. 0x03 0x63
// 0x00 0x00) is rejected for non-minimal encoding first and never gets there.
func bip34Coinbase(t *testing.T) *bt.Tx {
	t.Helper()

	privateKey, err := bec.NewPrivateKey()
	require.NoError(t, err)
	address, err := bscript.NewAddressFromPublicKey(privateKey.PubKey(), true)
	require.NoError(t, err)

	scriptSig := util.EncodeCoinbaseHeightPush(99)
	scriptSig = append(scriptSig, "/Test"...)

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))
	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes(scriptSig)
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress(address.AddressString, 50*100000000))

	return coinbaseTx
}

// TestValidateBlock_BIP34Reorder_Service is the freemans13 item 3 SERVICE-LEVEL coverage
// (bitcoin-sv/teranode#4692, C2/C3), driving ValidateBlock (OptimisticMining off → synchronous
// block.Valid). It proves the reorder's CLASSIFICATION consequence at the service boundary, so it
// FAILS if the reorder is reverted (block.Valid would then return corrupt for the merkle-bound
// block, and the service would strike + re-download instead of condemning it invalid):
//   - a block bound by its subtree merkle root, with a wrong BIP34 height, is condemned INVALID
//     (not corrupt);
//   - a coinbase-only block is bound by the coinbase txid (for a single-transaction block the
//     header merkle root IS the coinbase txid), so a wrong BIP34 height on it is equally INVALID.
//
// Both are persisted with invalid=true. StoreBlock's validateCoinbaseHeight
// (stores/blockchain/sql/StoreBlock.go) re-derives the height and rejects exactly the bad-BIP34
// condition, but it is skipped for blocks written as invalid — recording that a block failed a
// consensus rule must not re-apply the rule it records — so the invalid verdict is durable and a
// re-announcement is answered from the store instead of a full re-validation. The revert-failure
// property lives in the classification: reverting the merkle-first reorder makes block.Valid return
// corrupt here instead, and nothing is stored.
//
// The no-poison rule for an UNBOUND body is pinned elsewhere: at the service boundary by the
// emptied-subtree-list case (a body claiming no subtrees whose header merkle root is not the
// coinbase txid is corrupt and never stored), and at the model level by the nil-subtree-store case.
//
// CheckBlockSubtrees is mocked to succeed so the only failure comes from block.Valid's BIP34 gate;
// the merkle binding itself runs for real against the stored subtree.
func TestValidateBlock_BIP34Reorder_Service(t *testing.T) {
	initPrometheusMetrics()

	// Custom regtest: activate BIP34 at height 1 so a height-100 block runs the coinbase-height check.
	params := chaincfg.RegressionNetParams
	params.BIP0034Height = 1

	t.Run("merkle-bound bad-BIP34 -> condemned invalid (not corrupt) and persisted as invalid", func(t *testing.T) {
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
		// Confirm the block actually reached the BIP34 height-COMPARISON (not the coinbase-height
		// EXTRACTION error, which a non-minimal fixture would trip first): the mismatch error does not
		// wrap ErrBlockCoinbaseMissingHeight, the extraction error does.
		require.False(t, errors.Is(err, errors.ErrBlockCoinbaseMissingHeight),
			"must reach the height-mismatch check, not fail coinbase-height extraction, got: %v", err)
		// Classification is the revert discriminator: merkle-bound → invalid, never corrupt. Reverting
		// the merkle-first reorder makes block.Valid return corrupt here instead, flipping both.
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a merkle-bound wrong BIP34 height must be condemned invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "must NOT be corrupt")

		// The invalid verdict is REMEMBERED. storeInvalidBlock writes the block with invalid=true, and
		// the store's own coinbase-height guard (StoreBlock.validateCoinbaseHeight) is skipped for
		// invalid writes, so the write no longer fails on the very rule it is recording. Without that
		// skip the store error is logged and swallowed and the block is re-validated at full cost on
		// every re-announcement.
		exists, existsErr := blockchainClient.GetBlockExists(ctx, block.Header.Hash())
		require.NoError(t, existsErr)
		require.True(t, exists, "a condemned bad-BIP34 block must be persisted so the verdict is remembered")

		_, meta, metaErr := blockchainClient.GetBlockHeader(ctx, block.Header.Hash())
		require.NoError(t, metaErr)
		require.NotNil(t, meta)
		require.True(t, meta.Invalid, "the stored row must carry the invalid verdict, not just exist")
	})

	t.Run("coinbase-only bad-BIP34 -> bound by the coinbase txid, condemned invalid and persisted", func(t *testing.T) {
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

		// No subtrees, and the header merkle root IS the coinbase txid — the body's claim that the
		// block holds only the coinbase, checked against the header. That is a real merkle binding
		// (a single-transaction block's merkle root is its coinbase txid), so the body is BOUND and a
		// wrong BIP34 height on it is genuine consensus invalidity, exactly as for a subtree-bound
		// body. A coinbase-only body whose merkle root does NOT match is corrupt before BIP34 ever
		// runs, which is what keeps a truncated subtree list from being poisoned.
		hdr := minedBIP34Header(t, 4, tSettings.ChainCfgParams.GenesisHash, coinbaseTx.TxIDChainHash())
		block, err := model.NewBlock(hdr, coinbaseTx, []*chainhash.Hash{}, 1, uint64(coinbaseTx.Size()), 100, 0) //nolint:gosec
		require.NoError(t, err)

		bv := NewBlockValidation(ctx, ulogger.TestLogger{}, tSettings, blockchainClient, subtreeStore, txStore, utxoStore, nil, subtreeVal)

		err = bv.ValidateBlock(ctx, block, "http://localhost")
		require.Error(t, err)
		// Reached the height-COMPARISON, not the extraction error.
		require.False(t, errors.Is(err, errors.ErrBlockCoinbaseMissingHeight),
			"must reach the height-mismatch check, not fail coinbase-height extraction, got: %v", err)
		// Classification: bound by the coinbase txid → invalid, never corrupt.
		require.True(t, errors.Is(err, errors.ErrBlockInvalid),
			"a coinbase-only body is bound by the coinbase txid, so a wrong BIP34 height must be condemned invalid, got: %v", err)
		require.False(t, errors.IsBlockCorrupt(err), "a bound body must not be classified corrupt")

		// The verdict is remembered, for the same reason as the subtree-bound case above.
		exists, existsErr := blockchainClient.GetBlockExists(ctx, block.Header.Hash())
		require.NoError(t, existsErr)
		require.True(t, exists, "a condemned bad-BIP34 block must be persisted so the verdict is remembered")

		_, meta, metaErr := blockchainClient.GetBlockHeader(ctx, block.Header.Hash())
		require.NoError(t, metaErr)
		require.NotNil(t, meta)
		require.True(t, meta.Invalid, "the stored row must carry the invalid verdict, not just exist")
	})
}
