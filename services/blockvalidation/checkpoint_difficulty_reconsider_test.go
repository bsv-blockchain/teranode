package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestSkipExpectedDifficulty_RebuildsInvalidatedPrefix(t *testing.T) {
	ctx := context.Background()
	tSettings := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	// Two synthetic historical blocks suffice to exercise the actual invalidation
	// cascade and best-height lookup, independently of the historical DAA rules.
	prev := tSettings.ChainCfgParams.GenesisHash
	blocks := make([]*model.Block, 0, 2)
	for height := uint32(1); height <= 2; height++ {
		genesis := tSettings.ChainCfgParams.GenesisBlock
		header := genesis.Header
		header.PrevBlock = *prev
		header.Nonce += height
		coinbase := wire.NewMsgTx(1)
		coinbase.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{Index: 0xffffffff}, SignatureScript: []byte{1, byte(height)}, Sequence: 0xffffffff})
		coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100_000_000, PkScript: []byte{0x51}})
		header.MerkleRoot = coinbase.TxHash()
		block, err := model.NewBlockFromMsgBlock(&wire.MsgBlock{Header: header, Transactions: []*wire.MsgTx{coinbase}}, nil)
		require.NoError(t, err)
		require.NoError(t, client.AddBlock(ctx, block, "test"))
		block.Height = height
		blocks = append(blocks, block)
		prev = block.Hash()
	}
	tSettings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 2, Hash: blocks[1].Hash()}}
	u := &BlockValidation{settings: tSettings, blockchainClient: client, logger: ulogger.TestLogger{}}
	_, best, err := client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Equal(t, uint32(2), best.Height)
	require.False(t, u.skipExpectedDifficulty(ctx, blocks[0], false), "a completed prefix needs no stored-block exception")

	_, err = client.InvalidateBlock(ctx, blocks[0].Hash())
	require.NoError(t, err)
	_, best, err = client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Zero(t, best.Height, "invalidation must remove the whole descendant prefix")
	require.True(t, u.skipExpectedDifficulty(ctx, blocks[0], false), "rebuilding historical blocks must retain the syncing skip")
	require.True(t, u.skipExpectedDifficulty(ctx, blocks[1], false))
}

func TestSkipExpectedDifficultyRejectsStoredFork(t *testing.T) {
	ctx := context.Background()
	cfg := test.CreateBaseTestSettings(t)
	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.(interface{ Close() error }).Close()) })
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, cfg, store, nil, nil)
	require.NoError(t, err)
	blocks := testhelpers.CreateTestBlocksWithPrev(t, 2, cfg.ChainCfgParams.GenesisHash)
	for i, block := range blocks {
		block.Height = uint32(i + 1)
		require.NoError(t, client.AddBlock(ctx, block, "test"))
	}
	cfg.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 2, Hash: blocks[1].Hash()}}
	_, err = client.InvalidateBlock(ctx, blocks[0].Hash())
	require.NoError(t, err)
	fork := testhelpers.CreateTestBlocksWithPrev(t, 1, cfg.ChainCfgParams.GenesisHash)[0]
	fork.Height = 1
	fork.Header.Nonce++
	require.NoError(t, client.AddBlock(ctx, fork, "test"))
	u := &BlockValidation{settings: cfg, blockchainClient: client, logger: ulogger.TestLogger{}}
	require.False(t, u.skipExpectedDifficulty(ctx, fork, false), "being stored below checkpoint does not prove ancestry")
	require.True(t, u.skipExpectedDifficulty(ctx, blocks[0], false), "the invalidated checkpoint ancestor remains proven")
}
