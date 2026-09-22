package blockvalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
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
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	// Two synthetic blocks suffice to exercise the actual invalidation
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
	require.False(t, u.skipExpectedDifficulty(ctx, blocks[0]), "a completed prefix needs no stored-block exception")

	_, err = client.InvalidateBlock(ctx, blocks[0].Hash())
	require.NoError(t, err)
	_, best, err = client.GetBestBlockHeader(ctx)
	require.NoError(t, err)
	require.Zero(t, best.Height, "invalidation must remove the whole descendant prefix")
	require.True(t, u.skipExpectedDifficulty(ctx, blocks[0]), "rebuilding this synthetic post-DAA prefix retains the syncing skip")
	require.True(t, u.skipExpectedDifficulty(ctx, blocks[1]))
}

func TestSkipExpectedDifficulty_EnforcesHistoricalRules(t *testing.T) {
	ctx := context.Background()
	s := test.CreateBaseTestSettings(t)
	params := chaincfg.TestNetParams
	s.ChainCfgParams = &params
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, s)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(ctx)) })
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, s, store, nil, nil)
	require.NoError(t, err)
	u := &BlockValidation{settings: s, blockchainClient: client, logger: ulogger.TestLogger{}}
	for _, height := range []uint32{149, params.DaaForkHeight - 1, params.DaaForkHeight, params.DaaForkHeight + 1} {
		require.True(t, model.BelowCheckpoint(params.Checkpoints, height))
		// A fresh store has no authenticated checkpoint ancestry. Pre-DAA
		// deferral must reach the historical calculator in full validation.
		wantSkip := height > params.DaaForkHeight
		require.Equal(t, wantSkip, u.skipExpectedDifficulty(ctx, &model.Block{Height: height}), "height %d", height)
	}
}
