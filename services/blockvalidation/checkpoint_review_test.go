package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestQuickValidationAuthenticatesBodyBeforeMutation(t *testing.T) {
	for _, mode := range []string{"sequential", "pipeline", "async"} {
		for _, mutation := range []string{"duplicate", "late duplicate", "merkle", "coinbase only"} {
			t.Run(fmt.Sprintf("%s/%s", mode, mutation), func(t *testing.T) {
				ctx := context.Background()
				cfg := test.CreateBaseTestSettings(t)
				cfg.BlockValidation.SubtreeBatchSize = 1
				cfg.BlockValidation.SubtreeBatchPrefetchDepth = 0
				if mode == "pipeline" {
					cfg.BlockValidation.SubtreeBatchPrefetchDepth = 2
				}
				storeURL, err := url.Parse("sqlitememory:///")
				require.NoError(t, err)
				store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, cfg)
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, store.(interface{ Close() error }).Close()) })
				client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, cfg, store, nil, nil)
				require.NoError(t, err)
				blobs := memory.New()
				u := &BlockValidation{logger: ulogger.TestLogger{}, settings: cfg, blockchainClient: client, subtreeStore: blobs}
				block := testhelpers.CreateTestBlocksWithPrev(t, 1, cfg.ChainCfgParams.GenesisHash)[0]
				block.Height = 1
				subtree, err := subtreepkg.NewTreeByLeafCount(4)
				require.NoError(t, err)
				require.NoError(t, subtree.AddCoinbaseNode())
				for _, hash := range []chainhash.Hash{{1}, {2}} {
					require.NoError(t, subtree.AddNode(hash, 0, 1))
				}
				if mutation == "late duplicate" {
					require.NoError(t, subtree.AddNode(chainhash.Hash{3}, 0, 1))
				}
				root, err := subtree.RootHashWithReplaceRootNode(block.CoinbaseTx.TxIDChainHash(), 0, uint64(block.CoinbaseTx.Size()))
				require.NoError(t, err)
				block.Header.HashMerkleRoot = root
				if mutation == "duplicate" {
					require.NoError(t, subtree.AddNode(chainhash.Hash{2}, 0, 1))
				}
				block.Subtrees = []*chainhash.Hash{subtree.RootHash()}
				block.SubtreeSlices = []*subtreepkg.Subtree{subtree}
				if mutation == "late duplicate" {
					last, err := subtreepkg.NewTreeByLeafCount(4)
					require.NoError(t, err)
					for _, hash := range []chainhash.Hash{{4}, {5}, {6}} {
						require.NoError(t, last.AddNode(hash, 0, 1))
					}
					lastRoot := last.RootHash()
					combined := chainhash.DoubleHashH(append(root.CloneBytes(), lastRoot[:]...))
					block.Header.HashMerkleRoot = &combined
					require.NoError(t, last.AddNode(chainhash.Hash{6}, 0, 1))
					block.Subtrees = append(block.Subtrees, last.RootHash())
					block.SubtreeSlices = append(block.SubtreeSlices, last)
					data, err := last.Serialize()
					require.NoError(t, err)
					require.NoError(t, blobs.Set(ctx, last.RootHash()[:], fileformat.FileTypeSubtreeToCheck, data))
				}
				require.NoError(t, block.CheckMerkleRoot(ctx), "the duplicate body must retain the honest header commitment")
				serialized, err := subtree.Serialize()
				require.NoError(t, err)
				require.NoError(t, blobs.Set(ctx, block.Subtrees[0][:], fileformat.FileTypeSubtreeToCheck, serialized))
				if mutation == "merkle" {
					block.Header.HashMerkleRoot = &chainhash.Hash{99}
				}
				if mutation == "coinbase only" {
					block.Subtrees = nil
					block.SubtreeSlices = nil
				}
				block.SubtreeSlices = nil
				jobs := make(chan *SubtreeWriteJob, 10)
				if mode == "async" {
					err = u.quickValidateBlockAsync(ctx, block, "peer", "", jobs)
				} else {
					err = u.quickValidateBlock(ctx, block, "peer", "")
				}
				if mutation == "duplicate" || mutation == "late duplicate" {
					require.ErrorContains(t, err, "duplicate transaction")
				} else {
					require.ErrorContains(t, err, "body does not match header merkle root")
				}
				require.True(t, errors.Is(err, errors.ErrBlockInvalid))
				require.Zero(t, block.ID, "no ID may be assigned before the body is authenticated")
				require.Empty(t, jobs, "no async writes may be queued")
				exists, err := client.GetBlockExists(ctx, block.Hash())
				require.NoError(t, err)
				require.False(t, exists)
				for _, hash := range block.Subtrees {
					exists, err := blobs.Exists(ctx, hash[:], fileformat.FileTypeSubtree)
					require.NoError(t, err)
					require.False(t, exists)
				}
			})
		}
	}
}

func TestCatchupCheckpointProofAnchorsAndBounds(t *testing.T) {
	cfg := test.CreateBaseTestSettings(t)
	cfg.BlockValidation.CatchupAllowQuickValidation = true
	server := &Server{logger: ulogger.TestLogger{}, settings: cfg}
	blocks := testhelpers.CreateTestBlocks(t, 4)
	headers := make([]*model.BlockHeader, len(blocks))
	for i, block := range blocks {
		block.Height = uint32(i + 1)
		headers[i] = block.Header
	}
	c := &CatchupContext{
		blockUpTo:          blocks[3],
		commonAncestorHash: headers[0].HashPrevBlock,
		commonAncestorMeta: &model.BlockHeaderMeta{Height: 0},
		blockHeaders:       headers,
		checkpoints: []chaincfg.Checkpoint{
			{Height: 2, Hash: blocks[1].Hash()},
			{Height: 100, Hash: &chainhash.Hash{99}},
		},
	}
	require.NoError(t, server.verifyCheckpointsInHeaderChain(c))
	require.Equal(t, uint32(2), c.highestCheckpointHeight, "unseen checkpoints cannot prove later blocks")
	require.True(t, c.blockCheckpointProven(blocks[0]))
	require.True(t, c.blockCheckpointProven(blocks[1]))
	require.False(t, c.blockCheckpointProven(blocks[2]))
	cfg.BlockValidation.CatchupAllowQuickValidation = false
	require.NoError(t, server.verifyCheckpointsInHeaderChain(c))
	require.False(t, c.useQuickValidation)
	require.True(t, c.blockCheckpointProven(blocks[0]), "full validation still has authenticated ancestry")
	cfg.BlockValidation.CatchupAllowQuickValidation = true
	c.checkpoints = append(c.checkpoints, chaincfg.Checkpoint{Height: 3, Hash: &chainhash.Hash{77}})
	require.ErrorContains(t, server.verifyCheckpointsInHeaderChain(c), "CHECKPOINT VERIFICATION FAILED")
	require.Zero(t, c.highestCheckpointHeight, "a failed verification must publish no proof")
	c.checkpoints = c.checkpoints[:len(c.checkpoints)-1]
	require.NoError(t, server.verifyCheckpointsInHeaderChain(c))
	fork := testhelpers.CreateTestBlocks(t, 1)[0]
	fork.Height = 1
	fork.Header.Nonce++
	require.False(t, c.blockCheckpointProven(fork))
	c.commonAncestorHash = &chainhash.Hash{42}
	require.ErrorContains(t, server.verifyCheckpointsInHeaderChain(c), "does not connect to common ancestor")
	require.False(t, c.useQuickValidation)
	require.ErrorContains(t, server.buildHeaderCache(c), "doesn't match common ancestor")
	require.ErrorContains(t, server.verifyChainContinuity(context.Background(), c), "does not connect to common ancestor")
	c.commonAncestorHash = headers[0].HashPrevBlock
	headers[2].HashPrevBlock = &chainhash.Hash{42}
	require.ErrorContains(t, server.verifyCheckpointsInHeaderChain(c), "does not connect to common ancestor")
	require.ErrorContains(t, server.buildHeaderCache(c), "disconnected header")
}
