package blockvalidation

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// Stop immediately after the header gates. A removed gate must produce this
// distinct error rather than being hidden by a later generic invalid-block error.
type checkpointSubtreeBoundary struct{ subtreevalidation.Interface }

func (*checkpointSubtreeBoundary) CheckBlockSubtrees(context.Context, *model.Block, string, string) error {
	return errors.NewProcessingError("checkpoint subtree boundary reached")
}

func TestCheckpointValidationCallSites(t *testing.T) {
	for _, worker := range []bool{false, true} {
		entry := "ValidateBlockWithOptions"
		if worker {
			entry = "reValidateBlock"
		}
		t.Run(entry, func(t *testing.T) {
			for _, gate := range []string{"pow limit", "expected difficulty", "expected difficulty during IBD", "pinned checkpoint during IBD", "unmet target", "checkpoint mismatch", "matching checkpoint"} {
				t.Run(gate, func(t *testing.T) {
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					settings := test.CreateBaseTestSettings(t)
					if gate == "pow limit" {
						params := chaincfg.MainNetParams
						settings.ChainCfgParams = &params
					}
					storeURL, err := url.Parse("sqlitememory:///")
					require.NoError(t, err)
					store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, settings)
					require.NoError(t, err)
					client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, store, nil, nil)
					require.NoError(t, err)

					// Reach a synthetic checkpoint through real SQL chain state. A new fork
					// at height one must no longer inherit the historical difficulty skip.
					prev := settings.ChainCfgParams.GenesisHash
					for height := uint32(1); height <= 2; height++ {
						header := settings.ChainCfgParams.GenesisBlock.Header
						header.PrevBlock = *prev
						header.Nonce += height
						coinbase := wire.NewMsgTx(1)
						coinbase.AddTxIn(&wire.TxIn{PreviousOutPoint: wire.OutPoint{Index: 0xffffffff}, SignatureScript: []byte{1, byte(height)}, Sequence: 0xffffffff})
						coinbase.AddTxOut(&wire.TxOut{Value: 50 * 100_000_000, PkScript: []byte{0x51}})
						header.MerkleRoot = coinbase.TxHash()
						block, err := model.NewBlockFromMsgBlock(&wire.MsgBlock{Header: header, Transactions: []*wire.MsgTx{coinbase}}, nil)
						require.NoError(t, err)
						require.NoError(t, client.AddBlock(ctx, block, "test"))
						prev = block.Hash()
					}
					settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 2, Hash: prev}}
					_, best, err := client.GetBestBlockHeader(ctx)
					require.NoError(t, err)
					require.Equal(t, uint32(2), best.Height)

					header := settings.ChainCfgParams.GenesisBlock.Header
					header.PrevBlock = *settings.ChainCfgParams.GenesisHash
					header.Bits = 0x207fffff
					if gate == "expected difficulty" || gate == "expected difficulty during IBD" || gate == "pinned checkpoint during IBD" {
						header.Bits = 0x2070ffff
					}
					// Keep a distinct candidate and meet its declared target. Only the gate
					// under test may reject it, not HasMetTargetDifficulty.
					header.Timestamp = header.Timestamp.Add(10 * time.Minute)
					candidate, err := model.NewBlockFromMsgBlock(&wire.MsgBlock{Header: header, Transactions: settings.ChainCfgParams.GenesisBlock.Transactions}, nil)
					require.NoError(t, err)
					candidate.Height = 1
					candidate.ID = 0
					for {
						if met, _, _ := candidate.Header.HasMetTargetDifficulty(); met {
							break
						}
						candidate.Header.Nonce++
					}
					if gate == "unmet target" {
						candidate.Header.Bits = model.NBit{}
					}
					if gate == "checkpoint mismatch" {
						settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1, Hash: &chainhash.Hash{9}}}
					}
					if gate == "matching checkpoint" {
						settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1, Hash: candidate.Hash()}}
					}
					if gate == "expected difficulty during IBD" {
						settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 100, Hash: &chainhash.Hash{9}}}
					}
					if gate == "pinned checkpoint during IBD" {
						settings.ChainCfgParams.Checkpoints = []chaincfg.Checkpoint{{Height: 1, Hash: candidate.Hash()}, {Height: 100, Hash: &chainhash.Hash{9}}}
					}
					candidate.Subtrees = []*chainhash.Hash{{1}}
					candidate.SubtreeSlices = nil
					require.True(t, candidate.Height > 0 && candidate.Height <= blockchain.HighestCheckpointHeight(settings.ChainCfgParams.Checkpoints))

					u := NewBlockValidation(ctx, ulogger.TestLogger{}, settings, client, memory.New(), memory.New(), nil, nil, &checkpointSubtreeBoundary{})
					t.Cleanup(func() { cancel(); u.StopCaches() })
					if worker {
						err = u.reValidateBlock(revalidateBlockData{block: candidate})
					} else {
						err = u.ValidateBlockWithOptions(ctx, candidate, "", &ValidateBlockOptions{DisableOptimisticMining: true})
					}
					switch gate {
					case "pow limit":
						require.ErrorContains(t, err, "block declares a target easier than the network proof-of-work limit")
					case "expected difficulty", "expected difficulty during IBD":
						require.ErrorContains(t, err, "block has incorrect difficulty bits")
					case "unmet target":
						require.ErrorContains(t, err, "block does not meet target difficulty")
						require.NotContains(t, err.Error(), "%!")
						require.ErrorContains(t, err, "block header does not meet target")
					case "checkpoint mismatch":
						require.ErrorContains(t, err, "block conflicts with hardcoded checkpoint")
					case "matching checkpoint", "pinned checkpoint during IBD":
						require.ErrorContains(t, err, "checkpoint subtree boundary reached")
						return
					}
					require.True(t, errors.Is(err, errors.ErrBlockInvalid))
				})
			}
		})
	}
}
