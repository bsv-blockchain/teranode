package blockvalidation

import (
	"context"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

func TestProcessBlockFound_CheckpointHeight(t *testing.T) {
	for _, claimed := range []uint32{0, 1, 11111} {
		t.Run(fmt.Sprint(claimed), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			settings := test.CreateBaseTestSettings(t)
			storeURL, err := url.Parse("sqlitememory:///")
			require.NoError(t, err)
			store, err := blockchainstore.NewStore(ulogger.TestLogger{}, storeURL, settings)
			require.NoError(t, err)
			client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, store, nil, nil)
			require.NoError(t, err)
			header := settings.ChainCfgParams.GenesisBlock.Header
			header.PrevBlock = *settings.ChainCfgParams.GenesisHash
			header.Timestamp = header.Timestamp.Add(10 * time.Minute)
			block, err := model.NewBlockFromMsgBlock(&wire.MsgBlock{Header: header, Transactions: settings.ChainCfgParams.GenesisBlock.Transactions}, nil)
			require.NoError(t, err)
			for {
				met, _, _ := block.Header.HasMetTargetDifficulty()
				if met {
					break
				}
				block.Header.Nonce++
			}
			block.Height = claimed
			block.Subtrees = []*chainhash.Hash{{1}}
			block.SubtreeSlices = nil
			server := New(ulogger.TestLogger{}, settings, nil, memory.New(), nil, nil, client, nil, nil, nil)
			server.blockValidation = NewBlockValidation(ctx, ulogger.TestLogger{}, settings, client, memory.New(), memory.New(), nil, nil, &checkpointSubtreeBoundary{})
			t.Cleanup(server.blockValidation.StopCaches)
			err = server.processBlockFound(ctx, block.Hash(), "", "legacy", block)
			if claimed == 11111 {
				require.ErrorContains(t, err, "peer-inconsistent height")
				require.True(t, errors.Is(err, errors.ErrBlockInvalid))
			} else {
				require.ErrorContains(t, err, "checkpoint subtree boundary reached")
				require.Equal(t, uint32(1), block.Height)
			}
			exists, err := client.GetBlockExists(ctx, block.Hash())
			require.NoError(t, err)
			require.False(t, exists, "a supplied height must not poison the stored header")
		})
	}
}

func TestDeriveBlockHeight(t *testing.T) {
	const cp = 100 // a checkpoint height, for the poisoning case

	tests := []struct {
		name       string
		claimed    uint32
		parent     uint32
		want       uint32
		wantReject bool
	}{
		{name: "parent height overflow rejected", parent: ^uint32(0), wantReject: true},
		{name: "honest height passes", claimed: 501, parent: 500, want: 501},
		{name: "genesis child derived", claimed: 1, parent: 0, want: 1},
		{name: "wire height zeroed is derived", claimed: 0, parent: cp - 1, want: cp},
		{name: "honest height at a checkpoint", claimed: cp, parent: cp - 1, want: cp},
		// The poisoning attempt: relabel the honest tip (real height 501) as checkpoint height 100.
		{name: "spoofed checkpoint height rejected", claimed: cp, parent: 500, wantReject: true},
		{name: "wire height one past rejected", claimed: cp + 1, parent: cp - 1, wantReject: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := deriveBlockHeight(tc.claimed, tc.parent)
			if tc.wantReject {
				require.Error(t, err)
				require.True(t, errors.Is(err, errors.ErrBlockInvalid))
				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
