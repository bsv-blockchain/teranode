package subtreevalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// assemblyFSMClient only overrides FSM observations; all chain reads use SQLite.
type assemblyFSMClient struct {
	blockchain.ClientI
	state *blockchain.FSMStateType
	err   error
}

func (c *assemblyFSMClient) GetFSMCurrentState(context.Context) (*blockchain.FSMStateType, error) {
	return c.state, c.err
}

func TestBlockSubtreeAssemblyRequiresRunning(t *testing.T) {
	state := func(s blockchain.FSMStateType) *blockchain.FSMStateType { return &s }
	for _, path := range []string{"legacy", "block"} {
		t.Run(path, func(t *testing.T) {
			for _, tt := range []struct {
				name         string
				state        *blockchain.FSMStateType
				err          error
				wantAssembly bool
			}{
				{"running", state(blockchain.FSMStateRUNNING), nil, true},
				{"catchup", state(blockchain.FSMStateCATCHINGBLOCKS), nil, false},
				{"idle", state(blockchain.FSMStateIDLE), nil, false},
				{"unknown", state(blockchain.FSMStateType(99)), nil, false},
				{"missing", nil, nil, false},
				{"read error", nil, errors.NewProcessingError("FSM unavailable"), false},
			} {
				t.Run(tt.name, func(t *testing.T) {
					InitPrometheusMetrics()
					ctx := context.Background()
					logger := ulogger.TestLogger{}
					tSettings := test.CreateBaseTestSettings(t)
					storeURL, err := url.Parse("sqlitememory:///")
					require.NoError(t, err)
					chainStore, err := blockchainstore.NewStore(logger, storeURL, tSettings)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, chainStore.Close(context.Background())) })
					utxoStore, err := utxosql.New(ctx, logger, tSettings, storeURL)
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, utxoStore.Close(context.Background())) })
					subtreeStore, txStore := blobmemory.New(), blobmemory.New()
					localClient, err := blockchain.NewLocalClient(logger, tSettings, chainStore, subtreeStore, utxoStore)
					require.NoError(t, err)
					client := &assemblyFSMClient{ClientI: localClient, state: tt.state, err: tt.err}
					recorder := newRecordingValidatorClient(&validator.MockValidator{UtxoStore: utxoStore})
					consumer := &kafka.KafkaConsumerGroup{}
					server, err := New(ctx, logger, tSettings, subtreeStore, txStore, utxoStore, recorder, client, consumer, consumer, nil, nil)
					require.NoError(t, err)
					child := tx1.Clone()
					require.NoError(t, child.Inputs[0].PreviousTxIDAdd(parentTx1.TxIDChainHash()))
					child.Inputs[0].PreviousTxOutIndex = 0
					_, err = utxoStore.Create(ctx, parentTx1, 99)
					require.NoError(t, err)
					st, err := subtreepkg.NewTreeByLeafCount(1)
					require.NoError(t, err)
					require.NoError(t, st.AddNode(*child.TxIDChainHash(), 121, 0))
					serialized, err := st.Serialize()
					require.NoError(t, err)
					require.NoError(t, subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeToCheck, serialized))
					if path == "legacy" {
						require.NoError(t, subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeData, child.ExtendedBytes()))
						_, err = server.CheckSubtreeFromBlock(ctx, &subtreevalidation_api.CheckSubtreeFromBlockRequest{
							Hash: st.RootHash()[:], BaseUrl: "legacy", BlockHeight: 100,
							BlockHash: make([]byte, 32), PreviousBlockHash: tSettings.ChainCfgParams.GenesisHash[:],
						})
					} else {
						require.NoError(t, subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeData, child.Bytes()))
						block, blockErr := model.NewBlock(&model.BlockHeader{
							Version: 1, HashPrevBlock: tSettings.ChainCfgParams.GenesisHash,
							HashMerkleRoot: &chainhash.Hash{}, Timestamp: 1,
						}, &bt.Tx{Version: 1}, []*chainhash.Hash{st.RootHash()}, 2, 1000, 100, 0)
						require.NoError(t, blockErr)
						blockBytes, blockErr := block.Bytes()
						require.NoError(t, blockErr)
						_, err = server.CheckBlockSubtrees(ctx, &subtreevalidation_api.CheckBlockSubtreesRequest{Block: blockBytes, BaseUrl: "http://peer.invalid"})
					}
					if tt.err != nil {
						require.Error(t, err)
						require.Empty(t, recorder.recordedOptions(*child.TxIDChainHash()))
						return
					}
					require.NoError(t, err)
					recorded := recorder.recordedOptions(*child.TxIDChainHash())
					require.NotEmpty(t, recorded, "admitted block transactions must still validate")
					for _, opts := range recorded {
						require.Equal(t, tt.wantAssembly, opts.AddTXToBlockAssembly)
						require.True(t, opts.UnconfirmedParentsAtCandidateHeight, "pause must preserve consensus validation options")
					}
				})
			}
		})
	}
}
