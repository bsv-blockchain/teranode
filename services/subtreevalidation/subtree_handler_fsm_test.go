package subtreevalidation

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/validator"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	utxosql "github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	kafkamessage "github.com/bsv-blockchain/teranode/util/kafka/kafka_message"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type subtreeMessageFSMClient struct {
	*assemblyFSMClient
	reads atomic.Int32
}

func (c *subtreeMessageFSMClient) GetFSMCurrentState(ctx context.Context) (*blockchain.FSMStateType, error) {
	c.reads.Add(1)
	return c.assemblyFSMClient.GetFSMCurrentState(ctx)
}

// Exercise the Kafka entry point: its options must reach the validator even
// though ordinary peer validation bypasses both block-subtree handlers.
func TestSubtreeMessageHandlerAssemblyRequiresRunning(t *testing.T) {
	state := func(s blockchain.FSMStateType) *blockchain.FSMStateType { return &s }
	for _, tt := range []struct {
		name         string
		state        *blockchain.FSMStateType
		err          error
		wantValidate bool
		wantAssembly bool
		blocksOnly   bool
	}{
		{"running", state(blockchain.FSMStateRUNNING), nil, true, true, false},
		{"idle", state(blockchain.FSMStateIDLE), nil, true, false, false},
		{"unknown", state(blockchain.FSMStateType(99)), nil, true, false, false},
		{"missing", nil, nil, true, false, false},
		{"catchup", state(blockchain.FSMStateCATCHINGBLOCKS), nil, false, false, false},
		{"read error", nil, errors.NewProcessingError("FSM unavailable"), false, false, false},
		{"blocks only skips failing FSM read", nil, errors.NewProcessingError("FSM unavailable"), false, false, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			InitPrometheusMetrics()
			ctx := t.Context()
			logger := ulogger.TestLogger{}
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.SubtreeValidation.QuorumPath = t.TempDir()
			tSettings.SubtreeValidation.BlocksOnly = tt.blocksOnly
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
			client := &subtreeMessageFSMClient{assemblyFSMClient: &assemblyFSMClient{ClientI: localClient, state: tt.state, err: tt.err}}
			recorder := newRecordingValidatorClient(&validator.MockValidator{UtxoStore: utxoStore})
			consumer := &kafka.KafkaConsumerGroup{}
			server, err := New(ctx, logger, tSettings, subtreeStore, txStore, utxoStore, recorder, client, consumer, consumer, nil, nil)
			require.NoError(t, err)
			server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: 99})
			blockIDs := map[uint32]bool{}
			server.currentBlockIDsMap.Store(&blockIDs)

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
			require.NoError(t, subtreeStore.Set(ctx, st.RootHash()[:], fileformat.FileTypeSubtreeData, child.ExtendedBytes()))
			payload, err := proto.Marshal(&kafkamessage.KafkaSubtreeTopicMessage{
				Hash: st.RootHash().String(), URL: "http://peer.invalid", PeerId: "peer-1",
			})
			require.NoError(t, err)

			err = server.subtreeMessageHandler(ctx)(&kafka.KafkaMessage{Value: payload})
			if tt.blocksOnly {
				require.NoError(t, err, "BlocksOnly must not depend on FSM availability")
				require.Zero(t, client.reads.Load(), "BlocksOnly must not read FSM state")
			} else if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
			} else {
				require.NoError(t, err)
			}
			if !tt.wantValidate {
				require.Never(t, func() bool {
					return len(recorder.recordedOptions(*child.TxIDChainHash())) != 0
				}, 100*time.Millisecond, 10*time.Millisecond, "skipped messages must not start peer validation")
				return
			}

			// Waiting for the validated subtree also checks that IDLE keeps
			// ordinary peer intake and lets the asynchronous work finish.
			require.Eventually(t, func() bool {
				exists, existsErr := subtreeStore.Exists(ctx, st.RootHash()[:], fileformat.FileTypeSubtree)
				return existsErr == nil && exists
			}, 5*time.Second, 10*time.Millisecond)
			recorded := recorder.recordedOptions(*child.TxIDChainHash())
			require.NotEmpty(t, recorded)
			for _, opts := range recorded {
				require.Equal(t, tt.wantAssembly, opts.AddTXToBlockAssembly)
				require.False(t, opts.UnconfirmedParentsAtCandidateHeight, "peer intake must retain ordinary parent checks")
				require.False(t, opts.SkipPolicyChecks, "peer intake must retain policy checks")
			}
		})
	}
}
