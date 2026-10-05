package subtreevalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// fullFixture is a server in the RUNNING state whose validator calls are recorded, with a single
// child transaction stored as a subtree the handlers can load.
type fullFixture struct {
	server          *Server
	recordingClient *recordingValidatorClient
	blockchain      blockchain.ClientI
	subtree         *subtreepkg.Subtree
	childHash       *chainhash.Hash
}

func newFullFixture(t *testing.T) *fullFixture {
	t.Helper()

	// regtest CSVHeight is 576; stay below it so the handlers skip the candidate-parent MTP fetch,
	// which would require real headers.
	const blockHeight = uint32(100)

	parentHash := parentTx1.TxIDChainHash()

	childTx := tx1.Clone()
	require.NoError(t, childTx.Inputs[0].PreviousTxIDAdd(parentHash))
	childTx.Inputs[0].PreviousTxOutIndex = 0
	childHash := childTx.TxIDChainHash()

	utxoStore, _, txStore, subtreeStore, blockchainClient, deferFunc := setup(t)
	t.Cleanup(deferFunc)

	recordingClient := newRecordingValidatorClient(&validator.MockValidator{UtxoStore: utxoStore})

	childSubtree, err := subtreepkg.NewTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, childSubtree.AddNode(*childHash, 121, 0))

	subtreeBytes, err := childSubtree.Serialize()
	require.NoError(t, err)
	require.NoError(t, subtreeStore.Set(t.Context(), childSubtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))
	require.NoError(t, subtreeStore.Set(t.Context(), childSubtree.RootHash()[:], fileformat.FileTypeSubtreeData, childTx.ExtendedBytes()))

	nilConsumer := &kafka.KafkaConsumerGroup{}

	fsmClient := &fsmStateOverrideClient{ClientI: blockchainClient, state: blockchain.FSMStateRUNNING}

	server, err := New(context.Background(), ulogger.TestLogger{}, test.CreateBaseTestSettings(t), subtreeStore, txStore, utxoStore, recordingClient, fsmClient, nilConsumer, nilConsumer, nil, nil)
	require.NoError(t, err)

	server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: blockHeight - 1})
	blockIDsMap := map[uint32]bool{}
	server.currentBlockIDsMap.Store(&blockIDsMap)

	return &fullFixture{server: server, recordingClient: recordingClient, blockchain: blockchainClient, subtree: childSubtree, childHash: childHash}
}

func (f *fullFixture) setFull(t *testing.T, full bool) {
	t.Helper()

	// Drive the real notification block assembly publishes, rather than reaching into the client.
	require.NoError(t, f.blockchain.SendNotification(context.Background(), blockchain.NewBlockAssemblyFullNotification(full)))
	require.Equal(t, full, f.blockchain.IsBlockAssemblyFull())
}

// TestSubtreesHandlerSkipsPeerSubtreeWholeWhenFull pins the peer-announced subtree gate.
//
// A peer-announced subtree is not in a block yet. While block assembly is full it must be skipped
// whole: validating it but keeping it out of the template creates the UTXOs unmined, and a child
// arriving later would spend a parent that is in neither the template nor a block.
func TestSubtreesHandlerSkipsPeerSubtreeWholeWhenFull(t *testing.T) {
	baseURL, err := url.Parse("http://peer.invalid:8090")
	require.NoError(t, err)

	t.Run("with room the subtree is validated and its transactions reach block assembly", func(t *testing.T) {
		InitPrometheusMetrics()

		f := newFullFixture(t)
		f.setFull(t, false)

		require.NoError(t, f.server.subtreesHandler(context.Background(), f.subtree.RootHash(), baseURL, "peer-1"))

		recorded := f.recordingClient.recordedOptions(*f.childHash)
		require.NotEmpty(t, recorded, "the transaction must be validated when there is room")
		require.True(t, recorded[0].AddTXToBlockAssembly)
	})

	t.Run("while full nothing is validated and no UTXO can be created", func(t *testing.T) {
		InitPrometheusMetrics()

		f := newFullFixture(t)
		f.setFull(t, true)

		require.NoError(t, f.server.subtreesHandler(context.Background(), f.subtree.RootHash(), baseURL, "peer-1"))

		require.Empty(t, f.recordingClient.recordedOptions(*f.childHash),
			"a skipped subtree must not be validated at all, or its UTXOs would exist outside the template")
	})
}

// TestBlockSubtreesDoNotFeedBlockAssemblyWhenFull pins the block path. A subtree inside a block is
// never refused, because blocks must validate, but its transactions are mined by that block and need
// no place in a template that is already at its limit.
func TestBlockSubtreesDoNotFeedBlockAssemblyWhenFull(t *testing.T) {
	request := func(f *fullFixture) *subtreevalidation_api.CheckSubtreeFromBlockRequest {
		return &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			Hash:              f.subtree.RootHash()[:],
			BaseUrl:           "legacy",
			BlockHeight:       100,
			BlockHash:         make([]byte, 32),
			PreviousBlockHash: make([]byte, 32),
		}
	}

	for _, tc := range []struct {
		name string
		full bool
		want bool
	}{
		{"with room, RUNNING still feeds block assembly", false, true},
		{"while full, RUNNING no longer feeds block assembly", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			InitPrometheusMetrics()

			f := newFullFixture(t)
			f.setFull(t, tc.full)

			response, err := f.server.CheckSubtreeFromBlock(context.Background(), request(f))
			require.NoError(t, err)
			require.True(t, response.Blessed, "a block's subtree must validate whether or not block assembly is full")

			recorded := f.recordingClient.recordedOptions(*f.childHash)
			require.NotEmpty(t, recorded)

			for _, opts := range recorded {
				require.Equal(t, tc.want, opts.AddTXToBlockAssembly)
				require.Equal(t, tc.full, opts.LockUnmined,
					"records kept out of the template are created Locked, so a child cannot enter the template without its parent")
			}
		})
	}
}
