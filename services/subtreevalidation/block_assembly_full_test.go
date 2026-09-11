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
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestSubtreesHandlerKeepsTxsOutOfBlockAssemblyWhenFull pins the peer-announced subtree ingress gate.
//
// Peer subtree announcements are a transaction ingress point like propagation, legacy netsync and the
// sendrawtransaction RPC, and on a multi-node network they carry most of the volume: a subtree is
// announced ahead of the block that contains it. Left ungated, block assembly keeps growing past
// blockassembly_maxTransactionsInMemory through this path while the other three refuse, so the limit
// would not bound RAM at all.
//
// Only the block assembly insert may be skipped. The transactions must still be validated, so a
// later block carrying them validates normally, which is why this asserts that the transaction was
// validated in both cases and only AddTXToBlockAssembly differs.
func TestSubtreesHandlerKeepsTxsOutOfBlockAssemblyWhenFull(t *testing.T) {
	// regtest CSVHeight is 576; stay below it so the handler skips the candidate-parent MTP fetch,
	// which would require real headers.
	const blockHeight = uint32(100)

	// A child spending output 0 of parentTx1, the same fixture shape the peer-path option test uses,
	// so this exercises a subtree the handler is known to bless end to end.
	parentHash := parentTx1.TxIDChainHash()

	childTx := tx1.Clone()
	require.NoError(t, childTx.Inputs[0].PreviousTxIDAdd(parentHash))
	childTx.Inputs[0].PreviousTxOutIndex = 0
	childHash := childTx.TxIDChainHash()

	newServer := func(t *testing.T) (*Server, *recordingValidatorClient, blockchain.ClientI, *subtreepkg.Subtree) {
		utxoStore, _, txStore, subtreeStore, blockchainClient, deferFunc := setup(t)
		t.Cleanup(deferFunc)

		recordingClient := newRecordingValidatorClient(&validator.MockValidator{UtxoStore: utxoStore})

		childSubtree, err := subtreepkg.NewTreeByLeafCount(1)
		require.NoError(t, err)
		require.NoError(t, childSubtree.AddNode(*childHash, 121, 0))

		// the handler reads the FULL subtree serialization (NewSubtreeFromReader)
		subtreeBytes, err := childSubtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(t.Context(), childSubtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))
		require.NoError(t, subtreeStore.Set(t.Context(), childSubtree.RootHash()[:], fileformat.FileTypeSubtreeData, childTx.ExtendedBytes()))

		nilConsumer := &kafka.KafkaConsumerGroup{}
		tSettings := test.CreateBaseTestSettings(t)

		server, err := New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, recordingClient, blockchainClient, nilConsumer, nilConsumer, nil, nil)
		require.NoError(t, err)

		// the snapshot the subscription listener would normally maintain
		server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: blockHeight - 1})
		blockIDsMap := map[uint32]bool{}
		server.currentBlockIDsMap.Store(&blockIDsMap)

		return server, recordingClient, blockchainClient, childSubtree
	}

	// addToBlockAssembly runs the Kafka handler exactly as subtreeMessageHandler does — with no
	// validation options — and reports what the validator was told about block assembly.
	addToBlockAssembly := func(t *testing.T, full bool) bool {
		t.Helper()

		server, recordingClient, blockchainClient, childSubtree := newServer(t)

		ctx := context.Background()

		// Drive the real notification block assembly publishes, rather than reaching into the client.
		require.NoError(t, blockchainClient.SendNotification(ctx, blockchain.NewBlockAssemblyFullNotification(full)))
		require.Equal(t, full, blockchainClient.IsBlockAssemblyFull())

		baseURL, err := url.Parse("http://peer.invalid:8090")
		require.NoError(t, err)

		require.NoError(t, server.subtreesHandler(ctx, childSubtree.RootHash(), baseURL, "peer-1"))

		recorded := recordingClient.recordedOptions(*childHash)
		require.NotEmpty(t, recorded,
			"the transaction must still be validated whether or not block assembly is full")

		for i, opts := range recorded {
			require.NotNil(t, opts, "call #%d must carry resolved Options", i)
		}

		return recorded[0].AddTXToBlockAssembly
	}

	t.Run("block assembly has room, so transactions enter the mining template", func(t *testing.T) {
		InitPrometheusMetrics()

		require.True(t, addToBlockAssembly(t, false),
			"with room available the gate must be inert, so peer subtree transactions still reach block assembly")
	})

	t.Run("block assembly is full, so transactions are validated but kept out", func(t *testing.T) {
		InitPrometheusMetrics()

		require.False(t, addToBlockAssembly(t, true),
			"while block assembly is full its RAM must not keep growing through the peer subtree path")
	})

	// Guard against fixture drift: the child must really spend the parent, or the shape is vacuous.
	require.Equal(t, *parentHash, chainhash.Hash(childTx.Inputs[0].PreviousTxID()),
		"fixture: child must spend parentTx1")
}
