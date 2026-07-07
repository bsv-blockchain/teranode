package subtreevalidation

import (
	"context"
	"net/url"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// TestUnconfirmedParentsOptionDoesNotLeakToPeerPaths pins the no-leak side of
// the WithUnconfirmedParentsAtCandidateHeight contract after the option was
// widened to the block-validation path (CheckBlockSubtrees): the two
// PEER-FACING entry points must keep validating with the option OFF, so a
// child of an unconfirmed parent stays fail-closed when there is no
// PoW-checked block (and no block-level membership backstop) behind the
// request.
//
//   - the non-legacy branch of the CheckSubtreeFromBlock gRPC handler
//     (peer-announced subtree with a real base URL), and
//   - the Kafka subtree handler path (subtreesHandler, invoked by
//     subtreeMessageHandler with NO validation options).
//
// The block-path positive (option present on both CheckBlockSubtrees
// pipelines) is pinned by TestCheckBlockSubtrees_AssembledPath_SkipLevelAndMixedParent;
// the legacy-branch positive and the direct ValidateSubtreeInternal negative
// are pinned by TestCheckSubtreeFromBlockLegacyUnconfirmedParents. This test
// closes the remaining peer-facing entry points at the handler level, so a
// refactor that threads the option into shared plumbing below either handler
// is caught even when the option-set call sites look correct.
func TestUnconfirmedParentsOptionDoesNotLeakToPeerPaths(t *testing.T) {
	// regtest CSVHeight is 576; stay below it so the handlers skip the
	// candidate-parent MTP fetch, which would require real headers.
	const blockHeight = uint32(100)

	// child spends output 0 of parentTx1; cloning tx1 and repointing its
	// input keeps the tx extended — same fixture shape as
	// TestCheckSubtreeFromBlockLegacyUnconfirmedParents, where the parent is
	// in the UTXO store with empty BlockHeights (the sentinel shape).
	parentHash := parentTx1.TxIDChainHash()

	childTx := tx1.Clone()
	require.NoError(t, childTx.Inputs[0].PreviousTxIDAdd(parentHash))
	childTx.Inputs[0].PreviousTxOutIndex = 0
	childHash := childTx.TxIDChainHash()

	newServer := func(t *testing.T) (*Server, *recordingValidatorClient, *subtreepkg.Subtree) {
		utxoStore, _, txStore, subtreeStore, blockchainClient, deferFunc := setup(t)
		t.Cleanup(deferFunc)

		recordingClient := newRecordingValidatorClient(&validator.MockValidator{UtxoStore: utxoStore})

		childSubtree, err := subtreepkg.NewTreeByLeafCount(1)
		require.NoError(t, err)
		require.NoError(t, childSubtree.AddNode(*childHash, 121, 0))

		// the handlers read the FULL subtree serialization (NewSubtreeFromReader)
		subtreeBytes, err := childSubtree.Serialize()
		require.NoError(t, err)
		require.NoError(t, subtreeStore.Set(t.Context(), childSubtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))
		require.NoError(t, subtreeStore.Set(t.Context(), childSubtree.RootHash()[:], fileformat.FileTypeSubtreeData, childTx.ExtendedBytes()))

		nilConsumer := &kafka.KafkaConsumerGroup{}
		tSettings := test.CreateBaseTestSettings(t)

		server, err := New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, recordingClient, blockchainClient, nilConsumer, nilConsumer, nil, nil)
		require.NoError(t, err)

		return server, recordingClient, childSubtree
	}

	requireFlagNeverSet := func(t *testing.T, recordingClient *recordingValidatorClient, path string) {
		recorded := recordingClient.recordedOptions(*childHash)
		require.NotEmpty(t, recorded, "child transaction was not validated on the %s path", path)

		for i, opts := range recorded {
			require.NotNil(t, opts, "%s call #%d must carry resolved Options", path, i)
			require.False(t, opts.UnconfirmedParentsAtCandidateHeight,
				"%s path call #%d must NOT carry UnconfirmedParentsAtCandidateHeight — fail-open unconfirmed-parent resolution is only safe behind a PoW-checked block with the block.Valid membership backstop; neither exists on this path", path, i)
		}
	}

	t.Run("CheckSubtreeFromBlock non-legacy branch keeps the option off", func(t *testing.T) {
		InitPrometheusMetrics()

		server, recordingClient, childSubtree := newServer(t)

		// A non-"legacy" base URL routes to the peer-facing branch of
		// checkSubtreeFromBlock. The subtree and its data exist locally, so
		// the URL is never dereferenced.
		response, err := server.CheckSubtreeFromBlock(context.Background(), &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			Hash:              childSubtree.RootHash()[:],
			BaseUrl:           "http://peer.invalid:8090",
			BlockHeight:       blockHeight,
			BlockHash:         make([]byte, 32),
			PreviousBlockHash: make([]byte, 32),
		})
		require.NoError(t, err)
		require.True(t, response.Blessed)

		requireFlagNeverSet(t, recordingClient, "CheckSubtreeFromBlock non-legacy")
	})

	t.Run("Kafka subtreesHandler keeps the option off", func(t *testing.T) {
		InitPrometheusMetrics()

		server, recordingClient, childSubtree := newServer(t)

		// subtreeMessageHandler invokes subtreesHandler with NO validation
		// options; replicate that invocation directly. The handler needs the
		// best-block snapshot the subscription listener would normally
		// maintain.
		server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: blockHeight - 1})
		blockIDsMap := map[uint32]bool{}
		server.currentBlockIDsMap.Store(&blockIDsMap)

		baseURL, err := url.Parse("http://peer.invalid:8090")
		require.NoError(t, err)

		require.NoError(t, server.subtreesHandler(context.Background(), childSubtree.RootHash(), baseURL, "peer-1"))

		requireFlagNeverSet(t, recordingClient, "Kafka subtreesHandler")
	})

	// Guard against hash drift in the fixture: the child must really spend
	// the unconfirmed parent, otherwise the sentinel shape this test claims
	// to exercise is vacuous.
	require.Equal(t, *parentHash, chainhash.Hash(childTx.Inputs[0].PreviousTxID()),
		"fixture: child must spend parentTx1")
}
