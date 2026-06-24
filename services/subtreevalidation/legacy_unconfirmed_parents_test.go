package subtreevalidation

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// fsmStateOverrideClient wraps a blockchain client and pins GetFSMCurrentState
// to a fixed state. The LocalClient test double always reports RUNNING, but
// the legacy branch's option pairing is FSM-gated on
// CATCHINGBLOCKS — tests need to drive both sides of that gate.
type fsmStateOverrideClient struct {
	blockchain.ClientI
	state blockchain.FSMStateType
}

func (c *fsmStateOverrideClient) GetFSMCurrentState(_ context.Context) (*blockchain.FSMStateType, error) {
	state := c.state
	return &state, nil
}

// TestCheckSubtreeFromBlockLegacyUnconfirmedParents is the subtreevalidation
// regression for the legacy-sync wedge at testnet 1730003: a tx in a mined
// block spending a same-block parent found the parent in the UTXO store with
// empty BlockHeights, the validator stamped the unconfirmedParentHeight
// sentinel, and BDK rejected the block with
// bad-txns-unconfirmed-input-in-block.
//
// The legacy branch of checkSubtreeFromBlock now sets
// validator.WithUnconfirmedParentsAtCandidateHeight(true), which makes the
// validator resolve the sentinel to the candidate block height (the parent's
// true height on this path). These tests pin that the flag reaches the
// per-tx validation Options on the legacy branch — including the
// cross-subtree case — and does NOT leak onto the peer-facing branch.
// The validator-side consensus outcome (real GoBDK accept/reject) is pinned
// by TestValidate_ConsensusAcceptsUnconfirmedParentAtCandidateHeight and
// TestValidate_ConsensusRejectsUnconfirmedParent in services/validator.
func TestCheckSubtreeFromBlockLegacyUnconfirmedParents(t *testing.T) {
	// regtest CSVHeight is 576; stay below it so the handler skips the
	// candidate-parent MTP fetch, which would require real headers
	const blockHeight = uint32(100)

	// child spends output 0 of parentTx1; cloning tx1 and repointing its input
	// keeps the tx extended (PreviousTxSatoshis/Script survive the clone).
	parentHash := parentTx1.TxIDChainHash()

	childTx := tx1.Clone()
	require.NoError(t, childTx.Inputs[0].PreviousTxIDAdd(parentHash))
	childTx.Inputs[0].PreviousTxOutIndex = 0
	childHash := childTx.TxIDChainHash()

	// newServer builds a Server around a recording validator client and
	// stores the given (subtree, subtreeData) pairs as legacy files.
	type subtreeFixture struct {
		st   *subtreepkg.Subtree
		data []byte
	}

	newServerWithFSMState := func(t *testing.T, fsmState blockchain.FSMStateType, fixtures ...subtreeFixture) (*Server, *recordingValidatorClient) {
		utxoStore, _, txStore, subtreeStore, blockchainClient, deferFunc := setup(t)
		t.Cleanup(deferFunc)

		recordingClient := newRecordingValidatorClient(&validator.MockValidatorClient{UtxoStore: utxoStore})

		for _, f := range fixtures {
			// the legacy handler branch reads the FULL subtree serialization
			// (NewSubtreeFromReader), not just the nodes
			subtreeBytes, err := f.st.Serialize()
			require.NoError(t, err)

			require.NoError(t, subtreeStore.Set(t.Context(), f.st.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))
			require.NoError(t, subtreeStore.Set(t.Context(), f.st.RootHash()[:], fileformat.FileTypeSubtreeData, f.data))
		}

		nilConsumer := &kafka.KafkaConsumerGroup{}
		tSettings := test.CreateBaseTestSettings(t)

		fsmClient := &fsmStateOverrideClient{ClientI: blockchainClient, state: fsmState}

		server, err := New(context.Background(), ulogger.TestLogger{}, tSettings, subtreeStore, txStore, utxoStore, recordingClient, fsmClient, nilConsumer, nilConsumer, nil, nil)
		require.NoError(t, err)

		return server, recordingClient
	}

	newServer := func(t *testing.T, fixtures ...subtreeFixture) (*Server, *recordingValidatorClient) {
		return newServerWithFSMState(t, blockchain.FSMStateCATCHINGBLOCKS, fixtures...)
	}

	singleNodeSubtree := func(t *testing.T, hash *chainhash.Hash) *subtreepkg.Subtree {
		st, err := subtreepkg.NewTreeByLeafCount(1)
		require.NoError(t, err)
		require.NoError(t, st.AddNode(*hash, 121, 0))

		return st
	}

	legacyRequest := func(st *subtreepkg.Subtree) *subtreevalidation_api.CheckSubtreeFromBlockRequest {
		return &subtreevalidation_api.CheckSubtreeFromBlockRequest{
			Hash:              st.RootHash()[:],
			BaseUrl:           "legacy",
			BlockHeight:       blockHeight,
			BlockHash:         make([]byte, 32),
			PreviousBlockHash: make([]byte, 32),
		}
	}

	t.Run("legacy branch sets UnconfirmedParentsAtCandidateHeight during legacy sync", func(t *testing.T) {
		InitPrometheusMetrics()

		childSubtree := singleNodeSubtree(t, childHash)
		server, recordingClient := newServer(t, subtreeFixture{childSubtree, childTx.ExtendedBytes()})

		response, err := server.CheckSubtreeFromBlock(context.Background(), legacyRequest(childSubtree))
		require.NoError(t, err)
		require.True(t, response.Blessed)

		recorded := recordingClient.recordedOptions(*childHash)
		require.NotEmpty(t, recorded, "child transaction was not validated")

		for _, opts := range recorded {
			require.True(t, opts.UnconfirmedParentsAtCandidateHeight,
				"legacy branch must resolve unconfirmed (same-block) parents at the candidate height — without it the sentinel reaches BDK as MEMPOOL_HEIGHT and the block is rejected with bad-txns-unconfirmed-input-in-block")
			require.False(t, opts.AddTXToBlockAssembly,
				"during legacy sync, bulk-history txs must not feed block assembly (upstream behaviour)")
		}
	})

	t.Run("RUNNING state: flag stays on, assembly stays enabled", func(t *testing.T) {
		InitPrometheusMetrics()

		// Tip blocks arriving over the legacy bridge while RUNNING also reach
		// this branch — a restarted node has its FSM restored to RUNNING and
		// catches up over the legacy bridge (this wedged testnet at 1740437
		// when the flag was FSM-gated). The resolution flag must be active in
		// EVERY FSM state, while blessed txs keep feeding block assembly
		// (reorg resilience — upstream behaviour).
		childSubtree := singleNodeSubtree(t, childHash)
		server, recordingClient := newServerWithFSMState(t, blockchain.FSMStateRUNNING, subtreeFixture{childSubtree, childTx.ExtendedBytes()})

		response, err := server.CheckSubtreeFromBlock(context.Background(), legacyRequest(childSubtree))
		require.NoError(t, err)
		require.True(t, response.Blessed)

		recorded := recordingClient.recordedOptions(*childHash)
		require.NotEmpty(t, recorded, "child transaction was not validated")

		for _, opts := range recorded {
			require.True(t, opts.UnconfirmedParentsAtCandidateHeight,
				"resolution flag must be active in RUNNING too — FSM-gating it wedged testnet catch-up at 1740437")
			require.True(t, opts.AddTXToBlockAssembly,
				"RUNNING-state legacy blocks must keep feeding block assembly (reorg resilience) — upstream behaviour")
		}
	})

	t.Run("cross-subtree parent: two sequential calls both validate", func(t *testing.T) {
		InitPrometheusMetrics()

		// parent in subtree 0, child in subtree 1 — validated through two
		// sequential handler calls exactly as legacy netsync issues them.
		// After call 1 the parent exists in the UTXO store with empty
		// BlockHeights: precisely the sentinel case for call 2's child.
		parentSubtree := singleNodeSubtree(t, parentTx1.TxIDChainHash())
		childSubtree := singleNodeSubtree(t, childHash)

		server, recordingClient := newServer(t,
			subtreeFixture{parentSubtree, parentTx1.ExtendedBytes()},
			subtreeFixture{childSubtree, childTx.ExtendedBytes()},
		)

		response, err := server.CheckSubtreeFromBlock(context.Background(), legacyRequest(parentSubtree))
		require.NoError(t, err)
		require.True(t, response.Blessed)

		response, err = server.CheckSubtreeFromBlock(context.Background(), legacyRequest(childSubtree))
		require.NoError(t, err)
		require.True(t, response.Blessed)

		recorded := recordingClient.recordedOptions(*childHash)
		require.NotEmpty(t, recorded, "child transaction was not validated")

		for _, opts := range recorded {
			require.True(t, opts.UnconfirmedParentsAtCandidateHeight)
		}
	})

	t.Run("peer-facing branch does NOT set the flag", func(t *testing.T) {
		InitPrometheusMetrics()

		// ValidateSubtreeInternal is the peer-announced path (nil accumulator,
		// no legacy options). The fail-open resolution must not leak here.
		childSubtree := singleNodeSubtree(t, childHash)
		server, recordingClient := newServer(t, subtreeFixture{childSubtree, childTx.ExtendedBytes()})

		v := ValidateSubtree{
			SubtreeHash:   *childSubtree.RootHash(),
			BaseURL:       "legacy", // store-read path; options below are the peer set
			TxHashes:      []chainhash.Hash{*childHash},
			AllowFailFast: false,
		}

		_, err := server.ValidateSubtreeInternal(context.Background(), v, blockHeight, nil,
			validator.WithSkipPolicyChecks(true),
			validator.WithCreateConflicting(true),
			validator.WithIgnoreLocked(true),
		)
		require.NoError(t, err)

		recorded := recordingClient.recordedOptions(*childHash)
		require.NotEmpty(t, recorded, "child transaction was not validated")

		for _, opts := range recorded {
			require.False(t, opts.UnconfirmedParentsAtCandidateHeight,
				"fail-open unconfirmed-parent resolution must never be set on the peer-facing path")
		}
	})
}
