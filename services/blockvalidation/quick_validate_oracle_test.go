package blockvalidation

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation"
	"github.com/bsv-blockchain/teranode/services/validator"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util/kafka"
	"github.com/stretchr/testify/require"
)

// TestAdoptionPhase_SignatureInvalidTx_NotBlessedFromCachedMetadata closes the
// attack sequence at its last step (bitcoin-sv/teranode#4838).
//
// The finding's oracle is that subtree validation treats an existing UTXO record as
// proof the transaction was consensus-validated: TxMetaFieldsForDecorate asks for
// nothing that records provenance, so a complete record makes the transaction "set"
// and the Validator is never consulted for it. This test does not change that
// mechanism — items 4 and 5 of the issue are deferred — it proves its INPUT is gone:
// the failed quick-validation attempt writes no record, so the signature-invalid
// transaction is still put to the Validator and the subtree is rejected.
//
// The failed attempt is driven through the real production route rather than a
// hand-seeded record, over the SAME UTXO store and the SAME subtree store subtree
// validation then reads. That sharing is the point of the test.
//
// Mutation target: the bindSubtreeBodyToHeader call in quickValidateBlock. With it
// stashed, the attempt writes the record, the count is zero and the subtree is
// accepted — the auditor's harness.
func TestAdoptionPhase_SignatureInvalidTx_NotBlessedFromCachedMetadata(t *testing.T) {
	h := newPreBindHarness(t, nil)

	coinbase := preBindCoinbase(t, 0x0e)
	parent := h.storeGenuineParent(0xe1)
	child := preBindSpendOf(t, parent, 9_000)

	served := h.oneSubtreeBody(coinbase, child)

	// A header that commits to a different honest body: the served body never binds.
	honest := buildSubtreeOver(t, true, []*bt.Tx{preBindSpendOf(t, parent, 5_000)})

	block := h.newPreBindBlock(coinbase,
		[]*chainhash.Hash{served.RootHash()},
		composeBlockMerkleRoot(t, []chainhash.Hash{coinbaseSubstitutedRoot(t, honest, coinbase)}),
		2)

	require.Error(t, h.bv.quickValidateBlock(h.ctx, block, "peer", ""))

	h.requireNoUTXOMutation(block, parent, child)

	// The adoption phase: subtree validation over the very same stores.
	var validateCalls atomic.Int64

	mockValidator := &validator.MockValidator{
		ValidateFunc: func(_ context.Context, tx *bt.Tx) (*meta.Data, error) {
			validateCalls.Add(1)

			return nil, errors.NewTxInvalidError("script verification failed for %s", tx.TxIDChainHash().String())
		},
	}

	nilConsumer := &kafka.KafkaConsumerGroup{}

	subtreeValidation, err := subtreevalidation.New(h.ctx, h.bv.logger, h.bv.settings,
		h.subtreeStore, blobmemory.New(), h.utxoStore, mockValidator, h.chain,
		nilConsumer, nilConsumer, nil, nil)
	require.NoError(t, err)
	require.NoError(t, subtreeValidation.Init(h.ctx))

	t.Cleanup(func() { _ = subtreeValidation.Stop(context.Background()) })

	// The subtree_data blob the failed attempt read is still on disk, which is how
	// subtree validation sources the transaction body it has no record for.
	exists, err := h.subtreeStore.Exists(h.ctx, served.RootHash()[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.True(t, exists)

	_, err = subtreeValidation.ValidateSubtreeInternal(h.ctx, subtreevalidation.ValidateSubtree{
		SubtreeHash: *served.RootHash(),
		BaseURL:     "legacy",
		TxHashes:    []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue, *child.TxIDChainHash()},
	}, preBindHeight, nil)

	require.Error(t, err, "a subtree carrying a signature-invalid transaction must be rejected")
	require.Positive(t, validateCalls.Load(), "the validator must be consulted, not bypassed from cached metadata")
}
