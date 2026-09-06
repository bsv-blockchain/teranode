package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

// withNonMinimalInputCount rewrites the input-count prefix of the transaction
// starting at off from its minimal 1-byte form to the 3-byte form. go-bt accepts
// the over-long prefix and re-serializes it canonically, so every hash-based
// check downstream still passes — which is precisely why the encoding has to be
// caught at the parse.
func withNonMinimalInputCount(t *testing.T, payload []byte, off int) []byte {
	t.Helper()

	count := payload[off+4]
	require.Greater(t, count, byte(0x00), "fixture transaction must have inputs")
	require.Less(t, count, byte(0xfd), "fixture transaction must have a 1-byte input count")

	mangled := make([]byte, 0, len(payload)+2)
	mangled = append(mangled, payload[:off+4]...) // through version
	mangled = append(mangled, 0xfd, count, 0x00)  // non-minimal input count
	mangled = append(mangled, payload[off+5:]...) // the rest, unchanged

	return mangled
}

// TestFetchAndStoreSubtreeData_NonCanonicalEncoding pins issue 1421 on the
// catchup prewarm. This path runs BEFORE block validation and stores the
// re-serialized form, so an unchecked non-minimal payload is laundered to disk
// and every downstream check then sees only canonical bytes — the per-transaction
// check can never fire for that subtree. Review demonstrated the bypass with a
// throwaway probe; this is that probe made permanent.
func TestFetchAndStoreSubtreeData_NonCanonicalEncoding(t *testing.T) {
	baseURL := "http://non-canonical-peer:8000"
	peerID := "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"
	ctx := context.Background()

	txs := transactions.CreateTestTransactionChainWithCount(t, 5)

	st, err := subtree.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(*txs[1].TxIDChainHash(), 1, 11))
	require.NoError(t, st.AddNode(*txs[2].TxIDChainHash(), 2, 12))
	require.NoError(t, st.AddNode(*txs[3].TxIDChainHash(), 3, 13))

	subtreeHash := st.RootHash()
	subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())
	testBlock := &model.Block{Height: 100}

	// Canonical payload: the three non-coinbase transactions concatenated, as
	// the peer's on-demand generator produces them (coinbase omitted).
	var canonicalBuf bytes.Buffer
	for i := 1; i <= 3; i++ {
		canonicalBuf.Write(txs[i].Bytes())
	}

	canonical := canonicalBuf.Bytes()

	newServer := func() *Server {
		return &Server{
			logger:       ulogger.TestLogger{},
			subtreeStore: memory.New(),
			settings:     test.CreateBaseTestSettings(t),
		}
	}

	t.Run("canonical payload is accepted and stored", func(t *testing.T) {
		server := newServer()
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET", subtreeDataURL, httpmock.NewBytesResponder(200, canonical))

		require.NoError(t, server.fetchAndStoreSubtreeData(ctx, testBlock, subtreeHash, st, peerID, baseURL, false))
	})

	t.Run("non-minimal CompactSize is rejected, not laundered to disk", func(t *testing.T) {
		idx := bytes.Index(canonical, txs[1].Bytes())
		require.GreaterOrEqual(t, idx, 0, "fixture must contain the first transaction verbatim")

		mangled := withNonMinimalInputCount(t, canonical, idx)

		server := newServer()
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET", subtreeDataURL, httpmock.NewBytesResponder(200, mangled))

		err := server.fetchAndStoreSubtreeData(ctx, testBlock, subtreeHash, st, peerID, baseURL, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-canonical subtree data")
		require.True(t, errors.Is(err, errors.ErrProcessing))
		// Must NOT be ErrTxInvalid. On this prewarm path that code does not reach
		// storeInvalidBlock (the prewarm runs before ValidateBlock), but
		// releaseCatchupLock's classification tests ErrBlockInvalid/ErrTxInvalid
		// first, so it would report the serving peer as malicious for delivering
		// bytes that are not in fact invalid. See CheckCanonicalSubtreeData.
		require.False(t, errors.Is(err, errors.ErrTxInvalid))

		// The canonicalised form must NOT have been written: storing it would
		// discard the evidence and bless a subtree SV Node rejects at parse.
		exists, existsErr := server.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, existsErr)
		require.False(t, exists, "non-canonical payload must not be stored")
	})

	// A peer may equally serve the coinbase, which is what a subtree-data file
	// written from parsed transactions contains. go-subtree parses it into Txs[0]
	// but Serialize writes the file from index 1, so the parse legitimately
	// consumes bytes the serialization never re-emits. The check has to add the
	// coinbase's canonical size back, or every first subtree of every block from
	// such a peer is rejected and catchup stalls. An earlier commit on this branch
	// had to fix exactly that, which is why the branch is pinned here rather than
	// left to the util-level unit test.
	withCoinbase := append(txs[0].Bytes(), canonical...)

	t.Run("canonical payload including the coinbase is accepted", func(t *testing.T) {
		server := newServer()
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET", subtreeDataURL, httpmock.NewBytesResponder(200, withCoinbase))

		require.NoError(t, server.fetchAndStoreSubtreeData(ctx, testBlock, subtreeHash, st, peerID, baseURL, false))

		// The stored form omits the coinbase, so accepting the payload must not
		// mean storing the bytes that came off the wire.
		stored, storedErr := server.subtreeStore.Get(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, storedErr)
		require.Equal(t, canonical, stored)
	})

	t.Run("non-minimal coinbase prefix is rejected", func(t *testing.T) {
		// The coinbase's size is ADDED back rather than the consumed bytes being
		// ignored, so an over-long prefix on the one transaction the serialization
		// drops is still caught.
		mangled := withNonMinimalInputCount(t, withCoinbase, 0)

		server := newServer()
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()

		httpmock.RegisterResponder("GET", subtreeDataURL, httpmock.NewBytesResponder(200, mangled))

		err := server.fetchAndStoreSubtreeData(ctx, testBlock, subtreeHash, st, peerID, baseURL, false)
		require.Error(t, err)
		require.Contains(t, err.Error(), "non-canonical subtree data")
		require.True(t, errors.Is(err, errors.ErrProcessing))
		require.False(t, errors.Is(err, errors.ErrTxInvalid))

		exists, existsErr := server.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, existsErr)
		require.False(t, exists, "non-canonical payload must not be stored")
	})
}
