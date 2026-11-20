package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
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
// starting at off from its minimal 1-byte form to the 3-byte form. The txid is
// unchanged by this: it is computed over the canonical re-serialization.
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

// TestFetchAndStoreSubtreeData_NonMinimalVarInt pins issue 1421 on the catchup
// prewarm. go-bt (v2.7.0+, go-bt PR 174) rejects a non-minimal CompactSize at
// parse; before that it accepted the prefix and re-serialized it canonically, so
// the canonical form was stored with no trace of what the peer sent. Teranode no
// longer carries a detector of its own, so this test is what notices if a go-bt
// bump ever relaxes the check.
//
// The error class matters as much as the rejection. Non-minimality is a fault in
// how one peer delivered the bytes, not in the block: the txids and merkle root
// are identical either way, and the same subtree from an honest peer validates.
// A TxInvalid or BlockInvalid code here would let any peer we fetch from mark a
// valid block invalid, so the error must stay a peer-attributable processing
// error that sends us to the next peer.
func TestFetchAndStoreSubtreeData_NonMinimalVarInt(t *testing.T) {
	baseURL := "http://non-minimal-peer:8000"
	peerID := "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"
	ctx := context.Background()

	txs := transactions.CreateTestTransactionChainWithCount(t, 5)

	st, err := subtreepkg.NewIncompleteTreeByLeafCount(4)
	require.NoError(t, err)
	require.NoError(t, st.AddCoinbaseNode())
	require.NoError(t, st.AddNode(*txs[1].TxIDChainHash(), 1, 11))
	require.NoError(t, st.AddNode(*txs[2].TxIDChainHash(), 2, 12))
	require.NoError(t, st.AddNode(*txs[3].TxIDChainHash(), 3, 13))

	subtreeHash := st.RootHash()
	subtreeDataURL := fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String())
	testBlock := &model.Block{Height: 100}

	// The coinbase followed by the three subtree transactions, the shape a peer
	// serves when its subtree-data file was written from parsed transactions.
	var canonicalBuf bytes.Buffer
	for i := 0; i <= 3; i++ {
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

	fetch := func(t *testing.T, server *Server, body []byte) error {
		httpmock.ActivateNonDefault(util.HTTPClient())
		t.Cleanup(httpmock.DeactivateAndReset)

		httpmock.RegisterResponder("GET", subtreeDataURL, httpmock.NewBytesResponder(200, body))

		return server.fetchAndStoreSubtreeData(ctx, testBlock, subtreeHash, st, peerID, baseURL, false, nil)
	}

	t.Run("canonical payload is accepted and stored", func(t *testing.T) {
		server := newServer()
		require.NoError(t, fetch(t, server, canonical))

		exists, err := server.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.True(t, exists)
	})

	positions := []struct {
		name string
		tx   int
	}{
		{"coinbase", 0},
		{"subtree transaction", 2},
	}

	for _, p := range positions {
		t.Run("non-minimal prefix on the "+p.name+" is rejected as a delivery fault", func(t *testing.T) {
			off := bytes.Index(canonical, txs[p.tx].Bytes())
			require.GreaterOrEqual(t, off, 0, "fixture must contain the transaction verbatim")

			server := newServer()
			err := fetch(t, server, withNonMinimalInputCount(t, canonical, off))
			require.Error(t, err)
			require.ErrorIs(t, err, bt.ErrNonMinimalVarInt, "go-bt must be what rejects it")

			require.True(t, errors.Is(err, errors.ErrNetworkInvalidResponse), "a delivery fault must carry a peer code")
			require.False(t, isLocalProcessingFault(err), "must not read as a local fault")
			require.False(t, errors.Is(err, errors.ErrTxInvalid), "a delivery fault must not condemn the transaction")
			require.False(t, errors.Is(err, errors.ErrBlockInvalid), "a delivery fault must not condemn the block")
			require.False(t, errors.IsLocalError(err), "must not short-circuit the alternative-peer loop")

			exists, existsErr := server.subtreeStore.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
			require.NoError(t, existsErr)
			require.False(t, exists, "a canonicalised copy of the payload must not be stored")
		})
	}
}
