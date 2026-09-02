package subtreevalidation

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/subtreevalidation/subtreevalidation_api"
	"github.com/bsv-blockchain/teranode/services/validator"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// subtreeToCheckExistsStore wraps a blob.Store and simulates the production race
// where a concurrent block-validation worker (the same block announced by two
// peers) has already written a subtree's content-addressed .subtreeToCheck file
// between this worker's existence check and its Set. The underlying store still
// gets the bytes (write-through, since the "other worker" wrote identical content),
// but the Set reports ErrBlobAlreadyExists — exactly what the File store returns
// when the destination already exists and AllowOverwrite is off.
type subtreeToCheckExistsStore struct {
	blob.Store

	targetKey   []byte
	setAttempts atomic.Int32
}

func (s *subtreeToCheckExistsStore) Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error {
	if fileType == fileformat.FileTypeSubtreeToCheck && bytes.Equal(key, s.targetKey) {
		s.setAttempts.Add(1)
		// The concurrent worker's identical write is already present. Write it
		// through so the store holds the content the production race would leave
		// behind, but surface any genuine write failure rather than masking it as
		// "already exists" — otherwise the test could pass for the wrong reason.
		if err := s.Store.Set(ctx, key, fileType, value, opts...); err != nil {
			return err
		}

		return errors.NewBlobAlreadyExistsError("[File][allowOverwrite] [%x] already exists in store", key)
	}

	return s.Store.Set(ctx, key, fileType, value, opts...)
}

// TestCheckBlockSubtrees_SubtreeToCheckAlreadyExists asserts that CheckBlockSubtrees
// does NOT fail a block when storing a fetched subtree's .subtreeToCheck file returns
// ErrBlobAlreadyExists. On the scaling cluster this happened when a block announced by
// two peers spawned two concurrent block-validation workers that raced storing the same
// content-addressed subtree file; the loser got BLOB_EXISTS and the whole (valid) block
// was rejected, then re-rejected on every retry. The subtree filename IS its content
// hash (verified at the root-hash check), so an already-present file holds identical
// bytes and "already exists" must be treated as success.
func TestCheckBlockSubtrees_SubtreeToCheckAlreadyExists(t *testing.T) {
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()

	server, cleanup := setupTestServer(t)
	defer cleanup()

	server.blockchainClient.(*blockchain.Mock).On("GetBlockHeaderIDs",
		mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil).Maybe()
	server.blockchainClient.(*blockchain.Mock).On("GetBestBlockHeader",
		mock.Anything).
		Return(testHeadersOverwrite(t)[0], &model.BlockHeaderMeta{}, nil).Maybe()
	server.blockchainClient.(*blockchain.Mock).On("IsFSMCurrentState",
		mock.Anything, blockchain.FSMStateRUNNING).
		Return(true, nil).Maybe()

	// Let the mock validator bless txs against the server's utxo store.
	server.validatorClient.(*validator.MockValidator).UtxoStore = server.utxoStore

	// Build a real subtree (coinbase placeholder + one tx) so its root matches its
	// served node bytes and the root-hash check passes, taking us to the store call.
	tx, err := createTestTransaction("tx1")
	require.NoError(t, err)

	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	require.NoError(t, subtree.AddNode(*tx.TxIDChainHash(), 0, 0))
	subtreeHash := subtree.RootHash()

	// Wrap the store so this subtree's .subtreeToCheck Set reports already-exists.
	wrapped := &subtreeToCheckExistsStore{Store: server.subtreeStore, targetKey: subtreeHash[:]}
	server.subtreeStore = wrapped

	baseURL := testPeerURL

	// /subtree/<root> returns the raw node hashes (coinbase placeholder + tx hash).
	nodeBytes := make([]byte, 0, 2*chainhash.HashSize)
	nodeBytes = append(nodeBytes, subtreepkg.CoinbasePlaceholderHashValue[:]...)
	nodeBytes = append(nodeBytes, tx.TxIDChainHash()[:]...)
	httpmock.RegisterResponder("GET",
		fmt.Sprintf("%s/subtree/%s", baseURL, subtreeHash.String()),
		httpmock.NewBytesResponder(http.StatusOK, nodeBytes))

	// /subtree_data/<root> returns the tx bytes (coinbase placeholder omitted).
	httpmock.RegisterResponder("GET",
		fmt.Sprintf("%s/subtree_data/%s", baseURL, subtreeHash.String()),
		httpmock.NewBytesResponder(http.StatusOK, tx.Bytes()))

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          0,
	}
	coinbaseTx := &bt.Tx{Version: 1}
	block, err := model.NewBlock(header, coinbaseTx, []*chainhash.Hash{subtreeHash}, 2, 500, 0, 0)
	require.NoError(t, err)
	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	request := &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: baseURL,
	}

	_, err = server.CheckBlockSubtrees(context.Background(), request)

	// Core contract: a pre-existing content-addressed .subtreeToCheck must never be
	// the reason the block fails. Pre-fix, CheckBlockSubtrees returns the
	// "failed to store subtreeToCheck" / BLOB_EXISTS error and the block is rejected;
	// post-fix the block validates cleanly, so assert success directly.
	require.NoError(t, err,
		"BLOB_EXISTS on a content-addressed subtreeToCheck must be treated as benign, not fatal")

	require.Positive(t, wrapped.setAttempts.Load(),
		"test must actually exercise the subtreeToCheck store path (else it proves nothing)")
}

func testHeadersOverwrite(t *testing.T) []*model.BlockHeader {
	t.Helper()
	return []*model.BlockHeader{{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()), //nolint:gosec
		Bits:           model.NBit{},
		Nonce:          0,
	}}
}
