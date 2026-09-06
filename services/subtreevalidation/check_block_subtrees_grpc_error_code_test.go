package subtreevalidation

import (
	"context"
	"net"
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
	utxometa "github.com/bsv-blockchain/teranode/stores/utxo/meta"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// startCheckBlockSubtreesGRPC stands the given *Server up behind a real in-process
// gRPC server and returns a generated API client pointed at it.
//
// The round trip is the whole point of these tests: an *Error returned from a
// handler only loses its code when it is actually marshalled into a grpc.Status
// and reconstructed on the other side. Calling the handler method directly (as
// the rest of this package's tests do) hands back the *Error untouched and cannot
// observe the loss.
func startCheckBlockSubtreesGRPC(t *testing.T, srv *Server) subtreevalidation_api.SubtreeValidationAPIClient {
	t.Helper()

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	grpcServer := grpc.NewServer()
	subtreevalidation_api.RegisterSubtreeValidationAPIServer(grpcServer, srv)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)

	t.Cleanup(func() {
		_ = conn.Close()
		grpcServer.Stop()
	})

	return subtreevalidation_api.NewSubtreeValidationAPIClient(conn)
}

// blockWithOneSubtree builds the smallest block CheckBlockSubtrees will do real
// work for: one subtree, at height 1 so it sits below every network's CSV
// activation height and the candidate-parent MTP lookup is skipped.
func blockWithOneSubtree(t *testing.T) (*model.Block, chainhash.Hash) {
	t.Helper()

	var subtreeHash chainhash.Hash
	copy(subtreeHash[:], []byte("grpc_error_code_subtree_hash_32b"))

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{},
		Nonce:          0,
	}

	block, err := model.NewBlock(header, &bt.Tx{Version: 1}, []*chainhash.Hash{&subtreeHash}, 2, 250, 1, 0)
	require.NoError(t, err)

	return block, subtreeHash
}

// callCheckBlockSubtrees performs the same round trip the production Client
// performs: call the generated stub, then reconstruct the error with
// errors.UnwrapGRPC.
func callCheckBlockSubtrees(t *testing.T, client subtreevalidation_api.SubtreeValidationAPIClient, block *model.Block) error {
	t.Helper()

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	_, err = client.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "http://test.example",
	})
	require.Error(t, err)

	return errors.UnwrapGRPC(err)
}

// TestCheckBlockSubtrees_GRPCPreservesExistsSweepStorageCode covers the existence-check
// return site: u.subtreeStore.Exists fails, the handler wraps that as a processing
// error, and the caller must still be able to see the ErrStorageError underneath.
// Without errors.WrapGRPC on the handler's return the whole chain collapses to a
// single ERR_ERROR carrying only flattened message text, and every errors.Is check
// in block validation misses.
func TestCheckBlockSubtrees_GRPCPreservesExistsSweepStorageCode(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	block, subtreeHash := blockWithOneSubtree(t)

	store := &MockBlobStore{}
	store.On("Exists", mock.Anything, subtreeHash[:], fileformat.FileTypeSubtree).
		Return(false, errors.NewStorageError("simulated local disk fault"))
	server.subtreeStore = store

	client := startCheckBlockSubtreesGRPC(t, server)

	err := callCheckBlockSubtrees(t, client, block)

	require.True(t, errors.Is(err, errors.ErrStorageError),
		"caller lost ERR_STORAGE_ERROR across the gRPC boundary: %v", err)
}

// TestCheckBlockSubtrees_GRPCPreservesBatchLoadStorageCode covers the batch-pipeline
// return site, which is a different `return nil, err` from the one above: the subtree
// is present locally but unreadable, so loadSubtreeBatch fails with a storage error
// and the pipeline hands it back to the handler.
func TestCheckBlockSubtrees_GRPCPreservesBatchLoadStorageCode(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	block, subtreeHash := blockWithOneSubtree(t)

	server.blockchainClient.(*blockchain.Mock).
		On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1}, nil).Maybe()

	store := &MockBlobStore{}
	// Top-level existence sweep: absent, so the subtree counts as missing and the
	// batch pipeline runs.
	store.On("Exists", mock.Anything, subtreeHash[:], fileformat.FileTypeSubtree).
		Return(false, nil)
	// findLocalSubtreeFile inside loadSubtreeBatch: present as "downloaded from a
	// peer, pending validation", so no HTTP fetch is attempted.
	store.On("Exists", mock.Anything, subtreeHash[:], fileformat.FileTypeSubtreeToCheck).
		Return(true, nil)
	// ...but reading it back fails — a torn or mis-keyed local file.
	store.On("GetIoReader", mock.Anything, subtreeHash[:], fileformat.FileTypeSubtreeToCheck).
		Return(nil, errors.NewStorageError("simulated torn local subtree file"))
	server.subtreeStore = store

	client := startCheckBlockSubtreesGRPC(t, server)

	err := callCheckBlockSubtrees(t, client, block)

	require.True(t, errors.Is(err, errors.ErrStorageError),
		"caller lost ERR_STORAGE_ERROR across the gRPC boundary: %v", err)
}

// TestCheckBlockSubtrees_GRPCHoldsBackBatchTxInvalidCode is the deliberate
// carve-out, kept as its own test so the decision is impossible to miss in review.
//
// A transaction in the block fails validation with ERR_TX_INVALID inside the batch
// pipeline. Restoring the boundary naively would hand that code to block
// validation, which treats it as proof of a consensus violation: permanent
// invalid=true persistence, a cascade over every descendant, and a malicious-peer
// report. The UTXO store's error vocabulary cannot support that yet — it labels
// its own decode and blob-read failures ErrTxInvalid — so the consensus codes are
// held back and the caller sees a processing error carrying the full detail as
// text.
//
// The block is still rejected either way: the call fails, so the block is not
// accepted. What is withheld is only the irreversible bookkeeping.
func TestCheckBlockSubtrees_GRPCHoldsBackBatchTxInvalidCode(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	tx1, err := createTestTransaction("tx1")
	require.NoError(t, err)

	tx2, err := createTestTransaction("tx2")
	require.NoError(t, err)

	subtree, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, subtree.AddNode(*tx1.TxIDChainHash(), 1, 1))
	require.NoError(t, subtree.AddNode(*tx2.TxIDChainHash(), 2, 2))

	subtreeData := subtreepkg.NewSubtreeData(subtree)
	require.NoError(t, subtreeData.AddTx(tx1, 0))
	require.NoError(t, subtreeData.AddTx(tx2, 1))

	subtreeBytes, err := subtree.Serialize()
	require.NoError(t, err)

	subtreeDataBytes, err := subtreeData.Serialize()
	require.NoError(t, err)

	ctx := context.Background()
	require.NoError(t, server.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeToCheck, subtreeBytes))
	require.NoError(t, server.subtreeStore.Set(ctx, subtree.RootHash()[:], fileformat.FileTypeSubtreeData, subtreeDataBytes))

	server.blockchainClient.(*blockchain.Mock).
		On("GetBlockHeaderIDs", mock.Anything, mock.Anything, mock.Anything).
		Return([]uint32{1, 2, 3}, nil).Maybe()

	// The validator's verdict on the transaction bytes: genuinely invalid.
	server.validatorClient.(*validator.MockValidator).ValidateFunc = func(_ context.Context, tx *bt.Tx) (*utxometa.Data, error) {
		return nil, errors.NewTxInvalidError("simulated consensus violation in tx %s", tx.TxID())
	}

	header := &model.BlockHeader{
		Version:        1,
		HashPrevBlock:  &chainhash.Hash{},
		HashMerkleRoot: &chainhash.Hash{},
		Timestamp:      uint32(time.Now().Unix()),
		Bits:           model.NBit{},
		Nonce:          0,
	}

	block, err := model.NewBlock(header, &bt.Tx{Version: 1}, []*chainhash.Hash{subtree.RootHash()}, 3, 500, 1, 0)
	require.NoError(t, err)

	client := startCheckBlockSubtreesGRPC(t, server)

	err = callCheckBlockSubtrees(t, client, block)

	require.False(t, errors.Is(err, errors.ErrTxInvalid),
		"ERR_TX_INVALID reached the caller: block validation would persist invalid=true and ban the peer on the strength of it: %v", err)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid),
		"ERR_BLOCK_INVALID reached the caller: %v", err)

	// The detail is still there for an operator reading the logs — only the code
	// that triggers irreversible action is withheld.
	require.Contains(t, err.Error(), "simulated consensus violation",
		"the underlying reason must survive as text: %v", err)
}

// TestCheckBlockSubtrees_GRPCPreservesInvalidBaseUrlCode covers the BaseUrl guard,
// the one return site whose gRPC transport status is not codes.Internal.
//
// It matters twice over. The code has to survive so a caller can tell "you sent me
// a bad request" from "my disk broke", and the message has to be legible: because
// the wrap moves the chain into status details, the outermost message is now the
// whole of what a caller sees on the default log path. A format verb left
// unformatted there would be the entire error text.
func TestCheckBlockSubtrees_GRPCPreservesInvalidBaseUrlCode(t *testing.T) {
	server, cleanup := setupTestServer(t)
	defer cleanup()

	// The package default is on; set it explicitly so the assertion cannot be
	// silently voided by another test in this package turning it off.
	util.SetSSRFProtection(true)
	t.Cleanup(func() { util.SetSSRFProtection(true) })

	block, _ := blockWithOneSubtree(t)

	blockBytes, err := block.Bytes()
	require.NoError(t, err)

	client := startCheckBlockSubtreesGRPC(t, server)

	// The canonical SSRF target: link-local, so ValidateURL rejects it.
	_, err = client.CheckBlockSubtrees(context.Background(), &subtreevalidation_api.CheckBlockSubtreesRequest{
		Block:   blockBytes,
		BaseUrl: "http://169.254.169.254/subtree",
	})
	require.Error(t, err)

	require.Equal(t, codes.InvalidArgument, status.Code(err),
		"a malformed request must not cross as an internal server fault: %v", err)

	unwrapped := errors.UnwrapGRPC(err)

	require.True(t, errors.Is(unwrapped, errors.ErrInvalidArgument),
		"caller lost ERR_INVALID_ARGUMENT across the gRPC boundary: %v", unwrapped)

	require.Contains(t, unwrapped.Error(), "169.254.169.254",
		"the rejected address must survive so an operator can act on it: %v", unwrapped)

	// Guards the format string on the return site. errors.New* consumes a trailing
	// error argument as the wrapped link before formatting, so a "%v" in the message
	// is never substituted and would reach the caller literally.
	require.NotContains(t, status.Convert(err).Message(), "%v",
		"unformatted verb in the status message a caller logs: %q", status.Convert(err).Message())
}

// TestWrapCheckBlockSubtreesErrNilPassthrough pins the helper's nil contract.
//
// Several call sites hand it the result of an operation that may have succeeded;
// if the helper ever manufactured an error out of nil, CheckBlockSubtrees would
// fail requests that worked.
func TestWrapCheckBlockSubtreesErrNilPassthrough(t *testing.T) {
	require.NoError(t, wrapCheckBlockSubtreesErr(nil))
	require.Nil(t, wrapCheckBlockSubtreesErr(nil))
}
