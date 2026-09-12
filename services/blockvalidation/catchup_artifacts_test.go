package blockvalidation

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/adaptivefetch"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestCatchupArtifacts_InvalidBodyCanBeRetried(t *testing.T) {
	testCatchupArtifactsRetry(t, true)
}

func TestCatchupArtifacts_TemporaryFailurePreservesProgress(t *testing.T) {
	testCatchupArtifactsRetry(t, false)
}

func testCatchupArtifactsRetry(t *testing.T, invalidBody bool) {
	t.Helper()
	ctx := context.Background()
	bv, block, _, blobs := newQuickBodyFixture(t, "async")
	payloads := make(map[string][]byte)
	for i, hash := range block.Subtrees {
		nodes, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, err)
		st, err := subtreepkg.NewSubtreeFromBytes(nodes)
		require.NoError(t, err)
		var hashes []byte
		for _, node := range st.Nodes {
			hashes = append(hashes, node.Hash[:]...)
		}
		payloads["/subtree/"+hash.String()] = hashes
		payloads["/subtree_data/"+hash.String()], err = blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		if i < 2 {
			require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeToCheck))
			require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeData))
		}
	}
	bv.settings.BlockValidation.SubtreeFetchConcurrency = 1
	if invalidBody {
		block.TransactionCount = 7 // same genuine header, dishonest serialized body count
	}
	badBody, err := block.Bytes()
	require.NoError(t, err)
	block.TransactionCount = 6
	goodBody, err := block.Bytes()
	require.NoError(t, err)
	var goodPeer atomic.Bool
	var fetched atomic.Int32
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/blocks/"+block.Hash().String() {
			if goodPeer.Load() {
				_, _ = w.Write(goodBody)
			} else {
				_, _ = w.Write(badBody)
			}
			return
		}
		if data, ok := payloads[r.URL.Path]; ok {
			fetched.Add(1)
			if !invalidBody && !goodPeer.Load() && r.URL.Path == "/subtree_data/"+block.Subtrees[1].String() {
				w.WriteHeader(http.StatusTooManyRequests)
				return
			}
			_, _ = w.Write(data)
			return
		}
		http.NotFound(w, r)
	}))
	defer peer.Close()
	af, err := adaptivefetch.New(adaptivefetch.DefaultConfig(), "artifact-test", prometheus.NewRegistry())
	require.NoError(t, err)
	s := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockchainClient: bv.blockchainClient,
		blockValidation: bv, subtreeStore: blobs, adaptiveFetch: af, headerChainCache: catchup.NewHeaderChainCache(ulogger.TestLogger{})}
	attempt := func() error {
		return s.fetchAndValidateBlocks(ctx, &CatchupContext{blockUpTo: block, baseURL: peer.URL,
			blockHeaders: []*model.BlockHeader{block.Header}, commonAncestorMeta: &model.BlockHeaderMeta{Height: 0},
			useQuickValidation: true, highestCheckpointHeight: 1})
	}
	err = attempt()
	require.Error(t, err)
	require.Equal(t, invalidBody, errors.Is(err, errors.ErrBlockInvalid), "%v", err)
	require.EqualValues(t, 4, fetched.Load())
	for i, hash := range block.Subtrees {
		for _, kind := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
			exists, err := blobs.Exists(ctx, hash[:], kind)
			require.NoError(t, err)
			expected := i == 2
			if !invalidBody {
				expected = i != 1 || kind == fileformat.FileTypeSubtreeToCheck
			}
			require.Equal(t, expected, exists, "invalid bodies discard their files; transient failures retain progress")
		}
	}
	goodPeer.Store(true)
	require.NoError(t, attempt())
	if invalidBody {
		require.EqualValues(t, 8, fetched.Load(), "the next peer must refetch files discarded with the invalid body")
	} else {
		require.EqualValues(t, 5, fetched.Load(), "only the missing transaction data should be fetched on retry")
	}
	stored, err := bv.blockchainClient.GetBlock(ctx, block.Hash())
	require.NoError(t, err)
	require.EqualValues(t, 6, stored.TransactionCount)
}

func TestCatchupArtifacts_PreservesSharedFiles(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	blobs := memory.New()
	artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
	for i := byte(0); i < 3; i++ {
		hash := chainhash.Hash{i}
		if i == 0 {
			require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeData, []byte("original")))
		}
		require.NoError(t, artifacts.Set(ctx, hash[:], fileformat.FileTypeSubtreeData, []byte("new"), options.WithAllowOverwrite(true)))
		if i == 1 {
			// Validation accepted a shared subtree before another block failed.
			require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtree, []byte("validated")))
		}
	}
	cancel()
	artifacts.cleanup(ulogger.TestLogger{})
	for i := byte(0); i < 3; i++ {
		hash := chainhash.Hash{i}
		data, err := blobs.Get(context.Background(), hash[:], fileformat.FileTypeSubtreeData)
		if i == 2 {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		if i == 0 {
			require.Equal(t, "original", string(data))
		} else {
			require.Equal(t, "new", string(data))
		}
	}
}
