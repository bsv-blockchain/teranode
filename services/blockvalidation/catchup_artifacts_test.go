package blockvalidation

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/adaptivefetch"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func TestCatchupArtifacts_CachedCorruptionRecoversWithoutBlamingPeer(t *testing.T) {
	for _, kind := range []fileformat.FileType{fileformat.FileTypeSubtree, fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
		for _, damage := range []string{"truncated", "wrong root"} {
			if kind == fileformat.FileTypeSubtreeData && damage == "wrong root" {
				continue
			}
			t.Run(kind.String()+"/"+damage, func(t *testing.T) {
				ctx := context.Background()
				bv, block, txs, blobs := newQuickBodyFixture(t, "async")
				hash := block.Subtrees[2]
				nodes, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
				require.NoError(t, err)
				st, err := subtreepkg.NewSubtreeFromBytes(nodes)
				require.NoError(t, err)
				var hashes []byte
				for _, node := range st.Nodes {
					hashes = append(hashes, node.Hash[:]...)
				}
				data, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
				require.NoError(t, err)
				if kind == fileformat.FileTypeSubtree {
					require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeToCheck))
				}
				corrupt := []byte{1}
				if damage == "wrong root" {
					// Retain the requested store key but serialize a different cached root.
					corrupt = append([]byte(nil), nodes...)
					corrupt[0] ^= 1
				}
				require.NoError(t, blobs.Set(ctx, hash[:], kind, corrupt, options.WithAllowOverwrite(true)))
				body, err := block.Bytes()
				require.NoError(t, err)
				var fetched atomic.Int32
				peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/blocks/" + block.Hash().String():
						_, _ = w.Write(body)
					case "/subtree/" + hash.String():
						fetched.Add(1)
						_, _ = w.Write(hashes)
					case "/subtree_data/" + hash.String():
						fetched.Add(1)
						_, _ = w.Write(data)
					default:
						http.NotFound(w, r)
					}
				}))
				defer peer.Close()
				af, err := adaptivefetch.New(adaptivefetch.DefaultConfig(), "cache-repair", prometheus.NewRegistry())
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
				require.False(t, isUnvalidatablePeerError(err), "cached bytes are not evidence against this peer: %v", err)
				require.ErrorIs(t, err, errors.ErrServiceError, "local cache failure must bypass peer penalties")
				require.Zero(t, fetched.Load(), "first attempt should use only cached subtree files")
				for _, tx := range txs[1:] {
					_, err := bv.utxoStore.Get(ctx, tx.TxIDChainHash())
					require.ErrorIs(t, err, errors.ErrTxNotFound, "cache corruption must abort before UTXO mutation")
				}
				exists, err := blobs.Exists(ctx, hash[:], kind)
				require.NoError(t, err)
				require.False(t, exists, "the corrupt cached file must be evicted after workers join")
				for _, shared := range block.Subtrees[:2] {
					for _, sharedKind := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
						exists, err := blobs.Exists(ctx, shared[:], sharedKind)
						require.NoError(t, err)
						require.True(t, exists, "unrelated shared files must survive")
					}
				}
				require.NoError(t, attempt(), "a healthy peer must be able to repair the cache on retry")
				require.EqualValues(t, 1, fetched.Load(), "only the identified corrupt file needs refetching")
			})
		}
	}
}

func TestCatchupArtifacts_CurrentAttemptCorruptionIsInvalid(t *testing.T) {
	for _, kind := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
		t.Run(kind.String(), func(t *testing.T) {
			ctx := context.Background()
			bv, block, _, blobs := newQuickBodyFixture(t, "async")
			hash := block.Subtrees[2]
			require.NoError(t, blobs.Del(ctx, hash[:], kind))
			artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
			require.NoError(t, artifacts.Set(ctx, hash[:], kind, []byte{1}))
			ctx = context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
			cleanup, err := bv.authenticateQuickBlockBody(ctx, block)
			cleanup()
			require.ErrorIs(t, err, errors.ErrBlockInvalid, "newly downloaded corrupt bytes remain an invalid-body verdict")
			require.NotErrorIs(t, err, errors.ErrServiceError)
			artifacts.cleanup(ulogger.TestLogger{})
			exists, err := blobs.Exists(ctx, hash[:], kind)
			require.NoError(t, err)
			require.False(t, exists)
		})
	}
}

func TestCatchupArtifacts_CorruptDataPreservesPromotedNodes(t *testing.T) {
	ctx := context.Background()
	bv, block, _, blobs := newQuickBodyFixture(t, "async")
	hash := block.Subtrees[2]
	nodes, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtree, nodes))
	require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeToCheck))
	require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeData, []byte{1}, options.WithAllowOverwrite(true)))
	artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
	ctx = context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
	cleanup, err := bv.authenticateQuickBlockBody(ctx, block)
	cleanup()
	require.ErrorIs(t, err, errors.ErrServiceError)
	require.False(t, isUnvalidatablePeerError(err))
	artifacts.repair(ulogger.TestLogger{})
	retained, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.Equal(t, nodes, retained, "a corrupt data file must not invalidate healthy promoted nodes")
	exists, err := blobs.Exists(ctx, hash[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.False(t, exists)
}

type cleanupGateStore struct {
	blob.Store
	entered chan struct{}
	release chan struct{}
	active  atomic.Int32
	peak    atomic.Int32
}

func (s *cleanupGateStore) Exists(ctx context.Context, key []byte, kind fileformat.FileType, opts ...options.FileOption) (bool, error) {
	n := s.active.Add(1)
	defer s.active.Add(-1)
	for old := s.peak.Load(); n > old && !s.peak.CompareAndSwap(old, n); old = s.peak.Load() {
	}
	s.entered <- struct{}{}
	select {
	case <-s.release:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	return s.Store.Exists(ctx, key, kind, opts...)
}

func TestCatchupArtifacts_CleanupIsBoundedAndConcurrent(t *testing.T) {
	store := &cleanupGateStore{Store: memory.New(), entered: make(chan struct{}, 32), release: make(chan struct{})}
	artifacts := &catchupArtifacts{Store: store, files: make(map[catchupArtifactKey]*catchupArtifact)}
	for i := byte(0); i < 20; i++ {
		hash := chainhash.Hash{i}
		require.NoError(t, artifacts.Set(context.Background(), hash[:], fileformat.FileTypeSubtreeData, []byte{1}))
	}
	done := make(chan struct{})
	go func() { artifacts.cleanup(ulogger.TestLogger{}); close(done) }()
	for i := 0; i < 2; i++ {
		select {
		case <-store.entered:
		case <-time.After(time.Second):
			t.Error("cleanup did not start independent files concurrently")
		}
	}
	close(store.release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup did not join workers")
	}
	require.LessOrEqual(t, store.peak.Load(), int32(8), "default cleanup concurrency must be bounded")
	for file := range artifacts.files {
		exists, err := store.Store.Exists(context.Background(), file.hash[:], file.kind)
		require.NoError(t, err)
		require.False(t, exists)
	}
}

type cleanupTimeoutStore struct {
	blob.Store
	allBlocked bool
	started    atomic.Int32
}

func (s *cleanupTimeoutStore) Exists(ctx context.Context, key []byte, kind fileformat.FileType, opts ...options.FileOption) (bool, error) {
	s.started.Add(1)
	if s.allBlocked || key[0] == 0 {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return s.Store.Exists(ctx, key, kind, opts...)
}

type cleanupWarningLogger struct {
	ulogger.TestLogger
	warnings atomic.Int32
}

func (l *cleanupWarningLogger) Warnf(string, ...interface{}) { l.warnings.Add(1) }

func TestCatchupArtifacts_CleanupTimeouts(t *testing.T) {
	for _, allBlocked := range []bool{false, true} {
		t.Run(fmt.Sprintf("allBlocked=%v", allBlocked), func(t *testing.T) {
			store := &cleanupTimeoutStore{Store: memory.New(), allBlocked: allBlocked}
			artifacts := &catchupArtifacts{Store: store, files: make(map[catchupArtifactKey]*catchupArtifact),
				cleanupConcurrency: 2, cleanupFileTimeout: 20 * time.Millisecond, cleanupBudget: 100 * time.Millisecond}
			if !allBlocked {
				artifacts.cleanupBudget = 5 * time.Second
			}
			for i := byte(0); i < 100; i++ {
				hash := chainhash.Hash{i}
				require.NoError(t, artifacts.Set(context.Background(), hash[:], fileformat.FileTypeSubtreeData, []byte{1}))
			}
			logger := &cleanupWarningLogger{}
			done := make(chan struct{})
			go func() { artifacts.cleanup(logger); close(done) }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("cleanup exceeded its overall budget")
			}
			require.EqualValues(t, 1, logger.warnings.Load(), "failures must be summarized without per-file log flooding")
			if allBlocked {
				require.Less(t, store.started.Load(), int32(100), "stop scheduling when the overall budget expires")
			} else {
				for i := byte(0); i < 100; i++ {
					hash := chainhash.Hash{i}
					exists, err := store.Store.Exists(context.Background(), hash[:], fileformat.FileTypeSubtreeData)
					require.NoError(t, err)
					require.Equal(t, i == 0, exists, "one timeout must not prevent unrelated cleanup")
				}
			}
		})
	}
}

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
