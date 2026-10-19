package blockvalidation

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/adaptivefetch"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/test/utils/transactions"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

func testNodes(hashes ...chainhash.Hash) []subtreepkg.Node {
	nodes := make([]subtreepkg.Node, len(hashes))
	for i, h := range hashes {
		nodes[i] = subtreepkg.Node{Hash: h}
	}
	return nodes
}

func testNodesRoot(t *testing.T, hashes ...chainhash.Hash) *chainhash.Hash {
	t.Helper()
	merkles, err := subtreepkg.BuildMerkleTreeStoreFromBytes(testNodes(hashes...))
	require.NoError(t, err)
	root := (*merkles)[len(*merkles)-1]
	return &root
}

func testHashes(n int) []chainhash.Hash {
	hashes := make([]chainhash.Hash, n)
	for i := range hashes {
		hashes[i] = chainhash.DoubleHashH([]byte(fmt.Sprintf("leaf-%d", i)))
	}
	return hashes
}

func testNodeBytes(hashes ...chainhash.Hash) []byte {
	var b []byte
	for _, h := range hashes {
		b = append(b, h[:]...)
	}
	return b
}

func TestVerifySubtreeNodes(t *testing.T) {
	h := testHashes(8)
	var zero chainhash.Hash

	t.Run("canonical lists pass", func(t *testing.T) {
		for n := 1; n <= 8; n++ {
			require.NoError(t, verifySubtreeNodes(testNodes(h[:n]...), testNodesRoot(t, h[:n]...)), "length %d", n)
		}
		placeholder := append([]chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue}, h[:2]...)
		require.NoError(t, verifySubtreeNodes(testNodes(placeholder...), testNodesRoot(t, placeholder...)))
	})

	// Only a duplicated level tail reproduces a shorter list's root. Other
	// equal siblings are genuinely duplicate transactions, which the block
	// checks reject as invalid.
	t.Run("non-tail duplicates are left to the block checks", func(t *testing.T) {
		for _, list := range [][]chainhash.Hash{{h[0], h[0]}, {h[0], h[0], h[1], h[2]}, {h[0], h[1], h[1], h[2]}} {
			require.NoError(t, verifySubtreeNodes(testNodes(list...), testNodesRoot(t, list...)), "%v", list)
		}
	})

	t.Run("wrong root", func(t *testing.T) {
		require.Error(t, verifySubtreeNodes(testNodes(h[:4]...), testNodesRoot(t, h[1:5]...)))
	})

	// Each mutation reproduces the root of the shorter canonical list.
	mutations := map[string]struct{ canonical, mutated []chainhash.Hash }{
		"duplicated last leaf":  {h[:3], []chainhash.Hash{h[0], h[1], h[2], h[2]}},
		"zero padding":          {h[:3], []chainhash.Hash{h[0], h[1], h[2], zero}},
		"duplicated inner pair": {h[:6], []chainhash.Hash{h[0], h[1], h[2], h[3], h[4], h[5], h[4], h[5]}},
		"duplicated odd tail":   {h[:5], []chainhash.Hash{h[0], h[1], h[2], h[3], h[4], h[4]}},
	}
	for name, m := range mutations {
		t.Run(name, func(t *testing.T) {
			root := testNodesRoot(t, m.canonical...)
			require.Equal(t, root, testNodesRoot(t, m.mutated...), "premise: mutation keeps the root")
			require.Error(t, verifySubtreeNodes(testNodes(m.mutated...), root))
		})
	}
}

func TestFetchAndStoreSubtreeVerifiesNodes(t *testing.T) {
	h := testHashes(4)
	peerID := "12D3KooWL1NF6fdTJ9cucEuwvuX8V8KtpJZZnUE4umdLBuK15eUZ"

	fetched := map[string]struct {
		hash  *chainhash.Hash
		nodes []byte
	}{
		"wrong root":    {testNodesRoot(t, h[1:]...), testNodeBytes(h[:3]...)},
		"non-canonical": {testNodesRoot(t, h[:3]...), testNodeBytes(h[0], h[1], h[2], h[2])},
	}
	for name, f := range fetched {
		t.Run("fetched/"+name, func(t *testing.T) {
			suite := NewCatchupTestSuite(t)
			defer suite.Cleanup()
			httpmock.ActivateNonDefault(util.HTTPClient())
			defer httpmock.DeactivateAndReset()
			httpmock.RegisterResponder("GET", "http://test-peer/subtree/"+f.hash.String(), httpmock.NewBytesResponder(200, f.nodes))

			result, _, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, &model.Block{Height: 100}, f.hash, peerID, "http://test-peer")
			require.Error(t, err)
			require.Nil(t, result)
			require.False(t, errors.IsLocalError(err), "a bad peer response must allow alternative peers: %v", err)
			exists, err := suite.Server.subtreeStore.Exists(suite.Ctx, f.hash[:], fileformat.FileTypeSubtreeToCheck)
			require.NoError(t, err)
			require.False(t, exists, "unverified nodes must never be cached")
		})
	}

	t.Run("validated/wrong root", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		st, err := subtreepkg.NewTreeByLeafCount(4)
		require.NoError(t, err)
		for _, hash := range h {
			require.NoError(t, st.AddNode(hash, 0, 0))
		}
		raw, err := st.Serialize()
		require.NoError(t, err)
		hash := testNodesRoot(t, h[1:]...)
		require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, hash[:], fileformat.FileTypeSubtree, raw))

		artifacts := &catchupArtifacts{Store: suite.Server.subtreeStore, files: make(map[catchupArtifactKey]*catchupArtifact)}
		ctx := context.WithValue(suite.Ctx, catchupArtifactsKey{}, artifacts)
		result, _, err := suite.Server.fetchAndStoreSubtree(ctx, &model.Block{Height: 100}, hash, peerID, "http://test-peer")
		require.Nil(t, result)
		require.ErrorIs(t, err, errors.ErrServiceError, "cached bytes are not evidence against the peer")
		artifacts.repair(ulogger.TestLogger{})
		exists, err := suite.Server.subtreeStore.Exists(suite.Ctx, hash[:], fileformat.FileTypeSubtree)
		require.NoError(t, err)
		require.False(t, exists, "the mismatched cached file must be evicted")
	})

	t.Run("pending list is not reused", func(t *testing.T) {
		suite := NewCatchupTestSuite(t)
		defer suite.Cleanup()
		httpmock.ActivateNonDefault(util.HTTPClient())
		defer httpmock.DeactivateAndReset()
		hash := testNodesRoot(t, h...)
		collapsed, err := subtreepkg.NewTreeByLeafCount(1)
		require.NoError(t, err)
		require.NoError(t, collapsed.AddNode(*hash, 0, 0))
		raw, err := collapsed.Serialize()
		require.NoError(t, err)
		require.NoError(t, suite.Server.subtreeStore.Set(suite.Ctx, hash[:], fileformat.FileTypeSubtreeToCheck, raw))
		httpmock.RegisterResponder("GET", "http://test-peer/subtree/"+hash.String(), httpmock.NewBytesResponder(200, testNodeBytes(h...)))

		result, validated, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, &model.Block{Height: 100}, hash, peerID, "http://test-peer")
		require.NoError(t, err)
		require.False(t, validated)
		require.Equal(t, 4, result.Length(), "a pending node list may be a collapsed level; the peer's own list is used")
	})
}

// A cached non-canonical list matches its root, so the later body checks would
// otherwise convict whichever peer is current.
func TestQuickBodyPreflightNonCanonicalNodes(t *testing.T) {
	var zero chainhash.Hash
	for _, mutation := range []string{"duplicated last leaf", "zero padding"} {
		for _, cached := range []bool{true, false} {
			t.Run(fmt.Sprintf("%s/cached=%v", mutation, cached), func(t *testing.T) {
				ctx := context.Background()
				bv, block, txs, blobs := newQuickBodyFixture(t, "async")
				canonical := make([]chainhash.Hash, 0, 4)
				canonical = append(canonical, *txs[4].TxIDChainHash(), *txs[5].TxIDChainHash(), *txs[0].TxIDChainHash())
				last := canonical[2]
				if mutation == "zero padding" {
					last = zero
				}
				hash := testNodesRoot(t, canonical...)
				require.Equal(t, hash, testNodesRoot(t, append(canonical, last)...))
				st, err := subtreepkg.NewTreeByLeafCount(4)
				require.NoError(t, err)
				for _, leaf := range append(canonical, last) {
					require.NoError(t, st.AddNode(leaf, 0, 0))
				}
				raw, err := st.Serialize()
				require.NoError(t, err)
				block.Subtrees[2] = hash

				artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
				if cached {
					require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, raw))
				} else {
					require.NoError(t, artifacts.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, raw))
				}
				ctx = context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
				cleanup, err := bv.authenticateQuickBlockBody(ctx, block)
				cleanup()
				require.Error(t, err)
				if cached {
					require.ErrorIs(t, err, errors.ErrServiceError)
					require.False(t, isUnvalidatablePeerError(err), "cached bytes must not convict the current peer: %v", err)
					artifacts.repair(ulogger.TestLogger{})
					exists, err := blobs.Exists(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
					require.NoError(t, err)
					require.False(t, exists, "the non-canonical cached file must be evicted")
				} else {
					require.ErrorIs(t, err, errors.ErrBlockInvalid, "bytes downloaded in this attempt remain an invalid-body verdict")
					require.NotErrorIs(t, err, errors.ErrServiceError)
				}
			})
		}
	}
}

// A well-formed but wrong node list must not outlive the attempt that saw it,
// whether it came from this attempt's peer or an earlier cache.
func TestCatchupArtifacts_WrongNodeListDoesNotWedgeCatchup(t *testing.T) {
	for _, source := range []string{"fetched", "cached"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			bv, block, txs, blobs := newQuickBodyFixture(t, "async")
			hash := block.Subtrees[2]
			good := testNodeBytes(*txs[4].TxIDChainHash(), *txs[5].TxIDChainHash())
			wrong := testNodeBytes(*txs[5].TxIDChainHash(), *txs[4].TxIDChainHash())
			data, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
			require.NoError(t, err)
			require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeData))
			require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeToCheck))
			if source == "cached" {
				st, err := subtreepkg.NewTreeByLeafCount(2)
				require.NoError(t, err)
				require.NoError(t, st.AddNode(*txs[5].TxIDChainHash(), 0, 0))
				require.NoError(t, st.AddNode(*txs[4].TxIDChainHash(), 0, 0))
				raw, err := st.Serialize()
				require.NoError(t, err)
				require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, raw, options.WithAllowOverwrite(true)))
			}
			body, err := block.Bytes()
			require.NoError(t, err)
			var honest atomic.Bool
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/blocks/" + block.Hash().String():
					_, _ = w.Write(body)
				case "/subtree/" + hash.String():
					if honest.Load() {
						_, _ = w.Write(good)
					} else {
						_, _ = w.Write(wrong)
					}
				case "/subtree_data/" + hash.String():
					if !honest.Load() && source == "fetched" {
						w.WriteHeader(http.StatusTooManyRequests)
						return
					}
					_, _ = w.Write(data)
				default:
					http.NotFound(w, r)
				}
			}))
			defer peer.Close()
			af, err := adaptivefetch.New(adaptivefetch.DefaultConfig(), "node-list", prometheus.NewRegistry())
			require.NoError(t, err)
			s := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockchainClient: bv.blockchainClient,
				blockValidation: bv, subtreeStore: blobs, adaptiveFetch: af, headerChainCache: catchup.NewHeaderChainCache(ulogger.TestLogger{})}
			attempt := func() error {
				return s.fetchAndValidateBlocks(ctx, &CatchupContext{blockUpTo: block, baseURL: peer.URL,
					blockHeaders: []*model.BlockHeader{block.Header}, commonAncestorMeta: &model.BlockHeaderMeta{Height: 0},
					useQuickValidation: true, highestCheckpointHeight: 1})
			}
			require.Error(t, attempt())
			exists, err := blobs.Exists(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
			require.NoError(t, err)
			require.Equal(t, source == "cached", exists, "a peer's unverified node list is never stored; a stale cached one is ignored")
			honest.Store(true)
			require.NoError(t, attempt(), "an honest peer must be able to complete catchup")
		})
	}
}

type catchupErrorRecorder struct {
	P2PClientI
	peers []string
}

func (r *catchupErrorRecorder) RecordBytesDownloaded(context.Context, string, uint64) error {
	return nil
}

func (r *catchupErrorRecorder) UpdateCatchupError(_ context.Context, peerID string, _ string) error {
	r.peers = append(r.peers, peerID)
	return nil
}

func TestFetchAndStoreSubtreeChargesPeerForMismatchedNodes(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	recorder := &catchupErrorRecorder{}
	suite.Server.p2pClient = recorder
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	h := testHashes(4)
	hash := testNodesRoot(t, h[1:]...)
	httpmock.RegisterResponder("GET", "http://test-peer/subtree/"+hash.String(), httpmock.NewBytesResponder(200, testNodeBytes(h[:3]...)))

	_, _, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, &model.Block{Height: 100}, hash, "bad-peer", "http://test-peer")
	require.Error(t, err)
	require.Equal(t, []string{"bad-peer"}, recorder.peers)
}

// Any tree level is a valid node list for the root, so [R] passes the node
// check. Only the transactions can expose it; recovery must not convict
// anyone and must not depend on the file expiring.
func TestCatchupArtifacts_CollapsedNodeListRecovers(t *testing.T) {
	for _, source := range []string{"fetched, data throttled", "fetched with data", "cached"} {
		t.Run(source, func(t *testing.T) {
			ctx := context.Background()
			bv, block, txs, blobs := newQuickBodyFixture(t, "async")
			hash := block.Subtrees[2]
			good := testNodeBytes(*txs[4].TxIDChainHash(), *txs[5].TxIDChainHash())
			collapsed := testNodeBytes(*hash)
			require.NoError(t, verifySubtreeNodes(testNodes(*hash), hash), "premise: the collapsed list passes the node check")
			data, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
			require.NoError(t, err)
			require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeData))
			require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeToCheck))
			if source == "cached" {
				st, err := subtreepkg.NewTreeByLeafCount(1)
				require.NoError(t, err)
				require.NoError(t, st.AddNode(*hash, 0, 0))
				raw, err := st.Serialize()
				require.NoError(t, err)
				require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, raw))
			}
			body, err := block.Bytes()
			require.NoError(t, err)
			var honest atomic.Bool
			peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/blocks/" + block.Hash().String():
					_, _ = w.Write(body)
				case "/subtree/" + hash.String():
					if honest.Load() {
						_, _ = w.Write(good)
					} else {
						_, _ = w.Write(collapsed)
					}
				case "/subtree_data/" + hash.String():
					if !honest.Load() && source == "fetched, data throttled" {
						w.WriteHeader(http.StatusTooManyRequests)
						return
					}
					_, _ = w.Write(data)
				default:
					http.NotFound(w, r)
				}
			}))
			defer peer.Close()
			af, err := adaptivefetch.New(adaptivefetch.DefaultConfig(), "collapsed", prometheus.NewRegistry())
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
			require.False(t, isUnvalidatablePeerError(err), "a collapsed node list must not convict anyone: %v", err)
			honest.Store(true)
			// A collapsed list retained after a transient failure is found out,
			// and evicted, by the next attempt that sees the transactions.
			for i := 0; i < 2; i++ {
				if err = attempt(); err == nil {
					break
				}
				require.False(t, isUnvalidatablePeerError(err), "honest attempt %d must not convict: %v", i+1, err)
			}
			require.NoError(t, err, "an honest peer must complete catchup within two attempts")
		})
	}
}

// A collapsed node list changes the subtree's length and first node, so the
// body-level checks fail. Unless this attempt bound every node list to its
// transactions they must not convict: the list may be cached, downloaded
// without its data, or replaced after it was bound.
func TestQuickBodyPreflightCollapsedNodes(t *testing.T) {
	for _, idx := range []int{0, 1, 2} {
		for _, state := range []string{"cached", "fresh", "replaced after bind"} {
			t.Run(fmt.Sprintf("subtree%d/%s", idx, state), func(t *testing.T) {
				ctx := context.Background()
				bv, block, _, blobs := newQuickBodyFixture(t, "async")
				s := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, subtreeStore: blobs}
				hash := block.Subtrees[idx]
				st, err := subtreepkg.NewTreeByLeafCount(1)
				require.NoError(t, err)
				require.NoError(t, st.AddNode(*hash, 0, 0))
				raw, err := st.Serialize()
				require.NoError(t, err)
				artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
				ctx = context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
				switch state {
				case "cached":
					require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, raw, options.WithAllowOverwrite(true)))
				case "fresh":
					require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeToCheck))
					require.NoError(t, artifacts.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, raw))
				case "replaced after bind":
					for _, h := range block.Subtrees {
						honest, err := blobs.Get(ctx, h[:], fileformat.FileTypeSubtreeToCheck)
						require.NoError(t, err)
						nodes, err := subtreepkg.NewSubtreeFromBytes(honest)
						require.NoError(t, err)
						require.NoError(t, s.storeBoundSubtreeFile(ctx, h, fileformat.FileTypeSubtreeToCheck, honest, 100, nodes.Length(), true))
					}
					require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, raw, options.WithAllowOverwrite(true)))
				}
				cleanup, err := bv.authenticateQuickBlockBody(ctx, block)
				cleanup()
				require.Error(t, err)
				require.ErrorIs(t, err, errors.ErrServiceError)
				require.False(t, isUnvalidatablePeerError(err), "an unbound node list must not convict the current peer: %v", err)
				artifacts.repair(ulogger.TestLogger{})
				exists, err := blobs.Exists(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
				require.NoError(t, err)
				require.False(t, exists, "the unbound collapsed file must be evicted")
				for _, h := range block.Subtrees {
					exists, err := blobs.Exists(ctx, h[:], fileformat.FileTypeSubtreeData)
					require.NoError(t, err)
					require.True(t, exists, "data this attempt did not write unbound authenticates itself and is kept")
				}
			})
		}
	}
}

// With every node list bound in this attempt, a body-level failure is the
// peer's verdict.
func TestQuickBodyPreflightBoundBodyConvicts(t *testing.T) {
	ctx := context.Background()
	bv, block, _, blobs := newQuickBodyFixture(t, "async")
	s := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, subtreeStore: blobs}
	artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
	ctx = context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
	for _, h := range block.Subtrees {
		honest, err := blobs.Get(ctx, h[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, err)
		nodes, err := subtreepkg.NewSubtreeFromBytes(honest)
		require.NoError(t, err)
		require.NoError(t, s.storeBoundSubtreeFile(ctx, h, fileformat.FileTypeSubtreeToCheck, honest, 100, nodes.Length(), true))
	}
	block.TransactionCount = 7
	cleanup, err := bv.authenticateQuickBlockBody(ctx, block)
	cleanup()
	require.ErrorIs(t, err, errors.ErrBlockInvalid)
	require.NotErrorIs(t, err, errors.ErrServiceError)
}

// Transaction data already on disk must still be checked against a node list
// fetched in this attempt: otherwise a collapsed list from any peer in the
// fetch pool reaches the body checks as a fresh verdict and convicts the
// catchup peer.
func TestCatchupArtifacts_FreshCollapsedNodesWithStoredData(t *testing.T) {
	ctx := context.Background()
	bv, block, _, blobs := newQuickBodyFixture(t, "async")
	nodes := make(map[string][]byte)
	for _, hash := range block.Subtrees {
		raw, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
		require.NoError(t, err)
		st, err := subtreepkg.NewSubtreeFromBytes(raw)
		require.NoError(t, err)
		var b []byte
		for _, node := range st.Nodes {
			b = append(b, node.Hash[:]...)
		}
		nodes["/subtree/"+hash.String()] = b
		nodes["/subtree_data/"+hash.String()], err = blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
		require.NoError(t, err)
		require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeToCheck))
	}
	collapsedPath := "/subtree/" + block.Subtrees[1].String()
	body, err := block.Bytes()
	require.NoError(t, err)
	var honest atomic.Bool
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/blocks/"+block.Hash().String() {
			_, _ = w.Write(body)
			return
		}
		if r.URL.Path == collapsedPath && !honest.Load() {
			_, _ = w.Write(block.Subtrees[1][:])
			return
		}
		if b, ok := nodes[r.URL.Path]; ok {
			_, _ = w.Write(b)
			return
		}
		http.NotFound(w, r)
	}))
	defer peer.Close()
	af, err := adaptivefetch.New(adaptivefetch.DefaultConfig(), "stored-data", prometheus.NewRegistry())
	require.NoError(t, err)
	s := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockchainClient: bv.blockchainClient,
		blockValidation: bv, subtreeStore: blobs, adaptiveFetch: af, headerChainCache: catchup.NewHeaderChainCache(ulogger.TestLogger{})}
	attempt := func() error {
		return s.fetchAndValidateBlocks(ctx, &CatchupContext{blockUpTo: block, baseURL: peer.URL,
			blockHeaders: []*model.BlockHeader{block.Header}, commonAncestorMeta: &model.BlockHeaderMeta{Height: 0},
			useQuickValidation: true, highestCheckpointHeight: 1})
	}
	// The stored transactions authenticate themselves and yield the node
	// list, so the peer's collapsed list is never needed.
	require.NoError(t, attempt())
}

// A bound node list replaces a collapsed one, while identical bytes stay shared
// (not owned by the attempt, so an invalid body does not delete them).
func TestStoreBoundSubtreeFile(t *testing.T) {
	ctx := context.Background()
	blobs := memory.New()
	s := &Server{logger: ulogger.TestLogger{}, subtreeStore: blobs}
	artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
	ctx = context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
	h := testHashes(2)
	hash := testNodesRoot(t, h...)
	honest, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	for _, leaf := range h {
		require.NoError(t, honest.AddNode(leaf, 0, 0))
	}
	honestBytes, err := honest.Serialize()
	require.NoError(t, err)

	require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, honestBytes))
	require.NoError(t, s.storeBoundSubtreeFile(ctx, hash, fileformat.FileTypeSubtreeToCheck, honestBytes, 100, 2, true))
	artifacts.cleanup(ulogger.TestLogger{})
	exists, err := blobs.Exists(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.True(t, exists, "an identical shared file is not owned by the attempt")

	require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, []byte{1}, options.WithAllowOverwrite(true)))
	require.NoError(t, s.storeBoundSubtreeFile(ctx, hash, fileformat.FileTypeSubtreeToCheck, honestBytes, 100, 2, true))
	stored, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.Equal(t, honestBytes, stored, "a bound list replaces a wrong one")
	tracked, bound, leaves := boundCatchupArtifact(ctx, *hash)
	require.True(t, tracked)
	require.True(t, bound)
	require.Equal(t, 2, leaves)
}

type alternativePeers struct {
	catchupErrorRecorder
	alts []*p2p.PeerInfo
}

func (a *alternativePeers) GetPeersForCatchup(context.Context) ([]*p2p.PeerInfo, error) {
	return a.alts, nil
}

// A peer assigned a subtree must not be able to block it: whatever it serves,
// an honest alternative completes the subtree in the same attempt and the bad
// peer is charged.
func TestFetchAndStoreSubtreeAndSubtreeData_BadPeerFallsBackToAlternative(t *testing.T) {
	for _, bad := range []string{"collapsed nodes", "mismatching data"} {
		for _, cachedNodes := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/cachedNodes=%v", bad, cachedNodes), func(t *testing.T) {
				ctx := context.Background()
				bv, block, txs, blobs := newQuickBodyFixture(t, "async")
				hash := block.Subtrees[2]
				good := testNodeBytes(*txs[4].TxIDChainHash(), *txs[5].TxIDChainHash())
				data, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
				require.NoError(t, err)
				wrong, err := blobs.Get(ctx, block.Subtrees[1][:], fileformat.FileTypeSubtreeData)
				require.NoError(t, err)
				require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeData))
				if !cachedNodes {
					require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeToCheck))
				}
				serve := func(nodes, txData []byte) *httptest.Server {
					return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						switch r.URL.Path {
						case "/subtree/" + hash.String():
							_, _ = w.Write(nodes)
						case "/subtree_data/" + hash.String():
							_, _ = w.Write(txData)
						default:
							http.NotFound(w, r)
						}
					}))
				}
				badNodes, badData := good, wrong
				if bad == "collapsed nodes" {
					badNodes, badData = hash[:], data
				}
				mal := serve(badNodes, badData)
				defer mal.Close()
				hon := serve(good, data)
				defer hon.Close()
				pc := &alternativePeers{alts: []*p2p.PeerInfo{{ID: peer.ID("honest"), DataHubURL: hon.URL, Height: 10, ReputationScore: 50}}}
				s := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockchainClient: bv.blockchainClient,
					blockValidation: bv, subtreeStore: blobs, p2pClient: pc}
				artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
				actx := context.WithValue(ctx, catchupArtifactsKey{}, artifacts)

				served, err := s.fetchAndStoreSubtreeAndSubtreeData(actx, block, hash, "malicious", mal.URL)
				require.NoError(t, err)
				require.Equal(t, peer.ID("honest").String(), served)
				require.Equal(t, []string{"malicious"}, pc.peers, "only the bad peer is charged")
				tracked, bound, leaves := boundCatchupArtifact(actx, *hash)
				require.True(t, tracked)
				require.True(t, bound)
				require.Equal(t, 2, leaves)
				artifacts.repair(ulogger.TestLogger{})
				for _, kind := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
					exists, err := blobs.Exists(ctx, hash[:], kind)
					require.NoError(t, err)
					require.True(t, exists, "the honest files survive repair")
				}
			})
		}
	}
}

// A 64-byte transaction can serialize as the concatenation of two merkle
// children, so its txid is an internal node: its data cannot bind a node list.
func sixtyFourByteTx(t *testing.T) *bt.Tx {
	t.Helper()
	tx := bt.NewTx()
	tx.Inputs = append(tx.Inputs, &bt.Input{PreviousTxOutIndex: 0, SequenceNumber: 0xffffffff,
		UnlockingScript: bscript.NewFromBytes([]byte{1, 2, 3, 4})})
	require.NoError(t, tx.Inputs[0].PreviousTxIDAdd(&chainhash.Hash{7}))
	tx.Outputs = append(tx.Outputs, &bt.Output{Satoshis: 0, LockingScript: bscript.NewFromBytes(nil)})
	require.Equal(t, 64, tx.Size())
	return tx
}

func TestSubtreeFromData(t *testing.T) {
	txs := transactions.CreateTestTransactionChainWithCount(t, 4)
	coinbase, tx1, tx2 := txs[0], txs[1], txs[2]
	require.True(t, coinbase.IsCoinbase())
	join := func(list ...*bt.Tx) []byte {
		var b []byte
		for _, tx := range list {
			b = append(b, tx.Bytes()...)
		}
		return b
	}
	cases := map[string]struct {
		data         []byte
		coinbaseSlot bool
		want         []chainhash.Hash
	}{
		"plain":                 {join(tx1, tx2), false, []chainhash.Hash{*tx1.TxIDChainHash(), *tx2.TxIDChainHash()}},
		"slot without coinbase": {join(tx1), true, []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue, *tx1.TxIDChainHash()}},
		"slot with coinbase":    {join(coinbase, tx1), true, []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue, *tx1.TxIDChainHash()}},
		"slot only":             {nil, true, []chainhash.Hash{subtreepkg.CoinbasePlaceholderHashValue}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			st, ambiguous, err := subtreeFromData(bytes.NewReader(c.data), c.coinbaseSlot)
			require.NoError(t, err)
			require.False(t, ambiguous)
			var got []chainhash.Hash
			for _, node := range st.Nodes {
				got = append(got, node.Hash)
			}
			require.Equal(t, c.want, got)
		})
	}
	t.Run("empty", func(t *testing.T) {
		_, _, err := subtreeFromData(bytes.NewReader(nil), false)
		require.Error(t, err)
	})
	// Only data made entirely of 64-byte transactions can be a collapsed level.
	t.Run("64-byte transactions", func(t *testing.T) {
		t64 := sixtyFourByteTx(t)
		for _, c := range []struct {
			data         []byte
			coinbaseSlot bool
			ambiguous    bool
		}{
			{join(t64), false, true},
			{join(t64, t64), false, true},
			{join(tx1, t64), false, false},
			{join(t64), true, true},
			{join(coinbase, t64), true, true},
		} {
			_, ambiguous, err := subtreeFromData(bytes.NewReader(c.data), c.coinbaseSlot)
			require.NoError(t, err)
			require.Equal(t, c.ambiguous, ambiguous)
		}
	})
}

// Data made only of 64-byte transactions may be a collapsed tree level. It is
// stored but never binds its node list, and it is never reused from the store,
// so each attempt asks a peer again.
func TestFetchAndStoreSubtreeData_SixtyFourByteTxDoesNotBind(t *testing.T) {
	ctx := context.Background()
	tx := sixtyFourByteTx(t)
	hash := tx.TxIDChainHash()
	blobs := memory.New()
	cfg := test.CreateBaseTestSettings(t)
	var dataFetches atomic.Int32
	peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subtree/" + hash.String():
			_, _ = w.Write(hash[:])
		case "/subtree_data/" + hash.String():
			dataFetches.Add(1)
			_, _ = w.Write(tx.Bytes())
		default:
			http.NotFound(w, r)
		}
	}))
	defer peerServer.Close()
	s := &Server{logger: ulogger.TestLogger{}, settings: cfg, subtreeStore: blobs}
	block := &model.Block{Height: 1, Subtrees: []*chainhash.Hash{{1}, hash}}
	for attempt := 1; attempt <= 2; attempt++ {
		artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
		actx := context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
		_, err := s.fetchAndStoreSubtreeAndSubtreeData(actx, block, hash, "peer", peerServer.URL)
		require.NoError(t, err)
		_, bound, _ := boundCatchupArtifact(actx, *hash)
		require.False(t, bound, "attempt %d", attempt)
		require.EqualValues(t, attempt, dataFetches.Load(), "ambiguous stored data is not reused")
	}
}

// A peer serving a collapsed list with 64-byte data must not wedge catchup:
// the next attempt must ask a peer again instead of rebuilding the same list
// from the stored data, and nobody is convicted.
func TestCatchupArtifacts_AmbiguousDataIsNotReused(t *testing.T) {
	ctx := context.Background()
	bv, block, _, blobs := newQuickBodyFixture(t, "async")
	t64 := sixtyFourByteTx(t)
	b := t64.Bytes()
	var x, y chainhash.Hash
	copy(x[:], b[:32])
	copy(y[:], b[32:])
	root := testNodesRoot(t, x, y)
	require.Equal(t, *t64.TxIDChainHash(), *root, "premise: the 64-byte tx is the merkle parent of its halves")
	block.Subtrees[2] = root
	attacker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/subtree/" + root.String():
			_, _ = w.Write(root[:])
		case "/subtree_data/" + root.String():
			_, _ = w.Write(b)
		default:
			http.NotFound(w, r)
		}
	}))
	defer attacker.Close()
	var honestHits atomic.Int32
	honest := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		honestHits.Add(1)
		http.NotFound(w, r)
	}))
	defer honest.Close()
	s := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, subtreeStore: blobs}
	for attempt, url := range []string{attacker.URL, honest.URL} {
		a := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
		actx := context.WithValue(ctx, catchupArtifactsKey{}, a)
		var err error
		for _, h := range block.Subtrees {
			if _, err = s.fetchAndStoreSubtreeAndSubtreeData(actx, block, h, "p", url); err != nil {
				break
			}
		}
		if attempt == 0 {
			require.NoError(t, err)
			cleanup, err := bv.authenticateQuickBlockBody(actx, block)
			cleanup()
			require.Error(t, err)
			require.False(t, isUnvalidatablePeerError(err), "an ambiguous list must not convict: %v", err)
			a.repair(ulogger.TestLogger{})
			exists, err := blobs.Exists(ctx, root[:], fileformat.FileTypeSubtreeData)
			require.NoError(t, err)
			require.False(t, exists, "ambiguous data written by this attempt is evicted with its list")
			for _, h := range block.Subtrees[:2] {
				exists, err := blobs.Exists(ctx, h[:], fileformat.FileTypeSubtreeData)
				require.NoError(t, err)
				require.True(t, exists, "data this attempt did not write unbound is kept")
			}
			continue
		} else {
			require.Error(t, err, "the honest test peer serves nothing")
			require.NotZero(t, honestHits.Load(), "the next attempt must ask a peer again")
		}
		a.repair(ulogger.TestLogger{})
	}
}

// A validated subtree is authoritative: a stale pending list next to it must
// not cost a catchup attempt.
func TestCatchupArtifacts_ValidatedSubtreeOverridesStalePending(t *testing.T) {
	for _, withData := range []bool{true, false} {
		t.Run(fmt.Sprintf("data=%v", withData), func(t *testing.T) {
			ctx := context.Background()
			bv, block, _, blobs := newQuickBodyFixture(t, "async")
			hash := block.Subtrees[1]
			honest, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
			require.NoError(t, err)
			require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtree, honest))
			collapsed, err := subtreepkg.NewTreeByLeafCount(1)
			require.NoError(t, err)
			require.NoError(t, collapsed.AddNode(*hash, 0, 0))
			raw, err := collapsed.Serialize()
			require.NoError(t, err)
			require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, raw, options.WithAllowOverwrite(true)))
			data, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
			require.NoError(t, err)
			if !withData {
				require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeData))
			}
			body, err := block.Bytes()
			require.NoError(t, err)
			peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/blocks/" + block.Hash().String():
					_, _ = w.Write(body)
				case "/subtree_data/" + hash.String():
					_, _ = w.Write(data)
				default:
					http.NotFound(w, r)
				}
			}))
			defer peerServer.Close()
			af, err := adaptivefetch.New(adaptivefetch.DefaultConfig(), "validated", prometheus.NewRegistry())
			require.NoError(t, err)
			s := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, blockchainClient: bv.blockchainClient,
				blockValidation: bv, subtreeStore: blobs, adaptiveFetch: af, headerChainCache: catchup.NewHeaderChainCache(ulogger.TestLogger{})}
			require.NoError(t, s.fetchAndValidateBlocks(ctx, &CatchupContext{blockUpTo: block, baseURL: peerServer.URL,
				blockHeaders: []*model.BlockHeader{block.Header}, commonAncestorMeta: &model.BlockHeaderMeta{Height: 0},
				useQuickValidation: true, highestCheckpointHeight: 1}), "the validated nodes must be used in the first attempt")
		})
	}
}

// A duplicate that is not a level tail has no shorter honest list, so a body
// resting on bound node lists is convicted by the duplicate check.
func TestQuickBodyPreflightBoundDuplicateConvicts(t *testing.T) {
	ctx := context.Background()
	bv, block, txs, blobs := newQuickBodyFixture(t, "async")
	s := &Server{logger: ulogger.TestLogger{}, settings: bv.settings, subtreeStore: blobs}
	artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
	ctx = context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
	dup, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	for i := 0; i < 2; i++ {
		require.NoError(t, dup.AddNode(*txs[4].TxIDChainHash(), 0, 0))
	}
	block.Subtrees[2] = dup.RootHash()
	for _, h := range block.Subtrees {
		raw, err := blobs.Get(ctx, h[:], fileformat.FileTypeSubtreeToCheck)
		if h == block.Subtrees[2] {
			raw, err = dup.Serialize()
		}
		require.NoError(t, err)
		nodes, err := subtreepkg.NewSubtreeFromBytes(raw)
		require.NoError(t, err)
		require.NoError(t, s.storeBoundSubtreeFile(ctx, h, fileformat.FileTypeSubtreeToCheck, raw, 100, nodes.Length(), true))
	}
	cleanup, err := bv.authenticateQuickBlockBody(ctx, block)
	cleanup()
	require.ErrorIs(t, err, errors.ErrBlockInvalid)
	require.ErrorContains(t, err, "duplicate")
}

// Fetch side: only a subtree whose transactions are all 64 bytes is ambiguous;
// the coinbase slot does not count.
func TestAllMerkleNodeSized(t *testing.T) {
	txs := transactions.CreateTestTransactionChainWithCount(t, 3)
	t64 := sixtyFourByteTx(t)
	plain, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, plain.AddNode(chainhash.Hash{1}, 0, 0))
	first, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.True(t, allMerkleNodeSized(plain, []*bt.Tx{t64, t64}))
	require.False(t, allMerkleNodeSized(plain, []*bt.Tx{txs[1], t64}), "one ordinary transaction binds the list")
	require.True(t, allMerkleNodeSized(first, []*bt.Tx{txs[0], t64}), "the coinbase slot is not counted")
	require.False(t, allMerkleNodeSized(first, []*bt.Tx{txs[0]}), "a coinbase-only subtree cannot be collapsed")
}

// A validated subtree that is itself corrupt must not be bound over a pending list.
func TestBindPendingToValidatedVerifiesNodes(t *testing.T) {
	ctx := context.Background()
	blobs := memory.New()
	s := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: blobs}
	artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
	actx := context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
	h := testHashes(4)
	hash := testNodesRoot(t, h...)
	wrong, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, wrong.AddNode(h[0], 0, 0))
	require.NoError(t, wrong.AddNode(h[1], 0, 0))
	raw, err := wrong.Serialize()
	require.NoError(t, err)
	require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtree, raw))
	require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, []byte{1}))
	err = s.bindPendingToValidated(actx, &model.Block{Height: 1}, hash)
	require.ErrorIs(t, err, errors.ErrServiceError)
	_, bound, _ := boundCatchupArtifact(actx, *hash)
	require.False(t, bound)
}

// Data that parses against the node list without filling every slot must be a
// peer mismatch: go-subtree leaves unfilled slots nil (an empty body fills
// none), and a coinbase in second place overwrites slot 0.
func TestFetchAndStoreSubtreeData_RequiresEveryNodeMatched(t *testing.T) {
	txs := transactions.CreateTestTransactionChainWithCount(t, 4)
	coinbase, t0, t1 := txs[0], txs[1], txs[2]
	require.True(t, coinbase.IsCoinbase())
	oneNode := *t0.TxIDChainHash()
	// A missing transaction can be an honest server's aborted stream (the
	// asset server commits a 200 before streaming), so only a transaction that
	// contradicts its node charges the peer. Every case fails over.
	cases := map[string]struct {
		nodes   []chainhash.Hash
		data    []byte
		charged bool
	}{
		"empty data":             {[]chainhash.Hash{oneNode}, nil, false},
		"coinbase second":        {[]chainhash.Hash{*t0.TxIDChainHash(), *t1.TxIDChainHash()}, append(append(t0.Bytes(), coinbase.Bytes()...), t1.Bytes()...), true},
		"truncated after a node": {[]chainhash.Hash{*t0.TxIDChainHash(), *t1.TxIDChainHash()}, t0.Bytes(), false},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			hash := testNodesRoot(t, c.nodes...)
			blobs := memory.New()
			recorder := &catchupErrorRecorder{}
			peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/subtree/" + hash.String():
					_, _ = w.Write(testNodeBytes(c.nodes...))
				case "/subtree_data/" + hash.String():
					_, _ = w.Write(c.data)
				default:
					http.NotFound(w, r)
				}
			}))
			defer peerServer.Close()
			s := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: blobs, p2pClient: recorder}
			artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
			actx := context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
			block := &model.Block{Height: 1, Subtrees: []*chainhash.Hash{{1}, hash}}

			subtree, validated, err := s.fetchAndStoreSubtree(actx, block, hash, "peer", peerServer.URL)
			require.NoError(t, err)
			require.NotPanics(t, func() {
				err = s.fetchAndStoreSubtreeData(actx, block, hash, subtree, validated, "peer", peerServer.URL)
			})
			require.Error(t, err)
			require.False(t, errors.IsLocalError(err), "a bad peer response must allow alternative peers: %v", err)
			if c.charged {
				require.Equal(t, []string{"peer"}, recorder.peers, "a contradicting peer is charged")
			} else {
				require.Empty(t, recorder.peers, "a truncated stream is not charged")
			}
			for _, kind := range []fileformat.FileType{fileformat.FileTypeSubtreeToCheck, fileformat.FileTypeSubtreeData} {
				exists, err := blobs.Exists(ctx, hash[:], kind)
				require.NoError(t, err)
				require.False(t, exists, "nothing is stored from a mismatched response")
			}
		})
	}
}

// Stored data whose transactions form a padded list reproduces the root of the
// shorter list; the node check must reject it rather than bind it.
func TestBindStoredSubtreeRejectsPaddedData(t *testing.T) {
	ctx := context.Background()
	txs := transactions.CreateTestTransactionChainWithCount(t, 5)
	a, b, c := txs[1], txs[2], txs[3]
	hash := testNodesRoot(t, *a.TxIDChainHash(), *b.TxIDChainHash(), *c.TxIDChainHash())
	require.Equal(t, hash, testNodesRoot(t, *a.TxIDChainHash(), *b.TxIDChainHash(), *c.TxIDChainHash(), *c.TxIDChainHash()), "premise")
	blobs := memory.New()
	var data []byte
	for _, tx := range []*bt.Tx{a, b, c, c} {
		data = append(data, tx.Bytes()...)
	}
	require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtreeData, data))
	s := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: blobs}
	artifacts := &catchupArtifacts{Store: blobs, files: make(map[catchupArtifactKey]*catchupArtifact)}
	actx := context.WithValue(ctx, catchupArtifactsKey{}, artifacts)
	bound, err := s.bindStoredSubtree(actx, &model.Block{Height: 1, Subtrees: []*chainhash.Hash{{1}, hash}}, hash)
	require.NoError(t, err)
	require.False(t, bound, "padded stored data must be fetched again, not bound")
	exists, err := blobs.Exists(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
	require.NoError(t, err)
	require.False(t, exists)
}

// Honest data passes the slot check, including the first subtree's coinbase
// slot with and without the coinbase transaction.
func TestCheckSubtreeDataMatchesNodes(t *testing.T) {
	txs := transactions.CreateTestTransactionChainWithCount(t, 4)
	coinbase, t0, t1 := txs[0], txs[1], txs[2]
	first, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, first.AddCoinbaseNode())
	require.NoError(t, first.AddNode(*t0.TxIDChainHash(), 0, 0))
	plain, err := subtreepkg.NewTreeByLeafCount(2)
	require.NoError(t, err)
	require.NoError(t, plain.AddNode(*t0.TxIDChainHash(), 0, 0))
	require.NoError(t, plain.AddNode(*t1.TxIDChainHash(), 0, 0))
	for name, c := range map[string]struct {
		subtree *subtreepkg.Subtree
		data    []byte
		err     error
	}{
		"first with coinbase":    {first, append(coinbase.Bytes(), t0.Bytes()...), nil},
		"first without coinbase": {first, t0.Bytes(), nil},
		"plain":                  {plain, append(t0.Bytes(), t1.Bytes()...), nil},
		"plain truncated":        {plain, t0.Bytes(), subtreepkg.ErrSubtreeLengthMismatch},
		"plain coinbase second":  {plain, append(append(t0.Bytes(), coinbase.Bytes()...), t1.Bytes()...), subtreepkg.ErrTxHashMismatch},
	} {
		t.Run(name, func(t *testing.T) {
			data, err := subtreepkg.NewSubtreeDataFromReader(c.subtree, bytes.NewReader(c.data))
			require.NoError(t, err)
			err = checkSubtreeDataMatchesNodes(c.subtree, data)
			if c.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, c.err)
		})
	}
}

// A node payload that is not a whole number of hashes is a malformed response,
// even if its aligned prefix hashes to the subtree.
func TestFetchAndStoreSubtreeRejectsMisalignedNodes(t *testing.T) {
	suite := NewCatchupTestSuite(t)
	defer suite.Cleanup()
	recorder := &catchupErrorRecorder{}
	suite.Server.p2pClient = recorder
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	h := testHashes(2)
	hash := testNodesRoot(t, h...)
	httpmock.RegisterResponder("GET", "http://test-peer/subtree/"+hash.String(), httpmock.NewBytesResponder(200, append(testNodeBytes(h...), 1, 2, 3)))

	result, _, err := suite.Server.fetchAndStoreSubtree(suite.Ctx, &model.Block{Height: 100}, hash, "peer", "http://test-peer")
	require.Error(t, err)
	require.Nil(t, result)
	require.False(t, errors.IsLocalError(err))
	require.Equal(t, []string{"peer"}, recorder.peers)
}

// A validated subtree loaded with mmap must be released once its data has been
// checked, on success and on failure.
func TestFetchAndStoreSubtreeAndSubtreeData_ClosesMmapSubtree(t *testing.T) {
	for _, dataOK := range []bool{true, false} {
		t.Run(fmt.Sprintf("dataOK=%v", dataOK), func(t *testing.T) {
			ctx := context.Background()
			bv, block, _, blobs := newQuickBodyFixture(t, "async")
			hash := block.Subtrees[1]
			nodes, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeToCheck)
			require.NoError(t, err)
			require.NoError(t, blobs.Set(ctx, hash[:], fileformat.FileTypeSubtree, nodes))
			require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeToCheck))
			data, err := blobs.Get(ctx, hash[:], fileformat.FileTypeSubtreeData)
			require.NoError(t, err)
			require.NoError(t, blobs.Del(ctx, hash[:], fileformat.FileTypeSubtreeData))
			if !dataOK {
				data = data[:len(data)-1]
			}
			peerServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/subtree_data/"+hash.String() {
					_, _ = w.Write(data)
					return
				}
				http.NotFound(w, r)
			}))
			defer peerServer.Close()
			mmapDir := t.TempDir()
			cfg := *bv.settings
			cfg.BlockValidation.SubtreeMmapDir = mmapDir
			s := &Server{logger: ulogger.TestLogger{}, settings: &cfg, subtreeStore: blobs}

			_, err = s.fetchAndStoreSubtreeAndSubtreeData(ctx, block, hash, "peer", peerServer.URL)
			require.Equal(t, dataOK, err == nil, "%v", err)
			files, err := os.ReadDir(mmapDir)
			require.NoError(t, err)
			require.Empty(t, files, "the mmap-backed subtree must be closed")
		})
	}
}
