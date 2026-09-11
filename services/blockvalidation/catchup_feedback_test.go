package blockvalidation

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	"github.com/bsv-blockchain/teranode/services/p2p"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jarcoal/httpmock"
	"github.com/stretchr/testify/require"
)

func TestCatchupFeedback_PacingDoesNotReportPeerFailure(t *testing.T) {
	peers := &catchupPeersP2PMock{}
	server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), p2pClient: peers}
	server.settings.BlockValidation.PerPeerFetchRate = 1
	require.NoError(t, server.awaitPeerFetchSlot(context.Background(), "http://busy"))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	tokensBefore := server.peerFetchLimiter("http://busy").Tokens()
	err := server.awaitPeerFetchSlot(ctx, "http://busy")
	require.Error(t, err)
	require.GreaterOrEqual(t, server.peerFetchLimiter("http://busy").Tokens(), tokensBefore, "rejected reservations must not consume future request capacity")
	require.False(t, shouldStopPeerFailover(context.Background(), err))
	server.reportCatchupFailureForError(context.Background(), "busy", errors.NewProcessingError("alternative failed", err))
	require.Empty(t, peers.failures, "both alternative loops must preserve local pacing attribution")
	server.reportCatchupFailureForError(context.Background(), "bad", errors.NewNetworkTimeoutError("peer stalled"))
	require.Equal(t, []string{"bad"}, peers.failures)
}

func TestCatchupFeedback_IdleLimiterAndExpiredContext(t *testing.T) {
	server := &Server{settings: test.CreateBaseTestSettings(t)}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	require.NoError(t, server.awaitPeerFetchSlot(ctx, "http://idle"), "an available token needs no reservation delay")
	expired, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expire()
	err := server.awaitPeerFetchSlot(expired, "http://other-idle")
	require.Error(t, err)
	require.True(t, errors.IsLocalError(err))
	require.True(t, shouldStopPeerFailover(context.Background(), err), "an already-expired context is not evidence of a saturated peer queue")
}

type cancelSubtreeStore struct {
	blob.Store
	operation string
	cancel    context.CancelFunc
}

func (s *cancelSubtreeStore) Exists(ctx context.Context, key []byte, kind fileformat.FileType, opts ...options.FileOption) (bool, error) {
	if s.operation == "exists" {
		s.cancel()
		return false, ctx.Err()
	}
	return s.Store.Exists(ctx, key, kind, opts...)
}
func (s *cancelSubtreeStore) Get(ctx context.Context, key []byte, kind fileformat.FileType, opts ...options.FileOption) ([]byte, error) {
	if s.operation == "get" {
		s.cancel()
		return nil, ctx.Err()
	}
	return s.Store.Get(ctx, key, kind, opts...)
}
func (s *cancelSubtreeStore) Set(ctx context.Context, key []byte, kind fileformat.FileType, b []byte, opts ...options.FileOption) error {
	if s.operation == "set" {
		s.cancel()
		return ctx.Err()
	}
	return s.Store.Set(ctx, key, kind, b, opts...)
}

func TestCatchupFeedback_SubtreeStoreCancellationIsNotOutage(t *testing.T) {
	for _, operation := range []string{"exists", "get", "set"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &cancelSubtreeStore{Store: memory.New(), operation: operation, cancel: cancel}
			server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: store}
			block := testhelpers.CreateTestBlockChain(t, 1)[0]
			subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
			require.NoError(t, err)
			require.NoError(t, subtree.AddCoinbaseNode())
			hash := subtree.RootHash()
			if operation == "get" {
				require.NoError(t, store.Store.Set(ctx, hash[:], fileformat.FileTypeSubtreeToCheck, []byte{1}))
			}
			httpmock.ActivateNonDefault(util.HTTPClient())
			defer httpmock.DeactivateAndReset()
			httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/subtree/%s", hash), httpmock.NewBytesResponder(200, subtreepkg.CoinbasePlaceholderHashValue[:]))
			_, err = server.fetchAndStoreSubtree(ctx, block, hash, "peer", "http://peer", false)
			require.Error(t, err)
			require.True(t, errors.IsLocalError(err))
			require.False(t, errors.Is(err, errors.ErrStorageError), "shutdown must not emit a storage-outage alarm: %v", err)
		})
	}
}

type downloadContextPeer struct {
	catchupPeersP2PMock
	contextErr  error
	hasDeadline bool
	calls       int
}

func (p *downloadContextPeer) RecordBytesDownloaded(ctx context.Context, id string, n uint64) error {
	p.calls++
	p.contextErr = ctx.Err()
	_, p.hasDeadline = ctx.Deadline()
	return p.catchupPeersP2PMock.RecordBytesDownloaded(ctx, id, n)
}

func TestCatchupFeedback_DownloadAccountingSurvivesCancellation(t *testing.T) {
	for _, path := range []string{"subtree", "block"} {
		t.Run(path, func(t *testing.T) {
			peer := &downloadContextPeer{}
			server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), p2pClient: peer}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			hash := testhelpers.CreateTestBlockChain(t, 1)[0].Hash()
			httpmock.ActivateNonDefault(util.HTTPClient())
			defer httpmock.DeactivateAndReset()
			httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/subtree_data/%s", hash), httpmock.NewStringResponder(200, "data"))
			reader, err := server.fetchSubtreeDataFromPeer(ctx, hash, "peer", "http://peer", nil, false)
			require.NoError(t, err)
			var counted io.ReadCloser = reader
			if path == "block" {
				// Use the same response body without the subtree accounting callback.
				reader.onClose = nil
				counted = server.trackedBlockResponse(ctx, reader, hash, "peer", "test")
			}
			_, err = io.Copy(io.Discard, counted)
			require.NoError(t, err)
			cancel()
			require.NoError(t, counted.Close())
			require.Equal(t, 1, peer.calls)
			require.NoError(t, peer.contextErr, "accounting must not inherit a finished download's cancellation")
			require.True(t, peer.hasDeadline, "detached metrics RPC must still be bounded")
			require.Equal(t, uint64(4), peer.downloads["peer"])
		})
	}
}

func TestCatchupFeedback_DiscoveryFailurePreservesPrimaryEvidence(t *testing.T) {
	initPrometheusMetrics()
	peers := &catchupPeersP2PMock{}
	server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: memory.New(), p2pClient: peers}
	server.activeCatchupCtx = &CatchupContext{startTime: time.Now()}
	block := testhelpers.CreateTestBlockChain(t, 1)[0]
	hash := block.Hash()
	snapshot := &catchupPeerSnapshot{load: func() ([]*p2p.PeerInfo, bool, error) {
		return nil, false, errors.NewServiceError("registry restarting")
	}}
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://primary/subtree/%s", hash), httpmock.NewStringResponder(http.StatusNotFound, "missing"))
	_, err := server.fetchAndStoreSubtreeAndSubtreeData(context.Background(), context.Background(), block, hash, "primary", "http://primary", snapshot)
	require.Error(t, err)
	require.True(t, errors.IsTransientLocalError(err), "discovery remains a local outage")
	require.ErrorContains(t, err, "primary")
	require.Contains(t, server.activeCatchupCtx.failedPeers, "primary")
	server.activeCatchupCtx.blockUpTo = block
	server.releaseCatchupLock(server.activeCatchupCtx, &err)
	require.Equal(t, []string{"primary"}, peers.failures, "record the actual failed peer once despite the later local outage")
}
