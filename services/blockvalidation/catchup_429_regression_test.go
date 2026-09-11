package blockvalidation

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/catchup"
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

type maliciousCatchupPeer struct{ catchupPeersP2PMock }

func (*maliciousCatchupPeer) IsPeerMalicious(context.Context, string) (bool, string, error) {
	return true, "invalid data", nil
}

func TestCatchupHeaders_PeerURLCannotForgeLocalError(t *testing.T) {
	for _, failure := range []string{"circuit open", "malicious"} {
		t.Run(failure, func(t *testing.T) {
			const baseURL = "https://peer.example/context canceled"
			server := &Server{
				logger:              ulogger.TestLogger{},
				settings:            test.CreateBaseTestSettings(t),
				peerCircuitBreakers: catchup.NewPeerCircuitBreakers(catchup.DefaultCircuitBreakerConfig()),
			}
			if failure == "circuit open" {
				breaker := server.peerCircuitBreakers.GetBreaker(baseURL)
				for range catchup.DefaultCircuitBreakerConfig().FailureThreshold {
					breaker.RecordFailure()
				}
			} else {
				server.p2pClient = &maliciousCatchupPeer{}
			}
			block := testhelpers.CreateTestBlockChain(t, 1)[0]
			_, _, err := server.catchupGetBlockHeaders(context.Background(), block, "", baseURL)
			require.Error(t, err)
			require.False(t, errors.IsLocalError(err), "peer-controlled URL must not suppress failover or attribution: %v", err)
		})
	}
}

func TestCatchupPeerSnapshot_RetriesTransientLookupFailure(t *testing.T) {
	calls := 0
	snapshot := &catchupPeerSnapshot{
		load: func() ([]*p2p.PeerInfo, bool, error) {
			calls++
			if calls == 1 {
				return nil, false, errors.NewServiceError("registry restarting")
			}
			return []*p2p.PeerInfo{mkTestPeer("archival", "full", 100)}, false, nil
		},
	}
	_, _, err := snapshot.get()
	require.Error(t, err)
	for range 3 {
		peers, _, err := snapshot.get()
		require.NoError(t, err)
		require.Len(t, peers, 1)
	}
	require.Equal(t, 2, calls, "cache successful discovery, never a transient RPC error")
}

func TestDecodeBoundedBlock_BufferedCoinbaseUsesBlockBudget(t *testing.T) {
	// The response can hold a batch, but this block's coinbase cannot exceed the
	// accepted Bitcoin block size. Production puts bufio above the response cap.
	data := hostileCoinbaseBlock(t, 128, 4096)
	data = append(data, make([]byte, 4096)...)
	body := &trackedBlockResponseBody{data: data}
	limited := &io.LimitedReader{R: body, N: 8192}
	r := bufio.NewReaderSize(limited, blockStreamReadBufferMinSize)
	_, err := decodeBoundedBlock(r, limited, blockResponseLimits{
		maxTransportBytes: 8192,
		maxDeclaredBytes:  128,
		enforceDeclared:   true,
	})
	require.Error(t, err)
	require.Less(t, body.bytesRead.Load(), int64(512), "reject the advertised script before buffering it")
}

func TestDecodeBoundedBlock_BufferedCoinbaseUsesRemainingResponseBudget(t *testing.T) {
	data := hostileCoinbaseBlock(t, 128, 4096)
	limited := &io.LimitedReader{R: bytes.NewReader(data), N: 256}
	r := bufio.NewReaderSize(limited, blockStreamReadBufferMinSize)
	_, err := decodeBoundedBlock(r, limited, blockResponseLimits{maxTransportBytes: 8192})
	require.ErrorContains(t, err, "remaining transport budget", "inspect the remaining allowance through production's bufio layer")
}

// A store can time out independently of the peer. It must never be sent through
// the network-deadline classifier, even when the download bound also expires.
type timeoutSubtreeStore struct {
	blob.Store
	writeContext context.Context
}

func (s *timeoutSubtreeStore) Set(ctx context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	s.writeContext = ctx
	return context.DeadlineExceeded
}

func TestCatchupSubtreeData_StoreTimeoutIsLocal(t *testing.T) {
	store := &timeoutSubtreeStore{Store: memory.New()}
	server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: store}
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	hash := subtree.RootHash()
	block := testhelpers.CreateTestBlockChain(t, 1)[0]
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/subtree_data/%s", hash), httpmock.NewBytesResponder(200, nil))
	err = server.fetchAndStoreSubtreeData(context.Background(), context.Background(), block, hash, subtree, "peer", "http://peer", false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrStorageError), "local store deadline must remain a storage failure: %v", err)
	require.True(t, errors.IsLocalError(err))
	require.False(t, errors.Is(err, errors.ErrNetworkTimeout))
	require.NotNil(t, store.writeContext)
	_, hasDeadline := store.writeContext.Deadline()
	require.False(t, hasDeadline, "the peer download deadline must not govern the local store")
}

func TestCatchupSubtree_PacingSaturationFailsOverWithoutPeerPenalty(t *testing.T) {
	for _, saturatedRole := range []string{"primary", "alternative"} {
		t.Run(saturatedRole, func(t *testing.T) {
			server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: memory.New()}
			server.settings.BlockValidation.PerPeerFetchRate = 1
			server.settings.BlockValidation.SubtreeFetchTimeout = 20 * time.Millisecond
			server.activeCatchupCtx = &CatchupContext{}
			subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
			require.NoError(t, err)
			require.NoError(t, subtree.AddCoinbaseNode())
			hash := subtree.RootHash()
			require.NoError(t, server.subtreeStore.Set(context.Background(), hash[:], fileformat.FileTypeSubtreeData, []byte{0}))
			primary := mkTestPeer("primary", "full", 100)
			saturated := primary
			if saturatedRole == "alternative" {
				saturated = mkTestPeer("busy", "full", 100)
			}
			good := mkTestPeer("good", "full", 100)
			require.NoError(t, server.awaitPeerFetchSlot(context.Background(), saturated.DataHubURL))
			snapshot := &catchupPeerSnapshot{load: func() ([]*p2p.PeerInfo, bool, error) { return []*p2p.PeerInfo{saturated, good}, false, nil }}
			httpmock.ActivateNonDefault(util.HTTPClient())
			defer httpmock.DeactivateAndReset()
			httpmock.RegisterResponder("GET", fmt.Sprintf("%s/subtree/%s", primary.DataHubURL, hash), httpmock.NewStringResponder(http.StatusNotFound, "missing"))
			httpmock.RegisterResponder("GET", fmt.Sprintf("%s/subtree/%s", good.DataHubURL, hash), httpmock.NewBytesResponder(http.StatusOK, subtreepkg.CoinbasePlaceholderHashValue[:]))
			block := testhelpers.CreateTestBlockChain(t, 1)[0]
			got, err := server.fetchAndStoreSubtreeAndSubtreeData(context.Background(), context.Background(), block, hash, primary.ID.String(), primary.DataHubURL, snapshot)
			require.NoError(t, err)
			require.Equal(t, good.ID.String(), got)
			require.NotContains(t, server.activeCatchupCtx.failedPeers, saturated.ID.String(), "no request was sent to the saturated peer")
		})
	}
}

func TestCatchupBlockFetchBudgetAllowsBackoffAndBody(t *testing.T) {
	server := &Server{settings: test.CreateBaseTestSettings(t)}
	ctx, cancel := server.withCatchupFetchTimeout(context.Background())
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.Greater(t, time.Until(deadline), 90*time.Second, "a heavy fetch needs room for the 60s retry budget and its body, independent of header iterations")
}

func TestCatchupSubtreeDistribution_RequiresRequestedHeight(t *testing.T) {
	server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: memory.New()}
	server.settings.BlockValidation.CatchupParallelFetchEnabled = true
	primary := mkTestPeer("primary", "pruned", 200)
	behind := mkTestPeer("behind", "full", 50)
	server.p2pClient = &catchupPeersP2PMock{peers: []*p2p.PeerInfo{primary, behind}}
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	hash := subtree.RootHash()
	require.NoError(t, server.subtreeStore.Set(context.Background(), hash[:], fileformat.FileTypeSubtreeData, []byte{0}))
	block := testhelpers.CreateTestBlockChain(t, 1)[0]
	block.Height = 200
	block.Subtrees = append(block.Subtrees[:0], hash)
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	behindURL := fmt.Sprintf("%s/subtree/%s", behind.DataHubURL, hash)
	httpmock.RegisterResponder("GET", behindURL, httpmock.NewStringResponder(http.StatusNotFound, "not synced yet"))
	httpmock.RegisterResponder("GET", fmt.Sprintf("%s/subtree/%s", primary.DataHubURL, hash), httpmock.NewBytesResponder(http.StatusOK, subtreepkg.CoinbasePlaceholderHashValue[:]))
	_, err = server.fetchSubtreeDataForBlock(context.Background(), block, primary.ID.String(), primary.DataHubURL)
	require.NoError(t, err)
	require.Zero(t, httpmock.GetCallCountInfo()["GET "+behindURL], "never assign a subtree to a peer below its block height")
}

func TestCatchupFetchTimeouts_FiniteAndRespectParent(t *testing.T) {
	for _, subtree := range []bool{false, true} {
		for _, configured := range []time.Duration{0, -time.Second, 37 * time.Second} {
			t.Run(fmt.Sprintf("subtree=%t/configured=%s", subtree, configured), func(t *testing.T) {
				server := &Server{settings: test.CreateBaseTestSettings(t)}
				server.settings.BlockValidation.BlockFetchTimeout = configured
				server.settings.BlockValidation.SubtreeFetchTimeout = configured
				withTimeout := server.withCatchupFetchTimeout
				if subtree {
					withTimeout = server.withCatchupSubtreeFetchTimeout
				}
				want := configured
				if want <= 0 {
					want = 2 * time.Minute
				}
				ctx, cancel := withTimeout(context.Background())
				defer cancel()
				deadline, ok := ctx.Deadline()
				require.True(t, ok)
				require.WithinDuration(t, time.Now().Add(want), deadline, time.Second)
				parent, cancelParent := context.WithTimeout(context.Background(), time.Second)
				defer cancelParent()
				ctx, cancel = withTimeout(parent)
				defer cancel()
				deadline, ok = ctx.Deadline()
				require.True(t, ok)
				parentDeadline, _ := parent.Deadline()
				require.Equal(t, parentDeadline, deadline)
				cancelParent()
				require.ErrorIs(t, ctx.Err(), context.Canceled)
			})
		}
	}
}

func TestCatchupSubtreeData_StreamingReadSharesFetchDeadline(t *testing.T) {
	server := &Server{logger: ulogger.TestLogger{}, settings: test.CreateBaseTestSettings(t), subtreeStore: memory.New()}
	server.settings.BlockValidation.SubtreeDataFetchTimeout = 25 * time.Millisecond
	subtree, err := subtreepkg.NewIncompleteTreeByLeafCount(1)
	require.NoError(t, err)
	require.NoError(t, subtree.AddCoinbaseNode())
	hash := subtree.RootHash()
	block := testhelpers.CreateTestBlockChain(t, 1)[0]
	var body *stallingBlockResponseBody
	httpmock.ActivateNonDefault(util.HTTPClient())
	defer httpmock.DeactivateAndReset()
	httpmock.RegisterResponder("GET", fmt.Sprintf("http://peer/subtree_data/%s", hash), func(req *http.Request) (*http.Response, error) {
		body = &stallingBlockResponseBody{ctx: req.Context(), started: make(chan struct{})}
		return blockHTTPResponse(body), nil
	})
	err = server.fetchAndStoreSubtreeData(context.Background(), context.Background(), block, hash, subtree, "peer", "http://peer", false)
	require.ErrorIs(t, err, errors.ErrNetworkTimeout)
	require.False(t, errors.IsLocalError(err))
	require.NotNil(t, body)
	require.True(t, body.closed.Load())
	exists, err := server.subtreeStore.Exists(context.Background(), hash[:], fileformat.FileTypeSubtreeData)
	require.NoError(t, err)
	require.False(t, exists, "a stalled stream must not reach persistence")
}

func TestCatchupBlockBudgetRejection_DoesNotMarkPeerMalicious(t *testing.T) {
	block := testhelpers.CreateTestBlockChain(t, 1)[0]
	blockBytes, err := block.Bytes()
	require.NoError(t, err)
	body := append(append([]byte{}, blockBytes...), blockBytes...)
	limit := int64(len(body) - 20)
	limited := &io.LimitedReader{R: bytes.NewReader(body), N: limit}
	reader := bufio.NewReaderSize(limited, blockStreamReadBufferMinSize)
	limits := blockResponseLimits{maxTransportBytes: limit}
	_, err = decodeBoundedBlock(reader, limited, limits)
	require.NoError(t, err)
	_, err = decodeBoundedBlock(reader, limited, limits)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.ErrExternal)
	require.False(t, errors.Is(err, errors.ErrBlockInvalid), "our aggregate response limit is not a consensus failure: %v", err)
}
