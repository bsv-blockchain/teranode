package subtreevalidation

import (
	"context"
	"net/url"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// After a block, a backlog of peer subtrees arrives at once. Each one admitted
// allocates its per-subtree working set (~1 GiB for 1M transactions) before it
// waits for a cache-probe worker, so the gate must bound how many hold one.
func TestAdmissionGate_BoundsConcurrentHolders(t *testing.T) {
	const limit = 3

	var (
		gate    admissionGate
		holding atomic.Int32
		peak    atomic.Int32
		failed  atomic.Int32
		wg      sync.WaitGroup
	)

	for i := 0; i < 20; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			release, err := gate.acquire(context.Background(), limit)
			if err != nil {
				failed.Add(1)
				return
			}

			n := holding.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}

			time.Sleep(2 * time.Millisecond)
			holding.Add(-1)
			release()
		}()
	}

	wg.Wait()

	require.Zero(t, failed.Load())
	require.LessOrEqual(t, peak.Load(), int32(limit))
	require.Positive(t, peak.Load())
}

func TestAdmissionGate_NonPositiveLimitDisablesIt(t *testing.T) {
	var gate admissionGate

	releases := make([]func(), 0, 100)

	for i := 0; i < 100; i++ {
		release, err := gate.acquire(context.Background(), 0)
		require.NoError(t, err)

		releases = append(releases, release)
	}

	for _, release := range releases {
		release()
	}
}

func TestAdmissionGate_CancelWhileWaiting(t *testing.T) {
	var gate admissionGate

	release, err := gate.acquire(context.Background(), 1)
	require.NoError(t, err)

	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = gate.acquire(ctx, 1)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrContextCanceled), "got %v", err)
}

func newAdmissionTestServer(t *testing.T, subtreeExists bool) (*Server, *Quorum) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.SubtreeValidation.PeerSubtreeConcurrency = 1

	q, err := NewQuorum(ulogger.TestLogger{}, newMockExister(subtreeExists), t.TempDir(), WithTimeout(quorumOpTimeout))
	require.NoError(t, err)

	InitPrometheusMetrics()

	running := blockchain.FSMStateRUNNING
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&running, nil)

	server := &Server{
		logger:           ulogger.TestLogger{},
		settings:         tSettings,
		quorum:           q,
		blockchainClient: blockchainClient,
	}

	blockIDs := map[uint32]bool{}
	server.currentBlockIDsMap.Store(&blockIDs)
	server.bestBlockHeaderMeta.Store(&model.BlockHeaderMeta{Height: 1})

	return server, q
}

// requireSlotFree fails unless the server's single admission slot is free.
func requireSlotFree(t *testing.T, server *Server) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	release, err := server.peerSubtreeAdmission.acquire(ctx, 1)
	require.NoError(t, err, "the admission slot must have been released")
	release()
}

// A peer subtree waits for admission before it takes the subtree's quorum
// lock: block validation (CheckBlockSubtrees) waits on that lock, so a subtree
// queued at the gate while holding it would make a block wait for the queue.
func TestSubtreesHandler_WaitsForAdmissionBeforeTheQuorumLock(t *testing.T) {
	server, q := newAdmissionTestServer(t, false)

	release, err := server.peerSubtreeAdmission.acquire(context.Background(), 1)
	require.NoError(t, err)

	hash := chainhash.HashH([]byte("peer subtree"))
	baseURL, err := url.Parse("http://peer.invalid")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() { done <- server.subtreesHandler(ctx, &hash, baseURL, "peer-1") }()

	// While the handler is queued, nobody holds the subtree's lock.
	require.Eventually(t, func() bool { return server.peerSubtreeAdmission.waiting.Load() == 1 },
		5*time.Second, time.Millisecond, "the handler must be queued at the gate")

	gotLock, _, releaseLock, err := q.TryLockIfFileNotExists(context.Background(), &hash, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, gotLock, "a queued peer subtree must not hold the quorum lock")
	releaseLock()

	cancel()

	err = <-done
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrContextCanceled), "a peer subtree must wait at the gate, got %v", err)

	release()
	requireSlotFree(t, server)
}

// The slot is freed on every return: here the subtree already exists once the
// handler gets its slot, and the handler skips it.
func TestSubtreesHandler_ReleasesAdmissionWhenTheSubtreeExists(t *testing.T) {
	server, _ := newAdmissionTestServer(t, true)

	hash := chainhash.HashH([]byte("peer subtree"))
	baseURL, err := url.Parse("http://peer.invalid")
	require.NoError(t, err)

	err = server.subtreesHandler(context.Background(), &hash, baseURL, "peer-1")
	require.True(t, errors.Is(err, errors.ErrSubtreeExists), "got %v", err)

	requireSlotFree(t, server)
}

// The slot is freed on an error return too.
func TestSubtreesHandler_ReleasesAdmissionOnError(t *testing.T) {
	server, _ := newAdmissionTestServer(t, false)
	server.currentBlockIDsMap.Store(nil)

	hash := chainhash.HashH([]byte("peer subtree"))
	baseURL, err := url.Parse("http://peer.invalid")
	require.NoError(t, err)

	err = server.subtreesHandler(context.Background(), &hash, baseURL, "peer-1")
	require.Error(t, err)

	requireSlotFree(t, server)
}

// The FSM is checked when a Kafka message arrives, but a subtree can wait at
// the gate long enough for the node to leave RUNNING (CATCHINGBLOCKS, or IDLE
// after an operator STOP). Once it has a slot it checks again and is dropped,
// without taking the lock, if the node is no longer RUNNING.
func TestSubtreesHandler_SkipsWhenNoLongerRunningAfterTheWait(t *testing.T) {
	server, q := newAdmissionTestServer(t, false)

	catchingBlocks := blockchain.FSMStateCATCHINGBLOCKS
	blockchainClient := &blockchain.Mock{}
	blockchainClient.On("GetFSMCurrentState", mock.Anything).Return(&catchingBlocks, nil)
	server.blockchainClient = blockchainClient

	hash := chainhash.HashH([]byte("peer subtree"))
	baseURL, err := url.Parse("http://peer.invalid")
	require.NoError(t, err)

	require.NoError(t, server.subtreesHandler(context.Background(), &hash, baseURL, "peer-1"))

	gotLock, _, releaseLock, err := q.TryLockIfFileNotExists(context.Background(), &hash, fileformat.FileTypeSubtree)
	require.NoError(t, err)
	require.True(t, gotLock, "a dropped subtree must not have taken the lock")
	releaseLock()

	requireSlotFree(t, server)
}

func TestAdmissionGate_CountsWaiters(t *testing.T) {
	var gate admissionGate

	release, err := gate.acquire(context.Background(), 1)
	require.NoError(t, err)

	require.Zero(t, gate.waiting.Load(), "a caller that got a slot at once never waited")

	done := make(chan struct{})

	go func() {
		defer close(done)

		r, err := gate.acquire(context.Background(), 1)
		if err == nil {
			r()
		}
	}()

	require.Eventually(t, func() bool { return gate.waiting.Load() == 1 }, 5*time.Second, time.Millisecond)

	release()
	<-done

	require.Zero(t, gate.waiting.Load())
}
