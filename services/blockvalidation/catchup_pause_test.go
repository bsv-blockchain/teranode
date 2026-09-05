package blockvalidation

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/blockvalidation/testhelpers"
	blockchainstore "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/jellydator/ttlcache/v3"
	"github.com/ordishs/gocore"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestCatchupAdmission_CanceledTargetRetainsMarkers(t *testing.T) {
	settings := test.CreateBaseTestSettings(t)
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, store, nil, nil)
	require.NoError(t, err)
	hash := chainhash.HashH([]byte("pause target"))
	block := &model.Block{Header: &model.BlockHeader{HashPrevBlock: &hash, HashMerkleRoot: &hash}}
	server := &Server{
		settings: settings, logger: ulogger.TestLogger{}, blockchainClient: client,
		processBlockNotify:   ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives:  ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](),
	}
	server.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	server.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block}}, ttlcache.DefaultTTL)
	calls := 0
	server.catchupFunc = func(context.Context, *model.Block, string, string) error { calls++; return nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.processCatchupChItem(ctx, processBlockCatchup{block: block})
	require.Zero(t, calls, "canceled admission must not start catchup")
	require.NotNil(t, server.processBlockNotify.Get(*block.Hash()))
	require.NotNil(t, server.catchupAlternatives.Get(*block.Hash()))
	require.Nil(t, server.blockCatchupAttempts.Get(*block.Hash()))
}

// Admission faults are injected at the RPC boundary; queue/state behavior uses
// the production retry loop rather than a mocked blockchain client.
func TestCatchupAdmission_RetryAndCancel(t *testing.T) {
	t.Run("state and transport failures retain the unit until admitted", func(t *testing.T) {
		attempts := 0
		err := waitForCatchupAdmission(context.Background(), func(ctx context.Context) error {
			_, bounded := ctx.Deadline()
			require.True(t, bounded)
			attempts++
			switch attempts {
			case 1:
				return errors.NewStateError("paused")
			case 2:
				return errors.NewServiceError("transport unavailable")
			default:
				return nil
			}
		}, time.Second, time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, 3, attempts)
	})
	t.Run("per RPC deadline retries without losing the unit", func(t *testing.T) {
		attempts := 0
		err := waitForCatchupAdmission(context.Background(), func(ctx context.Context) error {
			attempts++
			if attempts == 1 {
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}, time.Millisecond, time.Millisecond)
		require.NoError(t, err)
		require.Equal(t, 2, attempts)
	})
	t.Run("shutdown interrupts retry delay", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		attempts := 0
		err := waitForCatchupAdmission(ctx, func(context.Context) error { attempts++; cancel(); return errors.NewStateError("paused") }, time.Second, time.Hour)
		require.ErrorIs(t, err, context.Canceled)
		require.Equal(t, 1, attempts)
	})
}

// The local store client deliberately has no FSM implementation. This adapter
// forwards admission to the real sqlitememory-backed blockchain server, while
// providing synthetic cached IDLE to prove scheduler admission never consults it.
type catchupAdmissionAuthority struct {
	blockchain.ClientI
	server      *blockchain.Blockchain
	denied      chan struct{}
	cachedReads atomic.Int32
	runCalls    atomic.Int32
	retryNotice func()
}

func (c *catchupAdmissionAuthority) CatchUpBlocks(ctx context.Context) error {
	_, err := c.server.CatchUpBlocks(ctx, &emptypb.Empty{})
	if err != nil {
		select {
		case c.denied <- struct{}{}:
		default:
		}
	}
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	return nil
}

func (c *catchupAdmissionAuthority) GetFSMCurrentState(context.Context) (*blockchain.FSMStateType, error) {
	c.cachedReads.Add(1)
	state := blockchain.FSMStateIDLE
	return &state, nil
}

func newCatchupAdmissionLocalClient(t *testing.T) blockchain.ClientI {
	t.Helper()
	settings := test.CreateBaseTestSettings(t)
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, store, nil, nil)
	require.NoError(t, err)
	return client
}

func newCatchupAdmissionAuthority(t *testing.T, state string) (*Server, *catchupAdmissionAuthority) {
	t.Helper()
	settings := test.CreateBaseTestSettings(t)
	store, err := blockchainstore.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, settings)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close(context.Background())) })
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	authority, err := blockchain.New(ctx, ulogger.TestLogger{}, settings, store, nil, state)
	require.NoError(t, err)
	require.NoError(t, authority.Init(ctx))
	authority.SetSubscriptionManagerReadyForTesting(true)
	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, settings, store, nil, nil)
	require.NoError(t, err)
	adapter := &catchupAdmissionAuthority{ClientI: client, server: authority, denied: make(chan struct{}, 1)}
	return &Server{settings: settings, logger: ulogger.TestLogger{}, stats: gocore.NewStat("catchup-pause-test"), blockchainClient: adapter,
		processBlockNotify:   ttlcache.New[chainhash.Hash, bool](),
		catchupAlternatives:  ttlcache.New[chainhash.Hash, []processBlockCatchup](),
		blockCatchupAttempts: ttlcache.New[chainhash.Hash, int](),
	}, adapter
}

func TestCatchupAdmission_ExplicitResumePreservesTarget(t *testing.T) {
	server, authority := newCatchupAdmissionAuthority(t, blockchain.FSMStateIDLE.String())
	block := testhelpers.CreateTestBlockChain(t, 2)[1]
	server.processBlockNotify.Set(*block.Hash(), true, ttlcache.DefaultTTL)
	server.catchupAlternatives.Set(*block.Hash(), []processBlockCatchup{{block: block}}, ttlcache.DefaultTTL)
	var calls atomic.Int32
	server.catchupFunc = func(context.Context, *model.Block, string, string) error { calls.Add(1); return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); server.processCatchupChItem(ctx, processBlockCatchup{block: block}) }()
	select {
	case <-authority.denied:
	case <-ctx.Done():
		t.Fatal("authority did not reject paused admission")
	}
	require.Zero(t, calls.Load())
	require.NotNil(t, server.processBlockNotify.Get(*block.Hash()))
	require.NotNil(t, server.catchupAlternatives.Get(*block.Hash()))
	require.Nil(t, server.blockCatchupAttempts.Get(*block.Hash()))
	_, err := authority.server.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain.FSMEventCATCHUPBLOCKS})
	require.NoError(t, err)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("retained target did not resume")
	}
	require.Equal(t, int32(1), calls.Load())
	require.Zero(t, authority.cachedReads.Load(), "synthetic IDLE must never control admission")
	require.Nil(t, server.processBlockNotify.Get(*block.Hash()))
}

func TestCatchupAdmission_PrefetchNextBlockWaitsForResume(t *testing.T) {
	server, authority := newCatchupAdmissionAuthority(t, blockchain.FSMStateCATCHINGBLOCKS.String())
	blocks := testhelpers.CreateTestBlockChain(t, 3)
	var calls atomic.Int32
	server.fetchSubtreeDataForBlockFn = func(ctx context.Context, _ *model.Block, _, _ string) (map[string]struct{}, error) {
		if calls.Add(1) == 1 {
			_, err := authority.server.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain.FSMEventIDLE})
			return nil, err
		}
		return nil, nil
	}
	queue := make(chan workItem, 2)
	results := make(chan resultItem, 2)
	queue <- workItem{block: blocks[1], index: 0}
	queue <- workItem{block: blocks[2], index: 1}
	close(queue)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- server.blockWorker(ctx, 0, queue, results, "", "http://peer", blocks[2]) }()
	select {
	case <-authority.denied:
	case <-ctx.Done():
		t.Fatal("second block was not denied admission")
	}
	require.Equal(t, int32(1), calls.Load(), "admitted first block finishes; successor stays unstarted")
	first := <-results
	require.NoError(t, first.err)
	require.Equal(t, 0, first.index)
	_, err := authority.server.SendFSMEvent(ctx, &blockchain_api.SendFSMEventRequest{Event: blockchain.FSMEventCATCHUPBLOCKS})
	require.NoError(t, err)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("prefetch did not resume")
	}
	require.Equal(t, int32(2), calls.Load())
	require.Equal(t, 1, (<-results).index)
	require.Zero(t, authority.cachedReads.Load())
}

func (c *catchupAdmissionAuthority) Run(ctx context.Context, _ string) error {
	c.runCalls.Add(1)
	_, err := c.server.Run(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	return nil
}

func TestCatchupAdmission_RestoreIgnoresSyntheticIdle(t *testing.T) {
	server, authority := newCatchupAdmissionAuthority(t, blockchain.FSMStateRUNNING.String())
	block := testhelpers.CreateTestBlockChain(t, 2)[1]
	server.restoreFSMState(context.Background(), &CatchupContext{blockUpTo: block})
	require.Equal(t, int32(1), authority.runCalls.Load(), "completion must reach authority despite synthetic cached IDLE")
	require.Zero(t, authority.cachedReads.Load())
}

func TestCatchupAdmission_ValidationRetainsBlockWhilePaused(t *testing.T) {
	server, authority := newCatchupAdmissionAuthority(t, blockchain.FSMStateIDLE.String())
	block := testhelpers.CreateTestBlockChain(t, 2)[1]
	queue := make(chan blockForValidation, 1)
	queue <- blockForValidation{block: block}
	close(queue)
	var remaining atomic.Int64
	remaining.Store(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- server.validateBlocksOnChannel(queue, ctx, &CatchupContext{blockUpTo: block}, &remaining, nil)
	}()
	select {
	case <-authority.denied:
	case <-ctx.Done():
		t.Fatal("validation did not wait for admission")
	}
	require.Equal(t, int64(1), remaining.Load())
	require.Zero(t, server.blocksValidated.Load())
	// No validation dependencies are installed: crossing admission while paused
	// would attempt a mutation and panic instead of returning cancellation.
	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Zero(t, authority.cachedReads.Load())
}

func TestCatchupAdmission_QueueOwnershipSurvivesCacheTTL(t *testing.T) {
	server, _ := newCatchupAdmissionAuthority(t, blockchain.FSMStateIDLE.String())
	server.catchupCh = make(chan processBlockCatchup, 1)
	server.processBlockNotify = ttlcache.New[chainhash.Hash, bool](ttlcache.WithTTL[chainhash.Hash, bool](time.Millisecond))
	server.catchupAlternatives = ttlcache.New[chainhash.Hash, []processBlockCatchup](ttlcache.WithTTL[chainhash.Hash, []processBlockCatchup](time.Millisecond))
	blocks := testhelpers.CreateTestBlockChain(t, 3)
	require.True(t, server.enqueueCatchup(processBlockCatchup{block: blocks[1], peerID: "first", baseURL: "http://first"}))
	require.True(t, server.enqueueCatchup(processBlockCatchup{block: blocks[1], peerID: "second", baseURL: "http://second"}))
	require.True(t, server.enqueueCatchup(processBlockCatchup{block: blocks[1], peerID: "second", baseURL: "http://second"}))
	require.False(t, server.enqueueCatchup(processBlockCatchup{block: blocks[2], peerID: "other", baseURL: "http://other"}), "full queue must release rejected ownership")
	time.Sleep(5 * time.Millisecond)
	server.processBlockNotify.DeleteExpired()
	server.catchupAlternatives.DeleteExpired()
	require.NotNil(t, server.processBlockNotify.Get(*blocks[1].Hash()))
	alternatives := server.catchupAlternatives.Get(*blocks[1].Hash())
	require.NotNil(t, alternatives)
	require.Len(t, alternatives.Value(), 1, "duplicate peer announcements must not grow retained alternatives")
	require.Len(t, server.catchupQueued, 1)
	require.Nil(t, server.processBlockNotify.Get(*blocks[2].Hash()))
	item := <-server.catchupCh
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	server.processCatchupChItem(ctx, item)
	require.Empty(t, server.catchupQueued, "shutdown must release queue ownership")
	time.Sleep(5 * time.Millisecond)
	server.processBlockNotify.DeleteExpired()
	server.catchupAlternatives.DeleteExpired()
	require.Nil(t, server.processBlockNotify.Get(*blocks[1].Hash()), "released advisory markers must regain expiry")
	require.Nil(t, server.catchupAlternatives.Get(*blocks[1].Hash()))
}

func TestCatchupAdmission_ShutdownReleasesQueuedOwnership(t *testing.T) {
	server, _ := newCatchupAdmissionAuthority(t, blockchain.FSMStateIDLE.String())
	server.catchupCh = make(chan processBlockCatchup, 1)
	block := testhelpers.CreateTestBlockChain(t, 2)[1]
	item := processBlockCatchup{block: block}
	require.True(t, server.enqueueCatchup(item))
	server.stopCatchupQueue()
	require.Empty(t, server.catchupQueued)
	require.Empty(t, server.catchupCh)
	require.Nil(t, server.processBlockNotify.Get(*block.Hash()))
	require.False(t, server.enqueueCatchup(item), "shutdown closes queue admission permanently")
}

func (c *catchupAdmissionAuthority) ReportPeerFailure(ctx context.Context, hash *chainhash.Hash, peerID, kind, message string) error {
	_, err := c.server.ReportPeerFailure(ctx, &blockchain_api.ReportPeerFailureRequest{Hash: hash.CloneBytes(), PeerId: peerID, FailureType: kind, Reason: message})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	if c.retryNotice != nil {
		c.retryNotice()
	}
	return nil
}

func TestCatchupAdmission_PeerFailureRetrySurvivesOldOwner(t *testing.T) {
	server, authority := newCatchupAdmissionAuthority(t, blockchain.FSMStateCATCHINGBLOCKS.String())
	server.catchupCh = make(chan processBlockCatchup, 1)
	defer server.stopCatchupQueue()
	block := testhelpers.CreateTestBlockChain(t, 2)[1]
	item := processBlockCatchup{block: block}
	require.True(t, server.enqueueCatchup(item))
	authority.retryNotice = func() { require.True(t, server.enqueueCatchup(item)) }
	server.catchupFunc = func(context.Context, *model.Block, string, string) error {
		return errors.NewExternalError("peer failed")
	}
	server.processCatchupChItem(context.Background(), <-server.catchupCh)
	require.Len(t, server.catchupCh, 1, "failure notification retry must survive terminal cleanup of previous owner")
	require.Len(t, server.catchupQueued, 1)
	marker := server.processBlockNotify.Get(*block.Hash())
	require.NotNil(t, marker)
	require.True(t, marker.ExpiresAt().IsZero(), "old deferred cleanup must not expire the new queued owner")
}

func TestCatchupAdmission_EarlyExitPreservesRunning(t *testing.T) {
	for _, capped := range []bool{false, true} {
		name := "no catchup range"
		if capped {
			name = "exhausted target"
		}
		t.Run(name, func(t *testing.T) {
			server, authority := newCatchupAdmissionAuthority(t, blockchain.FSMStateRUNNING.String())
			block := testhelpers.CreateTestBlockChain(t, 2)[1]
			server.catchupFunc = func(context.Context, *model.Block, string, string) error { return nil }
			if capped {
				server.settings.BlockValidation.CatchupMaxAttemptsPerBlock = 1
				server.blockCatchupAttempts.Set(*block.Hash(), 1, ttlcache.DefaultTTL)
				server.catchupFunc = func(context.Context, *model.Block, string, string) error {
					t.Error("exhausted target entered catchup")
					return nil
				}
			}
			server.processCatchupChItem(context.Background(), processBlockCatchup{block: block})
			state, err := authority.server.GetStoreFSMState(context.Background())
			require.NoError(t, err)
			require.Equal(t, blockchain.FSMStateRUNNING.String(), state, "admission must not strand RUNNING after a target with no validation work")
		})
	}
}

func (c *catchupAdmissionAuthority) AdmitCatchupWork(ctx context.Context) error {
	state, err := c.server.GetFSMCurrentState(ctx, &emptypb.Empty{})
	if err != nil {
		return errors.UnwrapGRPC(err)
	}
	if state.State != blockchain.FSMStateRUNNING && state.State != blockchain.FSMStateCATCHINGBLOCKS {
		select {
		case c.denied <- struct{}{}:
		default:
		}
		return errors.NewStateError("catchup admission is paused")
	}
	return nil
}

func TestCatchupAdmission_ReadinessRecoversWithoutOperatorResume(t *testing.T) {
	server, authority := newCatchupAdmissionAuthority(t, blockchain.FSMStateRUNNING.String())
	authority.server.SetSubscriptionManagerReadyForTesting(false)
	block := testhelpers.CreateTestBlockChain(t, 2)[1]
	var calls atomic.Int32
	server.catchupFunc = func(context.Context, *model.Block, string, string) error { calls.Add(1); return nil }
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); server.processCatchupChItem(ctx, processBlockCatchup{block: block}) }()
	select {
	case <-authority.denied:
	case <-ctx.Done():
		t.Fatal("unready authority did not suspend admission")
	}
	require.Zero(t, calls.Load())
	state, err := authority.server.GetStoreFSMState(ctx)
	require.NoError(t, err)
	require.Equal(t, blockchain.FSMStateRUNNING.String(), state)
	authority.server.SetSubscriptionManagerReadyForTesting(true)
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("readiness recovery did not resume retained work")
	}
	require.Equal(t, int32(1), calls.Load())
	require.Zero(t, authority.cachedReads.Load())
}
