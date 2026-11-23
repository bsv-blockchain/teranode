package collect

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/services/probe/guard"
	"github.com/bsv-blockchain/teranode/services/probe/snapshot"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

type chainRig struct {
	bc    blockchain.ClientI
	tip   *chainhash.Hash
	chain *Chain
}

func newChainRig(t *testing.T) *chainRig {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, &url.URL{Scheme: "sqlitememory"}, tSettings)
	require.NoError(t, err)

	bc, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	rig := &chainRig{bc: bc, tip: tSettings.ChainCfgParams.GenesisHash}
	rig.chain = NewChain(bc, ulogger.TestLogger{})

	return rig
}

func (r *chainRig) addBlock(t *testing.T, height uint32) {
	t.Helper()

	coinbaseTx := bt.NewTx()
	require.NoError(t, coinbaseTx.From("0000000000000000000000000000000000000000000000000000000000000000", 0xffffffff, "", 0))

	coinbaseTx.Inputs[0].UnlockingScript = bscript.NewFromBytes([]byte{0x03, byte(height & 0xff), byte((height >> 8) & 0xff), byte((height >> 16) & 0xff)})
	coinbaseTx.Inputs[0].SequenceNumber = 0xffffffff
	require.NoError(t, coinbaseTx.AddP2PKHOutputFromAddress("mrs6FYWPcb441b4qfcEPyvLvzj64WHtwCU", 5000000000))

	merkleRoot := chainhash.HashH(coinbaseTx.Bytes())
	subtreeHash := chainhash.HashH([]byte(fmt.Sprintf("subtree-%d", height)))

	block := &model.Block{
		Header: &model.BlockHeader{
			Version:        1,
			HashPrevBlock:  r.tip,
			HashMerkleRoot: &merkleRoot,
			Timestamp:      1231006505 + height,
			Bits:           model.NBit{0xff, 0xff, 0x00, 0x1d},
			Nonce:          height,
		},
		CoinbaseTx:       coinbaseTx,
		Subtrees:         []*chainhash.Hash{&subtreeHash},
		TransactionCount: 1,
		SizeInBytes:      1000,
	}

	require.NoError(t, r.bc.AddBlock(context.Background(), block, ""))

	r.tip = block.Header.Hash()
}

func collectSnapshot(t *testing.T, c *Chain) snapshot.Snapshot {
	t.Helper()

	patch, err := c.Collect(context.Background())
	require.NoError(t, err)
	require.NotNil(t, patch)

	var s snapshot.Snapshot

	patch(&s)

	return s
}

func TestCollectFSMAndTip(t *testing.T) {
	rig := newChainRig(t)
	rig.addBlock(t, 1)
	rig.addBlock(t, 2)

	s := collectSnapshot(t, rig.chain)

	require.Equal(t, uint32(2), s.Tip)
	require.Equal(t, "RUNNING", s.FSM)
	require.Equal(t, "chain", rig.chain.Name())
}

func TestCollectBlocksWaiting(t *testing.T) {
	rig := newChainRig(t)
	rig.addBlock(t, 1)
	rig.addBlock(t, 2)
	rig.addBlock(t, 3)

	s := collectSnapshot(t, rig.chain)

	require.Subset(t, s.Chain.BlocksWaiting, []uint32{1, 2, 3})
	require.IsIncreasing(t, s.Chain.BlocksWaiting)
	require.GreaterOrEqual(t, s.Chain.PersisterBehind.Value, 3.0)
	require.Equal(t, snapshot.StatusOK, s.Chain.PersisterBehind.Status)
	require.Empty(t, s.Chain.PersisterBehind.Note)
}

func TestCollectBlocksWaitingCapsAtTwentyAndIsNeverNil(t *testing.T) {
	rig := newChainRig(t)

	clock := time.Unix(1_700_000_000, 0)
	rig.chain.now = func() time.Time { return clock }

	empty, err := rig.chain.Collect(context.Background())
	require.NoError(t, err)

	var es snapshot.Snapshot

	empty(&es)
	require.NotNil(t, es.Chain.BlocksWaiting)

	for h := uint32(1); h <= 25; h++ {
		rig.addBlock(t, h)
	}

	clock = clock.Add(chainBacklogEvery)

	s := collectSnapshot(t, rig.chain)
	require.Len(t, s.Chain.BlocksWaiting, 20)
	require.IsIncreasing(t, s.Chain.BlocksWaiting)
	require.Equal(t, snapshot.StatusOK, s.Chain.BlocksWaitingCount.Status)
	require.GreaterOrEqual(t, s.Chain.BlocksWaitingCount.Value, 25.0)
	require.Equal(t, "25 or more", s.Chain.BlocksWaitingCount.Note)
	require.Equal(t, float64(len(es.Chain.BlocksWaiting)), es.Chain.BlocksWaitingCount.Value)
	require.Equal(t, snapshot.StatusOK, es.Chain.BlocksWaitingCount.Status)
}

func TestCollectOwnsOnlyItsFields(t *testing.T) {
	rig := newChainRig(t)
	rig.addBlock(t, 1)

	patch, err := rig.chain.Collect(context.Background())
	require.NoError(t, err)

	s := snapshot.Snapshot{}
	s.Chain.ForkDepth = snapshot.Reading{Value: 7, Status: snapshot.StatusWarn}
	s.Chain.SubscribersDropped = snapshot.Reading{Value: 3, Status: snapshot.StatusCrit}
	s.Assembly.TipLagBlocks = snapshot.Reading{Value: 9, Status: snapshot.StatusOK}

	s.PeerTip = 70
	s.Chain.PeerHeight = snapshot.Reading{Value: 70, Status: snapshot.StatusOK}

	patch(&s)

	require.Equal(t, uint32(70), s.PeerTip)
	require.Equal(t, 70.0, s.Chain.PeerHeight.Value)
	require.Equal(t, 7.0, s.Chain.ForkDepth.Value)
	require.Equal(t, 3.0, s.Chain.SubscribersDropped.Value)
	require.Equal(t, 9.0, s.Assembly.TipLagBlocks.Value)
}

func TestEventsSignalsOnBlock(t *testing.T) {
	rig := newChainRig(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := rig.chain.Events(ctx, guard.RecorderFunc(func(string, any) {}))

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("no initial signal")
	}

	rig.addBlock(t, 1)

	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("no signal after block")
	}

	cancel()

	require.Eventually(t, func() bool {
		select {
		case _, ok := <-ch:
			return !ok
		default:
			return false
		}
	}, 3*time.Second, 10*time.Millisecond)
}

func TestCollectFailureReturnsErrorAndNilPatch(t *testing.T) {
	rig := newChainRig(t)
	rig.addBlock(t, 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	patch, err := rig.chain.Collect(ctx)

	require.Error(t, err)
	require.Nil(t, patch)
}

type countingClient struct {
	blockchain.ClientI
	notPersistedCalls int
	failBacklog       bool
	failErr           error
}

func (c *countingClient) GetBlocksNotPersisted(ctx context.Context, limit int) ([]*model.Block, error) {
	c.notPersistedCalls++

	if c.failBacklog {
		if c.failErr != nil {
			return nil, c.failErr
		}

		return nil, errors.NewServiceError("backlog unavailable")
	}

	return c.ClientI.GetBlocksNotPersisted(ctx, limit)
}

func newCountingRig(t *testing.T) (*chainRig, *countingClient, *time.Time) {
	t.Helper()

	rig := newChainRig(t)
	cc := &countingClient{ClientI: rig.bc}
	rig.chain = NewChain(cc, ulogger.TestLogger{})

	clock := time.Unix(1_700_000_000, 0)
	rig.chain.now = func() time.Time { return clock }

	return rig, cc, &clock
}

func TestBacklogCallsRateLimitedToTenSeconds(t *testing.T) {
	rig, cc, clock := newCountingRig(t)
	rig.addBlock(t, 1)

	first := collectSnapshot(t, rig.chain)
	require.Contains(t, first.Chain.BlocksWaiting, uint32(1))
	require.NotContains(t, first.Chain.BlocksWaiting, uint32(2))

	rig.addBlock(t, 2)
	*clock = clock.Add(9 * time.Second)

	second := collectSnapshot(t, rig.chain)
	require.Equal(t, first.Chain.BlocksWaiting, second.Chain.BlocksWaiting)
	require.Equal(t, first.Chain.PersisterBehind, second.Chain.PersisterBehind)
	require.Equal(t, uint32(2), second.Tip)
	require.Equal(t, 1, cc.notPersistedCalls)

	*clock = clock.Add(time.Second)

	third := collectSnapshot(t, rig.chain)
	require.Contains(t, third.Chain.BlocksWaiting, uint32(2))
	require.Equal(t, 2, cc.notPersistedCalls)
}

func TestPersisterBehindLowerBoundNote(t *testing.T) {
	rig := newChainRig(t)

	for h := uint32(1); h <= 30; h++ {
		rig.addBlock(t, h)
	}

	s := collectSnapshot(t, rig.chain)

	require.Equal(t, 25.0, s.Chain.PersisterBehind.Value)
	require.Equal(t, "25 or more", s.Chain.PersisterBehind.Note)
	require.Equal(t, snapshot.StatusOK, s.Chain.PersisterBehind.Status)
}

func TestBacklogFailureKeepsFSMAndTipLive(t *testing.T) {
	rig, cc, _ := newCountingRig(t)
	rig.addBlock(t, 1)

	cc.failBacklog = true

	s := collectSnapshot(t, rig.chain)

	require.NotEmpty(t, s.FSM)
	require.Equal(t, uint32(1), s.Tip)
	require.Equal(t, snapshot.StatusNoData, s.Chain.PersisterBehind.Status)
	require.Equal(t, snapshot.StatusNoData, s.Chain.BlocksWaitingCount.Status)
	require.Equal(t, noteBacklogStale, s.Chain.BlocksWaitingCount.Note)
	require.Equal(t, noteBacklogStale, s.Chain.PersisterBehind.Note)
	require.Zero(t, s.Chain.BlocksWaitingCount.AgeSec, "a backlog that never had a good read has no age")
	require.Zero(t, s.Chain.PersisterBehind.AgeSec)
	require.NotNil(t, s.Chain.BlocksWaiting)
	require.Empty(t, s.Chain.BlocksWaiting)
}

func TestBacklogFailureLogRedactsCredentials(t *testing.T) {
	rig, cc, _ := newCountingRig(t)
	logger := &chainLogger{}
	rig.chain.logger = logger

	rig.addBlock(t, 1)

	cc.failBacklog = true
	cc.failErr = errors.NewServiceError("dial postgres://probe:s3cret-pass@db.internal:5432/teranode refused")

	collectSnapshot(t, rig.chain)

	warns := logger.matching("WARN", "chain backlog has no data")
	require.Len(t, warns, 1)
	require.NotContains(t, warns[0], "s3cret-pass")
	require.Contains(t, warns[0], "db.internal")
}

func TestBacklogFailureWaitsTenSecondsBeforeRetry(t *testing.T) {
	rig, cc, clock := newCountingRig(t)
	rig.addBlock(t, 1)

	cc.failBacklog = true

	collectSnapshot(t, rig.chain)
	require.Equal(t, 1, cc.notPersistedCalls)

	cc.failBacklog = false
	*clock = clock.Add(9 * time.Second)

	s := collectSnapshot(t, rig.chain)
	require.Equal(t, 1, cc.notPersistedCalls)
	require.Equal(t, snapshot.StatusNoData, s.Chain.PersisterBehind.Status)

	*clock = clock.Add(time.Second)

	s = collectSnapshot(t, rig.chain)
	require.Equal(t, 2, cc.notPersistedCalls)
	require.Contains(t, s.Chain.BlocksWaiting, uint32(1))
	require.Equal(t, snapshot.StatusOK, s.Chain.PersisterBehind.Status)
}

type chainLogger struct {
	ulogger.TestLogger

	mu    sync.Mutex
	lines []string
}

func (l *chainLogger) add(level, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines = append(l.lines, level+" "+fmt.Sprintf(format, args...))
}

func (l *chainLogger) Infof(format string, args ...interface{}) { l.add("INFO", format, args...) }
func (l *chainLogger) Warnf(format string, args ...interface{}) { l.add("WARN", format, args...) }

func (l *chainLogger) matching(level, text string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []string

	for _, line := range l.lines {
		if strings.HasPrefix(line, level+" ") && strings.Contains(line, text) {
			out = append(out, line)
		}
	}

	return out
}

func TestBacklogFailureKeepsLastValuesStaleAndLogsOnce(t *testing.T) {
	rig, cc, clock := newCountingRig(t)
	logger := &chainLogger{}
	rig.chain.logger = logger

	rig.addBlock(t, 1)
	rig.addBlock(t, 2)

	good := collectSnapshot(t, rig.chain)
	require.Contains(t, good.Chain.BlocksWaiting, uint32(2))
	require.Positive(t, good.Chain.BlocksWaitingCount.Value)
	require.Equal(t, snapshot.StatusOK, good.Chain.BlocksWaitingCount.Status)

	cc.failBacklog = true

	for i := 1; i <= 3; i++ {
		*clock = clock.Add(chainBacklogEvery)

		s := collectSnapshot(t, rig.chain)

		require.Equal(t, good.Chain.BlocksWaiting, s.Chain.BlocksWaiting, "the height chips keep their last values")
		require.Equal(t, good.Chain.BlocksWaitingCount.Value, s.Chain.BlocksWaitingCount.Value, "the count keeps its last value, not zero")
		require.Equal(t, snapshot.StatusNoData, s.Chain.BlocksWaitingCount.Status)
		require.Equal(t, snapshot.StatusNoData, s.Chain.PersisterBehind.Status)
		require.Equal(t, good.Chain.PersisterBehind.Value, s.Chain.PersisterBehind.Value)
		require.Equal(t, float64(i)*chainBacklogEvery.Seconds(), s.Chain.BlocksWaitingCount.AgeSec)
		require.Equal(t, uint32(2), s.Tip)
	}

	warns := logger.matching("WARN", "chain backlog has no data")
	require.Len(t, warns, 1)
	require.Contains(t, warns[0], "backlog unavailable")

	cc.failBacklog = false
	*clock = clock.Add(chainBacklogEvery)

	s := collectSnapshot(t, rig.chain)
	require.Equal(t, snapshot.StatusOK, s.Chain.BlocksWaitingCount.Status)
	require.Zero(t, s.Chain.BlocksWaitingCount.AgeSec)
	require.Len(t, logger.matching("INFO", "chain backlog is back"), 1)
}

type cachedIdleClient struct {
	blockchain.ClientI
	fresh    blockchain.FSMStateType
	freshErr error
}

func (c *cachedIdleClient) GetFSMCurrentState(context.Context) (*blockchain.FSMStateType, error) {
	idle := blockchain.FSMStateIDLE
	return &idle, nil
}

func (c *cachedIdleClient) FetchFSMCurrentState(context.Context) (*blockchain.FSMStateType, error) {
	if c.freshErr != nil {
		return nil, c.freshErr
	}

	return &c.fresh, nil
}

func TestFSMIsReadFromTheServerNotTheCachedPlaceholder(t *testing.T) {
	rig := newChainRig(t)
	rig.addBlock(t, 1)

	client := &cachedIdleClient{ClientI: rig.bc, fresh: blockchain.FSMStateCATCHINGBLOCKS}
	rig.chain = NewChain(client, ulogger.TestLogger{})

	s := collectSnapshot(t, rig.chain)
	require.Equal(t, "CATCHINGBLOCKS", s.FSM)

	client.freshErr = errors.NewServiceError("blockchain unreachable")

	patch, err := rig.chain.Collect(context.Background())
	require.Error(t, err, "an unknown FSM state must fail the source, not show IDLE")
	require.Nil(t, patch)
}

type fsmServer struct {
	blockchain_api.UnimplementedBlockchainAPIServer
	state atomic.Int32
	reads atomic.Int32
}

func (s *fsmServer) HealthGRPC(context.Context, *emptypb.Empty) (*blockchain_api.HealthResponse, error) {
	return &blockchain_api.HealthResponse{Ok: true}, nil
}

func (s *fsmServer) GetFSMCurrentState(context.Context, *emptypb.Empty) (*blockchain_api.GetFSMStateResponse, error) {
	state := blockchain_api.FSMStateType(s.state.Load())
	s.reads.Add(1)
	return &blockchain_api.GetFSMStateResponse{State: state}, nil
}

func (s *fsmServer) Subscribe(_ *blockchain_api.SubscribeRequest, stream blockchain_api.BlockchainAPI_SubscribeServer) error {
	<-stream.Context().Done()
	return nil
}

func TestRealBlockchainClientFetchesFSMInsteadOfCachedIdle(t *testing.T) {
	server := &fsmServer{}
	server.state.Store(int32(blockchain.FSMStateIDLE))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	blockchain_api.RegisterBlockchainAPIServer(grpcServer, server)
	go func() { _ = grpcServer.Serve(lis) }()
	t.Cleanup(grpcServer.Stop)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.MaxRetries = 0
	bc, err := blockchain.NewClientWithAddress(ctx, ulogger.TestLogger{}, tSettings, lis.Addr().String(), "probe-test")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bc.(*blockchain.Client).Close()) })

	require.Eventually(t, func() bool { return server.reads.Load() >= 2 }, time.Second, 10*time.Millisecond)
	server.state.Store(int32(blockchain.FSMStateCATCHINGBLOCKS))
	require.Eventually(t, func() bool {
		state, err := bc.GetFSMCurrentState(ctx)
		return err == nil && *state == blockchain.FSMStateIDLE
	}, time.Second, 10*time.Millisecond, "the real client must still hold the cached placeholder")

	got, err := NewChain(bc, ulogger.TestLogger{}).fsmState(ctx)
	require.NoError(t, err)
	require.Equal(t, "CATCHINGBLOCKS", got)
}

type noMinedNotSetClient struct {
	blockchain.ClientI
}

func (c *noMinedNotSetClient) GetBlocksMinedNotSet(context.Context) ([]*model.Block, error) {
	panic("the probe must not call GetBlocksMinedNotSet: it has no limit")
}

func TestBacklogNeverCallsTheUnboundedMinedNotSetQuery(t *testing.T) {
	rig := newChainRig(t)
	rig.addBlock(t, 1)

	rig.chain = NewChain(&noMinedNotSetClient{ClientI: rig.bc}, ulogger.TestLogger{})

	s := collectSnapshot(t, rig.chain)

	require.Contains(t, s.Chain.BlocksWaiting, uint32(1))
	require.Equal(t, snapshot.StatusOK, s.Chain.PersisterBehind.Status)
}

func TestWaitingHeights(t *testing.T) {
	blocks := func(hs ...uint32) []*model.Block {
		out := make([]*model.Block, 0, len(hs))
		for _, h := range hs {
			out = append(out, &model.Block{Height: h})
		}

		return out
	}

	many := make([]uint32, 0, 30)
	for h := uint32(30); h >= 1; h-- {
		many = append(many, h)
	}

	tests := []struct {
		name   string
		groups [][]*model.Block
		want   []uint32
	}{
		{"no groups", nil, []uint32{}},
		{"empty groups", [][]*model.Block{nil, {}}, []uint32{}},
		{"overlap deduped and sorted", [][]*model.Block{blocks(5, 3, 4), blocks(4, 2, 5)}, []uint32{2, 3, 4, 5}},
		{"nil blocks skipped", [][]*model.Block{{nil, {Height: 7}, nil}, {nil}}, []uint32{7}},
		{"duplicate inside group", [][]*model.Block{blocks(1, 1, 1)}, []uint32{1}},
		{"cap keeps the oldest twenty", [][]*model.Block{blocks(many...), blocks(31, 32)}, []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, waitingHeights(tc.groups...))
		})
	}
}
