package blockchain

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	"github.com/bsv-blockchain/teranode/settings"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// setLaneLeaseTimings gives hand-built settings the default lease timings, which Init checks.
func setLaneLeaseTimings(s *settings.Settings) {
	s.BlockChain.LaneLeaseRenewInterval = 30 * time.Second
	s.BlockChain.LaneLeaseStopAfter = 2 * time.Minute
	s.BlockChain.LaneLeaseCloseAfter = 3 * time.Minute
	s.BlockChain.LaneGrantDuration = 2 * time.Minute
}

// ageLaneLeases moves every lease's last renewal back by d on a SQLite blockchain store.
func ageLaneLeases(t *testing.T, store blockchain_store.Store, d time.Duration) {
	t.Helper()

	_, err := store.GetDB().Exec(`UPDATE lane_lease SET renewed_at = strftime('%Y-%m-%d %H:%M:%f', renewed_at, $1)`,
		fmt.Sprintf("-%.3f seconds", d.Seconds()))
	require.NoError(t, err)
}

func closeKind(t *testing.T, store blockchain_store.Store, id lane.LeaseID) string {
	t.Helper()

	var kind string

	err := store.GetDB().QueryRow(`SELECT kind FROM lane_close WHERE lease_id = $1`, id[:]).Scan(&kind)
	if err != nil {
		return ""
	}

	return kind
}

// TestServerLaneLeaseOpenRenewRelease covers the three handlers: the first grant's size comes from
// settings, a renewal that asks gets the next stretch above it, and a release closes the lease, so
// a later renewal is answered closed.
func TestServerLaneLeaseOpenRenewRelease(t *testing.T) {
	tc := setup(t)
	ctx := context.Background()

	tc.server.settings.BlockChain.LaneGrantMin = 500

	opens := testutil.ToFloat64(prometheusBlockchainLaneLeaseOpens)
	grants := testutil.ToFloat64(prometheusBlockchainLaneLeaseGrants.WithLabelValues("0"))
	cleans := testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("clean"))

	g, err := tc.server.OpenLaneLease(ctx, &blockchain_api.OpenLaneLeaseRequest{Holder: "validator-1"})
	require.NoError(t, err)
	require.Len(t, g.LeaseId, 16)
	require.Equal(t, uint32(0), g.Lane)
	require.Equal(t, uint64(1), g.First)
	require.Equal(t, uint64(500), g.Last)

	r, err := tc.server.RenewLaneLease(ctx, &blockchain_api.RenewLaneLeaseRequest{LeaseId: g.LeaseId, Lane: g.Lane, WantGrant: true})
	require.NoError(t, err)
	require.Equal(t, &blockchain_api.RenewLaneLeaseResponse{Granted: true, First: 501, Last: 1000}, r)

	rel, err := tc.server.ReleaseLaneLease(ctx, &blockchain_api.ReleaseLaneLeaseRequest{LeaseId: g.LeaseId, Lane: g.Lane, LastTaken: 600, SettlesFirst: 1, SettlesLastTaken: 500})
	require.NoError(t, err)
	require.False(t, rel.AlreadyClosed)

	r, err = tc.server.RenewLaneLease(ctx, &blockchain_api.RenewLaneLeaseRequest{LeaseId: g.LeaseId, Lane: g.Lane})
	require.NoError(t, err)
	require.True(t, r.Closed, "a released lease is answered closed")

	require.Equal(t, opens+1, testutil.ToFloat64(prometheusBlockchainLaneLeaseOpens))
	require.Equal(t, grants+2, testutil.ToFloat64(prometheusBlockchainLaneLeaseGrants.WithLabelValues("0")))
	require.Equal(t, cleans+1, testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("clean")))
}

func TestServerLaneLeaseRefusesBadRequests(t *testing.T) {
	tc := setup(t)
	ctx := context.Background()

	_, err := tc.server.OpenLaneLease(ctx, &blockchain_api.OpenLaneLeaseRequest{Holder: strings.Repeat("h", maxLaneHolderBytes+1)})
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrInvalidArgument), "got %v", err)

	_, err = tc.server.RenewLaneLease(ctx, &blockchain_api.RenewLaneLeaseRequest{LeaseId: []byte{1, 2, 3}})
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrInvalidArgument), "got %v", err)

	_, err = tc.server.ReleaseLaneLease(ctx, &blockchain_api.ReleaseLaneLeaseRequest{LeaseId: make([]byte, 16), Lane: 256})
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrInvalidArgument), "got %v", err)
}

// TestClientLaneLeaseOverGRPC sends the lease calls through a real gRPC client and server.
func TestClientLaneLeaseOverGRPC(t *testing.T) {
	_, s, _ := startAuthenticatedBlockchain(t, false)
	s.BlockChain.LaneGrantMin = 100

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := NewClient(ctx, ulogger.NewErrorTestLogger(t), s, "lane-client")
	require.NoError(t, err)

	defer func() { require.NoError(t, client.(*Client).Close()) }()

	var src lane.LeaseSource = client

	id, g, err := src.OpenLaneLease(ctx, "holder-a")
	require.NoError(t, err)
	require.False(t, id.IsZero())
	require.Equal(t, lane.Grant{Lane: 0, First: 1, Last: 100}, g)

	res, err := src.RenewLaneLease(ctx, lane.Renewal{Lease: id, Lane: g.Lane, WantGrant: true})
	require.NoError(t, err)
	require.Equal(t, lane.RenewResult{Granted: true, Grant: lane.Grant{Lane: 0, First: 101, Last: 200}}, res)

	res, err = src.RenewLaneLease(ctx, lane.Renewal{Lease: id, Lane: g.Lane, Rate: 3})
	require.NoError(t, err)
	require.Equal(t, lane.RenewResult{Grant: lane.Grant{Lane: 0}}, res)

	already, err := src.ReleaseLaneLease(ctx, lane.Release{Lease: id, Lane: g.Lane, LastTaken: 120})
	require.NoError(t, err)
	require.False(t, already)

	already, err = src.ReleaseLaneLease(ctx, lane.Release{Lease: id, Lane: g.Lane, LastTaken: 120})
	require.NoError(t, err)
	require.True(t, already)

	id2, g2, err := src.OpenLaneLease(ctx, "holder-b")
	require.NoError(t, err)
	require.NotEqual(t, id, id2)
	require.Equal(t, lane.Grant{Lane: 1, First: 1, Last: 100}, g2, "a never-used lane comes before a closed one")
}

func newLocalLeaseClient(t *testing.T) (*LocalClient, blockchain_store.Store) {
	t.Helper()

	// The lease close counters are registered by the service, which these tests do not build.
	initPrometheusMetrics()

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.LaneGrantMin = 100
	tSettings.BlockChain.LaneGrantDuration = 10 * time.Second

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	client, err := NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	return client.(*LocalClient), store
}

// An open sweeps first, so a lease silent past the close time is closed as lapsed when the next
// process opens one, even with no blockchain service running a sweep.
func TestLocalClientLaneLeaseClosesAStaleLeaseOnOpen(t *testing.T) {
	ctx := context.Background()
	client, store := newLocalLeaseClient(t)

	idA, gA, err := client.OpenLaneLease(ctx, "holder-a")
	require.NoError(t, err)
	require.Equal(t, uint8(0), gA.Lane)

	ageLaneLeases(t, store, 10*time.Minute)

	lapsed := testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("lapsed"))
	cleans := testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("clean"))

	_, gB, err := client.OpenLaneLease(ctx, "holder-b")
	require.NoError(t, err)
	require.Equal(t, uint8(1), gB.Lane)
	require.Equal(t, "lapsed", closeKind(t, store, idA))
	require.Equal(t, lapsed+1, testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("lapsed")), "the open's lapsed close is counted")
	require.Equal(t, cleans, testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("clean")))

	res, err := client.RenewLaneLease(ctx, lane.Renewal{Lease: idA, Lane: gA.Lane})
	require.NoError(t, err)
	require.True(t, res.Closed)
}

func TestServerSweepClosesASilentLease(t *testing.T) {
	tc := setup(t)

	tc.server.settings.BlockChain.LaneLeaseCloseAfter = 50 * time.Millisecond
	tc.server.settings.BlockChain.LaneLeaseRenewInterval = 10 * time.Millisecond

	lapsed := testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("lapsed"))

	g, err := tc.server.OpenLaneLease(context.Background(), &blockchain_api.OpenLaneLeaseRequest{Holder: "validator-1"})
	require.NoError(t, err)

	var id lane.LeaseID

	copy(id[:], g.LeaseId)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go tc.server.startLaneLeaseSweep(ctx)

	require.Eventually(t, func() bool { return closeKind(t, tc.server.store, id) == "lapsed" }, time.Second, 10*time.Millisecond)
	require.Equal(t, lapsed+1, testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("lapsed")))
}

func TestServerRefusesToStartWithStopNotShorterThanClose(t *testing.T) {
	tc := setup(t)

	tSettings := *tc.server.settings
	tSettings.BlockChain.LaneLeaseStopAfter = 3 * time.Minute
	tSettings.BlockChain.LaneLeaseCloseAfter = 3 * time.Minute

	b, err := New(context.Background(), tc.logger, &tSettings, tc.server.store, nil)
	require.NoError(t, err)

	err = b.Init(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "blockchain_laneLeaseStopAfter")
}

// bestHeightStore answers with a best block at a chosen height, so the height a grant records is
// not the zero a fresh store would give anyway.
type bestHeightStore struct {
	blockchain_store.Store
	height uint32
}

func (s *bestHeightStore) GetBestBlockHeader(context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, &model.BlockHeaderMeta{Height: s.height}, nil
}

// Each grant's hand-out row records the best block's height at the time, on open and on renewal.
func TestServerLaneLeaseRecordsTheBestBlockHeight(t *testing.T) {
	tc := setup(t)

	store := tc.server.store
	tc.server.store = &bestHeightStore{Store: store, height: 812_345}

	g, err := tc.server.OpenLaneLease(context.Background(), &blockchain_api.OpenLaneLeaseRequest{Holder: "validator-1"})
	require.NoError(t, err)

	tc.server.store = &bestHeightStore{Store: store, height: 812_346}

	r, err := tc.server.RenewLaneLease(context.Background(), &blockchain_api.RenewLaneLeaseRequest{LeaseId: g.LeaseId, Lane: g.Lane, WantGrant: true})
	require.NoError(t, err)

	for first, want := range map[uint64]int64{g.First: 812_345, r.First: 812_346} {
		var height int64

		require.NoError(t, store.GetDB().QueryRow(`SELECT height FROM lane_handout WHERE lane = $1 AND first = $2`,
			int(g.Lane), lane.ToSigned(first)).Scan(&height))
		require.Equal(t, want, height)
	}
}

// switchable is a lease source that a test can take down, as a blockchain service restart does.
type switchable struct {
	lane.LeaseSource
	down atomic.Bool
}

func (s *switchable) OpenLaneLease(ctx context.Context, holder string) (lane.LeaseID, lane.Grant, error) {
	if s.down.Load() {
		return lane.LeaseID{}, lane.Grant{}, errors.NewServiceUnavailableError("blockchain service restarting")
	}

	return s.LeaseSource.OpenLaneLease(ctx, holder)
}

func (s *switchable) RenewLaneLease(ctx context.Context, r lane.Renewal) (lane.RenewResult, error) {
	if s.down.Load() {
		return lane.RenewResult{}, errors.NewServiceUnavailableError("blockchain service restarting")
	}

	return s.LeaseSource.RenewLaneLease(ctx, r)
}

func (s *switchable) ReleaseLaneLease(ctx context.Context, r lane.Release) (bool, error) {
	return s.LeaseSource.ReleaseLaneLease(ctx, r)
}

// TestLanePositionsSurviveABlockchainRestart takes the blockchain service away and builds a new one
// over the same store, while an allocator keeps taking positions. There is no startup renewal: an
// outage shorter than the stop costs nothing, and a longer one closes the lease, after which the
// allocator opens a new one. Either way no position is given out twice.
func TestLanePositionsSurviveABlockchainRestart(t *testing.T) {
	timings := lane.Timings{Renew: 20 * time.Millisecond, Stop: 300 * time.Millisecond, GrantDuration: time.Minute}

	run := func(t *testing.T, outage time.Duration) (taken []uint64, failures int, store blockchain_store.Store) {
		t.Helper()

		tc := setup(t)
		tc.server.settings.BlockChain.LaneGrantMin = 1_000_000
		tc.server.settings.BlockChain.LaneLeaseRenewInterval = timings.Renew
		tc.server.settings.BlockChain.LaneLeaseStopAfter = timings.Stop
		tc.server.settings.BlockChain.LaneLeaseCloseAfter = 450 * time.Millisecond

		client, err := NewLocalClient(tc.logger, tc.server.settings, tc.server.store, nil, nil)
		require.NoError(t, err)

		src := &switchable{LeaseSource: client}
		a := lane.NewAllocator(src, "validator-1", timings, nil)
		t.Cleanup(func() { _ = a.Close() })

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		go tc.server.startLaneLeaseSweep(ctx)

		take := func(d time.Duration) {
			for end := time.Now().Add(d); time.Now().Before(end); {
				tctx, tcancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
				v, err := a.Next(tctx)

				tcancel()

				if err != nil {
					failures++
					continue
				}

				taken = append(taken, v)
			}
		}

		take(100 * time.Millisecond)

		// The service goes away; a new one is built over the same store and starts sweeping.
		src.down.Store(true)

		restarted, err := New(context.Background(), tc.logger, tc.server.settings, tc.server.store, nil)
		require.NoError(t, err)
		require.NoError(t, restarted.Init(context.Background()))

		go restarted.startLaneLeaseSweep(ctx)

		take(outage)
		src.down.Store(false)

		failures = 0 // only failures after the service is back count

		take(time.Second)

		return taken, failures, tc.server.store
	}

	requireUnique := func(t *testing.T, taken []uint64) {
		t.Helper()

		seen := make(map[uint64]bool, len(taken))
		for _, v := range taken {
			require.False(t, seen[v], "position %d given out twice", v)
			seen[v] = true
		}
	}

	t.Run("an outage shorter than the stop keeps the lease", func(t *testing.T) {
		taken, failures, store := run(t, 150*time.Millisecond)
		requireUnique(t, taken)

		var closes int
		require.NoError(t, store.GetDB().QueryRow(`SELECT COUNT(*) FROM lane_close`).Scan(&closes))
		require.Zero(t, closes, "no lease closed")
		require.LessOrEqual(t, failures, 2, "creates resume once the service is back")
	})

	t.Run("an outage longer than the close opens a new lease", func(t *testing.T) {
		taken, _, store := run(t, 800*time.Millisecond)
		requireUnique(t, taken)

		var closes int
		require.NoError(t, store.GetDB().QueryRow(`SELECT COUNT(*) FROM lane_close WHERE kind = 'lapsed'`).Scan(&closes))
		require.Equal(t, 1, closes, "the silent lease lapsed")

		var leases int
		require.NoError(t, store.GetDB().QueryRow(`SELECT COUNT(*) FROM lane_lease WHERE closed_at IS NULL`).Scan(&leases))
		require.Equal(t, 1, leases, "the allocator holds a new lease")
	})
}

// The daemon cancels its context before it closes its clients, so a lane client built on that
// context would have closed its connection before the allocator's release went out. The release
// must still land, and close the lease clean.
func TestLaneClientReleasesAfterItsContextEnds(t *testing.T) {
	b, s, _ := startAuthenticatedBlockchain(t, false)

	ctx, cancel := context.WithCancel(context.Background())

	client, err := NewLaneClient(ctx, ulogger.NewErrorTestLogger(t), s)
	require.NoError(t, err)

	a := lane.NewAllocator(client, "pod", lane.Timings{Renew: time.Hour, Stop: 2 * time.Hour, GrantDuration: time.Hour}, nil)

	_, err = a.Next(context.Background())
	require.NoError(t, err)

	// Shutdown: the context ends first, and the clients close after.
	cancel()
	time.Sleep(300 * time.Millisecond)

	require.NoError(t, a.Close())

	var kind string
	require.NoError(t, b.store.GetDB().QueryRow(`SELECT kind FROM lane_close`).Scan(&kind))
	require.Equal(t, "clean", kind)
}

// A blockchain service that accepts the connection and never answers must not hold up a process
// that is shutting down: the lane client's startup calls end with its context, even though the
// client it would build is detached from that context.
func TestLaneClientStartEndsWithItsContext(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}

			t.Cleanup(func() { _ = conn.Close() }) // hold it open, answer nothing
		}
	}()

	s := test.CreateBaseTestSettings(t)
	s.BlockChain.GRPCAddress = ln.Addr().String()
	s.BlockChain.MaxRetries = 0

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)

	go func() {
		_, err := NewLaneClient(ctx, ulogger.TestLogger{}, s)
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("NewLaneClient did not return after its context ended")
	}
}

// TestLaneLeaseRateSettlesAndLastTakenCrossTheWire sends a renewal and a release through a real
// gRPC client and server and reads the rows they wrote. The rate and the grant time set the grant's
// size, the settles pair lands on the grant's hand-out row, and the last position taken and the
// settles pair land on the close row. A second release of the closed lease is not a clean close.
func TestLaneLeaseRateSettlesAndLastTakenCrossTheWire(t *testing.T) {
	server, s, _ := startTunedBlockchain(t, false, func(tuned *settings.Settings) {
		tuned.BlockChain.LaneGrantMin = 100
		tuned.BlockChain.LaneGrantDuration = 10 * time.Second
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := NewClient(ctx, ulogger.NewErrorTestLogger(t), s, "lane-wire-client")
	require.NoError(t, err)

	defer func() { require.NoError(t, client.(*Client).Close()) }()

	requireLaneLeaseRoundTrip(t, client, server.store)
}

// TestLocalClientCarriesRateSettlesAndLastTaken is the same round trip through LocalClient, which
// a single-process daemon uses in place of the gRPC client.
func TestLocalClientCarriesRateSettlesAndLastTaken(t *testing.T) {
	client, store := newLocalLeaseClient(t)

	requireLaneLeaseRoundTrip(t, client, store)
}

// requireLaneLeaseRoundTrip opens, renews with a rate and a settles pair, and releases through
// src, and reads the rows the service wrote. It needs blockchain_laneGrantMin 100 and
// blockchain_laneGrantDuration 10s on a store with no lease yet.
func requireLaneLeaseRoundTrip(t *testing.T, src lane.LeaseSource, store blockchain_store.Store) {
	t.Helper()

	ctx := t.Context()

	cleans := testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("clean"))

	id, opened, err := src.OpenLaneLease(ctx, "holder-wire")
	require.NoError(t, err)
	require.Equal(t, lane.Grant{Lane: 0, First: 1, Last: 100}, opened)

	// 1,000 positions a second for 10 seconds is 10,000 positions, far above the minimum of 100.
	res, err := src.RenewLaneLease(ctx, lane.Renewal{
		Lease: id, Lane: opened.Lane, Rate: 1000, WantGrant: true,
		Settles: lane.Settles{First: 1, LastTaken: 80},
	})
	require.NoError(t, err)
	require.Equal(t, lane.RenewResult{Granted: true, Grant: lane.Grant{Lane: 0, First: 101, Last: 10100}}, res)

	var settlesFirst, settlesLastTaken sql.NullInt64

	err = store.GetDB().QueryRow(`SELECT settles_first, settles_last_taken FROM lane_handout WHERE lane = 0 AND first = 101`).
		Scan(&settlesFirst, &settlesLastTaken)
	require.NoError(t, err)
	require.Equal(t, sql.NullInt64{Int64: 1, Valid: true}, settlesFirst)
	require.Equal(t, sql.NullInt64{Int64: 80, Valid: true}, settlesLastTaken)

	already, err := src.ReleaseLaneLease(ctx, lane.Release{
		Lease: id, Lane: opened.Lane, LastTaken: 9000,
		Settles: lane.Settles{First: 101, LastTaken: 9000},
	})
	require.NoError(t, err)
	require.False(t, already)

	var last int64

	err = store.GetDB().QueryRow(`SELECT last, settles_first, settles_last_taken FROM lane_close WHERE lease_id = $1`, id[:]).
		Scan(&last, &settlesFirst, &settlesLastTaken)
	require.NoError(t, err)
	require.Equal(t, int64(9000), last)
	require.Equal(t, sql.NullInt64{Int64: 101, Valid: true}, settlesFirst)
	require.Equal(t, sql.NullInt64{Int64: 9000, Valid: true}, settlesLastTaken)

	already, err = src.ReleaseLaneLease(ctx, lane.Release{Lease: id, Lane: opened.Lane, LastTaken: 9000})
	require.NoError(t, err)
	require.True(t, already)

	require.Equal(t, cleans+1, testutil.ToFloat64(prometheusBlockchainLaneLeaseCloses.WithLabelValues("clean")), "only the first release is a clean close")
}

// TestStartedServiceSweepsASilentLease checks that Start runs the sweep: a lease that nobody
// renews is closed as lapsed with no call to the sweep from the test.
func TestStartedServiceSweepsASilentLease(t *testing.T) {
	server, _, _ := startTunedBlockchain(t, false, func(tuned *settings.Settings) {
		tuned.BlockChain.LaneLeaseRenewInterval = 10 * time.Millisecond
		tuned.BlockChain.LaneLeaseStopAfter = 30 * time.Millisecond
		tuned.BlockChain.LaneLeaseCloseAfter = 60 * time.Millisecond
	})

	g, err := server.OpenLaneLease(context.Background(), &blockchain_api.OpenLaneLeaseRequest{Holder: "validator-silent"})
	require.NoError(t, err)

	var id lane.LeaseID

	copy(id[:], g.LeaseId)

	require.Eventually(t, func() bool { return closeKind(t, server.store, id) == "lapsed" }, 5*time.Second, 10*time.Millisecond)
}

// deadlineStore records how long the context of a sweep had left when the sweep began.
type deadlineStore struct {
	blockchain_store.Store
	left atomic.Int64
}

func (s *deadlineStore) CloseStaleLaneLeases(ctx context.Context, _ time.Duration) (int, error) {
	if deadline, ok := ctx.Deadline(); ok {
		s.left.Store(int64(time.Until(deadline)))
	}

	return 0, nil
}

// A sweep holds row locks that every open needs, so it runs under a deadline of one renewal
// interval, and at least 10 seconds when the interval is shorter.
func TestLaneLeaseSweepHasADeadline(t *testing.T) {
	for _, tc := range []struct {
		name  string
		renew time.Duration
		want  time.Duration
	}{
		{"a short interval gets 10 seconds", 10 * time.Millisecond, 10 * time.Second},
		{"a long interval gets the interval", 30 * time.Second, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setupCtx := setup(t)
			setupCtx.server.settings.BlockChain.LaneLeaseRenewInterval = tc.renew

			store := &deadlineStore{Store: setupCtx.server.store}
			b, err := New(context.Background(), setupCtx.logger, setupCtx.server.settings, store, nil)
			require.NoError(t, err)

			b.sweepLaneLeases(context.Background())

			left := time.Duration(store.left.Load())
			require.LessOrEqual(t, left, tc.want, "a sweep must not run without its deadline")
			require.Greater(t, left, tc.want-time.Second)
		})
	}
}
