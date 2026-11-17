package blockchain

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
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

	r, err := tc.server.RenewLaneLease(ctx, &blockchain_api.RenewLaneLeaseRequest{LeaseId: g.LeaseId, Lane: g.Lane, Taken: 250, WantGrant: true})
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

	res, err := src.RenewLaneLease(ctx, lane.Renewal{Lease: id, Lane: g.Lane, Taken: 40, WantGrant: true})
	require.NoError(t, err)
	require.Equal(t, lane.RenewResult{Granted: true, Grant: lane.Grant{Lane: 0, First: 101, Last: 200}}, res)

	res, err = src.RenewLaneLease(ctx, lane.Renewal{Lease: id, Lane: g.Lane, Taken: 120, Settles: lane.Settles{First: 1, LastTaken: 100}})
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

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.LaneGrantMin = 100

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

	_, gB, err := client.OpenLaneLease(ctx, "holder-b")
	require.NoError(t, err)
	require.Equal(t, uint8(1), gB.Lane)
	require.Equal(t, "lapsed", closeKind(t, store, idA))

	res, err := client.RenewLaneLease(ctx, lane.Renewal{Lease: idA, Lane: gA.Lane})
	require.NoError(t, err)
	require.True(t, res.Closed)
}

// The blockchain service treats every open lease as renewed when it starts, so a restart longer
// than the close time closes nothing.
func TestServerInitRenewsEveryOpenLease(t *testing.T) {
	tc := setup(t)
	ctx := context.Background()

	g, err := tc.server.OpenLaneLease(ctx, &blockchain_api.OpenLaneLeaseRequest{Holder: "validator-1"})
	require.NoError(t, err)

	var id lane.LeaseID

	copy(id[:], g.LeaseId)

	ageLaneLeases(t, tc.server.store, 10*time.Minute)

	restarted, err := New(ctx, tc.logger, tc.server.settings, tc.server.store, nil)
	require.NoError(t, err)
	require.NoError(t, restarted.Init(ctx))

	restarted.sweepLaneLeases(ctx)
	require.Empty(t, closeKind(t, tc.server.store, id), "the lease survives the restart")

	// The control: the same silence with no restart in between closes it.
	ageLaneLeases(t, tc.server.store, 10*time.Minute)
	restarted.sweepLaneLeases(ctx)
	require.Equal(t, "lapsed", closeKind(t, tc.server.store, id))
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
