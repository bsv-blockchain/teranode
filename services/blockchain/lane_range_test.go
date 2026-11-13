package blockchain

import (
	"context"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain/blockchain_api"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestServerReserveLaneRange covers the handler: size comes from settings, a second ask for
// the same lane continues above the first range, and a lane above 255 is rejected.
func TestServerReserveLaneRange(t *testing.T) {
	tc := setup(t)
	ctx := context.Background()

	tc.server.settings.BlockChain.LaneRangeSize = 500

	before := testutil.ToFloat64(prometheusBlockchainLaneHandouts.WithLabelValues("0"))

	first, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "validator-1"})
	require.NoError(t, err)
	require.Equal(t, &blockchain_api.ReserveLaneRangeResponse{Lane: 0, First: 1, Last: 500}, first)

	second, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "validator-1", WantLane: first.Lane, HasLane: true})
	require.NoError(t, err)
	require.Equal(t, uint64(501), second.First)
	require.Equal(t, uint64(1000), second.Last)

	// A second holder naming no lane is assigned one that is free.
	other, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "validator-2"})
	require.NoError(t, err)
	require.NotEqual(t, first.Lane, other.Lane, "a lane with a recent hand-out is not free")

	require.Equal(t, before+2, testutil.ToFloat64(prometheusBlockchainLaneHandouts.WithLabelValues("0")))

	_, err = tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "x", WantLane: 256, HasLane: true})
	require.Error(t, err)
}

// A holder is kept for ever in the insert-only hand-out table, so its length is bounded.
func TestServerReserveLaneRangeRefusesALongHolder(t *testing.T) {
	tc := setup(t)
	ctx := context.Background()

	_, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: strings.Repeat("h", maxLaneHolderBytes+1)})
	require.Error(t, err)
	require.True(t, errors.Is(errors.UnwrapGRPC(err), errors.ErrInvalidArgument), "got %v", err)

	_, err = tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: strings.Repeat("h", maxLaneHolderBytes)})
	require.NoError(t, err, "the limit itself is allowed")
}

// blockchain_laneFreeAfter has to reach the store. A never-used lane wins before the window is
// consulted, so this fills all 256 lanes first, and then the setting alone decides the answer.
func TestServerReserveLaneRangeUsesLaneFreeAfter(t *testing.T) {
	tc := setup(t)
	ctx := context.Background()

	tc.server.settings.BlockChain.LaneRangeSize = 10
	tc.server.settings.BlockChain.LaneFreeAfter = time.Hour

	for l := 0; l <= lane.MaxLane; l++ {
		_, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "filler", WantLane: uint32(l), HasLane: true}) //nolint:gosec // l is at most 255
		require.NoError(t, err)
	}

	_, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "new"})
	require.Error(t, err, "every lane had a hand-out inside the window")
	require.Contains(t, err.Error(), "no free lane")

	tc.server.settings.BlockChain.LaneFreeAfter = 0

	r, err := tc.server.ReserveLaneRange(ctx, &blockchain_api.ReserveLaneRangeRequest{Holder: "new"})
	require.NoError(t, err, "with no window every lane is free")
	require.Equal(t, uint64(11), r.First, "a reused lane carries on above its old positions")
}

// A LocalClient reaches the shared hand-out code with no Server or gRPC Client built, and those are
// what register the metrics. In a run where nothing else in the package has registered them yet,
// this panics on a nil metric without the registration in reserveLaneRange.
func TestLocalClientReserveLaneRange(t *testing.T) {
	ctx := context.Background()

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.LaneRangeSize = 100

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	client, err := NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	var src lane.RangeSource = client

	r1, err := src.ReserveLaneRange(ctx, "holder-a", 0, false)
	require.NoError(t, err)
	require.Equal(t, lane.Range{Lane: 0, First: 1, Last: 100}, r1)

	r2, err := src.ReserveLaneRange(ctx, "holder-a", r1.Lane, true)
	require.NoError(t, err)
	require.Equal(t, lane.Range{Lane: 0, First: 101, Last: 200}, r2)

	r3, err := src.ReserveLaneRange(ctx, "holder-b", 0, false)
	require.NoError(t, err)
	require.Equal(t, lane.Range{Lane: 1, First: 1, Last: 100}, r3)
}

// TestLocalClientReserveLaneRange cannot guard the metric registration in a normal package run,
// because another test has usually run the sync.Once by then. A process running only that test
// starts with the metrics unregistered, and panics on a nil metric without the registration.
func TestLocalClientRegistersTheMetricsInAFreshProcess(t *testing.T) {
	cmd := exec.Command(os.Args[0], "-test.run=^TestLocalClientReserveLaneRange$", "-test.count=1", "-test.v") //nolint:gosec // re-runs this test binary
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "%s", output)
	require.Contains(t, string(output), "--- PASS: TestLocalClientReserveLaneRange", "the test ran in the child process")
}

// bestHeightStore answers with a best block at a chosen height, so the height a hand-out records
// is not the zero a fresh store would give anyway.
type bestHeightStore struct {
	blockchain_store.Store
	height uint32
}

func (s *bestHeightStore) GetBestBlockHeader(context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error) {
	return nil, &model.BlockHeaderMeta{Height: s.height}, nil
}

// The hand-out's row records the best block's height at the time.
func TestServerReserveLaneRangeRecordsTheBestBlockHeight(t *testing.T) {
	tc := setup(t)

	store := tc.server.store
	tc.server.store = &bestHeightStore{Store: store, height: 812_345}

	r, err := tc.server.ReserveLaneRange(context.Background(), &blockchain_api.ReserveLaneRangeRequest{Holder: "validator-1"})
	require.NoError(t, err)

	var height int64

	require.NoError(t, store.GetDB().QueryRow(`SELECT height FROM lane_handout WHERE lane = $1 AND first = $2`,
		int(r.Lane), lane.ToSigned(r.First)).Scan(&height))
	require.Equal(t, int64(812_345), height)
}

// TestClientReserveLaneRangeOverGRPC sends the request through a real gRPC client and server.
func TestClientReserveLaneRangeOverGRPC(t *testing.T) {
	_, s, _ := startAuthenticatedBlockchain(t, false)
	s.BlockChain.LaneRangeSize = 100

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	client, err := NewClient(ctx, ulogger.NewErrorTestLogger(t), s, "lane-client")
	require.NoError(t, err)

	defer func() { require.NoError(t, client.(*Client).Close()) }()

	var src lane.RangeSource = client

	r1, err := src.ReserveLaneRange(ctx, "holder-a", 0, false)
	require.NoError(t, err)
	require.Equal(t, lane.Range{Lane: 0, First: 1, Last: 100}, r1)

	r2, err := src.ReserveLaneRange(ctx, "holder-a", r1.Lane, true)
	require.NoError(t, err)
	require.Equal(t, lane.Range{Lane: 0, First: 101, Last: 200}, r2)

	r3, err := src.ReserveLaneRange(ctx, "holder-b", 0, false)
	require.NoError(t, err)
	require.Equal(t, lane.Range{Lane: 1, First: 1, Last: 100}, r3, "the wire carries no lane as no lane, not as lane 0")
}
