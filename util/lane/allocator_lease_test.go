package lane_test

import (
	"context"
	"net/url"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	blockchain_store "github.com/bsv-blockchain/teranode/stores/blockchain"
	"github.com/bsv-blockchain/teranode/stores/blockchain/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// cuttable is a lease source a test can cut off from the blockchain service, as a network
// partition would: every call fails until it is restored.
type cuttable struct {
	lane.LeaseSource
	cut atomic.Bool
}

var errCut = errors.NewServiceUnavailableError("cut off from the blockchain service")

func (c *cuttable) OpenLaneLease(ctx context.Context, holder string) (lane.LeaseID, lane.Grant, error) {
	if c.cut.Load() {
		return lane.LeaseID{}, lane.Grant{}, errCut
	}

	return c.LeaseSource.OpenLaneLease(ctx, holder)
}

func (c *cuttable) RenewLaneLease(ctx context.Context, r lane.Renewal) (lane.RenewResult, error) {
	if c.cut.Load() {
		return lane.RenewResult{}, errCut
	}

	return c.LeaseSource.RenewLaneLease(ctx, r)
}

func (c *cuttable) ReleaseLaneLease(ctx context.Context, r lane.Release) (bool, error) {
	if c.cut.Load() {
		return false, errCut
	}

	return c.LeaseSource.ReleaseLaneLease(ctx, r)
}

// localLeaseService is a blockchain service on SQLite in memory, reached through a LocalClient.
func localLeaseService(t *testing.T, grantMin uint64, closeAfter time.Duration) (blockchain.ClientI, blockchain_store.Store) {
	t.Helper()

	storeURL, err := url.Parse("sqlitememory:///")
	require.NoError(t, err)

	tSettings := test.CreateBaseTestSettings(t)
	tSettings.BlockChain.LaneGrantMin = grantMin
	tSettings.BlockChain.LaneGrantDuration = time.Second
	tSettings.BlockChain.LaneLeaseCloseAfter = closeAfter

	store, err := blockchain_store.NewStore(ulogger.TestLogger{}, storeURL, tSettings)
	require.NoError(t, err)

	client, err := blockchain.NewLocalClient(ulogger.TestLogger{}, tSettings, store, nil, nil)
	require.NoError(t, err)

	return client, store
}

// sqliteTime reads a time the SQLite blockchain store wrote, whichever form the driver returns.
func sqliteTime(t *testing.T, v any) time.Time {
	t.Helper()

	switch x := v.(type) {
	case time.Time:
		return x
	case string:
		at, err := time.Parse("2006-01-02 15:04:05.000", x)
		require.NoError(t, err)

		return at
	default:
		require.Failf(t, "unexpected time", "%T %v", v, v)
		return time.Time{}
	}
}

// TestAllocatorStopsBeforeTheServiceCloses: an allocator cut off from the service gives out its
// last position before the service closes its lease, so the lane cannot pass to another holder
// while this one still uses it.
func TestAllocatorStopsBeforeTheServiceCloses(t *testing.T) {
	const closeAfter = 900 * time.Millisecond

	client, store := localLeaseService(t, 1_000_000, closeAfter)
	src := &cuttable{LeaseSource: client}

	a := lane.NewAllocator(src, "pod-3", lane.Timings{Renew: 50 * time.Millisecond, Stop: 300 * time.Millisecond, GrantDuration: time.Hour}, nil)
	t.Cleanup(func() { _ = a.Close() })

	_, err := a.Next(context.Background())
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)
	src.cut.Store(true)

	sweepDone := make(chan struct{})

	go func() {
		defer close(sweepDone)

		for {
			n, err := store.CloseStaleLaneLeases(context.Background(), closeAfter)
			if err != nil || n > 0 {
				return
			}

			time.Sleep(5 * time.Millisecond)
		}
	}()

	var lastOK time.Time

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		_, err := a.Next(ctx)

		cancel()

		if err != nil {
			break
		}

		lastOK = time.Now()
	}

	select {
	case <-sweepDone:
	case <-time.After(10 * time.Second):
		require.Fail(t, "the service never closed the lease")
	}

	var closedAt any
	require.NoError(t, store.GetDB().QueryRow(`SELECT closed_at FROM lane_close`).Scan(&closedAt))
	require.False(t, lastOK.IsZero())
	require.True(t, lastOK.Before(sqliteTime(t, closedAt)), "last position at %s, lease closed at %v", lastOK.UTC(), closedAt)
}

// TestTwoAllocatorsOnOneLaneNeverSharePosition is the end-state test at this layer. Every other
// lane is held, so lanes 0 and 1 pass from one allocator to the next: lane 0 through a release,
// lane 1 through a lapse, where the old holder is cut off and keeps being asked for positions while
// a new holder waits for the lane. No position is ever given out twice, and every one lies inside a
// grant the service recorded in its lane.
func TestTwoAllocatorsOnOneLaneNeverSharePosition(t *testing.T) {
	const closeAfter = 180 * time.Millisecond

	client, store := localLeaseService(t, 200, closeAfter)
	timings := lane.Timings{Renew: 20 * time.Millisecond, Stop: 120 * time.Millisecond, GrantDuration: time.Second}

	newAllocator := func() (*lane.Allocator, *cuttable) {
		src := &cuttable{LeaseSource: client}
		a := lane.NewAllocator(src, "pod", timings, nil)
		require.NoError(t, a.Start(context.Background()))

		return a, src
	}

	waitReady := func(a *lane.Allocator) {
		require.Eventually(t, func() bool { return a.Ready() == nil }, 5*time.Second, time.Millisecond)
	}

	var slots [2]atomic.Pointer[lane.Allocator]

	first, _ := newAllocator()
	waitReady(first)
	slots[0].Store(first)

	second, secondSrc := newAllocator()
	waitReady(second)
	slots[1].Store(second)

	// Every other lane is held for the whole test. The fillers are opened on the store with a long
	// close time, so opening the later ones does not sweep the earlier ones.
	for l := 2; l < 254; l++ {
		_, _, _, err := store.OpenLaneLease(context.Background(), options.LaneLeaseOpen{Holder: "filler", GrantMin: 1, CloseAfter: time.Hour})
		require.NoError(t, err)
	}

	_, err := store.GetDB().Exec(`UPDATE lane_lease SET renewed_at = strftime('%Y-%m-%d %H:%M:%f', 'now', '+3600 seconds') WHERE holder = 'filler'`)
	require.NoError(t, err)

	var (
		mu    sync.Mutex
		taken []uint64
		stop  atomic.Bool
		wg    sync.WaitGroup
	)

	for slot := range slots {
		for w := 0; w < 4; w++ {
			wg.Add(1)

			go func() {
				defer wg.Done()

				var mine []uint64

				for !stop.Load() {
					ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
					v, err := slots[slot].Load().Next(ctx)

					cancel()

					if err == nil {
						mine = append(mine, v)
					}
				}

				mu.Lock()
				taken = append(taken, mine...)
				mu.Unlock()
			}()
		}
	}

	var retired []*lane.Allocator

	// A fixed number of rounds, not a time budget, so a slow runner still passes each lane on.
	for round := 0; round < 12; round++ {
		time.Sleep(150 * time.Millisecond)

		if round%2 == 0 {
			// Lane 0 passes through a release.
			old := slots[0].Load()
			require.NoError(t, old.Close())

			next, _ := newAllocator()
			waitReady(next)
			slots[0].Store(next)

			retired = append(retired, old)

			continue
		}

		// Lane 1 passes through a lapse. The old holder stays in use until the new one holds the lane.
		old := slots[1].Load()
		secondSrc.cut.Store(true)

		next, nextSrc := newAllocator()
		waitReady(next)
		slots[1].Store(next)

		secondSrc = nextSrc
		retired = append(retired, old)
	}

	stop.Store(true)
	wg.Wait()

	for _, a := range append(retired, slots[0].Load(), slots[1].Load()) {
		_ = a.Close()
	}

	require.NotEmpty(t, taken)

	seen := make(map[uint64]bool, len(taken))
	for _, v := range taken {
		require.False(t, seen[v], "position %d:%d given out twice", lane.Lane(v), v&lane.MaxPosition)
		seen[v] = true
	}

	type grant struct{ first, last int64 }

	grants := map[int][]grant{}
	leases := map[int]map[string]bool{}

	rows, err := store.GetDB().Query(`SELECT lane, first, last, lease_id FROM lane_handout WHERE lane < 2`)
	require.NoError(t, err)

	for rows.Next() {
		var (
			l  int
			g  grant
			id []byte
		)

		require.NoError(t, rows.Scan(&l, &g.first, &g.last, &id))
		grants[l] = append(grants[l], g)

		if leases[l] == nil {
			leases[l] = map[string]bool{}
		}

		leases[l][string(id)] = true
	}

	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	require.GreaterOrEqual(t, len(leases[0]), 3, "lane 0 passed between holders")
	require.GreaterOrEqual(t, len(leases[1]), 3, "lane 1 passed between holders")

	for l := range grants {
		sort.Slice(grants[l], func(i, j int) bool { return grants[l][i].first < grants[l][j].first })
	}

	for _, v := range taken {
		l, p := lane.Unpack(v)
		gs := grants[int(l)]
		i := sort.Search(len(gs), func(i int) bool { return gs[i].last >= int64(p) })                             //nolint:gosec // test positions are small
		require.True(t, i < len(gs) && gs[i].first <= int64(p), "position %d:%d lies in no recorded grant", l, p) //nolint:gosec // test positions are small
	}
}
