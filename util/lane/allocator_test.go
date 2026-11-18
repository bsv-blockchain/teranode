package lane

import (
	"context"
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// fakeLeases stands in for the blockchain service at the LeaseSource boundary. It follows the
// service's rules: a lease opens on the lowest lane with no open lease, every grant in a lane starts
// one above that lane's last grant, and a renewal or release of a closed lease is answered closed.
type fakeLeases struct {
	mu       sync.Mutex
	size     uint64
	high     map[uint8]uint64
	open     map[LeaseID]uint8
	down     bool
	gate     chan struct{} // when set, a grant request waits for it to close
	openGate chan struct{} // when set, an open waits for it to close, ignoring the caller's context
	opening  chan struct{} // when set, closed as an open starts waiting on openGate
	asking   chan struct{} // when set, closed as a grant request starts waiting on gate
	opens    int
	renewals []Renewal
	releases []Release
	seq      byte
}

func newFakeLeases(size uint64) *fakeLeases {
	return &fakeLeases{size: size, high: map[uint8]uint64{}, open: map[LeaseID]uint8{}}
}

func (f *fakeLeases) OpenLaneLease(context.Context, string) (LeaseID, Grant, error) {
	f.mu.Lock()
	gate, opening := f.openGate, f.opening
	f.mu.Unlock()

	if gate != nil {
		close(opening)
		<-gate
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.down {
		return LeaseID{}, Grant{}, errors.NewServiceUnavailableError("blockchain service down")
	}

	held := map[uint8]bool{}
	for _, l := range f.open {
		held[l] = true
	}

	var l uint8
	for held[l] {
		l++
	}

	f.seq++
	f.opens++

	id := LeaseID{f.seq}
	f.open[id] = l

	return id, f.grantLocked(l, f.size), nil
}

func (f *fakeLeases) grantLocked(l uint8, size uint64) Grant {
	g := Grant{Lane: l, First: f.high[l] + 1, Last: f.high[l] + size}
	f.high[l] = g.Last

	return g
}

func (f *fakeLeases) RenewLaneLease(_ context.Context, r Renewal) (RenewResult, error) {
	f.mu.Lock()
	gate, asking := f.gate, f.asking
	if r.WantGrant && gate != nil {
		f.asking = nil
	}
	f.mu.Unlock()

	if r.WantGrant && gate != nil {
		if asking != nil {
			close(asking)
		}

		<-gate
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	if f.down {
		return RenewResult{}, errors.NewServiceUnavailableError("blockchain service down")
	}

	f.renewals = append(f.renewals, r)

	if l, ok := f.open[r.Lease]; !ok || l != r.Lane {
		return RenewResult{Closed: true}, nil
	}

	if !r.WantGrant {
		return RenewResult{}, nil
	}

	return RenewResult{Granted: true, Grant: f.grantLocked(r.Lane, f.size)}, nil
}

func (f *fakeLeases) ReleaseLaneLease(_ context.Context, r Release) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.releases = append(f.releases, r)

	if _, ok := f.open[r.Lease]; !ok {
		return true, nil
	}

	delete(f.open, r.Lease)

	return false, nil
}

func (f *fakeLeases) setDown(down bool) {
	f.mu.Lock()
	f.down = down
	f.mu.Unlock()
}

// closeAll closes every lease, as the service's sweep would.
func (f *fakeLeases) closeAll() {
	f.mu.Lock()
	f.open = map[LeaseID]uint8{}
	f.mu.Unlock()
}

func (f *fakeLeases) grantRequests() []Renewal {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []Renewal

	for _, r := range f.renewals {
		if r.WantGrant {
			out = append(out, r)
		}
	}

	return out
}

func (f *fakeLeases) openCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.opens
}

// stepClock is a clock a test moves by hand.
type stepClock struct {
	mu sync.Mutex
	t  time.Time
}

func newStepClock() *stepClock {
	return &stepClock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *stepClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.t
}

func (c *stepClock) step(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// quiet timings: no renewal tick inside a test, so only the half-way and leave signals drive the loop.
var quiet = Timings{Renew: time.Hour, Stop: 2 * time.Hour, GrantDuration: 2 * time.Minute}

func newTestAllocator(t *testing.T, src LeaseSource, tm Timings, clk *stepClock) *Allocator {
	t.Helper()

	a := NewAllocator(src, "pod-3", tm, nil)
	a.takeWait = 2 * time.Second

	if clk != nil {
		a.now = clk.Now
	}

	t.Cleanup(func() { _ = a.Close() })

	return a
}

// next takes one lane position and fails the test if none comes.
func next(t *testing.T, a *Allocator) uint64 {
	t.Helper()

	v, err := a.Next(context.Background())
	require.NoError(t, err)

	return v
}

func (a *Allocator) hasSpareGrant() bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.hasSpare
}

func TestAllocatorOpensALeaseAndHandsOutConsecutivePositions(t *testing.T) {
	a := newTestAllocator(t, newFakeLeases(1000), quiet, nil)
	require.NoError(t, a.Start(context.Background()))

	require.Equal(t, Pack(0, 1), next(t, a))
	require.Equal(t, Pack(0, 2), next(t, a))
	require.Equal(t, Pack(0, 3), next(t, a))
}

// The next grant arrives at half and waits as the spare; positions keep coming from the current
// grant until it is used up, and only then from the spare.
func TestAllocatorKeepsTheNextGrantAsASpare(t *testing.T) {
	src := newFakeLeases(10)
	a := newTestAllocator(t, src, quiet, nil)

	for i := uint64(1); i <= 5; i++ {
		require.Equal(t, Pack(0, i), next(t, a))
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond, "the spare arrives at half")

	for i := uint64(6); i <= 10; i++ {
		require.Equal(t, Pack(0, i), next(t, a), "the current grant is used up first")
	}

	require.Equal(t, Pack(0, 11), next(t, a), "then the spare")
}

func TestAllocatorAsksForTheNextGrantAtHalf(t *testing.T) {
	src := newFakeLeases(10)
	a := newTestAllocator(t, src, quiet, nil)

	for i := 0; i < 4; i++ {
		next(t, a)
	}

	require.Never(t, func() bool { return len(src.grantRequests()) > 0 }, 100*time.Millisecond, 5*time.Millisecond, "not before half")

	next(t, a)

	require.Eventually(t, func() bool { return len(src.grantRequests()) == 1 }, 5*time.Second, time.Millisecond)
}

// A grant is used for at most GrantDuration from its first take. Then the allocator moves to the
// spare, and the grant it left is settled at the highest position it took, which the release carries.
func TestAllocatorMovesToTheSpareTwoMinutesAfterTheFirstTake(t *testing.T) {
	clk := newStepClock()
	src := newFakeLeases(100)
	a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: 10 * time.Minute, GrantDuration: 2 * time.Minute}, clk)

	for i := uint64(1); i <= 50; i++ {
		require.Equal(t, Pack(0, i), next(t, a))
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)

	clk.step(2*time.Minute - time.Second)
	require.Equal(t, Pack(0, 51), next(t, a), "still inside two minutes")

	clk.step(time.Second)
	require.Equal(t, Pack(0, 101), next(t, a), "two minutes after the first take it moves to the spare")

	require.NoError(t, a.Close())

	src.mu.Lock()
	defer src.mu.Unlock()

	require.Len(t, src.releases, 1)
	require.Equal(t, uint64(101), src.releases[0].LastTaken)
	require.Equal(t, Settles{First: 1, LastTaken: 51}, src.releases[0].Settles, "an aged grant settles at the highest position taken")
}

// A used-up grant settles at its last position, and the pair travels on the next grant request.
func TestAllocatorSendsTheLeftGrantsFinalNumberOnTheNextRequest(t *testing.T) {
	src := newFakeLeases(10)
	a := newTestAllocator(t, src, quiet, nil)

	for i := 0; i < 10; i++ {
		next(t, a)
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)
	require.Equal(t, Settles{}, src.grantRequests()[0].Settles, "the first request has nothing to settle")

	for i := 0; i < 5; i++ {
		next(t, a) // 11 to 15: leaves grant 1 used up, and reaches half of grant 2
	}

	require.Eventually(t, func() bool { return len(src.grantRequests()) == 2 }, 5*time.Second, time.Millisecond)
	require.Equal(t, Settles{First: 1, LastTaken: 10}, src.grantRequests()[1].Settles)
}

// The stop: with renewals failing, the allocator gives out positions until two minutes after it
// sent the last renewal that succeeded, and none after.
func TestAllocatorStopsTwoMinutesAfterItsLastGoodRenewal(t *testing.T) {
	clk := newStepClock()
	src := newFakeLeases(1_000_000)
	a := newTestAllocator(t, src, Timings{Renew: 10 * time.Millisecond, Stop: 2 * time.Minute, GrantDuration: time.Hour}, clk)
	a.takeWait = 200 * time.Millisecond

	next(t, a)
	src.setDown(true)

	clk.step(2*time.Minute - time.Second)
	next(t, a)

	clk.step(time.Second)

	start := time.Now()
	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	require.Less(t, time.Since(start), 5*time.Second, "inside its bound")
	require.Error(t, a.Ready())
}

func TestAllocatorTakesANewLeaseWhenItsLeaseWasClosed(t *testing.T) {
	src := newFakeLeases(1000)
	a := newTestAllocator(t, src, Timings{Renew: 10 * time.Millisecond, Stop: time.Hour, GrantDuration: time.Hour}, nil)

	before := next(t, a)

	src.closeAll()

	require.Eventually(t, func() bool { return src.openCount() == 2 }, 5*time.Second, time.Millisecond)

	after := next(t, a)
	require.Equal(t, Lane(before), Lane(after), "the fake gives the lowest free lane again")
	require.Greater(t, after, before, "the new lease's grant starts above the old one")
}

// lyingLeases forgets every lease and every lane's history on each open, so it grants lane 0 from
// position 1 every time, positions this process has already given out.
type lyingLeases struct {
	*fakeLeases
}

func (l lyingLeases) OpenLaneLease(ctx context.Context, holder string) (LeaseID, Grant, error) {
	l.mu.Lock()
	l.high = map[uint8]uint64{}
	l.open = map[LeaseID]uint8{}
	l.mu.Unlock()

	return l.fakeLeases.OpenLaneLease(ctx, holder)
}

func TestAllocatorRefusesAGrantNotAboveItsOwnHighest(t *testing.T) {
	src := lyingLeases{newFakeLeases(1000)}
	a := newTestAllocator(t, src, Timings{Renew: 10 * time.Millisecond, Stop: time.Hour, GrantDuration: time.Hour}, nil)
	a.takeWait = 300 * time.Millisecond

	require.Equal(t, Pack(0, 1), next(t, a))

	refused := testutil.ToFloat64(prometheusLaneRefusedGrants)

	src.closeAll()

	require.Eventually(t, func() bool { return testutil.ToFloat64(prometheusLaneRefusedGrants) > refused }, 5*time.Second, time.Millisecond)

	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, errors.ErrServiceUnavailable, "position 1 is never given out twice")

	// The refused lease is released with nothing taken, so it does not hold lane 0 until it lapses
	// while each retry of open holds another lane.
	src.mu.Lock()
	defer src.mu.Unlock()

	require.NotEmpty(t, src.releases, "a refused lease is released")
	require.Equal(t, uint8(0), src.releases[0].Lane)
	require.Zero(t, src.releases[0].LastTaken)
}

func TestAllocatorReleaseReportsTheHighestPositionTaken(t *testing.T) {
	src := newFakeLeases(1000)
	a := newTestAllocator(t, src, quiet, nil)

	for i := 0; i < 7; i++ {
		next(t, a)
	}

	require.NoError(t, a.Close())

	src.mu.Lock()
	defer src.mu.Unlock()

	require.Len(t, src.releases, 1)
	require.Equal(t, uint64(7), src.releases[0].LastTaken)
	require.Empty(t, src.open, "the lease is closed")
}

func TestNextFailsAfterClose(t *testing.T) {
	a := newTestAllocator(t, newFakeLeases(1000), quiet, nil)
	next(t, a)

	require.NoError(t, a.Close())

	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	require.ErrorIs(t, a.Ready(), errors.ErrServiceUnavailable, "a closed allocator is not ready, though its lease was recent")
}

// laneMetrics reads the per-process lane counters, which are process-wide, so tests compare deltas.
type laneMetrics struct{ positions, grants, waits, failures float64 }

func readLaneMetrics() laneMetrics {
	return laneMetrics{
		positions: testutil.ToFloat64(prometheusLanePositions),
		grants:    testutil.ToFloat64(prometheusLaneGrants),
		waits:     testutil.ToFloat64(prometheusLaneWaits),
		failures:  testutil.ToFloat64(prometheusLaneTakeFailures),
	}
}

func (m laneMetrics) since(before laneMetrics) laneMetrics {
	return laneMetrics{m.positions - before.positions, m.grants - before.grants, m.waits - before.waits, m.failures - before.failures}
}

// Every way a take can end moves the counters: a take served at once or after a wait counts a
// position, a take that waits counts a wait, and each way of failing (the wait running out, the
// caller giving up, a close while waiting, a close before the take) counts a failure. The open and
// the spare each count a grant, and the current-lane gauge shows the lane leased.
func TestAllocatorCountsEveryTakeAndGrant(t *testing.T) {
	src := newFakeLeases(10)
	src.open[LeaseID{0xff}] = 0 // lane 0 is held elsewhere, so this allocator leases lane 1

	a := newTestAllocator(t, src, quiet, nil)
	a.takeWait = 100 * time.Millisecond

	before := readLaneMetrics()

	for i := uint64(1); i <= 10; i++ {
		require.Equal(t, Pack(1, i), next(t, a))
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)
	require.Equal(t, laneMetrics{positions: 10, grants: 2, waits: 1}, readLaneMetrics().since(before),
		"the first take waits for the open; the open and the spare are two grants")
	require.Equal(t, float64(1), testutil.ToFloat64(prometheusLaneCurrent))

	// With the service down, the spare is used up and the next take waits out takeWait.
	src.setDown(true)

	for i := 0; i < 10; i++ {
		next(t, a)
	}

	before = readLaneMetrics()
	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	require.Equal(t, laneMetrics{waits: 1, failures: 1}, readLaneMetrics().since(before), "the wait ran out")

	// The caller gives up while it waits.
	a.takeWait = time.Hour
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	before = readLaneMetrics()
	_, err = a.Next(ctx)
	cancel()
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	require.Equal(t, laneMetrics{waits: 1, failures: 1}, readLaneMetrics().since(before), "the caller gave up")

	// A close while a take waits fails that take.
	before = readLaneMetrics()
	failed := make(chan error)

	go func() {
		_, err := a.Next(context.Background())
		failed <- err
	}()

	require.Eventually(t, func() bool { return readLaneMetrics().since(before).waits == 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, a.Close())
	require.ErrorIs(t, <-failed, errors.ErrServiceUnavailable)
	require.Equal(t, laneMetrics{waits: 1, failures: 1}, readLaneMetrics().since(before), "closed while waiting")

	// A take after the close fails at once, with no wait.
	before = readLaneMetrics()
	_, err = a.Next(context.Background())
	require.ErrorIs(t, err, errors.ErrServiceUnavailable)
	require.Equal(t, laneMetrics{failures: 1}, readLaneMetrics().since(before), "closed before the take")
}

// Next runs at least once per record created, so with a grant in hand it must allocate nothing.
func TestNextDoesNotAllocate(t *testing.T) {
	a := newTestAllocator(t, newFakeLeases(10_000_000), quiet, nil)
	next(t, a)

	ctx := context.Background()

	require.Zero(t, testing.AllocsPerRun(1000, func() {
		if _, err := a.Next(ctx); err != nil {
			t.Fatal(err)
		}
	}))
}

// A process with no validator starts nothing until its first create, so a process that only reads
// the store never takes a lane.
func TestTheFirstNextStartsTheAllocator(t *testing.T) {
	src := newFakeLeases(1000)
	a := newTestAllocator(t, src, quiet, nil)

	require.Never(t, func() bool { return src.openCount() > 0 }, 50*time.Millisecond, 5*time.Millisecond, "building an allocator opens nothing")

	require.Equal(t, Pack(0, 1), next(t, a))
}

// Readiness tracks a live lease, not a grant in hand.
func TestReadyStaysNilWhileAGrantIsUsedUpForAMoment(t *testing.T) {
	src := newFakeLeases(2)
	src.gate = make(chan struct{})
	a := newTestAllocator(t, src, quiet, nil)

	require.Error(t, a.Ready(), "no lease before Start")
	require.NoError(t, a.Start(context.Background()))
	require.Eventually(t, func() bool { return a.Ready() == nil }, 5*time.Second, time.Millisecond)

	next(t, a)
	next(t, a)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := a.Next(ctx)
	require.ErrorIs(t, err, errors.ErrServiceUnavailable, "the grant is used up and the next one is held back")
	require.NoError(t, a.Ready())

	close(src.gate)
	require.Equal(t, Pack(0, 3), next(t, a))
}

// A pair recorded while a grant request is in flight was not on that request, so it stays for the
// next one. Here grant 1 is used up while the request sent at its half waits on the service.
func TestAllocatorKeepsAPairRecordedWhileAGrantRequestIsInFlight(t *testing.T) {
	asking := make(chan struct{})
	src := newFakeLeases(2)
	src.gate, src.asking = make(chan struct{}), asking
	a := newTestAllocator(t, src, quiet, nil)

	require.Equal(t, Pack(0, 1), next(t, a)) // half of grant 1, so a grant request goes out
	<-asking
	require.Equal(t, Pack(0, 2), next(t, a))

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := a.Next(ctx)
	require.ErrorIs(t, err, errors.ErrServiceUnavailable, "grant 1 is used up while the request waits")

	close(src.gate)
	require.Equal(t, Pack(0, 3), next(t, a)) // half of grant 2, so the next grant request goes out

	require.Eventually(t, func() bool { return len(src.grantRequests()) == 2 }, 5*time.Second, time.Millisecond)
	require.Equal(t, Settles{}, src.grantRequests()[0].Settles, "the request in flight carried nothing")
	require.Equal(t, Settles{First: 1, LastTaken: 2}, src.grantRequests()[1].Settles, "grant 1's pair travels on the next request")
}

// hangingLeases makes its first hangOpens opens and its first hangRenews grant requests wait until
// the caller's context ends, as a call on a half-open connection would.
type hangingLeases struct {
	*fakeLeases
	hangOpens, hangRenews atomic.Int32
}

func (h *hangingLeases) OpenLaneLease(ctx context.Context, holder string) (LeaseID, Grant, error) {
	if h.hangOpens.Add(-1) >= 0 {
		<-ctx.Done()
		return LeaseID{}, Grant{}, ctx.Err()
	}

	return h.fakeLeases.OpenLaneLease(ctx, holder)
}

func (h *hangingLeases) RenewLaneLease(ctx context.Context, r Renewal) (RenewResult, error) {
	if r.WantGrant && h.hangRenews.Add(-1) >= 0 {
		<-ctx.Done()
		return RenewResult{}, ctx.Err()
	}

	return h.fakeLeases.RenewLaneLease(ctx, r)
}

// An open that hangs is given up after attemptTimeout and retried, so one stuck call does not
// hold the lease back until Close.
func TestAllocatorGivesUpOnAHungOpenAndRetries(t *testing.T) {
	src := &hangingLeases{fakeLeases: newFakeLeases(1000)}
	src.hangOpens.Store(1)

	a := newTestAllocator(t, src, quiet, nil)
	a.attemptTimeout = 50 * time.Millisecond

	require.Equal(t, Pack(0, 1), next(t, a))
	require.Equal(t, 1, src.openCount(), "the hung open was given up and the retry opened the lease")
}

// A grant request that hangs is given up after attemptTimeout and retried, so the spare still comes.
func TestAllocatorGivesUpOnAHungGrantRequestAndRetries(t *testing.T) {
	src := &hangingLeases{fakeLeases: newFakeLeases(10)}
	src.hangRenews.Store(1)

	a := newTestAllocator(t, src, quiet, nil)
	a.attemptTimeout = 50 * time.Millisecond

	for i := 0; i < 5; i++ {
		next(t, a) // the fifth is half of the grant, so a grant request goes out and hangs
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond, "the retry after the hung request brings the spare")
}

// timedDownLeases always fails, and records when each request came.
type timedDownLeases struct {
	mu    sync.Mutex
	times []time.Time
}

func (s *timedDownLeases) OpenLaneLease(context.Context, string) (LeaseID, Grant, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.times = append(s.times, time.Now())

	return LeaseID{}, Grant{}, errors.NewServiceUnavailableError("blockchain service down")
}

func (s *timedDownLeases) RenewLaneLease(context.Context, Renewal) (RenewResult, error) {
	return RenewResult{}, errors.NewServiceUnavailableError("blockchain service down")
}

func (s *timedDownLeases) ReleaseLaneLease(context.Context, Release) (bool, error) {
	return false, nil
}

func (s *timedDownLeases) requestTimes() []time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]time.Time(nil), s.times...)
}

// With the service down the loop must not spin: the wait before each retry doubles from retryMin.
// Timers only run late, so each gap being at least its backoff holds on a loaded machine.
func TestAllocatorBacksOffBetweenFailedRequests(t *testing.T) {
	src := &timedDownLeases{}
	a := newTestAllocator(t, src, quiet, nil)
	require.NoError(t, a.Start(context.Background()))

	const requests = 4

	require.Eventually(t, func() bool { return len(src.requestTimes()) >= requests }, 30*time.Second, 5*time.Millisecond)

	times := src.requestTimes()
	backoff := retryMin

	for i := 1; i < requests; i++ {
		require.GreaterOrEqual(t, times[i].Sub(times[i-1]), backoff, "the gap before request %d", i)

		backoff *= 2
	}
}

// Two grants can be left before a request carries the first one's pair: leave grant 1, the request
// at half of grant 2 fails, and grant 2 is used up too. Neither pair may be lost.
func TestAllocatorKeepsBothPairsWhenTwoGrantsAreLeftBeforeARequestSucceeds(t *testing.T) {
	src := newFakeLeases(10)
	a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: time.Hour, GrantDuration: time.Hour}, nil)
	a.takeWait = 50 * time.Millisecond

	for i := 0; i < 10; i++ {
		next(t, a)
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)

	src.setDown(true)

	for i := 0; i < 10; i++ {
		next(t, a) // 11 to 20: leaves grant 1, and the request at half of grant 2 fails
	}

	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, errors.ErrServiceUnavailable, "grant 2 is used up and no grant 3 came")

	src.setDown(false)

	a.takeWait = 5 * time.Second // the loop is in backoff after the outage
	require.Equal(t, Pack(0, 21), next(t, a))

	for i := 0; i < 5; i++ {
		next(t, a)
	}

	require.Eventually(t, func() bool {
		var sent []Settles
		for _, r := range src.grantRequests() {
			sent = append(sent, r.Settles)
		}

		return len(sent) >= 3 && sent[len(sent)-2] == (Settles{First: 1, LastTaken: 10}) && sent[len(sent)-1] == (Settles{First: 11, LastTaken: 20})
	}, 5*time.Second, time.Millisecond, "grant 1's pair and then grant 2's travel on the next two requests")
}

// With two pairs held at the release, the release carries the older, and its LastTaken is the newer
// one's final number.
func TestAllocatorReleaseCarriesTheOlderOfTwoPairs(t *testing.T) {
	src := newFakeLeases(10)
	a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: time.Hour, GrantDuration: time.Hour}, nil)
	a.takeWait = 50 * time.Millisecond

	for i := 0; i < 10; i++ {
		next(t, a)
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond)

	src.setDown(true)

	for i := 0; i < 10; i++ {
		next(t, a)
	}

	_, err := a.Next(context.Background())
	require.Error(t, err)

	src.setDown(false)
	require.NoError(t, a.Close())

	src.mu.Lock()
	defer src.mu.Unlock()

	require.Len(t, src.releases, 1)
	require.Equal(t, Settles{First: 1, LastTaken: 10}, src.releases[0].Settles)
	require.Equal(t, uint64(20), src.releases[0].LastTaken)
}

// closingLeases records the order of its release and its Close.
type closingLeases struct {
	*fakeLeases
	order []string
}

func (c *closingLeases) ReleaseLaneLease(ctx context.Context, r Release) (bool, error) {
	c.order = append(c.order, "release")

	return c.fakeLeases.ReleaseLaneLease(ctx, r)
}

func (c *closingLeases) Close() error {
	c.order = append(c.order, "close")

	return nil
}

// The allocator owns its blockchain client: Close releases the lease first and closes the client
// after, so the release can still go out.
func TestAllocatorReleasesBeforeItsClientCloses(t *testing.T) {
	src := &closingLeases{fakeLeases: newFakeLeases(1000)}
	a := NewAllocator(src, "pod-3", quiet, nil)

	next(t, a)
	require.NoError(t, a.Close())
	require.Equal(t, []string{"release", "close"}, src.order)

	// A process that never took a lease still closes its client.
	src2 := &closingLeases{fakeLeases: newFakeLeases(1000)}
	require.NoError(t, NewAllocator(src2, "pod-4", quiet, nil).Close())
	require.Equal(t, []string{"close"}, src2.order)
}

// An open that returns while Close waits for the loop is released, not left to lapse: Close stops
// the loop before it reads the lease to release. The fake answers even after Close cancels the
// call; a real client's open cut short by the cancel is not recorded, and its lease lapses.
func TestCloseReleasesALeaseOpenedWhileItRuns(t *testing.T) {
	src := newFakeLeases(1000)
	src.openGate, src.opening = make(chan struct{}), make(chan struct{})

	a := NewAllocator(src, "pod-3", quiet, nil)
	require.NoError(t, a.Start(context.Background()))

	<-src.opening

	closed := make(chan error, 1)

	go func() { closed <- a.Close() }()

	require.Never(t, func() bool { return len(closed) > 0 }, 50*time.Millisecond, 5*time.Millisecond, "Close waits for the loop")

	close(src.openGate)
	require.NoError(t, <-closed)

	src.mu.Lock()
	defer src.mu.Unlock()

	require.Len(t, src.releases, 1, "the lease opened during Close is released")
	require.Empty(t, src.open)
}

// slowLeases moves a step clock while a call is in flight, as a slow reply would: it steps the
// clock by delay before it answers an open or a renewal.
type slowLeases struct {
	*fakeLeases
	clk   *stepClock
	delay atomic.Int64
}

func (s *slowLeases) setDelay(d time.Duration) { s.delay.Store(int64(d)) }

func (s *slowLeases) OpenLaneLease(ctx context.Context, holder string) (LeaseID, Grant, error) {
	s.clk.step(time.Duration(s.delay.Load()))

	return s.fakeLeases.OpenLaneLease(ctx, holder)
}

func (s *slowLeases) RenewLaneLease(ctx context.Context, r Renewal) (RenewResult, error) {
	s.clk.step(time.Duration(s.delay.Load()))

	return s.fakeLeases.RenewLaneLease(ctx, r)
}

// requireStopsTwoMinutesAfter checks that a holder whose last good call went out at start gives out
// a position one second before start plus two minutes, and none from then on.
func requireStopsTwoMinutesAfter(t *testing.T, a *Allocator, clk *stepClock, start time.Time) {
	t.Helper()

	clk.step(start.Add(2*time.Minute - time.Second).Sub(clk.Now()))
	next(t, a)

	clk.step(time.Second)

	_, err := a.Next(context.Background())
	require.ErrorIs(t, err, errors.ErrServiceUnavailable, "the stop counts from when the call was sent, not from its reply")
}

// The stop counts from when the open was sent. The reply came a minute later, which must not push
// the stop out, because the service measures its close time from when it received the call.
func TestAllocatorStopCountsFromWhenTheOpenWasSent(t *testing.T) {
	clk := newStepClock()
	start := clk.Now()
	src := &slowLeases{fakeLeases: newFakeLeases(1_000_000), clk: clk}
	src.setDelay(time.Minute)

	a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: 2 * time.Minute, GrantDuration: time.Hour}, clk)
	a.takeWait = 200 * time.Millisecond

	next(t, a)
	require.Equal(t, time.Minute, clk.Now().Sub(start), "the open took a minute to answer")

	requireStopsTwoMinutesAfter(t, a, clk, start)
}

// The same for a renewal: it went out at the start, its reply a minute later.
func TestAllocatorStopCountsFromWhenTheRenewalWasSent(t *testing.T) {
	clk := newStepClock()
	start := clk.Now()
	src := &slowLeases{fakeLeases: newFakeLeases(10), clk: clk}

	a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: 2 * time.Minute, GrantDuration: time.Hour}, clk)
	a.takeWait = 200 * time.Millisecond

	next(t, a)
	src.setDelay(time.Minute)

	for i := 0; i < 4; i++ {
		next(t, a) // the fifth position is half of the grant, so a renewal with a grant request goes out
	}

	require.Eventually(t, a.hasSpareGrant, 5*time.Second, time.Millisecond, "the renewal succeeded")
	require.Equal(t, time.Minute, clk.Now().Sub(start), "the renewal took a minute to answer")

	requireStopsTwoMinutesAfter(t, a, clk, start)
}

// badGrantLeases answers with grant instead of the real one, on an open or on a renewal with a
// grant request.
type badGrantLeases struct {
	*fakeLeases
	onOpen  bool
	onRenew bool
	grant   Grant
}

func (b *badGrantLeases) OpenLaneLease(ctx context.Context, holder string) (LeaseID, Grant, error) {
	id, g, err := b.fakeLeases.OpenLaneLease(ctx, holder)
	if err == nil && b.onOpen {
		return id, b.grant, nil
	}

	return id, g, err
}

func (b *badGrantLeases) RenewLaneLease(ctx context.Context, r Renewal) (RenewResult, error) {
	res, err := b.fakeLeases.RenewLaneLease(ctx, r)
	if err == nil && b.onRenew && res.Granted {
		res.Grant = b.grant
	}

	return res, err
}

// A grant the allocator refuses is never used, on an open or a renewal: one with no positions, one
// that starts at position 0 or ends above the largest position, one on the coinbase lane, and one
// that does not start above what the process already gave out in the lane.
func TestAllocatorRefusesABadGrant(t *testing.T) {
	cases := []struct {
		name      string
		grant     Grant
		renewOnly bool // a first grant is judged against nothing given out yet
	}{
		{"empty", Grant{Lane: 0, First: 20, Last: 19}, false},
		{"starts at zero", Grant{Lane: 0, First: 0, Last: 5}, false},
		{"ends above the largest position", Grant{Lane: 0, First: 20, Last: MaxPosition + 1}, false},
		{"on the coinbase lane", Grant{Lane: firstUnleasedLane, First: 20, Last: 30}, false},
		{"not above the highest given out", Grant{Lane: 0, First: 3, Last: 30}, true},
	}

	for _, tc := range cases {
		// TestAllocatorRefusesAGrantNotAboveItsOwnHighest covers the open for the renewOnly case.
		if !tc.renewOnly {
			t.Run("open "+tc.name, func(t *testing.T) {
				src := &badGrantLeases{fakeLeases: newFakeLeases(10), onOpen: true, grant: tc.grant}
				a := newTestAllocator(t, src, Timings{Renew: 10 * time.Millisecond, Stop: time.Hour, GrantDuration: time.Hour}, nil)
				a.takeWait = 300 * time.Millisecond

				_, err := a.Next(context.Background())
				require.ErrorIs(t, err, errors.ErrServiceUnavailable, "no position comes from a refused first grant")
			})
		}

		t.Run("renewal "+tc.name, func(t *testing.T) {
			src := &badGrantLeases{fakeLeases: newFakeLeases(10), onRenew: true, grant: tc.grant}
			a := newTestAllocator(t, src, quiet, nil)
			a.takeWait = 300 * time.Millisecond

			for i := 0; i < 10; i++ {
				next(t, a) // the fifth asks for the next grant, which the fake answers with the bad one
			}

			_, err := a.Next(context.Background())
			require.ErrorIs(t, err, errors.ErrServiceUnavailable, "the refused spare is not moved to")
			require.False(t, a.hasSpareGrant())
		})
	}
}

// The rate a renewal reports is the positions given out since the renewal before last, per second,
// so the service can size the next grant to it.
func TestAllocatorReportsItsRateOnAGrantRequest(t *testing.T) {
	clk := newStepClock()
	src := newFakeLeases(1000)
	a := newTestAllocator(t, src, Timings{Renew: time.Hour, Stop: 2 * time.Hour, GrantDuration: time.Hour}, clk)

	next(t, a)
	clk.step(10 * time.Second)

	for i := 1; i < 500; i++ {
		next(t, a) // the 500th is half of the grant, so a grant request goes out
	}

	require.Eventually(t, func() bool { return len(src.grantRequests()) == 1 }, 5*time.Second, time.Millisecond)
	require.Equal(t, uint64(50), src.grantRequests()[0].Rate, "500 positions in 10 seconds")
}

// scriptedLeases is the lease source of the invariant test. The test sets how it answers the next
// open or renewal; an "ok" answer follows the service's rules. It records what it was sent.
type scriptedLeases struct {
	clk      *stepClock
	rng      *rand.Rand
	highest  map[uint8]uint64 // the model's highest position given out in each lane
	high     map[uint8]uint64
	open     map[LeaseID]uint8
	seq      byte
	answer   string // ok, slow, error, closed, bad or nogrant
	during   func() // runs while a renewal is in flight
	last     Grant  // the grant last answered
	sent     []Renewal
	answers  []string // the answer to each renewal sent
	releases []Release
	closedAt int // releases sent before the first Close, -1 until then
}

func (f *scriptedLeases) grant(l uint8) Grant {
	g := Grant{Lane: l, First: f.high[l] + 1, Last: f.high[l] + 1 + f.rng.Uint64N(8)}
	f.high[l] = g.Last

	return g
}

// badGrant is a grant the allocator must refuse: malformed, on the coinbase lane, or, once the
// process has given out a position in the lane, not above it.
func (f *scriptedLeases) badGrant(l uint8) Grant {
	bad := []Grant{
		{Lane: l, First: 20, Last: 19}, {Lane: l, First: 0, Last: 5}, {Lane: l, First: 20, Last: MaxPosition + 1},
		{Lane: firstUnleasedLane, First: 20, Last: 30},
	}
	if h := f.highest[l]; h > 0 {
		bad = append(bad, Grant{Lane: l, First: h, Last: h + 3})
	}

	return bad[f.rng.IntN(len(bad))]
}

func (f *scriptedLeases) OpenLaneLease(context.Context, string) (LeaseID, Grant, error) {
	switch f.answer {
	case "slow":
		f.clk.step(time.Minute)
	case "error":
		return LeaseID{}, Grant{}, errors.NewServiceUnavailableError("blockchain service down")
	}

	var l uint8
	for slices.Contains(slices.Collect(maps.Values(f.open)), l) {
		l++
	}

	f.seq++
	id := LeaseID{f.seq}
	f.open[id] = l

	f.last = f.grant(l)
	if f.answer == "bad" {
		f.last = f.badGrant(l)
	}

	return id, f.last, nil
}

func (f *scriptedLeases) RenewLaneLease(_ context.Context, r Renewal) (RenewResult, error) {
	f.sent, f.answers = append(f.sent, r), append(f.answers, f.answer)

	if f.during != nil {
		f.during()
	}

	switch f.answer {
	case "slow":
		f.clk.step(time.Minute)
	case "error":
		return RenewResult{}, errors.NewServiceUnavailableError("blockchain service down")
	case "closed":
		delete(f.open, r.Lease)
		return RenewResult{Closed: true}, nil
	}

	if !r.WantGrant || f.answer == "nogrant" {
		return RenewResult{}, nil
	}

	f.last = f.grant(r.Lane)
	if f.answer == "bad" {
		f.last = f.badGrant(r.Lane)
	}

	return RenewResult{Granted: true, Grant: f.last}, nil
}

func (f *scriptedLeases) ReleaseLaneLease(_ context.Context, r Release) (bool, error) {
	f.releases = append(f.releases, r)
	delete(f.open, r.Lease)

	return false, nil
}

func (f *scriptedLeases) Close() error {
	if f.closedAt < 0 {
		f.closedAt = len(f.releases)
	}

	return nil
}

// allocModel is what the allocator should do, kept by the test from the rules alone.
type allocModel struct {
	t         Timings
	leased    bool
	lease     LeaseID
	lane      uint8
	cur       Grant
	next      uint64
	firstTake time.Time
	spare     Grant
	hasSpare  bool
	pending   []Settles
	taken     uint64 // the highest position this lease gave out
	highest   map[uint8]uint64
	lastGood  time.Time
	released  bool
	releases  []Release
}

func (m *allocModel) install(g Grant) { m.cur, m.next, m.firstTake = g, g.First, time.Time{} }

func (m *allocModel) acceptable(g Grant) bool {
	return g.Len() > 0 && g.First > 0 && g.Last <= MaxPosition && g.Lane < firstUnleasedLane && g.First > m.highest[g.Lane]
}

// take is the position a Next at now gives out, or None.
func (m *allocModel) take(now time.Time) uint64 {
	if m.released || !m.leased || now.Sub(m.lastGood) >= m.t.Stop {
		return None
	}

	if m.next != 0 && (m.next > m.cur.Last || (!m.firstTake.IsZero() && now.Sub(m.firstTake) >= m.t.GrantDuration)) {
		m.pending = append(m.pending, Settles{First: m.cur.First, LastTaken: m.next - 1})
		m.next = 0

		if m.hasSpare {
			m.install(m.spare)
			m.hasSpare = false
		}
	}

	if m.next == 0 {
		return None
	}

	if m.firstTake.IsZero() {
		m.firstTake = now
	}

	m.highest[m.cur.Lane], m.taken = m.next, m.next
	m.next++

	return Pack(m.cur.Lane, m.next-1)
}

// want reports whether the allocator should ask for the next grant: no spare, and the current grant
// missing, half used, used up or aged out.
func (m *allocModel) want(now time.Time) bool {
	return m.leased && !m.hasSpare && (m.next == 0 || m.next > m.cur.Last || m.next-m.cur.First >= (m.cur.Len()+1)/2 ||
		(!m.firstTake.IsZero() && now.Sub(m.firstTake) >= m.t.GrantDuration))
}

func (m *allocModel) oldestPending() Settles {
	if len(m.pending) == 0 {
		return Settles{}
	}

	return m.pending[0]
}

// TestAllocatorKeepsItsInvariants drives seeded random sequences of takes, clock steps and loop
// turns with every kind of answer from the service: a grant, a slow reply, an error, a closed lease,
// a grant the allocator must refuse, a renewal without the grant asked for, and takes while a
// request is in flight. The test turns the loop itself, so the seed fixes every interleaving. It
// checks each answer and each request against a model kept from the rules, and then the invariants.
func TestAllocatorKeepsItsInvariants(t *testing.T) {
	for seed := uint64(1); seed <= 40; seed++ {
		runAllocModel(t, seed, 300)
	}
}

func runAllocModel(t *testing.T, seed uint64, steps int) {
	rng := rand.New(rand.NewPCG(seed, seed)) //nolint:gosec // test randomness
	clk := newStepClock()
	tm := Timings{Renew: time.Hour, Stop: 2 * time.Minute, GrantDuration: 90 * time.Second}
	m := &allocModel{t: tm, highest: map[uint8]uint64{}}
	src := &scriptedLeases{clk: clk, rng: rng, highest: m.highest, high: map[uint8]uint64{}, open: map[LeaseID]uint8{}, closedAt: -1}

	// The test turns the loop, so the loop never starts and Close has none to wait for.
	a := NewAllocator(src, "pod-3", tm, nil)
	a.now, a.started = clk.Now, true
	close(a.done)

	type leaseGrant struct {
		lease LeaseID
		g     Grant
	}

	type takeAt struct {
		v     uint64
		lease LeaseID
		at    time.Time
	}

	var (
		ops    []string
		takes  []takeAt
		grants = map[LeaseID][]Grant{} // the grants each lease accepted, in order
	)

	defer func() {
		if t.Failed() {
			t.Logf("seed %d, ops:\n%s", seed, strings.Join(ops, "\n"))
		}
	}()

	noWait, cancel := context.WithCancel(context.Background())
	cancel() // so a Next with no position fails at once

	takeOne := func() {
		now, lease := clk.Now(), m.lease
		want := m.take(now)
		v, err := a.Next(noWait)
		ops = append(ops, fmt.Sprintf("next at %s -> %d:%d", now.Format("15:04:05"), Lane(v), v&MaxPosition))

		if want == None {
			require.ErrorIs(t, err, errors.ErrServiceUnavailable)
			return
		}

		require.NoError(t, err)
		require.Equal(t, want, v)

		takes = append(takes, takeAt{v, lease, now})
	}

	for step := 0; step <= steps && !m.released; step++ {
		now := clk.Now()
		require.Equal(t, !m.released && m.leased && now.Sub(m.lastGood) < tm.Stop, a.Ready() == nil, "ready")

		op := rng.IntN(100)
		if step == steps {
			op = 99 // every run ends with a Close
		}

		switch {
		case op < 45:
			takeOne()

		case op < 65:
			d := []time.Duration{0, 10 * time.Second, 40 * time.Second, 70 * time.Second, 3 * time.Minute}[rng.IntN(5)]
			ops = append(ops, fmt.Sprintf("step %s", d))
			clk.step(d)

		case op < 99:
			// One turn of the allocator's loop, as run takes it.
			src.answer = []string{"ok", "ok", "ok", "ok", "slow", "error", "closed", "bad", "nogrant"}[rng.IntN(9)]
			src.during = nil
			tick := rng.IntN(4) == 0

			a.mu.Lock()
			leased, want := a.leased, a.leased && a.wantGrantLocked()
			a.mu.Unlock()

			ops = append(ops, fmt.Sprintf("loop at %s leased %v want %v tick %v answer %s", now.Format("15:04:05"), leased, want, tick, src.answer))
			require.Equal(t, m.leased, leased)
			require.Equal(t, m.want(now), want, "want a grant")

			if !leased {
				err := a.open()

				switch {
				case src.answer == "error":
					require.Error(t, err)
				case !m.acceptable(src.last):
					require.Error(t, err)
					m.releases = append(m.releases, Release{Lease: LeaseID{src.seq}, Lane: src.last.Lane})
				default:
					require.NoError(t, err)

					m.leased, m.lease, m.lane, m.hasSpare, m.pending, m.taken, m.lastGood = true, LeaseID{src.seq}, src.last.Lane, false, nil, 0, now
					m.install(src.last)
					grants[m.lease] = append(grants[m.lease], src.last)
				}

				break
			}

			if !want && !tick {
				break
			}

			req := Renewal{Lease: m.lease, Lane: m.lane, WantGrant: want}
			if want {
				req.Settles = m.oldestPending()
			}

			if rng.IntN(4) == 0 {
				src.during = func() { // takes while the request is in flight
					for range 1 + rng.IntN(3) {
						takeOne()
					}
				}
			}

			sent := len(src.sent)
			err := a.renew(want)

			require.Len(t, src.sent, sent+1)
			got := src.sent[sent]
			got.Rate = 0 // TestAllocatorReportsItsRateOnAGrantRequest covers the rate
			require.Equal(t, req, got, "the renewal sent")

			switch {
			case src.answer == "error":
				require.Error(t, err)
			case src.answer == "closed":
				require.NoError(t, err)

				m.leased, m.next, m.hasSpare, m.pending = false, 0, false, nil
			case !want:
				require.NoError(t, err)

				m.lastGood = now
			default:
				m.lastGood = now
				if len(m.pending) > 0 && m.pending[0] == req.Settles {
					m.pending = m.pending[1:]
				}

				if src.answer == "nogrant" || !m.acceptable(src.last) {
					require.Error(t, err)
					break
				}

				require.NoError(t, err)

				grants[m.lease] = append(grants[m.lease], src.last)

				if m.next == 0 {
					m.install(src.last)
				} else {
					m.spare, m.hasSpare = src.last, true
				}
			}

		default:
			ops = append(ops, "close")

			m.released = true
			if m.leased {
				m.releases = append(m.releases, Release{Lease: m.lease, Lane: m.lane, LastTaken: m.taken, Settles: m.oldestPending()})
			}

			require.NoError(t, a.Close())
			require.Equal(t, len(src.releases), src.closedAt, "the release goes out before the client closes")
			takeOne()
		}
	}

	require.Equal(t, m.releases, src.releases)

	// The invariants, from what the allocator gave out and sent alone. No position lies outside a
	// grant its lease accepted, or comes twice in a lane, or comes once GrantDuration has passed
	// since the grant's first take. (The model checks the stop on every take.)
	var (
		highest    = map[uint8]uint64{}
		firstTake  = map[leaseGrant]time.Time{}
		takenIn    = map[leaseGrant]uint64{} // the highest position taken in each grant
		leaseTaken = map[LeaseID]uint64{}
	)

	for _, tk := range takes {
		l, p := Unpack(tk.v)
		require.Greater(t, p, highest[l], "lane %d: position %d is not above every earlier one", l, p)
		highest[l] = p

		i := slices.IndexFunc(grants[tk.lease], func(g Grant) bool { return g.Lane == l && g.First <= p && p <= g.Last })
		require.GreaterOrEqual(t, i, 0, "position %d:%d lies in no grant its lease accepted", l, p)

		key := leaseGrant{tk.lease, grants[tk.lease][i]}
		if _, ok := firstTake[key]; !ok {
			firstTake[key] = tk.at
		}

		require.Less(t, tk.at.Sub(firstTake[key]), tm.GrantDuration, "a grant is used past GrantDuration from its first take")

		takenIn[key], leaseTaken[tk.lease] = p, p
	}

	// Every grant left is settled once and oldest first, on a grant request that was answered, or on
	// the release. The release carries the highest position its lease took, which settles the grant
	// in use. A lease the service closed may lose the pairs it still held.
	for id, gs := range grants {
		used := []Settles{}

		for _, g := range gs {
			if p := takenIn[leaseGrant{id, g}]; p != 0 {
				used = append(used, Settles{First: g.First, LastTaken: p})
			}
		}

		settled := []Settles{}

		for i, r := range src.sent {
			if r.Lease == id && r.WantGrant && r.Settles != (Settles{}) && src.answers[i] != "error" && src.answers[i] != "closed" {
				settled = append(settled, r.Settles)
			}
		}

		i := slices.IndexFunc(src.releases, func(r Release) bool { return r.Lease == id })
		if i >= 0 {
			require.Equal(t, leaseTaken[id], src.releases[i].LastTaken, "the release carries the highest position taken")

			if st := src.releases[i].Settles; st != (Settles{}) {
				settled = append(settled, st)
			}

			if n := len(used); n > 0 && len(settled) < n {
				used = used[:n-1] // the grant in use is settled by the release's LastTaken
			}

			require.Equal(t, used, settled, "lease %x: the pairs settled", id[0])
		} else {
			require.LessOrEqual(t, len(settled), len(used))
			require.Equal(t, used[:len(settled)], settled, "lease %x: the pairs settled", id[0])
		}
	}

	// A first grant the allocator refused is released with nothing taken.
	for _, r := range src.releases {
		if _, ok := grants[r.Lease]; !ok {
			require.Equal(t, Release{Lease: r.Lease, Lane: r.Lane}, r)
		}
	}
}

// BenchmarkAllocatorNextParallel measures Next with every goroutine of the run contending on the
// allocator's one mutex, as concurrent creates in one process do. The grants are large enough that
// a run never waits on the lease loop, so it measures the lock and the take alone. Run it with
// -cpu 1,4,8,16 to see how the per-take cost grows with contention.
func BenchmarkAllocatorNextParallel(b *testing.B) {
	a := NewAllocator(newFakeLeases(1<<40), "pod-3", quiet, nil)
	require.NoError(b, a.Start(context.Background()))

	b.Cleanup(func() { _ = a.Close() })

	_, err := a.Next(context.Background())
	require.NoError(b, err)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		ctx := context.Background()
		for pb.Next() {
			if _, err := a.Next(ctx); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
