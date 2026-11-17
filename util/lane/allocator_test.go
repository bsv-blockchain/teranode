package lane

import (
	"context"
	"sync"
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
	gate := f.gate
	f.mu.Unlock()

	if r.WantGrant && gate != nil {
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

// An open the service commits while Close runs is released, not left to lapse: Close stops the loop
// before it reads the lease to release.
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
