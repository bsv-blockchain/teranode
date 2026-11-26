package lane

import (
	"context"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	retryMin = 100 * time.Millisecond
	retryMax = 10 * time.Second

	// attemptTimeout bounds one request to the blockchain service. The context the loop owns only
	// ends at Close, and nothing below it sets a deadline, so without this a call that hangs would
	// stop the renewals and the logging until it returned.
	attemptTimeout = 10 * time.Second

	// defaultTakeWait bounds how long one Next waits for a position before it fails the create: one
	// round trip to the blockchain service when that service is up.
	defaultTakeWait = 5 * time.Second

	// releaseTimeout bounds the release Close sends.
	releaseTimeout = 5 * time.Second

	// maxReturned bounds the positions of one grant held for reuse. Next hands a returned position
	// out before a new one, so in steady running the stack holds one or two; the bound keeps a flood
	// of returns from growing it, and a return past it stays burned.
	maxReturned = 256

	// firstUnleasedLane is the first lane a lease never holds: 254 is the coinbase lane and 255 the
	// backfill lane.
	firstUnleasedLane = 254
)

// Timings are the lease timings a holder runs on (the blockchain_laneLease* and
// blockchain_laneGrantDuration settings).
type Timings struct {
	// Renew is how often the holder renews its lease.
	Renew time.Duration
	// Stop is how long after the last renewal that succeeded was sent the holder stops giving out
	// positions. That renewal went out up to Renew before the service failed, so an outage stops
	// creates up to Renew sooner than Stop. Positions can run out sooner still: the spare is asked
	// for only at half of a grant, so at a steady rate a holder can use up its grant about half a
	// GrantDuration into an outage. Set it shorter than the service's close time, on every
	// process: each process can check only its own settings, so nothing here compares it with the
	// close time the blockchain service runs on.
	Stop time.Duration
	// GrantDuration bounds both the wait for a grant's first take, from sending its request,
	// and its use after that first take. No position comes from a grant older than twice this.
	GrantDuration time.Duration
}

// Allocator hands out lane positions to the records one process creates, under a lease on one
// lane from the blockchain service.
//
// It holds the grant it is handing out and one spare. At half the current grant used it asks for
// the next grant and keeps it as the spare. It takes from the current grant until the grant is used
// up, with no returned position (Return) still waiting in it, or GrantDuration has passed since its
// first position was taken, and only then moves to the spare. A grant that ages out with no spare is
// left by the next take, which waits for a grant: the service starts that grant right after the last
// position taken, so a slow process leaves no unused tail between its grants. An unused grant expires GrantDuration
// after its request was sent, including a spare.
// The grant it leaves is settled by the next request: the grant's first position and its
// final number, which is the grant's last position if used up or the highest position taken if it
// aged out, travel on the next grant request or on the release.
//
// It renews the lease every Renew. Once Stop has passed since the last renewal that succeeded was
// sent, it gives out no position, because the service closes a silent lease after its close time
// and may then give the lane to another holder. With no position to give, Next waits a bounded time
// and then fails with service unavailable, and the create fails with it: no record goes out without
// a position. A lease the service reports closed is dropped, and a new one opened, usually on
// another lane.
//
// A store hands back, with Return, a position it took and can prove no record carries. Next gives
// a returned position out again before the grant's next new one, but only from the grant it came
// from: leaving a grant drops its returned positions. Those below its settled final number stay
// burned; those at the top of its taken range lie above that number, so a grant that continues the
// left one gives them out again. A returned position is
// always below the grant's next new position, so at or below the highest position taken. Returned
// positions still waiting at the top of a grant's taken range when it is settled lower its final
// number, and the release's, so both name only positions that may carry a record
// (settledFinalLocked).
type Allocator struct {
	src    LeaseSource
	holder string
	t      Timings
	logger ulogger.Logger

	// now is the clock the stop and the grant age are measured on. A field so a test can step it.
	now func() time.Time
	// attemptTimeout and takeWait are the consts of those names. Fields so a test can shorten them.
	attemptTimeout time.Duration
	takeWait       time.Duration

	mu sync.Mutex
	// leased is true while the allocator holds a lease the service has not reported closed.
	leased bool
	lease  LeaseID
	lane   uint8
	// cur is the grant positions come from; next is its next position, and 0 when there is none.
	cur  Grant
	next uint64
	// curFirstTake is when the first position of cur was taken, zero until then.
	curFirstTake time.Time
	// halfAsked is true once Next has signalled the loop that cur is half used.
	halfAsked bool
	// returned holds the numbers of positions of cur that were given out and handed back by Return,
	// each below next and each once. Next takes from it before next. Its capacity is maxReturned,
	// fixed when the allocator is built, so neither a take nor a return allocates.
	returned []uint64
	spare    Grant
	hasSpare bool
	// pending settles the grants left and not yet reported, oldest first, until grant requests or
	// the release carry them (pairToSendLocked picks which). It holds at most two. A request made
	// while a grant is current carries the oldest pair, so after it succeeds at most that grant and a
	// spare can be left before the next one does. Once two are left no grant is current, so none can
	// be left again until a request succeeds. A request with no grant current carries the newest pair
	// and may leave one older pair, which the next request made while a grant is current carries.
	pending  [2]Settles
	npending int
	// leaseTaken is the highest position this lease has given out that may carry a record; 0 for
	// none. A grant left with returned positions at its top lowers it past them (settledFinalLocked).
	leaseTaken uint64
	// leaseTakenBefore is leaseTaken as it stood when cur was installed, the number the lease falls
	// back to when every position cur gave out came back.
	leaseTakenBefore uint64
	// highest is the highest position this process has given out in each lane, across leases, that
	// may carry a record: leaving a grant lowers it past returned positions at the top of what the
	// grant gave out. A grant that does not start above it is refused.
	highest [MaxLane + 1]uint64
	// lastGood is when the last renewal (or the open) that succeeded was sent.
	lastGood time.Time
	released bool
	// changed is closed and replaced whenever a waiting Next could now succeed or should give up.
	changed chan struct{}
	// taken counts new positions given out, not returned ones given out again, and samples hold it
	// at the last two renewals, for the rate.
	taken   uint64
	samples [2]rateSample

	wake      chan struct{}
	startOnce sync.Once
	started   bool

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// Keep grant dates separate from the frequently read take/lease fields. They date grants from
	// request send, before the service records them; renewal alone never changes them.
	curRequested   time.Time
	spareRequested time.Time
}

type rateSample struct {
	at    time.Time
	taken uint64
}

// NewAllocator returns an allocator that takes leases from src in holder's name. It opens its lease
// on Start, or on the first Next.
func NewAllocator(src LeaseSource, holder string, t Timings, logger ulogger.Logger) *Allocator {
	if logger == nil {
		logger = ulogger.TestLogger{}
	}

	// Registered here, before any Next, so the counters are never swapped under a running allocator.
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())

	return &Allocator{
		src:            src,
		holder:         holder,
		t:              t,
		logger:         logger,
		now:            time.Now,
		attemptTimeout: attemptTimeout,
		takeWait:       defaultTakeWait,
		changed:        make(chan struct{}),
		returned:       make([]uint64, 0, maxReturned),
		wake:           make(chan struct{}, 1),
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
	}
}

// Start opens the lease in the background and keeps it. It never waits, so it never blocks a
// create; Ready reports when a lease is held. Calling it again does nothing.
func (a *Allocator) Start(context.Context) error {
	a.startOnce.Do(func() {
		a.mu.Lock()
		a.started = true
		a.mu.Unlock()

		go a.run()
	})

	return nil
}

// Timings returns the lease timings the allocator runs on.
func (a *Allocator) Timings() Timings {
	return a.t
}

// Ready returns nil while the allocator holds a lease whose stop has not passed. It does not ask
// for a grant in hand: a grant used up for a moment is not a reason to take a process out of
// service.
func (a *Allocator) Ready() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	switch {
	case a.released:
		return errors.NewServiceUnavailableError("[lane] %s has released its lane lease", a.holder)
	case !a.leased:
		return errors.NewServiceUnavailableError("[lane] %s holds no lane lease yet", a.holder)
	case !a.withinStopLocked():
		return errors.NewServiceUnavailableError("[lane] %s has had no good lane lease renewal for %s", a.holder, a.t.Stop)
	default:
		return nil
	}
}

// Close stops the allocator, tries to release its lease with the highest position it gave out that
// may carry a record (settledFinalLocked lowers it past returned positions at the top), and then
// closes src if src is an io.Closer. A release that fails, such as one sent to a blockchain service
// that has already stopped, is logged and the lease lapses at the service's close time. Positions not handed out are lost, which is safe: the service never
// hands them out again.
func (a *Allocator) Close() error {
	a.mu.Lock()
	a.released = true
	started := a.started
	a.broadcastLocked()
	a.mu.Unlock()

	// The loop stops first, so a grant it took, or an open that returned before the cancel, is in
	// the state read below and released with it. Two leases are not: an open the cancel cuts short,
	// and one opened by a loop that a Next racing this Close started after started was read. Each
	// lapses at the service's close time, as the lease of a process that dies does.
	a.cancel()

	if started {
		<-a.done
	}

	a.mu.Lock()
	// The grant still current settles here, by the release's LastTaken, so returned positions at its
	// top lower that number as they would its settles pair.
	if a.next != 0 && !a.curFirstTake.IsZero() {
		a.leaseTaken = a.leaseTakenAfterLocked(a.settledFinalLocked(a.next - 1))
	}

	// With two pairs held the release carries the older. The newer is the grant last left: its final
	// number is the LastTaken the release carries, or, when that grant gave back every position it
	// gave out and settled at 0, LastTaken stays the number before it and the newer pair is not sent.
	// Either way no position this lease gave out above LastTaken carries a record.
	leased, release := a.leased, Release{Lease: a.lease, Lane: a.lane, LastTaken: a.leaseTaken, Settles: a.pending[0]}
	a.mu.Unlock()

	if leased {
		ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)

		if _, err := a.src.ReleaseLaneLease(ctx, release); err != nil {
			a.logger.Warnf("[lane] could not release the lease on lane %d for %s; it closes as lapsed: %v", release.Lane, a.holder, err)
		}

		cancel()
	}

	if closer, ok := a.src.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

// Next returns the next lane position. A create calls it once per attempt to write a record, and
// with a position in hand it takes a lock and an add and allocates nothing. With none in hand it
// waits until one comes, at most takeWait or until ctx ends, and then fails with service
// unavailable. The first call of a process with no Start starts the allocator.
func (a *Allocator) Next(ctx context.Context) (uint64, error) {
	a.mu.Lock()

	if !a.started {
		a.mu.Unlock()
		_ = a.Start(ctx)
		a.mu.Lock()
	}

	v, ok, changed, err := a.takeLocked()
	a.mu.Unlock()

	if ok {
		prometheusLanePositions.Inc()
		return v, nil
	}

	if err != nil {
		prometheusLaneTakeFailures.Inc()
		return None, err
	}

	prometheusLaneWaits.Inc()

	timer := time.NewTimer(a.takeWait)
	defer timer.Stop()

	for {
		select {
		case <-changed:
		case <-timer.C:
			prometheusLaneTakeFailures.Inc()
			return None, errors.NewServiceUnavailableError("[lane] no lane position for %s within %s", a.holder, a.takeWait)
		case <-ctx.Done():
			prometheusLaneTakeFailures.Inc()
			return None, errors.NewServiceUnavailableError("[lane] no lane position for %s before the caller gave up", a.holder, ctx.Err())
		}

		a.mu.Lock()
		v, ok, changed, err = a.takeLocked()
		a.mu.Unlock()

		if ok {
			prometheusLanePositions.Inc()
			return v, nil
		}

		if err != nil {
			prometheusLaneTakeFailures.Inc()
			return None, err
		}
	}
}

// takeLocked gives out the next position if it can. Otherwise it returns the channel to wait on,
// or an error when waiting cannot help.
func (a *Allocator) takeLocked() (v uint64, ok bool, changed chan struct{}, err error) {
	if a.released {
		return None, false, nil, errors.NewServiceUnavailableError("[lane] %s has released its lane lease", a.holder)
	}

	// One clock read serves the stop, the grant age and the first take.
	now := a.now()

	if !a.leased || now.Sub(a.lastGood) >= a.t.Stop {
		return None, false, a.changed, nil
	}

	for a.next != 0 {
		usedUp := a.next > a.cur.Last
		aged := !a.curFirstTake.IsZero() && now.Sub(a.curFirstTake) >= a.t.GrantDuration
		unusedExpired := a.curFirstTake.IsZero() && now.Sub(a.curRequested) >= a.t.GrantDuration

		// A used-up grant with returned positions stays current until they are given out, inside
		// the same age limit as any other take from it.
		if (!usedUp || len(a.returned) > 0) && !aged && !unusedExpired {
			break
		}

		a.leaveCurrentLocked(usedUp)
	}

	if a.next == 0 {
		return None, false, a.changed, nil
	}

	// A returned position goes out first. It is below next, so the highest taken and the lease's last
	// taken stay as they are, and it is not counted in the rate: the service sizes grants by the new
	// positions a holder uses.
	if n := len(a.returned); n > 0 {
		reused := a.returned[n-1]
		a.returned = a.returned[:n-1]

		prometheusLaneReused.Inc()

		return Pack(a.cur.Lane, reused), true, nil, nil
	}

	v = Pack(a.cur.Lane, a.next)

	if a.curFirstTake.IsZero() {
		a.curFirstTake = now
	}

	a.highest[a.cur.Lane] = a.next
	a.leaseTaken = a.next
	a.taken++

	if !a.halfAsked && a.next-a.cur.First+1 >= (a.cur.Len()+1)/2 {
		a.halfAsked = true
		a.signal()
	}

	a.next++

	return v, true, nil, nil
}

// leaveCurrentLocked settles the current grant and moves to the spare, or to no grant when there is
// no spare, in which case the loop asks for one at once. The grant's returned positions are dropped.
// Those at the top of the taken range settledFinalLocked removes, and the grant settles below them,
// so a grant that continues this one gives them out again; those left sit below that final number,
// stay burned, and installLocked clears them. With no
// grant installed next is 0, which Return and takeLocked both refuse.
func (a *Allocator) leaveCurrentLocked(usedUp bool) {
	final := a.next - 1
	if usedUp {
		final = a.cur.Last
	}

	if !a.curFirstTake.IsZero() {
		final = a.settledFinalLocked(final)
		a.pending[a.npending] = Settles{First: a.cur.First, LastTaken: final}
		a.npending++
		a.leaseTaken = a.leaseTakenAfterLocked(final)

		// Returned positions above final carry no record, so a grant that continues right after
		// final, as the service makes one when this grant is still the lane's newest, is accepted.
		if final != 0 {
			a.highest[a.cur.Lane] = final
		}
	}

	if a.hasSpare {
		a.installLocked(a.spare, a.spareRequested)
		a.hasSpare = false
	} else {
		a.next = 0
	}

	a.signal()
}

// installLocked makes g the current grant, dropping the returned positions still held from the grant
// before it. settledFinalLocked has already removed any at the top of that grant's taken range, and
// until installLocked runs every other way off a grant sets next to 0, which Return and takeLocked
// both refuse.
func (a *Allocator) installLocked(g Grant, requested time.Time) {
	a.cur, a.next, a.curFirstTake, a.halfAsked = g, g.First, time.Time{}, false
	a.curRequested = requested
	a.returned = a.returned[:0]
	a.leaseTakenBefore = a.leaseTaken
}

// settledFinalLocked lowers final, the highest position cur gave out, past every returned position
// still waiting at the top of the taken range, removing each from the set, so the number a grant
// settles at, and the release, names only positions that may carry a record. Take grant 1 to 100
// taken up to 7 with 7 and 6 returned: it settles at 5. A returned position below a used one, 3
// under a used 5, changes nothing and is dropped with the grant. When every position cur gave out
// came back it returns 0: the grant gave out nothing that may carry a record.
func (a *Allocator) settledFinalLocked(final uint64) uint64 {
	for final >= a.cur.First {
		i := slices.Index(a.returned, final)
		if i < 0 {
			return final
		}

		a.returned = slices.Delete(a.returned, i, i+1)
		final--
	}

	return 0
}

// leaseTakenAfterLocked is the lease's last-taken number once cur settles at final: final itself, or
// the number before cur when cur settles at 0.
func (a *Allocator) leaseTakenAfterLocked(final uint64) uint64 {
	if final == 0 {
		return a.leaseTakenBefore
	}

	return final
}

// Return hands back v, a position Next gave out that no record carries, so a later Next gives it
// out again. The caller must know that no record carries it, not just that its own write failed:
// a position returned while a record carries it goes to a second record. Return takes v back only
// while the allocator is on the grant v came from, only below the grant's next new position, and
// only once while it waits; it reports whether it did. A position it does not take back stays
// burned, which is safe.
func (a *Allocator) Return(v uint64) bool {
	l, n := Unpack(v)

	a.mu.Lock()
	defer a.mu.Unlock()

	if a.released || !a.leased || a.next == 0 || l != a.cur.Lane || n < a.cur.First || n >= a.next {
		return false
	}

	if len(a.returned) == cap(a.returned) || slices.Contains(a.returned, n) {
		return false
	}

	a.returned = append(a.returned, n)

	return true
}

func (a *Allocator) withinStopLocked() bool {
	return a.now().Sub(a.lastGood) < a.t.Stop
}

// wantGrantLocked reports whether the loop should ask for the next grant: no spare is held, and
// the current grant is missing, half used or used up. An aged-out grant alone is not a reason: the
// next take leaves it first, so the request carries its settles pair and the service can start
// the next grant right after its last taken position instead of above its unused tail.
func (a *Allocator) wantGrantLocked() bool {
	if a.hasSpare {
		return false
	}

	if a.next == 0 || a.next > a.cur.Last {
		return true
	}

	return a.next-a.cur.First >= (a.cur.Len()+1)/2
}

func (a *Allocator) broadcastLocked() {
	close(a.changed)
	a.changed = make(chan struct{})
}

func (a *Allocator) signal() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// run keeps the lease: it opens one when it has none, renews it every Renew, and asks for the next
// grant when wantGrantLocked says so. A failed request is retried with a backoff from retryMin to
// retryMax.
func (a *Allocator) run() {
	defer close(a.done)

	ticker := time.NewTicker(a.t.Renew)
	defer ticker.Stop()

	backoff := retryMin
	renewDue := false

	for {
		if a.ctx.Err() != nil {
			return
		}

		a.mu.Lock()
		leased, want := a.leased, a.leased && a.wantGrantLocked()
		a.mu.Unlock()

		var err error

		switch {
		case !leased:
			err = a.open()
		case want || renewDue:
			err = a.renew(want)
			if err == nil {
				renewDue = false
			}
		}

		if err != nil {
			if a.ctx.Err() != nil {
				return
			}

			a.logger.Warnf("[lane] lane lease request for %s failed, retrying in %s: %v", a.holder, backoff, err)

			select {
			case <-time.After(backoff):
			case <-ticker.C:
				renewDue = true
			case <-a.ctx.Done():
				return
			}

			backoff = min(2*backoff, retryMax)

			continue
		}

		backoff = retryMin

		a.mu.Lock()
		again := !a.leased || a.wantGrantLocked()
		a.mu.Unlock()

		if again {
			continue
		}

		select {
		case <-ticker.C:
			renewDue = true
		case <-a.wake:
		case <-a.ctx.Done():
			return
		}
	}
}

// acceptableLocked checks a grant before it is used: well formed, in a leasable lane, and above
// every position this process has already given out in that lane.
func (a *Allocator) acceptableLocked(g Grant) error {
	if g.Len() == 0 || g.First == 0 || g.Last > MaxPosition || g.Lane >= firstUnleasedLane {
		return errors.NewProcessingError("[lane] blockchain service granted an invalid stretch %d:%d-%d", g.Lane, g.First, g.Last)
	}

	if g.First <= a.highest[g.Lane] {
		prometheusLaneRefusedGrants.Inc()
		return errors.NewProcessingError("[lane] refused grant %d:%d-%d: this process already gave out position %d in that lane", g.Lane, g.First, g.Last, a.highest[g.Lane])
	}

	return nil
}

func (a *Allocator) open() error {
	sent := a.now()

	ctx, cancel := context.WithTimeout(a.ctx, a.attemptTimeout)
	id, g, err := a.src.OpenLaneLease(ctx, a.holder)
	cancel()

	if err != nil {
		return err
	}

	a.mu.Lock()

	// Recorded even when Close has begun: Close waits for this loop, so it releases a lease whose
	// open returned rather than leaving it to lapse.
	if err = a.acceptableLocked(g); err != nil {
		a.mu.Unlock()
		a.logger.Errorf("%v", err)
		a.releaseRefused(id, g.Lane)

		return err
	}

	defer a.mu.Unlock()

	a.leased, a.lease, a.lane = true, id, g.Lane
	a.installLocked(g, sent)
	a.hasSpare, a.pending, a.npending, a.leaseTaken, a.leaseTakenBefore = false, [2]Settles{}, 0, 0, 0
	a.lastGood = sent
	a.samples = [2]rateSample{{sent, a.taken}, {sent, a.taken}}
	a.broadcastLocked()

	prometheusLaneCurrent.Set(float64(g.Lane))
	prometheusLaneGrants.Inc()

	a.logger.Infof("[lane] %s holds lane %d, positions %d to %d", a.holder, g.Lane, g.First, g.Last)

	return nil
}

// releaseRefused closes a lease whose first grant this process refused, with no position taken.
// Left open, it would hold its lane until it lapsed, and every retry of open would hold another
// lane the same way. The release is best effort: if it fails, the lease lapses as before.
func (a *Allocator) releaseRefused(id LeaseID, l uint8) {
	ctx, cancel := context.WithTimeout(context.Background(), releaseTimeout)
	defer cancel()

	if _, err := a.src.ReleaseLaneLease(ctx, Release{Lease: id, Lane: l}); err != nil {
		a.logger.Warnf("[lane] could not release the refused lease on lane %d for %s; it closes as lapsed: %v", l, a.holder, err)
	}
}

// pairToSendLocked is the settles pair a grant request carries. With no grant current and no spare
// held, the newest pair settles the grant this allocator left last, which is the lane's newest
// hand-out, so the request carries it and the service can start the grant right after it. Take a
// burst that moved from grant 1 to spare 2, then slowed so spare 2 aged out: pending holds both
// pairs, and carrying grant 1's would start the next grant above spare 2's tail. Otherwise the
// request carries the oldest pair.
func (a *Allocator) pairToSendLocked() Settles {
	if a.npending == 0 {
		return Settles{}
	}

	if a.next == 0 && !a.hasSpare {
		return a.pending[a.npending-1]
	}

	return a.pending[0]
}

func (a *Allocator) renew(want bool) error {
	sent := a.now()

	a.mu.Lock()
	r := Renewal{Lease: a.lease, Lane: a.lane, Rate: a.rateLocked(sent), WantGrant: want}
	if want {
		r.Settles = a.pairToSendLocked()
	}
	a.mu.Unlock()

	ctx, cancel := context.WithTimeout(a.ctx, a.attemptTimeout)
	res, err := a.src.RenewLaneLease(ctx, r)
	cancel()

	if err != nil {
		return err
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.leased || a.lease != r.Lease {
		return nil
	}

	if res.Closed {
		a.logger.Warnf("[lane] the blockchain service closed the lease of %s on lane %d; opening a new one", a.holder, a.lane)

		a.leased, a.next, a.hasSpare, a.pending, a.npending = false, 0, false, [2]Settles{}, 0
		a.broadcastLocked()

		return nil
	}

	a.lastGood = sent
	a.samples[0], a.samples[1] = a.samples[1], rateSample{sent, a.taken}

	// A pair is reported once a request that carried it succeeds. A pair recorded while the request
	// was in flight stays for the next one.
	if want && r.Settles != (Settles{}) {
		for i := 0; i < a.npending; i++ {
			if a.pending[i] == r.Settles {
				copy(a.pending[i:], a.pending[i+1:])
				a.pending[len(a.pending)-1] = Settles{}
				a.npending--

				break
			}
		}
	}

	if want && !res.Granted {
		return errors.NewProcessingError("[lane] blockchain service renewed the lease of %s without the grant it asked for", a.holder)
	}

	if res.Granted {
		if err = a.acceptableLocked(res.Grant); err != nil {
			a.logger.Errorf("%v", err)
			a.broadcastLocked()

			return err
		}

		if a.next == 0 {
			a.installLocked(res.Grant, sent)
		} else {
			a.spare, a.hasSpare = res.Grant, true
			a.spareRequested = sent
		}

		prometheusLaneGrants.Inc()
	}

	a.broadcastLocked()

	return nil
}

// rateLocked is new positions given out per second, returned ones given out again left out, since
// the request before last that succeeded, renewal or grant request alike: between 30 and 60 seconds
// at a 30-second renewal with no grant request in between, and shorter when grant requests come
// close together.
func (a *Allocator) rateLocked(at time.Time) uint64 {
	base := a.samples[0]

	secs := at.Sub(base.at).Seconds()
	if secs <= 0 {
		return 0
	}

	return uint64(float64(a.taken-base.taken) / secs)
}
