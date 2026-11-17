package lane

import (
	"context"
	"io"
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

	// firstUnleasedLane is the first lane a lease never holds: 254 is the coinbase lane and 255 the
	// backfill lane.
	firstUnleasedLane = 254
)

// Timings are the lease timings a holder runs on (the blockchain_laneLease* and
// blockchain_laneGrantDuration settings).
type Timings struct {
	// Renew is how often the holder renews its lease.
	Renew time.Duration
	// Stop is how long after the last renewal that succeeded the holder stops giving out positions.
	// It must be shorter than the service's close time, so the holder stops before its lane can go
	// to another holder.
	Stop time.Duration
	// GrantDuration is how long a grant is used, counted from its first position taken.
	GrantDuration time.Duration
}

// Allocator hands out lane positions to the records one process creates, under a lease on one
// lane from the blockchain service.
//
// It holds the grant it is handing out and one spare. At half the current grant used it asks for
// the next grant and keeps it as the spare. It takes from the current grant until the grant is used
// up or GrantDuration has passed since its first position was taken, and only then moves to the
// spare. The grant it leaves is settled by the next request: the grant's first position and its
// final number, which is the grant's last position if used up or the highest position taken if it
// aged out, travel on the next grant request or on the release.
//
// It renews the lease every Renew. Once Stop has passed since the last renewal that succeeded was
// sent, it gives out no position, because the service closes a silent lease after its close time
// and may then give the lane to another holder. With no position to give, Next waits a bounded time
// and then fails with service unavailable, and the create fails with it: no record goes out without
// a position. A lease the service reports closed is dropped, and a new one opened, usually on
// another lane.
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
	spare     Grant
	hasSpare  bool
	// pending settles the grants left and not yet reported, oldest first, until grant requests or
	// the release carry them. It holds at most two: a second grant is left only once the first was
	// left with no spare to move to, and then no grant is current, so none can be left again until
	// a request succeeds and carries the oldest.
	pending  [2]Settles
	npending int
	// leaseTaken is the highest position this lease has given out; 0 for none.
	leaseTaken uint64
	// highest is the highest position this process has given out in each lane, across leases. A
	// grant that does not start above it is refused.
	highest [MaxLane + 1]uint64
	// lastGood is when the last renewal (or the open) that succeeded was sent.
	lastGood time.Time
	released bool
	// changed is closed and replaced whenever a waiting Next could now succeed or should give up.
	changed chan struct{}
	// taken counts positions given out, and samples hold it at the last two renewals, for the rate.
	taken   uint64
	samples [2]rateSample

	wake      chan struct{}
	startOnce sync.Once
	started   bool

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
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

// Close stops the allocator, releases its lease with the last position it took, and then closes
// src if src is an io.Closer. Positions not handed out are lost, which is safe: the service never
// hands them out again.
func (a *Allocator) Close() error {
	a.mu.Lock()
	a.released = true
	// With two pairs held the release carries the older. The newer is the grant last left, and its
	// final number is the LastTaken the release carries anyway.
	leased, release := a.leased, Release{Lease: a.lease, Lane: a.lane, LastTaken: a.leaseTaken, Settles: a.pending[0]}
	started := a.started
	a.broadcastLocked()
	a.mu.Unlock()

	a.cancel()

	if started {
		<-a.done
	}

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

	if !a.leased || !a.withinStopLocked() {
		return None, false, a.changed, nil
	}

	if a.next != 0 {
		usedUp := a.next > a.cur.Last
		aged := !a.curFirstTake.IsZero() && a.now().Sub(a.curFirstTake) >= a.t.GrantDuration

		if usedUp || aged {
			a.leaveCurrentLocked(usedUp)
		}
	}

	if a.next == 0 {
		return None, false, a.changed, nil
	}

	v = Pack(a.cur.Lane, a.next)

	if a.curFirstTake.IsZero() {
		a.curFirstTake = a.now()
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
// no spare, in which case the loop asks for one at once.
func (a *Allocator) leaveCurrentLocked(usedUp bool) {
	final := a.next - 1
	if usedUp {
		final = a.cur.Last
	}

	a.pending[a.npending] = Settles{First: a.cur.First, LastTaken: final}
	a.npending++

	if a.hasSpare {
		a.installLocked(a.spare)
		a.hasSpare = false
	} else {
		a.next = 0
	}

	a.signal()
}

// installLocked makes g the current grant.
func (a *Allocator) installLocked(g Grant) {
	a.cur, a.next, a.curFirstTake, a.halfAsked = g, g.First, time.Time{}, false
}

func (a *Allocator) withinStopLocked() bool {
	return a.now().Sub(a.lastGood) < a.t.Stop
}

// wantGrantLocked reports whether the loop should ask for the next grant: no spare is held, and
// the current grant is missing, half used, used up or aged out.
func (a *Allocator) wantGrantLocked() bool {
	if a.hasSpare {
		return false
	}

	if a.next == 0 || a.next > a.cur.Last {
		return true
	}

	if a.next-a.cur.First >= (a.cur.Len()+1)/2 {
		return true
	}

	return !a.curFirstTake.IsZero() && a.now().Sub(a.curFirstTake) >= a.t.GrantDuration
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
	defer a.mu.Unlock()

	if a.released {
		return nil
	}

	if err = a.acceptableLocked(g); err != nil {
		// The lease is left to lapse; this process will not use it.
		a.logger.Errorf("%v", err)
		return err
	}

	a.leased, a.lease, a.lane = true, id, g.Lane
	a.installLocked(g)
	a.hasSpare, a.pending, a.npending, a.leaseTaken = false, [2]Settles{}, 0, 0
	a.lastGood = sent
	a.samples = [2]rateSample{{sent, a.taken}, {sent, a.taken}}
	a.broadcastLocked()

	prometheusLaneCurrent.Set(float64(g.Lane))
	prometheusLaneGrants.Inc()

	a.logger.Infof("[lane] %s holds lane %d, positions %d to %d", a.holder, g.Lane, g.First, g.Last)

	return nil
}

func (a *Allocator) renew(want bool) error {
	sent := a.now()

	a.mu.Lock()
	r := Renewal{Lease: a.lease, Lane: a.lane, Rate: a.rateLocked(sent), WantGrant: want}
	if want {
		r.Settles = a.pending[0]
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

	if a.released || !a.leased || a.lease != r.Lease {
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

	// The oldest pair is reported once a request that carried it succeeds. A pair recorded while the
	// request was in flight stays for the next one.
	if want && a.npending > 0 && a.pending[0] == r.Settles {
		a.pending[0], a.pending[1] = a.pending[1], Settles{}
		a.npending--
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
			a.installLocked(res.Grant)
		} else {
			a.spare, a.hasSpare = res.Grant, true
		}

		prometheusLaneGrants.Inc()
	}

	a.broadcastLocked()

	return nil
}

// rateLocked is positions given out per second since the renewal before last: about the last
// minute at a 30-second renewal.
func (a *Allocator) rateLocked(at time.Time) uint64 {
	base := a.samples[0]

	secs := at.Sub(base.at).Seconds()
	if secs <= 0 {
		return 0
	}

	return uint64(float64(a.taken-base.taken) / secs)
}
