package lane

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	retryMin = 100 * time.Millisecond
	retryMax = 10 * time.Second

	// attemptTimeout bounds one request for a range. The context the fetch loop owns only ends
	// at Close, and nothing below it sets a deadline, so without this a call that hangs would
	// stop the retries and the logging until it returned.
	attemptTimeout = 10 * time.Second

	// defaultFirstRangeWait bounds the one wait of a process, on its first create, for its first
	// range: one round trip to the blockchain service when that service is up.
	defaultFirstRangeWait = 5 * time.Second
)

// Allocator hands out lane positions to the records one process creates.
//
// It holds the range it is handing out and one spare, and fetches the next
// spare in the background as soon as it starts on the last one, so the create
// path never waits on the blockchain service: Next is a lock and an add. With
// the default range of 10 million positions, the spare covers about two
// minutes of a validator's share at 1.5 million transactions a second, which is
// how long the blockchain service can be away before records go out without a
// lane position.
//
// When both ranges are used up and no new one has come, Next reports false and
// the caller creates the record without a lane position. Nothing reads lane
// positions yet, so a record without one is correct, only unlabelled; the
// alternative, holding the create until the service answers, would stall every
// validator on a blockchain outage.
//
// The lane number is kept in a file, so a restart asks for the same lane and
// carries on above the positions it already used. A missing or unreadable file
// means asking with no lane. The service gives a different lane than the one
// asked for only when that lane is exhausted; the file is then rewritten.
type Allocator struct {
	src    RangeSource
	holder string
	file   string
	logger ulogger.Logger
	// attemptTimeout is the const of that name. A field so a test can shorten it.
	attemptTimeout time.Duration

	mu       sync.Mutex
	cur      Range
	next     uint64 // next position in cur; cur is spent when next > cur.Last
	spare    Range
	hasSpare bool
	lane     uint8
	hasLane  bool

	wake      chan struct{}
	ready     chan struct{}
	readyOnce sync.Once
	startOnce sync.Once

	firstUse       sync.Once
	started        atomic.Bool
	firstRangeWait time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// NewAllocator returns an allocator that asks src for ranges in holder's name
// and keeps its lane number in file. Call Start before Next.
func NewAllocator(src RangeSource, holder, file string, logger ulogger.Logger) *Allocator {
	if logger == nil {
		logger = ulogger.TestLogger{}
	}

	// Registered here, before any Next, so the counters are never swapped
	// under a running allocator.
	initPrometheusMetrics()

	ctx, cancel := context.WithCancel(context.Background())

	return &Allocator{
		src:    src,
		holder: holder,
		file:   file,
		logger: logger,

		attemptTimeout: attemptTimeout,

		firstRangeWait: defaultFirstRangeWait,
		wake:           make(chan struct{}, 1),
		ready:          make(chan struct{}),
		ctx:            ctx,
		cancel:         cancel,
		done:           make(chan struct{}),
	}
}

// Start reads the saved lane and starts fetching ranges in the background. It
// waits for the first range until ctx ends, and returns an error if none came;
// the allocator keeps trying either way, so an error here is not fatal. Calling
// it again only waits.
func (a *Allocator) Start(ctx context.Context) error {
	// Whatever this wait ends in, no later Next waits: the fetch loop carries on regardless.
	defer a.started.Store(true)

	a.startOnce.Do(func() {
		if l, ok := readLaneFile(a.file); ok {
			a.lane, a.hasLane = l, true
		}

		go a.run()
	})

	select {
	case <-a.ready:
		return nil
	case <-ctx.Done():
		return errors.NewServiceUnavailableError("[lane] no range from the blockchain service yet for %s", a.holder, ctx.Err())
	}
}

// Close stops the background fetch. Positions not handed out are lost, which
// is safe: the service never hands them out again.
func (a *Allocator) Close() error {
	a.cancel()

	// A never-started allocator has no fetch loop to wait for.
	a.startOnce.Do(func() { close(a.done) })

	<-a.done

	return nil
}

// Next returns the next lane position, or false if no range is in hand. A create calls it once per
// record, so it takes a lock and an add and allocates nothing. The first call of a process starts
// the allocator and waits for the first range, at most firstRangeWait; no later call waits.
func (a *Allocator) Next() (uint64, bool) {
	if !a.started.Load() {
		a.startOnFirstUse()
	}

	a.mu.Lock()
	v, ok := a.nextLocked()
	a.mu.Unlock()

	if ok {
		prometheusLanePositions.Inc()
	} else {
		prometheusLaneExhausted.Inc()
	}

	return v, ok
}

// startOnFirstUse is why a process that only reads the store never takes a lane: most services that
// hold the UTXO store never create a record. Callers that arrive during the wait wait with it.
func (a *Allocator) startOnFirstUse() {
	a.firstUse.Do(func() {
		ctx, cancel := context.WithTimeout(a.ctx, a.firstRangeWait)
		defer cancel()

		if err := a.Start(ctx); err != nil {
			a.logger.Warnf("[lane] new records go out without a lane position until a range arrives: %v", err)
		}
	})
}

func (a *Allocator) nextLocked() (uint64, bool) {
	if a.next == 0 || a.next > a.cur.Last {
		if !a.hasSpare {
			return None, false
		}

		a.cur, a.next, a.hasSpare = a.spare, a.spare.First, false
		a.signal()
	}

	v := Pack(a.cur.Lane, a.next)
	a.next++

	return v, true
}

func (a *Allocator) signal() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// needsRange reports whether the allocator should fetch, and which lane to ask
// for. It fetches only with no spare in hand. With a spare held and the current
// range spent, the next create swaps the spare in and signals, and the fetch
// happens then. Fetching at that point instead would hand back a newer range
// ahead of the spare, and positions in the lane would go backwards.
func (a *Allocator) needsRange() (need bool, wantLane uint8, hasLane bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	return !a.hasSpare, a.lane, a.hasLane
}

func (a *Allocator) run() {
	defer close(a.done)

	backoff := retryMin

	for {
		need, wantLane, hasLane := a.needsRange()
		if !need {
			select {
			case <-a.wake:
				continue
			case <-a.ctx.Done():
				return
			}
		}

		attemptCtx, cancel := context.WithTimeout(a.ctx, a.attemptTimeout)
		r, err := a.src.ReserveLaneRange(attemptCtx, a.holder, wantLane, hasLane)
		cancel()

		if err == nil && (r.Len() == 0 || r.First == 0 || r.Last > MaxPosition) {
			err = errors.NewProcessingError("[lane] blockchain service handed out an invalid range %d:%d-%d", r.Lane, r.First, r.Last)
		}

		if err != nil {
			if a.ctx.Err() != nil {
				return
			}

			a.logger.Warnf("[lane] could not get a range for %s, retrying in %s: %v", a.holder, backoff, err)

			select {
			case <-time.After(backoff):
			case <-a.ctx.Done():
				return
			}

			backoff = min(2*backoff, retryMax)

			continue
		}

		backoff = retryMin

		a.install(r)
	}
}

func (a *Allocator) install(r Range) {
	a.mu.Lock()

	asked, hadLane := a.lane, a.hasLane
	a.lane, a.hasLane = r.Lane, true

	if (a.next == 0 || a.next > a.cur.Last) && !a.hasSpare {
		a.cur, a.next = r, r.First
	} else {
		a.spare, a.hasSpare = r, true
	}

	a.mu.Unlock()

	// Outside the lock: Next holds it on the create path, and a create must never
	// wait on the disk.
	if !hadLane || r.Lane != asked {
		if hadLane {
			a.logger.Warnf("[lane] %s asked for lane %d and was given lane %d", a.holder, asked, r.Lane)
		}

		if err := writeLaneFile(a.file, r.Lane); err != nil {
			// The next start will ask with no lane and get a fresh one, which is
			// safe: positions are unique whichever lane a process uses.
			a.logger.Errorf("[lane] could not save lane %d to %s: %v", r.Lane, a.file, err)
		}
	}

	prometheusLaneCurrent.Set(float64(r.Lane))
	prometheusLaneRanges.Inc()

	a.readyOnce.Do(func() { close(a.ready) })
}

func readLaneFile(file string) (uint8, bool) {
	b, err := os.ReadFile(file)
	if err != nil {
		return 0, false
	}

	v, err := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 8)
	if err != nil {
		return 0, false
	}

	return uint8(v), true
}

// writeLaneFile replaces the file through a rename, so a crash mid-write
// leaves the old lane or the new one, never a torn file.
func writeLaneFile(file string, l uint8) error {
	if err := os.MkdirAll(filepath.Dir(file), 0o755); err != nil {
		return err
	}

	tmp := file + ".tmp"

	if err := os.WriteFile(tmp, []byte(strconv.Itoa(int(l))+"\n"), 0o600); err != nil {
		return err
	}

	return os.Rename(tmp, file)
}

// FileName is the name of the file a process keeps its lane number in, made
// from its client name with anything but letters, digits, dashes and
// underscores replaced, so a client name can never escape the lane folder.
func FileName(clientName string) string {
	clean := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, clientName)

	if clean == "" {
		clean = "default"
	}

	return clean + ".lane"
}
