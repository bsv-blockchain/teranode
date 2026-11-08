package lane

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
)

const (
	retryMin = 100 * time.Millisecond
	retryMax = 10 * time.Second
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
// means asking with no lane. The service may give a different lane than the
// one asked for, if another holder took it after it sat idle; the file is then
// rewritten with the new lane.
type Allocator struct {
	src    RangeSource
	holder string
	file   string
	logger ulogger.Logger

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
		wake:   make(chan struct{}, 1),
		ready:  make(chan struct{}),
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
	}
}

// Start reads the saved lane and starts fetching ranges in the background. It
// waits for the first range until ctx ends, and returns an error if none came;
// the allocator keeps trying either way, so an error here is not fatal.
func (a *Allocator) Start(ctx context.Context) error {
	if l, ok := readLaneFile(a.file); ok {
		a.lane, a.hasLane = l, true
	}

	go a.run()

	select {
	case <-a.ready:
		return nil
	case <-ctx.Done():
		return errors.NewServiceUnavailableError("[lane] no range from the blockchain service yet for %s", a.holder, ctx.Err())
	}
}

// Close stops the background fetch. Positions not handed out are lost, which
// is safe: the service never hands them out again.
func (a *Allocator) Close() {
	a.cancel()
	<-a.done
}

// Next returns the next lane position, or false if no range is in hand.
func (a *Allocator) Next() (uint64, bool) {
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

// Fill writes the next lane positions into dst, in order, and returns how many
// it wrote. The rest of dst is set to None, which the stores write as no lane
// position.
func (a *Allocator) Fill(dst []uint64) int {
	n := 0

	a.mu.Lock()

	for n < len(dst) {
		v, ok := a.nextLocked()
		if !ok {
			break
		}

		dst[n] = v
		n++
	}

	a.mu.Unlock()

	for i := n; i < len(dst); i++ {
		dst[i] = None
	}

	prometheusLanePositions.Add(float64(n))

	if n < len(dst) {
		prometheusLaneExhausted.Add(float64(len(dst) - n))
	}

	return n
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

// needsRange reports whether the allocator is short of a current or a spare
// range, and which lane to ask for.
func (a *Allocator) needsRange() (need bool, wantLane uint8, hasLane bool) {
	a.mu.Lock()
	defer a.mu.Unlock()

	curSpent := a.next == 0 || a.next > a.cur.Last

	return curSpent || !a.hasSpare, a.lane, a.hasLane
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

		r, err := a.src.ReserveLaneRange(a.ctx, a.holder, wantLane, hasLane)
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

	if !a.hasLane || r.Lane != a.lane {
		if a.hasLane {
			a.logger.Warnf("[lane] %s asked for lane %d and was given lane %d", a.holder, a.lane, r.Lane)
		}

		a.lane, a.hasLane = r.Lane, true

		if err := writeLaneFile(a.file, r.Lane); err != nil {
			// The next start will ask with no lane and get a fresh one, which is
			// safe: positions are unique whichever lane a process uses.
			a.logger.Errorf("[lane] could not save lane %d to %s: %v", r.Lane, a.file, err)
		}
	}

	if a.next == 0 || a.next > a.cur.Last {
		a.cur, a.next = r, r.First
	} else {
		a.spare, a.hasSpare = r, true
	}

	a.mu.Unlock()

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
