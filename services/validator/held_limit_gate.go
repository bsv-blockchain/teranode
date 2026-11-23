package validator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/settings"
)

const (
	// defaultHeldGatePollInterval is the poll cadence when the backpressure poll interval is not
	// set to a positive value.
	defaultHeldGatePollInterval = time.Second

	// heldGateIdleInterval is how often the gate re-reads the stats while block assembly reports no
	// in-memory limit or the last read failed, so a limit enabled later is noticed.
	heldGateIdleInterval = 10 * time.Second

	// heldGateFreshnessPolls is how many poll intervals a reading stays valid. A gate that has not
	// heard from block assembly for longer reports no limit, so a dark signal never sheds.
	heldGateFreshnessPolls = 5
)

// heldLimitGate gives the validator a cheap view of block assembly's in-memory transaction limit.
// A transaction that block assembly would refuse at that limit is shed before the validator
// touches the UTXO store, so the steady state at the limit costs no spend, create or unwind.
//
// The gate reads the same queue stats as the Kafka backpressure controller. When the controller
// runs it feeds each reading to observe, and the gate's own poll skips a read that a fresh
// observation already covers, so the stats are read once per interval, not twice. The gate does
// not need the controller to be enabled.
//
// It fails open. Without a fresh reading, with no reported limit, or once failed reads have aged
// the last reading past the freshness window, atLimit is false and the transaction takes the
// ordinary path, where block assembly still refuses it and the validator unwinds.
type heldLimitGate struct {
	reader   queueStatsReader
	interval time.Duration
	timeout  time.Duration
	now      func() time.Time

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once

	observedAt atomic.Int64
	limited    atomic.Bool
	heldCount  atomic.Int64
	maxHeld    atomic.Int64
}

// newHeldLimitGate builds a gate that reads from reader. It starts no goroutine. The first call to
// atLimit starts the poll loop, which ends when ctx is cancelled or stop is called.
func newHeldLimitGate(ctx context.Context, cfg settings.ValidatorKafkaBackpressureSettings, reader queueStatsReader) *heldLimitGate {
	interval := cfg.PollInterval
	if interval <= 0 {
		interval = defaultHeldGatePollInterval
	}

	timeout := cfg.ReadTimeout
	if timeout <= 0 {
		timeout = defaultKafkaBackpressureReadTimeout
	}

	gateCtx, cancel := context.WithCancel(ctx)

	return &heldLimitGate{
		reader:   reader,
		interval: interval,
		timeout:  timeout,
		now:      time.Now,
		ctx:      gateCtx,
		cancel:   cancel,
	}
}

// atLimit reports whether block assembly holds as many transactions in memory as its limit
// allows. It is safe to call on a nil gate, which reports false.
func (g *heldLimitGate) atLimit() bool {
	if g == nil {
		return false
	}

	g.once.Do(func() { go g.run(g.ctx) })

	if !g.limited.Load() {
		return false
	}

	return g.now().UnixNano()-g.observedAt.Load() <= int64(heldGateFreshnessPolls)*int64(g.interval)
}

// held returns the held count and limit of the last reading, for the shed error.
func (g *heldLimitGate) held() (held, limit int64) {
	return g.heldCount.Load(), g.maxHeld.Load()
}

// observe records a reading taken by another reader of the same stats.
func (g *heldLimitGate) observe(stats blockassembly.QueueStats) {
	if g == nil {
		return
	}

	g.heldCount.Store(stats.HeldCount)
	g.maxHeld.Store(stats.MaxHeld)
	g.limited.Store(stats.MaxHeld > 0 && stats.HeldCount >= stats.MaxHeld)
	g.observedAt.Store(g.now().UnixNano())
}

// stop ends the poll loop. It is safe to call on a nil gate.
func (g *heldLimitGate) stop() {
	if g != nil {
		g.cancel()
	}
}

// poll reads the stats once unless a fresh observation already covers this interval. A failed
// read leaves the last reading to age out, and once it is older than the freshness window it
// clears the limited state, so the gate fails open. It returns how long to wait before the next
// poll.
func (g *heldLimitGate) poll(ctx context.Context) time.Duration {
	if g.now().UnixNano()-g.observedAt.Load() < 2*int64(g.interval) {
		return g.wait()
	}

	readCtx, cancel := context.WithTimeout(ctx, g.timeout)
	stats, err := g.reader.GetBlockAssemblyQueueStats(readCtx)

	cancel()

	if err != nil {
		// A reading inside the freshness window keeps the poll cadence, so one slow read does not
		// leave the gate idle for ten seconds. The reading is not renewed, so atLimit fails open
		// once the window ends.
		if g.now().UnixNano()-g.observedAt.Load() <= int64(heldGateFreshnessPolls)*int64(g.interval) {
			return g.interval
		}

		g.limited.Store(false)
		g.maxHeld.Store(0)

		return heldGateIdleInterval
	}

	g.observe(stats)

	return g.wait()
}

func (g *heldLimitGate) wait() time.Duration {
	if g.maxHeld.Load() > 0 {
		return g.interval
	}

	return heldGateIdleInterval
}

// run polls until ctx is cancelled.
func (g *heldLimitGate) run(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			timer.Reset(g.poll(ctx))
		}
	}
}
