package blockchain

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	dto "github.com/prometheus/client_model/go"
)

// Routes an unmined transaction can enter the node by. Each is a label value on
// prometheusUnminedTxRefused, so a dashboard can tell which intake is busy while
// the node syncs.
const (
	UnminedTxRouteLegacyTx             = "legacy_tx"
	UnminedTxRouteLegacyOrphan         = "legacy_orphan"
	UnminedTxRouteLegacyOrphanEviction = "legacy_orphan_eviction"
	UnminedTxRoutePropagation          = "propagation"
	UnminedTxRouteValidatorKafka       = "validator_kafka"
	UnminedTxRouteRPCSendRawTx         = "rpc_sendrawtransaction"
)

// unminedTxRefusedLogInterval bounds the refusal log to one line per route per
// interval. A syncing node connected to busy peers can be offered thousands of
// transactions a second, and one line each would bury the sync's own logging.
const unminedTxRefusedLogInterval = 30 * time.Second

// bestHeightReadInterval is the shortest time between scheduling two
// best-height reads by one gate. Refuses runs once per transaction and the read
// is a gRPC call, so between reads the gate answers from what it last learned.
const bestHeightReadInterval = time.Second

// bestHeightReadTimeout bounds one best-height read, so a blockchain service
// that never answers cannot hold the gate's one read slot.
const bestHeightReadTimeout = 2 * time.Second

var prometheusUnminedTxRefused = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "teranode",
		Name:      "unmined_transactions_refused_while_syncing_total",
		Help:      "Unmined transactions dropped at intake because the node was syncing (FSM CATCHINGBLOCKS, or a netting store below the highest checkpoint), by route",
	},
	[]string{"route"},
)

// IsSyncingState reports whether a node in this FSM state must refuse unmined
// transactions from outside.
//
// Only CATCHINGBLOCKS counts. IDLE does not: the blockchain client sets its
// cached state to IDLE when its heartbeat goes stale or a state fetch fails
// (Client.go, the subscription heartbeat and the reconnect path), so a healthy
// node at the tip reads IDLE while the blockchain service restarts. Treating
// IDLE as a sync would make that node drop every transaction for the length of
// the restart. IDLE is unknown, and unknown accepts.
func IsSyncingState(state FSMStateType) bool {
	return state == FSMStateCATCHINGBLOCKS
}

// UnminedTxGate refuses unmined transactions at an intake while the node syncs.
//
// A syncing node has no use for an unmined transaction: it cannot mine it and
// cannot judge it against a current chain. Worse, teranode keeps unmined and
// mined transactions in one UTXO store, so accepting one removes the coins it
// spends. A block further up the chain that spends the same coin then finds it
// gone, which stops a netting store's below-checkpoint sync for good (issue
// 2003).
//
// The gate belongs at the intakes, never inside Validator.Validate: block and
// subtree validation call the validator during catch-up to check a block's own
// transactions, and a check there would make the node refuse its own sync.
//
// On a store that nets below the checkpoint (utxoset), the gate also refuses
// while the best height is below the highest checkpoint, in every FSM state.
// The FSM rule alone leaves a window there: an operator STOP parks the FSM in
// IDLE while a batch of blocks is still being written, and the client reports
// IDLE while the blockchain service restarts. A transaction let in then can
// delete a coin that a block further up the batch spends, and the netted write
// has no fallback. This rule fails closed: the gate refuses until a height read
// returns the checkpoint or above. The read runs in the background, so Refuses
// never waits on it, and once the checkpoint is reached the gate stops reading.
// That answer is kept until the process restarts: a tip moved back below the
// checkpoint afterwards, by invalidateblock, does not close the gate again.
//
// A refused transaction is dropped, counted and logged at most once per
// interval. It is not queued: holding transactions until RUNNING would replay
// hours-old transactions at the moment the node catches up.
//
// A nil gate, or one built without a client, refuses nothing, so code paths
// and tests that construct a service without a blockchain client keep working.
type UnminedTxGate struct {
	client  ClientI
	logger  ulogger.Logger
	route   string
	refused prometheus.Counter

	// lastLogNanos is the unix time of the last refusal log line, and
	// sinceLog the refusals since then. Both are atomics because the
	// propagation and validator intakes call Refuses from many goroutines.
	lastLogNanos atomic.Int64
	sinceLog     atomic.Uint64

	// start is when the gate was built. The read times below are nanoseconds
	// since start, measured on the monotonic clock, so a wall clock stepped
	// back cannot stall the reads.
	start time.Time

	// highestCheckpoint is the height below which the gate refuses whatever
	// the FSM says. Zero when the store does not net below the checkpoint or
	// the network has no checkpoints, which turns the rule off.
	highestCheckpoint uint32

	// reached is set once a height read returns highestCheckpoint or above,
	// and readOKNanos is when the last successful read was scheduled.
	// heightReading is true while a read is in flight, heightReadNanos is when
	// the last read was scheduled, and lastWarnNanos the last time a failed
	// read was logged, all as nanoseconds since start.
	reached         atomic.Bool
	readOKNanos     atomic.Int64
	heightReading   atomic.Bool
	heightReadNanos atomic.Int64
	lastWarnNanos   atomic.Int64
}

// NewUnminedTxGate returns a gate for one intake route. route is stored as
// given; production callers pass one of the UnminedTxRoute constants. tSettings decides whether the checkpoint rule is
// on: it is when the UTXO store URL selects a store that nets below the
// checkpoint and the network has checkpoints. A nil tSettings turns it off.
func NewUnminedTxGate(logger ulogger.Logger, client ClientI, tSettings *settings.Settings, route string) *UnminedTxGate {
	var highest uint32
	if tSettings != nil && tSettings.ChainCfgParams != nil && utxo.NetsBelowCheckpointURL(tSettings.UtxoStore.UtxoStore) {
		highest = model.HighestCheckpointHeight(tSettings.ChainCfgParams.Checkpoints)
	}

	return &UnminedTxGate{
		client:            client,
		logger:            logger,
		route:             route,
		refused:           prometheusUnminedTxRefused.WithLabelValues(route),
		highestCheckpoint: highest,
		start:             time.Now(),
	}
}

// Refuses reports whether the transaction must be dropped because the node is
// syncing. When it returns true it has already counted the refusal and, at
// most once per interval, logged it. txHash names the latest refused
// transaction in that log line and may be nil.
//
// A failed state read accepts, for the same reason IDLE does: the read failing
// says nothing about whether the node is syncing. The checkpoint rule still
// applies then.
func (g *UnminedTxGate) Refuses(ctx context.Context, txHash *chainhash.Hash) bool {
	if g == nil || g.client == nil {
		return false
	}

	// Judged once, before scheduling, so a read that lands during this call
	// cannot change its answer. The read keeps the height current through a
	// sync too, so the gate knows the checkpoint is reached by the time the
	// FSM stops refusing.
	belowCheckpoint := g.highestCheckpoint != 0 && !g.reached.Load()
	if belowCheckpoint {
		g.scheduleHeightRead()
	}

	state, err := g.client.GetFSMCurrentState(ctx)
	if err == nil && state != nil && IsSyncingState(*state) {
		g.refuse(txHash, "the FSM is "+state.String())

		return true
	}

	if belowCheckpoint {
		g.refuse(txHash, "the best height is not known to have reached the highest checkpoint")

		return true
	}

	return false
}

// scheduleHeightRead starts a background height read when none is in flight,
// the checkpoint is not yet known to be reached, and the last read was
// scheduled at least bestHeightReadInterval ago. Both conditions are checked
// while holding the read slot, so a caller that checked them before another
// caller's read landed cannot start a second read at once.
func (g *UnminedTxGate) scheduleHeightRead() {
	if !g.heightReading.CompareAndSwap(false, true) {
		return
	}

	now := g.sinceStart()
	if last := g.heightReadNanos.Load(); g.reached.Load() || (last != 0 && now-last < int64(bestHeightReadInterval)) {
		g.heightReading.Store(false)

		return
	}

	g.heightReadNanos.Store(now)

	go g.readBestHeight(now)
}

// readBestHeight reads the best height once and sets reached when it is at or
// above the highest checkpoint. scheduled is when the read was scheduled. It
// runs detached from any caller's context, because it outlives the Refuses call
// that started it.
func (g *UnminedTxGate) readBestHeight(scheduled int64) {
	defer g.heightReading.Store(false)

	ctx, cancel := context.WithTimeout(context.Background(), bestHeightReadTimeout)
	defer cancel()

	height, _, err := g.client.GetBestHeightAndTime(ctx)
	if err != nil {
		g.warnReadFailure(err)

		return
	}

	// reached before readOKNanos: a waiter that sees this read as fresh must
	// also see its answer.
	if height >= g.highestCheckpoint {
		g.reached.Store(true)
	}

	g.readOKNanos.Store(scheduled)
}

// checkpointAnswerPoll is how often WaitForCheckpointAnswer looks again until
// the checkpoint is reached, the FSM is syncing, or a height read scheduled
// after the call began has succeeded. Reads stay spaced by
// bestHeightReadInterval.
const checkpointAnswerPoll = 50 * time.Millisecond

// WaitForCheckpointAnswer returns once the checkpoint rule can judge a
// transaction by a current height: the rule is off, the checkpoint is known to
// be reached, the FSM is syncing (Refuses refuses then whatever the height), or
// a height read scheduled after this call began has succeeded. Until then it
// keeps scheduling reads. It returns ctx's error if ctx ends first.
//
// An intake that has already acknowledged its transactions calls it before
// Refuses, so a gate whose last answer is missing or stale does not drop them.
// The validator's Kafka intake is one: propagation acknowledged each message
// before queueing it, and a refused message is committed and lost.
func (g *UnminedTxGate) WaitForCheckpointAnswer(ctx context.Context) error {
	if g == nil || g.client == nil || g.highestCheckpoint == 0 || g.reached.Load() {
		return nil
	}

	began := g.sinceStart()

	ticker := time.NewTicker(checkpointAnswerPoll)
	defer ticker.Stop()

	for !g.reached.Load() && g.readOKNanos.Load() < began {
		if state, err := g.client.GetFSMCurrentState(ctx); err == nil && state != nil && IsSyncingState(*state) {
			return nil
		}

		g.scheduleHeightRead()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}

	return nil
}

// RefusesAfterCheckpointAnswer judges an acknowledged transaction after the
// wait. If the FSM stops syncing between the wait and this decision, it waits
// for a fresh height instead of dropping the transaction on an unknown height.
func (g *UnminedTxGate) RefusesAfterCheckpointAnswer(ctx context.Context, txHash *chainhash.Hash) (bool, error) {
	if g == nil || g.client == nil {
		return false, nil
	}

	began := g.sinceStart()
	if err := g.WaitForCheckpointAnswer(ctx); err != nil {
		return false, err
	}

	// Made only when the decision has to wait, so the usual answer allocates no
	// timer per message.
	var ticker *time.Ticker

	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()

	for {
		state, err := g.client.GetFSMCurrentState(ctx)
		if err == nil && state != nil && IsSyncingState(*state) {
			g.refuse(txHash, "the FSM is "+state.String())

			return true, nil
		}

		// Freshness before reached: readBestHeight stores reached first, so a
		// fresh read seen here has its reached answer visible to the next load.
		fresh := g.readOKNanos.Load() >= began

		if g.highestCheckpoint == 0 || g.reached.Load() {
			return false, nil
		}

		if fresh {
			g.refuse(txHash, "the best height is not known to have reached the highest checkpoint")

			return true, nil
		}

		g.scheduleHeightRead()

		if ticker == nil {
			ticker = time.NewTicker(checkpointAnswerPoll)
		}

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-ticker.C:
		}
	}
}

// warnReadFailure logs a failed height read at most once per refusal log
// interval. Without it a node that keeps refusing because the blockchain
// service never answers looks the same as one still below the checkpoint.
func (g *UnminedTxGate) warnReadFailure(err error) {
	now := g.sinceStart()
	last := g.lastWarnNanos.Load()

	if (last != 0 && now-last < int64(unminedTxRefusedLogInterval)) || !g.lastWarnNanos.CompareAndSwap(last, now) {
		return
	}

	g.logger.Warnf("[UnminedTxGate][%s] cannot read the best height, so unmined transactions stay refused until it reaches checkpoint %d: %v", g.route, g.highestCheckpoint, err)
}

// Route returns the route string the gate was built with, which in production
// is one of the UnminedTxRoute constants. A nil gate returns "".
func (g *UnminedTxGate) Route() string {
	if g == nil {
		return ""
	}

	return g.route
}

// sinceStart returns the nanoseconds since the gate was built, from the
// monotonic clock. It is at least 1, so a stored time is never the zero value
// that means "never".
func (g *UnminedTxGate) sinceStart() int64 {
	return int64(time.Since(g.start)) + 1
}

// CheckpointHeight returns the height below which the gate refuses whatever
// the FSM says, or 0 when that rule is off. A nil gate returns 0.
func (g *UnminedTxGate) CheckpointHeight() uint32 {
	if g == nil {
		return 0
	}

	return g.highestCheckpoint
}

func (g *UnminedTxGate) refuse(txHash *chainhash.Hash, reason string) {
	g.refused.Inc()
	g.logRefusal(txHash, reason)
}

// RefusedTotal returns how many transactions this gate's route has refused
// since the process started. The counter is shared by every gate on the same
// route, so a test compares it before and after rather than against zero.
func (g *UnminedTxGate) RefusedTotal() float64 {
	if g == nil {
		return 0
	}

	var metric dto.Metric
	if err := g.refused.Write(&metric); err != nil {
		return 0
	}

	return metric.GetCounter().GetValue()
}

func (g *UnminedTxGate) logRefusal(txHash *chainhash.Hash, reason string) {
	count := g.sinceLog.Add(1)

	now := time.Now().UnixNano()
	last := g.lastLogNanos.Load()

	if now-last < int64(unminedTxRefusedLogInterval) || !g.lastLogNanos.CompareAndSwap(last, now) {
		return
	}

	// Subtract what this line reports rather than storing zero, so refusals
	// counted by other goroutines between the Add and here go into the next line.
	g.sinceLog.Add(^(count - 1))

	latest := "unknown"
	if txHash != nil {
		latest = txHash.String()
	}

	g.logger.Infof("[UnminedTxGate][%s] refused %d unmined transactions while syncing, latest %s refused because %s", g.route, count, latest, reason)
}
