package blockchain

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
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
	UnminedTxRouteLegacyOrphanEviction = "legacy_orphan_eviction"
	UnminedTxRoutePropagation          = "propagation"
	UnminedTxRouteValidatorKafka       = "validator_kafka"
	UnminedTxRouteRPCSendRawTx         = "rpc_sendrawtransaction"
)

// unminedTxRefusedLogInterval bounds the refusal log to one line per route per
// interval. A syncing node connected to busy peers can be offered thousands of
// transactions a second, and one line each would bury the sync's own logging.
const unminedTxRefusedLogInterval = 30 * time.Second

var prometheusUnminedTxRefused = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "teranode",
		Name:      "unmined_transactions_refused_while_syncing_total",
		Help:      "Unmined transactions dropped at intake because the blockchain FSM was CATCHINGBLOCKS, by route",
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
}

// NewUnminedTxGate returns a gate for one intake route. route is one of the
// UnminedTxRoute constants.
func NewUnminedTxGate(logger ulogger.Logger, client ClientI, route string) *UnminedTxGate {
	return &UnminedTxGate{
		client:  client,
		logger:  logger,
		route:   route,
		refused: prometheusUnminedTxRefused.WithLabelValues(route),
	}
}

// Refuses reports whether the transaction must be dropped because the node is
// syncing. When it returns true it has already counted the refusal and, at
// most once per interval, logged it. txHash names the latest refused
// transaction in that log line and may be nil.
//
// A failed state read accepts, for the same reason IDLE does: the read failing
// says nothing about whether the node is syncing.
func (g *UnminedTxGate) Refuses(ctx context.Context, txHash *chainhash.Hash) bool {
	if g == nil || g.client == nil {
		return false
	}

	state, err := g.client.GetFSMCurrentState(ctx)
	if err != nil || state == nil || !IsSyncingState(*state) {
		return false
	}

	g.refused.Inc()
	g.logRefusal(txHash, *state)

	return true
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

func (g *UnminedTxGate) logRefusal(txHash *chainhash.Hash, state FSMStateType) {
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

	g.logger.Infof("[UnminedTxGate][%s] refused %d unmined transactions while the FSM is %s, latest %s", g.route, count, state, latest)
}
