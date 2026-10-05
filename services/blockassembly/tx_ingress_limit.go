package blockassembly

import (
	"context"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain"
)

const (
	// txIngressEvaluateInterval is the default interval at which the block assembler compares the
	// transactions it holds in memory against the configured limit.
	txIngressEvaluateInterval = 1 * time.Second

	// txIngressHeartbeatInterval is the default interval at which the block assembler re-announces
	// a standing refusal, so subscribers that missed a transition still converge. It must stay well
	// below blockchain.blockAssemblyFullTTL (60s).
	txIngressHeartbeatInterval = 10 * time.Second

	// txIngressPublishTimeout bounds one publish. The notification bus ends in a blocking send, so
	// without a deadline a stalled fan-out would park the monitor with nothing logged.
	txIngressPublishTimeout = 5 * time.Second

	// txIngressMinLimit is the smallest limit that can have a resume watermark. The count always
	// includes the coinbase placeholder node, so it can never be below 1, and the resume watermark
	// has to sit at or above 1 and strictly below the limit for the count to reach it.
	txIngressMinLimit = 2
)

// resumeWatermark derives the low watermark at which transaction ingress resumes.
//
// A configured resume value is used as-is when it is at least 1 and below the limit. Otherwise it
// falls back to 90% of the limit. Requiring the resume value to sit strictly below the limit is
// what gives the hysteresis: if the two were equal the node would flap between accepting and
// refusing on every transaction once it settled at the limit.
//
// The result is never below 1. TransactionsInMemory includes the coinbase placeholder node that
// the first subtree always carries, so a resume watermark of 0 is a value the count can never
// reach and ingress would stay refused for good. The limit must therefore be at least
// txIngressMinLimit; normalizeTxIngressLimit enforces that before this is called.
func resumeWatermark(limit, configuredResume uint64) uint64 {
	if limit == 0 {
		return 0
	}

	if configuredResume >= 1 && configuredResume < limit {
		return configuredResume
	}

	// 90% of the limit, expressed so it cannot overflow and stays exact for small limits.
	// For limits 2 to 9 the integer division leaves limit/10 at 0, so resume equals limit and is
	// pulled down to limit-1, the largest value strictly below the limit.
	resume := limit - limit/10
	if resume >= limit {
		resume = limit - 1
	}

	return max(resume, 1)
}

// normalizeTxIngressLimit returns the limit that is actually enforced, and whether it differs from
// the configured value. A limit of 1 cannot work, because the count never drops below 1 and so
// never reaches any resume watermark below the limit.
func normalizeTxIngressLimit(limit uint64) (uint64, bool) {
	if limit > 0 && limit < txIngressMinLimit {
		return txIngressMinLimit, true
	}

	return limit, false
}

// TransactionsInMemory returns the number of transactions block assembly currently holds in RAM.
//
// This is the sum of the transactions that reached a subtree and those still waiting in the queue.
// The two are counted separately because the subtree processor increments its own counter only once
// a transaction is dequeued into a subtree.
//
// The result can under-report slightly, because the subtree processor removes a transaction from
// the queue and increments its counter as two separate steps, so transactions can be in flight
// between them. That does not matter against a limit sized to available RAM.
//
// The count includes the coinbase placeholder node that the first subtree always carries.
func (b *BlockAssembler) TransactionsInMemory() uint64 {
	count := b.subtreeProcessor.TxCount()

	if queued := b.subtreeProcessor.QueueLength(); queued > 0 {
		count += uint64(queued)
	}

	return count
}

// IsTxIngressFull reports whether block assembly has reached its in-memory transaction limit.
func (b *BlockAssembler) IsTxIngressFull() bool {
	return b.txIngressFull.Load()
}

// RefusesTransactions reports whether block assembly must refuse a batch of transactions now.
//
// It is the hard bound. The published flag lags by up to the evaluation interval and is read by
// other processes, so on its own it would let the validator keep draining its Kafka backlog into
// block assembly well past the limit. Checking the count here, at the one place every validator
// handoff passes through, bounds the in-memory count by the limit plus the batches that pass the
// check at the same moment, however long that backlog is, and covers every path that reaches block
// assembly, including ones with no gate of their own such as the validator's own listeners.
func (b *BlockAssembler) RefusesTransactions() bool {
	if b.txIngressLimit == 0 {
		return false
	}

	count := b.TransactionsInMemory()

	// During startup the published flag is held true, but what this assembler holds is still
	// small. Shedding every handoff for that whole window would drop transactions the Kafka path has
	// already acknowledged, for no memory benefit, so the hard bound is the count alone then.
	if b.txIngressStartupPending.Load() {
		return count >= b.txIngressLimit
	}

	return b.txIngressFull.Load() || count >= b.txIngressLimit
}

// evaluateTxIngressFull recomputes the ingress flag from the transactions currently held in memory.
//
// Returns:
//   - bool: the flag value after evaluation
//   - bool: whether the flag changed
func (b *BlockAssembler) evaluateTxIngressFull() (full bool, changed bool) {
	// While Start is running, what this assembler holds does not describe what it is about to
	// hold: it measures near empty and then refills to whatever made it full in the first place.
	// Refuse for the whole of that window rather than announce room we are about to take back.
	//
	// This must not depend on the flag already being set. A process that restarted while full
	// comes up with txIngressFull at its zero value, so a guard that only suppressed clearing
	// would be inert exactly when it is needed: every ingress point would expire its cached
	// refusal after the TTL and reopen partway through a reload that can run for minutes. So the
	// refusal is established here rather than merely held.
	//
	// This is startup only. loadUnminedTransactions also runs from reset(), which is steady-state
	// operation after a reorg, and a reset is measured like any other moment. Pinning ingress for
	// the length of every reorg reload would turn nodes away when block assembly is nearly empty.
	if b.txIngressLimit > 0 && b.txIngressStartupPending.Load() {
		if b.txIngressFull.Swap(true) {
			return true, false
		}

		b.logger.Warnf("[BlockAssembler] transaction ingress full=true while block assembly starts, holding %d transactions in memory (limit %d, resume %d)",
			b.TransactionsInMemory(), b.txIngressLimit, b.txIngressResume)

		return true, true
	}

	// The first evaluation after startup decides against the high watermark. Inheriting the
	// startup refusal would hold ingress closed until the count fell to the resume watermark,
	// although startup is over and there may be room below the limit.
	if b.txIngressStartupEnded.CompareAndSwap(true, false) && b.txIngressLimit > 0 {
		count := b.TransactionsInMemory()
		full = count >= b.txIngressLimit

		if b.txIngressFull.Swap(full) == full {
			return full, false
		}

		b.logger.Warnf("[BlockAssembler] transaction ingress full=%t after start, holding %d transactions in memory (limit %d, resume %d)",
			full, count, b.txIngressLimit, b.txIngressResume)

		return full, true
	}

	return b.applyTxIngressCount(b.TransactionsInMemory())
}

// applyTxIngressCount moves the ingress flag for a given number of transactions held in memory.
//
// The flag moves on two different thresholds. It is set once the count reaches the limit, and
// cleared only once the count falls back to the resume watermark. That hysteresis stops the node
// flapping between accepting and refusing, which would otherwise produce a notification storm
// every time a single transaction crossed the limit.
//
// This holds the hysteresis rule on its own, separate from measuring the subtree processor, so the
// boundary behaviour can be exercised directly.
func (b *BlockAssembler) applyTxIngressCount(count uint64) (full bool, changed bool) {
	if b.txIngressLimit == 0 {
		// the limit is disabled, so ingress is never refused
		return false, b.txIngressFull.Swap(false)
	}

	wasFull := b.txIngressFull.Load()

	switch {
	case !wasFull && count >= b.txIngressLimit:
		full = true
	case wasFull && count <= b.txIngressResume:
		full = false
	default:
		// between the watermarks, so hold the current value
		return wasFull, false
	}

	if b.txIngressFull.Swap(full) == full {
		return full, false
	}

	b.logger.Warnf("[BlockAssembler] transaction ingress full=%t, holding %d transactions in memory (limit %d, resume %d)",
		full, count, b.txIngressLimit, b.txIngressResume)

	return full, true
}

// publishTxIngressFull broadcasts the current ingress flag to the rest of the node.
//
// It goes over the blockchain notification bus rather than the FSM, because fullness is orthogonal
// to the node lifecycle: a full node must stay RUNNING so that p2p sync, catchup and legacy sync
// keep working. Ingress points cache the value and read it per transaction.
//
// The send is bounded by txIngressPublishTimeout. It returns nil when there is no blockchain client
// to send to.
func (b *BlockAssembler) publishTxIngressFull(ctx context.Context, full bool) error {
	if b.blockchainClient == nil {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, txIngressPublishTimeout)
	defer cancel()

	if err := b.blockchainClient.SendNotification(ctx, blockchain.NewBlockAssemblyFullNotification(full)); err != nil {
		b.logger.Errorf("[BlockAssembler] error publishing transaction ingress full=%t: %v", full, err)

		return err
	}

	return nil
}

// startTxIngressLimitMonitor watches how many transactions block assembly holds in memory and keeps
// the rest of the node informed.
//
// It publishes on every transition, and re-announces a standing refusal on a slower heartbeat so a
// subscriber that starts or reconnects after a transition converges rather than holding its
// default. The monitor does nothing when the limit is disabled.
//
// The heartbeat is what keeps a refusal alive: the ingress points expire a cached full=true when the
// heartbeat stops, so a block assembly that is reconfigured without a limit, or that stops
// altogether, releases them rather than leaving them refusing forever. See blockAssemblyFullTTL in
// services/blockchain. Only a refusal is re-announced, because that expiry is also how a subscriber
// converges on not-full, so repeating not-full would add nothing.
//
// Start runs this before the slow parts of startup, so a process that restarted while full
// re-establishes the refusal rather than letting it expire mid-reload.
func (b *BlockAssembler) startTxIngressLimitMonitor(ctx context.Context) {
	if b.txIngressLimit == 0 {
		b.logger.Infof("[BlockAssembler] no in-memory transaction limit configured, ingress is never refused")
		return
	}

	// fall back to the defaults for an assembler that was not built through NewBlockAssembler,
	// so a zero interval can never reach time.NewTicker
	if b.txIngressEvaluateInterval <= 0 {
		b.txIngressEvaluateInterval = txIngressEvaluateInterval
	}

	if b.txIngressHeartbeatInterval <= 0 {
		b.txIngressHeartbeatInterval = txIngressHeartbeatInterval
	}

	b.wg.Add(1)

	go func() {
		defer b.wg.Done()

		// On the way out, reset the local flag and gauge. Nothing else writes them, so a monitor
		// that exits while full would leave a permanent 1 on a process that is no longer refusing
		// anything, and GetBlockAssemblyState would keep reporting it.
		//
		// No clearing notification is published. In a split deployment block assembly may be
		// restarting, and announcing room while it is down would reopen the ingress points against
		// a backlog it is about to reload. Their cached refusal lapses on its own after the TTL.
		defer func() {
			b.txIngressFull.Store(false)

			prometheusBlockAssemblerTxIngressFull.Set(0)
		}()

		// Set while a transition has been decided but not yet successfully announced. Owned by
		// this goroutine alone, so it needs no synchronisation.
		publishPending := false

		evaluateTicker := time.NewTicker(b.txIngressEvaluateInterval)
		defer evaluateTicker.Stop()

		heartbeatTicker := time.NewTicker(b.txIngressHeartbeatInterval)
		defer heartbeatTicker.Stop()

		for {
			select {
			case <-ctx.Done():
				b.logger.Infof("[BlockAssembler] stopping transaction ingress limit monitor")
				return

			case <-evaluateTicker.C:
				full, changed := b.evaluateTxIngressFull()
				if changed {
					publishPending = true

					prometheusBlockAssemblerTxIngressFull.Set(boolToFloat64(full))
				}

				// Retry a transition whose publish failed, and keep retrying until one lands.
				//
				// A lost full=true recovers on its own, because the heartbeat below re-announces
				// it. A lost full=false does not: not-full is deliberately never repeated, so
				// nothing else would ever carry it. Without this retry the ingress points would go
				// on refusing until their cached refusal expired, which is up to
				// blockAssemblyFullTTL of a node turning away transactions it has room for.
				//
				// The value published is the current one rather than the one that failed. A
				// further transition in the meantime supersedes it, and the ingress points only
				// ever want the latest.
				if publishPending {
					if err := b.publishTxIngressFull(ctx, full); err == nil {
						publishPending = false
					}
				}

			case <-heartbeatTicker.C:
				// Re-announce a refusal, and only a refusal. That is the value the ingress points
				// cannot recover on their own, because their cached full=true expires without it.
				// A repeated full=false carries no information: a client that has heard nothing
				// already accepts transactions, and one that missed the clearing transition
				// converges through the same expiry.
				if b.txIngressFull.Load() {
					_ = b.publishTxIngressFull(ctx, true)
				}
			}
		}
	}()
}

// errTxIngressFull builds the error a refused batch returns. It reuses the threshold class the
// ingest queue bound already sheds with, so every existing consumer treats it the same way: the
// gRPC status is ResourceExhausted, which the retry interceptor deliberately does not retry,
// propagation answers HTTP 503, and the validator unwinds its UTXO work for the shed transactions.
func (b *BlockAssembler) errTxIngressFull() error {
	return errors.NewThresholdExceededError(
		"block assembly holds %d transactions in memory, limit %d",
		b.TransactionsInMemory(), b.txIngressLimit)
}

// boolToFloat64 converts a boolean to the 0/1 representation Prometheus gauges use.
func boolToFloat64(b bool) float64 {
	if b {
		return 1
	}

	return 0
}
