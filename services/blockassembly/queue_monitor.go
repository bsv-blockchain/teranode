package blockassembly

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/teranode/ulogger"
)

// QueueMonitorInterval is how often a QueueMonitor samples block assembly's
// queue depth. One lightweight RPC per interval per monitor; reading the
// verdict is a single atomic load.
const QueueMonitorInterval = time.Second

// queueMonitorTimeout bounds each queue-depth poll. Deliberately longer than
// the tick so a slow-but-answering block assembly still produces a verdict
// rather than a stream of DeadlineExceeded (ticks that fire while a poll is in
// flight are effectively skipped because the loop is synchronous).
const queueMonitorTimeout = 4 * QueueMonitorInterval

// queueMonitorFailOpenAfter is the number of CONSECUTIVE poll errors after
// which the monitor clears an overloaded verdict. Keeping the last verdict
// through a couple of failed polls avoids flapping on a transient blip, but a
// persistently unreachable block assembly (crash-looping is the realistic case
// when its queue really did fill) must not leave callers shedding forever on a
// stale verdict.
const queueMonitorFailOpenAfter = 5

// QueueMonitor watches block assembly's ingest-queue depth and caches whether
// it exceeds a configured limit, so hot paths can consult a single atomic bool
// instead of making an RPC per transaction. It is the shared enforcement
// helper for blockassembly_max_queued_transactions, used by the validator
// (pre-validation shed) and by propagation (ingress shed, before a transaction
// is stored or published — the only point where every submitter path still
// gets a synchronous, retryable rejection).
//
// The polls use the dedicated GetQueueLength RPC — an atomic read server-side
// that never round-trips the subtree processor's main loop, so it keeps
// answering during exactly the block-movement stalls the limit exists for.
//
// Failure policy: a poll error keeps the last verdict for up to
// queueMonitorFailOpenAfter consecutive failures, then fails OPEN (verdict
// cleared, loud warning). Fail-open is safe because if block assembly is truly
// down its Store() calls fail loudly on their own; fail-closed would reject
// the world on a stale verdict. The polling cadence also means the queue can
// overshoot the limit by up to one interval of ingest before verdicts flip;
// the limit is a soft bound with bounded overshoot, not an exact cap.
type QueueMonitor struct {
	logger     ulogger.Logger
	client     ClientI
	limit      int64
	overloaded atomic.Bool
}

// NewQueueMonitor starts a queue monitor polling client for the service
// lifetime (bounded by ctx). Returns nil when the limit is not positive or the
// client is nil, so callers can unconditionally consult the result via the
// nil-safe Overloaded.
func NewQueueMonitor(ctx context.Context, logger ulogger.Logger, client ClientI, limit int64) *QueueMonitor {
	if limit <= 0 || client == nil {
		return nil
	}

	m := &QueueMonitor{
		logger: logger,
		client: client,
		limit:  limit,
	}

	go m.run(ctx)

	return m
}

// Overloaded reports the cached verdict. Nil-safe: a nil monitor (limit
// disabled) always reports false.
func (m *QueueMonitor) Overloaded() bool {
	return m != nil && m.overloaded.Load()
}

func (m *QueueMonitor) run(ctx context.Context) {
	ticker := time.NewTicker(QueueMonitorInterval)
	defer ticker.Stop()

	consecutiveErrors := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pollCtx, cancel := context.WithTimeout(ctx, queueMonitorTimeout)
			queueLength, err := m.client.GetQueueLength(pollCtx)
			cancel()

			if err != nil {
				consecutiveErrors++

				if consecutiveErrors == queueMonitorFailOpenAfter && m.overloaded.Swap(false) {
					m.logger.Warnf("[QueueMonitor] %d consecutive queue-depth poll failures; failing OPEN (clearing overloaded verdict) — ingest resumes, block assembly Store() errors will surface on their own: %v", consecutiveErrors, err)
				} else {
					m.logger.Debugf("[QueueMonitor] failed to get block assembly queue length (%d consecutive): %v", consecutiveErrors, err)
				}

				continue
			}

			consecutiveErrors = 0

			overloaded := queueLength > m.limit
			if overloaded != m.overloaded.Swap(overloaded) {
				if overloaded {
					m.logger.Warnf("[QueueMonitor] block assembly queue %d exceeds limit %d; shedding mempool ingest until it drains", queueLength, m.limit)
				} else {
					m.logger.Infof("[QueueMonitor] block assembly queue %d back under limit %d; accepting mempool ingest", queueLength, m.limit)
				}
			}
		}
	}
}
