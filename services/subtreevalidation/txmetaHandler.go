// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"encoding/binary"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/util/kafka"
)

const (
	// txmetaActionADD represents the ADD action for txmeta batch messages
	txmetaActionADD = byte(0)
	// txmetaActionDELETE represents the DELETE action for txmeta batch messages
	txmetaActionDELETE = byte(1)
)

// txmetaCacheJob is a parsed Kafka batch ready for the cache-write worker to apply.
// We parse on the Kafka consumer goroutine (cheap) and ship the result to the workers,
// so the workers spend their time only on cache writes (the expensive part).
type txmetaCacheJob struct {
	addKeys   [][]byte
	addValues [][]byte
	delHashes []chainhash.Hash
	enqueued  time.Time
}

// startTxmetaCacheWorkers spins up the bounded worker pool that consumes parsed txmeta
// batches from u.txmetaCacheJobCh and writes them into the cache. It is called once
// from Server.start() before the txmeta Kafka consumer is started.
//
// Why a worker pool with batched cache writes:
//
// The previous implementation spawned an unbounded `go func()` per Kafka message and,
// inside that goroutine, called SetCacheFromBytes once per entry sequentially. Under burst
// load this produced thousands of goroutines all serializing through the cache's per-bucket
// write locks (improved_cache.go bucket.mu.Lock), causing the visible "0 → 2.4M → 0"
// oscillation in Kafka throughput dashboards. A small bounded pool emitting one
// SetCacheMulti per batch lets the cache fan out to its sharded buckets internally,
// taking each touched bucket lock once per Kafka message instead of once per entry.
func (u *Server) startTxmetaCacheWorkers(ctx context.Context) {
	workers := u.settings.SubtreeValidation.TxmetaCacheKafkaWorkers
	if workers <= 0 {
		workers = 1
	}

	for i := 0; i < workers; i++ {
		u.txmetaCacheWg.Add(1)
		go u.txmetaCacheWorker(ctx)
	}

	u.logger.Infof("[txmetaHandler] cache-write worker pool started: workers=%d queue=%d",
		workers, cap(u.txmetaCacheJobCh))
}

// txmetaCacheWorker consumes parsed batches and applies them to the cache.
//
// Shutdown is driven by closing u.txmetaCacheDone (NOT by closing the work channel).
// Closing the work channel would race with the Kafka consumer goroutine that calls
// txmetaHandler: if the consumer is mid-send when we close, the runtime panics with
// "send on closed channel". The done-signal pattern lets both sides observe shutdown
// safely without depending on the consumer being fully joined.
//
// When the done signal fires, each worker drains any remaining buffered jobs with a
// non-blocking read loop, then exits. We do NOT use a single select over (done, ch)
// at the outer level because Go's pseudorandom selection can pick the done branch
// even when jobs are still buffered, dropping work on the floor.
//
// ctx is still passed through to the cache writes so that long-running ops can be
// interrupted, but the loop's lifecycle is owned by the done signal.
func (u *Server) txmetaCacheWorker(ctx context.Context) {
	defer u.txmetaCacheWg.Done()

	for {
		select {
		case <-u.txmetaCacheDone:
			for {
				select {
				case job := <-u.txmetaCacheJobCh:
					u.applyTxmetaCacheJob(ctx, job)
				default:
					return
				}
			}
		case job := <-u.txmetaCacheJobCh:
			u.applyTxmetaCacheJob(ctx, job)
		}
	}
}

// applyTxmetaCacheJob does the actual cache writes for one parsed Kafka batch.
// One SetCacheMulti call covers all ADDs; deletes go through one-at-a-time because
// the volume is tiny and the cache exposes them per-key.
func (u *Server) applyTxmetaCacheJob(ctx context.Context, job txmetaCacheJob) {
	if len(job.addKeys) > 0 {
		if err := u.SetTxMetaCacheMulti(ctx, job.addKeys, job.addValues); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Debugf("[txmetaHandler] failed to set %d tx meta entries: %v", len(job.addKeys), err)
		}
		// One observation per entry preserves the historical metric semantics. The
		// observed latency is per-batch wall time — biased upward versus the old
		// per-entry stopwatch, but reflects real cache-write cost, which is what
		// you actually want to alert on.
		elapsed := time.Since(job.enqueued).Seconds()
		for range job.addKeys {
			prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(elapsed)
		}
	}

	for i := range job.delHashes {
		hash := job.delHashes[i]
		if err := u.DelTxMetaCache(ctx, &hash); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Errorf("[txmetaHandler][%s] failed to delete tx meta data: %v", hash, err)
		}
		prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(time.Since(job.enqueued).Seconds())
	}
}

// txmetaMessageHandler returns a Kafka message handler for transaction metadata operations.
//
// This wrapper provides the context to the actual handler function.
func (u *Server) txmetaMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		return u.txmetaHandler(ctx, msg)
	}
}

// txmetaHandler processes Kafka messages for transaction metadata cache operations.
// Messages use a binary batch format:
// [4 bytes]  - entry count (uint32, little-endian)
// For each entry:
//
//	[32 bytes] - tx hash (raw bytes)
//	[1 byte]   - action (0=ADD, 1=DELETE)
//	[4 bytes]  - content length (uint32, little-endian) - 0 for DELETE
//	[N bytes]  - content (metaBytes) - only for ADD
//
// Parsing happens on the Kafka consumer goroutine. The parsed batch is then enqueued onto
// txmetaCacheJobCh; if the channel is full this enqueue blocks, which intentionally
// backpressures the Kafka consumer rather than letting goroutines pile up unboundedly.
//
// Processing errors on malformed messages are logged and the message is acked (return nil)
// to avoid infinite retry loops.
func (u *Server) txmetaHandler(ctx context.Context, msg *kafka.KafkaMessage) error {
	if msg == nil || len(msg.Value) < 4 {
		return nil
	}

	job, ok := parseTxmetaBatch(u.logger, msg.Value)
	if !ok {
		return nil
	}
	if len(job.addKeys) == 0 && len(job.delHashes) == 0 {
		return nil
	}
	job.enqueued = time.Now()

	// Blocking enqueue: applies backpressure to the Kafka consumer when workers are busy.
	// Selecting on txmetaCacheDone makes shutdown safe — if Stop has been called we drop
	// the job rather than block (or panic, which would happen if we instead closed the
	// work channel from Stop while the Kafka consumer was mid-send).
	select {
	case <-u.txmetaCacheDone:
		return nil
	case <-ctx.Done():
		return nil
	case u.txmetaCacheJobCh <- job:
		return nil
	}
}

// parseTxmetaBatch decodes a single Kafka batch message into a txmetaCacheJob.
// On a truncated or malformed message the partial result is discarded and (zero, false)
// is returned, matching the prior behavior.
func parseTxmetaBatch(logger interface {
	Errorf(format string, args ...interface{})
}, data []byte) (txmetaCacheJob, bool) {
	if len(data) < 4 {
		return txmetaCacheJob{}, false
	}

	offset := 0
	entryCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	job := txmetaCacheJob{
		addKeys:   make([][]byte, 0, entryCount),
		addValues: make([][]byte, 0, entryCount),
	}

	for i := uint32(0); i < entryCount; i++ {
		if offset+32+1+4 > len(data) {
			logger.Errorf("[txmetaHandler] truncated message at entry %d", i)
			return txmetaCacheJob{}, false
		}

		// Hash. Copy onto the heap because the message buffer may be reused once
		// we return from the handler — and the worker pool reads it later.
		key := make([]byte, 32)
		copy(key, data[offset:offset+32])
		offset += 32

		action := data[offset]
		offset++

		contentLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		switch action {
		case txmetaActionDELETE:
			var hash chainhash.Hash
			copy(hash[:], key)
			job.delHashes = append(job.delHashes, hash)
		case txmetaActionADD:
			if offset+int(contentLen) > len(data) {
				logger.Errorf("[txmetaHandler] truncated content at entry %d", i)
				return txmetaCacheJob{}, false
			}
			// Same heap-copy reasoning as for the key.
			value := make([]byte, contentLen)
			copy(value, data[offset:offset+int(contentLen)])
			offset += int(contentLen)
			job.addKeys = append(job.addKeys, key)
			job.addValues = append(job.addValues, value)
		default:
			// Unknown action — skip the content if any.
			offset += int(contentLen)
		}
	}

	return job, true
}

// initTxmetaCacheWorkerPool sets up the channel, done signal, and waitgroup used by the
// worker pool. Called from Server.New so tests that don't run start() still get a usable
// (no-op-able) channel; workers are not actually spawned until start() calls
// startTxmetaCacheWorkers.
func (u *Server) initTxmetaCacheWorkerPool() {
	queueSize := u.settings.SubtreeValidation.TxmetaCacheKafkaQueueSize
	if queueSize < 0 {
		queueSize = 0
	}
	u.txmetaCacheJobCh = make(chan txmetaCacheJob, queueSize)
	u.txmetaCacheDone = make(chan struct{})
	u.txmetaCacheWg = &sync.WaitGroup{}
	u.txmetaCacheCloseOnce = &sync.Once{}
}

// stopTxmetaCacheWorkers closes the done signal and waits for in-flight workers to drain.
// The work channel itself is intentionally NOT closed — see the comment on
// txmetaCacheDone in Server.go for the race that motivates this. Idempotent (sync.Once
// on the close) so callers can defer it without worrying about double-close panics.
// Called from Server.Stop().
func (u *Server) stopTxmetaCacheWorkers() {
	if u.txmetaCacheCloseOnce != nil && u.txmetaCacheDone != nil {
		u.txmetaCacheCloseOnce.Do(func() {
			close(u.txmetaCacheDone)
		})
	}
	if u.txmetaCacheWg != nil {
		u.txmetaCacheWg.Wait()
	}
}
