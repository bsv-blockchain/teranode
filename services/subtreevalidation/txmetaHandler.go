// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"encoding/binary"
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

// txmetaCacheJob is a parsed Kafka batch ready to be applied to the cache.
// Parsing happens on the Kafka consumer goroutine (cheap); applying happens in
// a fire-and-forget goroutine spawned by txmetaHandler so the consumer can
// immediately move on to the next record.
type txmetaCacheJob struct {
	addKeys   [][]byte
	addValues [][]byte
	delHashes []chainhash.Hash
	enqueued  time.Time
}

// applyTxmetaCacheJob does the actual cache writes for one parsed Kafka batch.
//
// Writes are emitted per-entry via SetCacheFromBytes — NOT batched into a single
// SetCacheMulti call. Reason: cache.SetMulti's bucket fan-out takes each touched
// bucket's write lock for the full duration of writing all keys mapped to that
// bucket (typically ~1 ms for 1024-key batches under load). With many concurrent
// writers, contenders queue up behind that 1 ms-ish lock holder and end-of-queue
// wait inflates with concurrency.
//
// Per-entry SetCacheFromBytes acquires/releases the bucket lock for ONE key at a
// time (~1 µs holds). Aggregate work is the same; instantaneous queue depth on
// each bucket lock is much smaller, so 99th-percentile contention drops sharply.
// This restores the lock-contention profile the txmetaHandler had before #834,
// when production was observed sustaining 2M+ ops/sec on this path.
func (u *Server) applyTxmetaCacheJob(ctx context.Context, job txmetaCacheJob) {
	for i := range job.addKeys {
		if err := u.SetTxMetaCacheFromBytes(ctx, job.addKeys[i], job.addValues[i]); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Debugf("[txmetaHandler] failed to set tx meta entry: %v", err)
		}
		prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(time.Since(job.enqueued).Seconds())
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
// Parses on the Kafka consumer goroutine and immediately spawns one goroutine per
// message to apply the writes — no channel hop, no worker pool. This is the
// minimum-latency design: every µs of queueing matters because cache fill is
// always racing against subtree validation arrival, and a stale cache forces
// fall-through to the UTXO store which is far more expensive.
//
// Per-entry SetCacheFromBytes inside applyTxmetaCacheJob keeps each bucket-lock
// hold short (~1 µs), so even thousands of concurrent goroutines drain the
// bucket-lock queues quickly without piling up.
//
// Processing errors on malformed messages are logged and the message is acked
// (return nil) to avoid infinite retry loops.
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

	// Fire-and-forget: don't block the Kafka consumer on cache writes. Errors
	// are logged inside applyTxmetaCacheJob; the Kafka offset is committed by
	// the consumer once consumerFn returns nil.
	go u.applyTxmetaCacheJob(ctx, job)

	return nil
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
		// we return from the handler — and the apply goroutine reads it later.
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
