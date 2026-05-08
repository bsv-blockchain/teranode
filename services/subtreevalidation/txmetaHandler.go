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

// applyTxmetaCacheJob fans every ADD entry out to its own goroutine. Each goroutine
// does ONE SetCacheFromBytes and exits — no waiting, no bounding, no per-batch
// sequential walk. ADDs are best-effort: a missed write falls through to the UTXO
// store on the next BatchDecorate. We therefore optimise for latency-to-cache,
// not for delivery guarantees. Spawning per-entry maximises bucket-shard
// parallelism — each writer holds the bucket lock ~1 µs, and across 8192 buckets
// the contention queue stays shallow even at multi-million ops/sec.
//
// DELETEs are NOT handled here — they run synchronously in txmetaHandler so the
// Kafka offset commits only after the delete actually lands. See txmetaHandler
// for why.
//
// txmetaHandler invokes us via `go applyTxmetaCacheJob(...)` so the per-entry
// spawn cost (~1 µs per goroutine × N entries) is paid off the Kafka consumer's
// critical path and never blocks PollFetches.
func (u *Server) applyTxmetaCacheJob(ctx context.Context, job txmetaCacheJob) {
	for i := range job.addKeys {
		key := job.addKeys[i]
		value := job.addValues[i]

		go func() {
			if err := u.SetTxMetaCacheFromBytes(ctx, key, value); err != nil {
				prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
				u.logger.Debugf("[txmetaHandler] failed to set tx meta entry: %v", err)
			}
			prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(time.Since(job.enqueued).Seconds())
		}()
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
// Two-tier execution model:
//
//   - DELETEs run SYNCHRONOUSLY in this function (the Kafka consumer goroutine).
//     A delete typically corresponds to a tx being conflicted/replaced; if the
//     cache forgets to drop the old entry, future BatchDecorate calls will return
//     stale metadata and validation will be incorrect. Delete must therefore land
//     before the offset is committed. Returning an error from the handler causes
//     the consumer to skip the rest of this batch without committing, so on
//     restart the message will be re-delivered and the delete retried.
//
//   - ADDs are spawned via `go applyTxmetaCacheJob(...)` and from there each
//     entry gets its own goroutine. ADDs are best-effort: a missed write falls
//     through to the UTXO store on the next BatchDecorate. We therefore optimise
//     for latency-to-cache here, not for delivery guarantees, and impose no
//     goroutine ceiling.
//
// Processing errors on malformed messages are logged and the message is acked
// (return nil) to avoid infinite retry loops on corrupt input.
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

	// DELETEs first, synchronously. If any delete fails we surface the error so
	// the consumer doesn't commit this record. The (rare) consequence is that
	// any ADDs in this same batch get applied twice across re-delivery — that's
	// idempotent for SetCacheFromBytes (writing the same value), so safe.
	for i := range job.delHashes {
		hash := job.delHashes[i]
		if err := u.DelTxMetaCache(ctx, &hash); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Errorf("[txmetaHandler][%s] failed to delete tx meta data: %v", hash, err)
			return err
		}
		prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(time.Since(job.enqueued).Seconds())
	}

	if len(job.addKeys) > 0 {
		// Fire-and-forget for ADDs: don't block the Kafka consumer on cache
		// writes. Errors are logged inside applyTxmetaCacheJob.
		go u.applyTxmetaCacheJob(ctx, job)
	}

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
