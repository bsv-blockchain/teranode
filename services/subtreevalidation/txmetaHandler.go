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
// Two-tier execution model, no intermediate job struct or apply goroutine — we are
// already running inside the Kafka consumer's per-partition goroutine, so we just
// dispatch directly:
//
//   - DELETEs run SYNCHRONOUSLY here. A delete typically corresponds to a tx being
//     conflicted/replaced; if the cache forgets to drop the old entry, future
//     BatchDecorate calls will return stale metadata and validation will be wrong.
//     Delete must therefore land before the Kafka offset is committed. Returning
//     an error makes the consumer skip the rest of this record without committing,
//     so on restart the message is re-delivered and the delete retried.
//
//   - ADDs spawn one fire-and-forget goroutine per entry. Best-effort: a missed
//     write falls through to the UTXO store on the next BatchDecorate. No
//     goroutine ceiling — we optimise for latency-to-cache.
//
// Memory: keys and values are SUBSLICES of msg.Value (no per-entry copies).
// franz-go's Record.Value is stable for the lifetime of the record — not pooled
// or reused — so as long as any apply goroutine holds a subslice, Go's GC keeps
// the underlying buffer alive. The only per-entry copy is the 32-byte
// chainhash.Hash for DELETEs, forced by the [32]byte array type (value copy, no
// allocation).
//
// Processing errors on malformed messages are logged and the message is acked
// (return nil) to avoid infinite retry loops on corrupt input.
func (u *Server) txmetaHandler(ctx context.Context, msg *kafka.KafkaMessage) error {
	if msg == nil || len(msg.Value) < 4 {
		return nil
	}

	data := msg.Value
	enqueued := time.Now()

	offset := 0
	entryCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	var hash chainhash.Hash

	for i := uint32(0); i < entryCount; i++ {
		if offset+32+1+4 > len(data) {
			u.logger.Errorf("[txmetaHandler] truncated message at entry %d", i)
			return nil
		}

		key := data[offset : offset+32]
		offset += 32

		action := data[offset]
		offset++

		contentLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		switch action {
		case txmetaActionDELETE:
			copy(hash[:], key)
			if err := u.DelTxMetaCache(ctx, &hash); err != nil {
				prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
				u.logger.Errorf("[txmetaHandler][%s] failed to delete tx meta data: %v", hash, err)
				return err
			}
			prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(time.Since(enqueued).Seconds())

		case txmetaActionADD:
			if offset+int(contentLen) > len(data) {
				u.logger.Errorf("[txmetaHandler] truncated content at entry %d", i)
				return nil
			}
			value := data[offset : offset+int(contentLen)]
			offset += int(contentLen)

			go func(k, v []byte) {
				if err := u.SetTxMetaCacheFromBytes(ctx, k, v); err != nil {
					prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
					u.logger.Debugf("[txmetaHandler] failed to set tx meta entry: %v", err)
				}
				prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(time.Since(enqueued).Seconds())
			}(key, value)

		default:
			// Unknown action — skip the content if any.
			offset += int(contentLen)
		}
	}

	return nil
}
