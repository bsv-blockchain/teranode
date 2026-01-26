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
// Processing errors are logged and the message is marked as completed
// to prevent infinite retry loops on malformed data.
func (u *Server) txmetaHandler(ctx context.Context, msg *kafka.KafkaMessage) error {
	if msg == nil || len(msg.ConsumerMessage.Value) < 4 {
		return nil
	}

	// Process the message asynchronously to avoid blocking the Kafka consumer.
	// Errors are logged but do not prevent message acknowledgment.
	go func() {
		startTime := time.Now()

		data := msg.ConsumerMessage.Value
		offset := 0

		// Read entry count
		if len(data) < 4 {
			return
		}
		entryCount := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		var hash chainhash.Hash

		// Process each entry
		for i := uint32(0); i < entryCount; i++ {
			// Check minimum bytes for hash + action + length
			if offset+32+1+4 > len(data) {
				u.logger.Errorf("[txmetaHandler] truncated message at entry %d", i)
				return
			}

			// Read hash (32 bytes)
			copy(hash[:], data[offset:offset+32])
			offset += 32

			// Read action (1 byte)
			action := data[offset]
			offset++

			// Read content length (4 bytes)
			contentLen := binary.LittleEndian.Uint32(data[offset:])
			offset += 4

			if action == txmetaActionDELETE {
				// Handle DELETE
				if err := u.DelTxMetaCache(ctx, &hash); err != nil {
					prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
					u.logger.Errorf("[txmetaHandler][%s] failed to delete tx meta data: %v", hash, err)
				}
				prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
			} else {
				// Handle ADD
				if offset+int(contentLen) > len(data) {
					u.logger.Errorf("[txmetaHandler] truncated content at entry %d", i)
					return
				}

				content := data[offset : offset+int(contentLen)]
				offset += int(contentLen)

				if err := u.SetTxMetaCacheFromBytes(ctx, hash[:], content); err != nil {
					prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
					u.logger.Debugf("[txmetaHandler][%s] failed to set tx meta data: %v", hash, err)
				}
				prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(float64(time.Since(startTime).Microseconds()) / 1_000_000)
			}
		}
	}()

	return nil
}
