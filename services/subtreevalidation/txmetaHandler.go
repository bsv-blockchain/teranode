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
	"github.com/bsv-blockchain/teranode/stores/txmetacache"
	"github.com/bsv-blockchain/teranode/util/kafka"
)

// Txmeta Kafka wire-format constants live in stores/txmetacache (see wire.go
// in that package). They are imported here as the single source of truth
// shared between the producer (services/validator) and all consumers.

// txmetaParseBuffers is the per-Kafka-message scratch space the handler
// reuses across invocations via txmetaParseBuffersPool. keys[i] and values[i]
// are sub-slices of msg.Value itself: every cache backend copies keys and
// values before its Set call returns, so nothing references msg.Value once the
// cache call is done, and the bytes are copied once (wire to cache) instead of
// twice.
type txmetaParseBuffers struct {
	keys    [][]byte
	values  [][]byte
	hashes  []uint64
	deletes []chainhash.Hash
}

var txmetaParseBuffersPool = sync.Pool{
	New: func() any { return &txmetaParseBuffers{} },
}

// reset clears length without dropping the backing arrays — the pool keeps
// the capacity alive for the next caller.
// release drops the references keys and values hold into msg.Value before the
// buffers go back to the pool, so a pooled entry does not keep the last Kafka
// message's buffer reachable.
func (b *txmetaParseBuffers) release() {
	clear(b.keys)
	clear(b.values)
	b.reset()
}

func (b *txmetaParseBuffers) reset() {
	b.keys = b.keys[:0]
	b.values = b.values[:0]
	b.hashes = b.hashes[:0]
	b.deletes = b.deletes[:0]
}

// txmetaMessageHandler returns a Kafka message handler for transaction metadata operations.
func (u *Server) txmetaMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		return u.txmetaHandler(ctx, msg)
	}
}

// txmetaHandler processes a Kafka message of transaction metadata operations.
//
// Two wire formats are supported, distinguished by a multi-byte signature at
// the start of the message. See stores/txmetacache/wire.go for the layout
// definitions and the rationale for the detection rules below.
//
// v2 detection requires the full 4-byte header signature (magic + version +
// 2 reserved zero bytes) AND a plausible entry count for the remaining
// buffer; otherwise the message is parsed as v1. This avoids
// misclassifying v1 messages whose entry count happens to begin with 0xFF
// (counts 255, 511, 767, ...).
//
// Dispatch model: all ADDs in the message are collected into one
// SetCacheMultiSequential call (which is partition-aware — see
// ImprovedCache.SetMultiSequential). DELETEs are processed inline. No worker
// pool, no shard fan-out — parallelism is provided by the per-partition
// Kafka consumer goroutines (see util/kafka/kafka_consumer.go) which call
// this handler concurrently across partitions.
//
// When the producer ships v2 and partitions are aligned with disjoint cache
// bucket ranges, concurrent per-partition handler invocations touch disjoint
// buckets and run lock-free relative to each other.
//
// Memory: keys and ADD content are handed to the cache as sub-slices of
// msg.Value. Kafka may recycle that buffer after the handler returns, which is
// safe because the cache copies them before its Set call returns. v2 saves the
// xxhash on receive; v1 must xxhash each key.
//
// Truncated messages are logged and acked (returning nil) so a corrupt input
// doesn't cause infinite redelivery.
func (u *Server) txmetaHandler(ctx context.Context, msg *kafka.KafkaMessage) error {
	if msg == nil || len(msg.Value) < 4 {
		return nil
	}

	data := msg.Value
	enqueuedAt := time.Now()

	var (
		offset  int
		entries uint32
		isV2    bool
	)

	// Speculative v2 detection: require the full header signature
	// (magic + version + reserved bytes) and an entry count that fits in the
	// remaining buffer at the minimum v2 entry size. Any failure falls
	// through to v1 — never silently drops a valid v1 message whose entry
	// count happens to begin with 0xFF (counts 255, 511, 767, ...).
	if len(data) >= txmetacache.WireV2HeaderLen &&
		data[0] == txmetacache.WireV2Magic &&
		data[1] == txmetacache.WireV2Version &&
		data[2] == 0 && data[3] == 0 {
		candidateCount := binary.LittleEndian.Uint32(data[4:])
		remaining := uint64(len(data) - txmetacache.WireV2HeaderLen)
		if uint64(candidateCount)*uint64(txmetacache.WireV2MinEntrySize) <= remaining {
			entries = candidateCount
			offset = txmetacache.WireV2HeaderLen
			isV2 = true
		}
	}

	if !isV2 {
		entries = binary.LittleEndian.Uint32(data[:4])
		offset = 4
	}

	// Per-entry sizes (excluding content). The shared constants in
	// stores/txmetacache encode the same numbers; using them here keeps
	// the producer and the receiver pinned to one source of truth.
	entryHeaderSize := txmetacache.WireV1MinEntrySize
	if isV2 {
		entryHeaderSize = txmetacache.WireV2MinEntrySize
	}

	// Reject implausibly large entry counts before sizing the pool buffers.
	// The pool pre-allocates `entries` slice slots, so an unbounded count
	// read straight from the wire is a DoS surface. The plausibility bound is the same shape as the v2
	// detection check: each entry needs at least `entryHeaderSize` bytes
	// excluding content, so a count larger than the remaining buffer can
	// fit is malformed. The v2 path is already guarded by the detection-
	// time check above; the guard here matters for the v1 fallback path.
	remainingForEntries := uint64(len(data) - offset)
	maxEntries := remainingForEntries / uint64(entryHeaderSize)
	if uint64(entries) > maxEntries {
		u.logger.Errorf("[txmetaHandler] entry count %d exceeds buffer capacity (%d max for %d-byte payload)",
			entries, maxEntries, remainingForEntries)
		return nil
	}

	// Pool-backed scratch: up to `entries` slice slots.
	bufs := txmetaParseBuffersPool.Get().(*txmetaParseBuffers)
	bufs.reset()
	defer func() {
		bufs.release()
		txmetaParseBuffersPool.Put(bufs)
	}()

	entriesInt := int(entries)
	if cap(bufs.keys) < entriesInt {
		bufs.keys = make([][]byte, 0, entriesInt)
	}
	if cap(bufs.values) < entriesInt {
		bufs.values = make([][]byte, 0, entriesInt)
	}
	if isV2 && cap(bufs.hashes) < entriesInt {
		bufs.hashes = make([]uint64, 0, entriesInt)
	}

	for i := uint32(0); i < entries; i++ {
		if offset+entryHeaderSize > len(data) {
			u.logger.Errorf("[txmetaHandler] truncated message at entry %d", i)
			break
		}

		var entryHash uint64
		if isV2 {
			entryHash = binary.LittleEndian.Uint64(data[offset:])
			offset += 8
		}

		// Sub-slices of msg.Value, capped so an append can never write into
		// the next entry (see txmetaParseBuffers).
		key := data[offset : offset+32 : offset+32]
		offset += 32

		action := data[offset]
		offset++

		contentLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(contentLen) > len(data) {
			u.logger.Errorf("[txmetaHandler] truncated content at entry %d", i)
			break
		}

		switch action {
		case txmetacache.WireActionADD:
			end := offset + int(contentLen)
			bufs.keys = append(bufs.keys, key)
			bufs.values = append(bufs.values, data[offset:end:end])
			if isV2 {
				bufs.hashes = append(bufs.hashes, entryHash)
			}
		case txmetacache.WireActionDELETE:
			// chainhash.Hash is a value type: the copy does not reference
			// msg.Value.
			var h chainhash.Hash
			copy(h[:], key)
			bufs.deletes = append(bufs.deletes, h)
		default:
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Errorf("[txmetaHandler] unknown txmeta action: %d", action)
		}
		offset += int(contentLen)
	}

	if len(bufs.keys) > 0 {
		var err error
		if isV2 {
			err = u.SetTxMetaCacheMultiSequentialWithHashes(ctx, bufs.keys, bufs.values, bufs.hashes)
		} else {
			err = u.SetTxMetaCacheMultiSequential(ctx, bufs.keys, bufs.values)
		}
		if err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Debugf("[txmetaHandler] failed to set tx meta data batch (%d items): %v", len(bufs.keys), err)
		}
		elapsed := float64(time.Since(enqueuedAt).Microseconds()) / 1_000_000
		prometheusSubtreeValidationSetTXMetaCacheKafkaBatch.Observe(elapsed)
		prometheusSubtreeValidationSetTXMetaCacheKafkaCount.Add(float64(len(bufs.keys)))
	}

	// A DELETE is a demotion to conflicting. The cache keeps it out against a
	// late ADD from another partition and against read-through writes (see
	// TxMetaCache.DeleteDemoted).
	if len(bufs.deletes) > 0 {
		if err := u.DelTxMetaCacheDemoted(ctx, bufs.deletes); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Errorf("[txmetaHandler] failed to delete %d demoted tx meta entries: %v", len(bufs.deletes), err)
		}

		elapsed := float64(time.Since(enqueuedAt).Microseconds()) / 1_000_000
		for range bufs.deletes {
			prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(elapsed)
		}
	}

	return nil
}
