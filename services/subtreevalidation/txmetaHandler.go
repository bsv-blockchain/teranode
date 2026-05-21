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
	// txmetaActionADD represents the ADD action for txmeta batch messages.
	txmetaActionADD = byte(0)
	// txmetaActionDELETE represents the DELETE action for txmeta batch messages.
	txmetaActionDELETE = byte(1)

	// txmetaWireV2Magic marks a v2 txmeta Kafka message. v1 messages start with
	// the low byte of a uint32 entry count, which can never be 0xFF for any
	// realistic batch size (it would require >4 billion entries per message).
	txmetaWireV2Magic = byte(0xFF)
	// txmetaWireV2Version is the only v2 sub-version defined today. The header
	// reserves room for additional sub-versions without breaking the magic-byte
	// detection.
	txmetaWireV2Version = byte(0x02)
)

// txmetaParseBuffers is the per-Kafka-message scratch space the handler
// reuses across invocations via txmetaParseBuffersPool. Each parse fills the
// slices straight from the wire format and hands them to the cache; the
// cache copies values internally (chunk-append on bucket.Set + the height
// suffix in appendHeightToValue), so the buffers are safe to recycle as soon
// as the cache call returns.
//
// Layout of the two raw-byte arenas:
//
//   - keysBuf:    one contiguous arena holding all 32-byte tx hashes back to
//     back. keys[i] is a sub-slice into this arena, never a separate alloc.
//   - contentBuf: one contiguous arena holding all entries' content bytes.
//     values[i] is a sub-slice into this arena.
//
// Both arenas are sized BEFORE the parse loop to upper bounds known from the
// Kafka message header, so append never grows them mid-loop and existing
// sub-slices never dangle.
type txmetaParseBuffers struct {
	keys       [][]byte
	values     [][]byte
	hashes     []uint64
	deletes    []chainhash.Hash
	keysBuf    []byte
	contentBuf []byte
}

var txmetaParseBuffersPool = sync.Pool{
	New: func() any { return &txmetaParseBuffers{} },
}

// reset clears length without dropping the backing arrays — the pool keeps
// the capacity alive for the next caller.
func (b *txmetaParseBuffers) reset() {
	b.keys = b.keys[:0]
	b.values = b.values[:0]
	b.hashes = b.hashes[:0]
	b.deletes = b.deletes[:0]
	b.keysBuf = b.keysBuf[:0]
	b.contentBuf = b.contentBuf[:0]
}

// txmetaMessageHandler returns a Kafka message handler for transaction metadata operations.
func (u *Server) txmetaMessageHandler(ctx context.Context) func(msg *kafka.KafkaMessage) error {
	return func(msg *kafka.KafkaMessage) error {
		return u.txmetaHandler(ctx, msg)
	}
}

// txmetaHandler processes a Kafka message of transaction metadata operations.
//
// Two wire formats are supported, distinguished by the first byte:
//
//	v1 (legacy)
//	  [4 bytes]   entry count (uint32 LE)
//	  per entry:
//	    [32 bytes] tx hash
//	    [1 byte]   action (0=ADD, 1=DELETE)
//	    [4 bytes]  content length (uint32 LE) — 0 for DELETE
//	    [N bytes]  content
//
//	v2 (partition-aware; producer-side rollout pending)
//	  [1 byte]    magic = 0xFF
//	  [1 byte]    version = 0x02
//	  [2 bytes]   reserved
//	  [4 bytes]   entry count (uint32 LE)
//	  per entry:
//	    [8 bytes]  xxhash(tx hash) (uint64 LE)
//	    [32 bytes] tx hash
//	    [1 byte]   action
//	    [4 bytes]  content length
//	    [N bytes]  content
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
// Memory: ADD content is COPIED out of msg.Value (Kafka may recycle the
// underlying buffer after the handler returns). v2 saves the xxhash on
// receive; v1 must xxhash each key.
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

	if data[0] == txmetaWireV2Magic {
		if len(data) < 8 {
			u.logger.Errorf("[txmetaHandler] truncated v2 header (%d bytes)", len(data))
			return nil
		}
		if data[1] != txmetaWireV2Version {
			u.logger.Errorf("[txmetaHandler] unknown v2 wire version %d", data[1])
			return nil
		}
		// data[2:4] reserved.
		entries = binary.LittleEndian.Uint32(data[4:])
		offset = 8
		isV2 = true
	} else {
		entries = binary.LittleEndian.Uint32(data[:4])
		offset = 4
	}

	// Per-entry sizes (excluding content).
	entryHeaderSize := 32 + 1 + 4
	if isV2 {
		entryHeaderSize += 8
	}

	// Pool-backed scratch. Sized to upper bounds derived from the wire:
	//   - up to `entries` slice slots
	//   - up to `entries * 32` bytes of hashes (each is 32 B)
	//   - up to `len(data)` bytes of content (the remainder of the buffer
	//     after the header is a strict upper bound on total content size)
	bufs := txmetaParseBuffersPool.Get().(*txmetaParseBuffers)
	bufs.reset()
	defer txmetaParseBuffersPool.Put(bufs)

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
	keysBufNeeded := entriesInt * 32
	if cap(bufs.keysBuf) < keysBufNeeded {
		bufs.keysBuf = make([]byte, 0, keysBufNeeded)
	}
	contentMax := len(data) - offset
	if cap(bufs.contentBuf) < contentMax {
		bufs.contentBuf = make([]byte, 0, contentMax)
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

		// Append the 32-byte hash into keysBuf at its current tail; the
		// sub-slice handed to keys[i] points into the same backing array.
		// Pre-sizing keysBuf above guarantees append never grows it, so
		// earlier entries' sub-slices stay valid for the whole loop.
		hashStart := len(bufs.keysBuf)
		bufs.keysBuf = append(bufs.keysBuf, data[offset:offset+32]...)
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
		case txmetaActionADD:
			contentStart := len(bufs.contentBuf)
			bufs.contentBuf = append(bufs.contentBuf, data[offset:offset+int(contentLen)]...)
			bufs.keys = append(bufs.keys, bufs.keysBuf[hashStart:hashStart+32])
			bufs.values = append(bufs.values, bufs.contentBuf[contentStart:contentStart+int(contentLen)])
			if isV2 {
				bufs.hashes = append(bufs.hashes, entryHash)
			}
		case txmetaActionDELETE:
			// chainhash.Hash is a value type — copying into deletes[]
			// captures by value, so the keysBuf arena can be recycled
			// without worry.
			var h chainhash.Hash
			copy(h[:], bufs.keysBuf[hashStart:hashStart+32])
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
		prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(elapsed)
	}

	for i := range bufs.deletes {
		if err := u.DelTxMetaCache(ctx, &bufs.deletes[i]); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Errorf("[txmetaHandler][%s] failed to delete tx meta data: %v", bufs.deletes[i], err)
		}
		prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(float64(time.Since(enqueuedAt).Microseconds()) / 1_000_000)
	}

	return nil
}
