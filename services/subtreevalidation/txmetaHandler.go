// Package subtreevalidation provides functionality for validating subtrees in a blockchain context.
// It handles the validation of transaction subtrees, manages transaction metadata caching,
// and interfaces with blockchain and validation services.
package subtreevalidation

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/util/kafka"
)

const (
	// txmetaActionADD represents the ADD action for txmeta batch messages
	txmetaActionADD = byte(0)
	// txmetaActionDELETE represents the DELETE action for txmeta batch messages
	txmetaActionDELETE = byte(1)

	// txmetaWorkerShardCount shards work by hash byte to preserve per-key ordering.
	txmetaWorkerShardCount = 256
	// txmetaWorkerQueueSize bounds in-flight work per shard to avoid unbounded goroutine growth.
	txmetaWorkerQueueSize = 256
)

type txmetaWorkItem struct {
	ctx        context.Context
	hash       chainhash.Hash
	action     byte
	content    []byte
	enqueuedAt time.Time
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
// Processing errors are logged and the message is marked as completed
// to prevent infinite retry loops on malformed data.
func (u *Server) txmetaHandler(ctx context.Context, msg *kafka.KafkaMessage) error {
	if msg == nil || len(msg.Value) < 4 {
		return nil
	}

	if err := u.ensureTxmetaWorkers(ctx); err != nil {
		return err
	}

	data := msg.Value
	offset := 0

	// Read entry count
	entryCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	var hash chainhash.Hash

	// Parse and dispatch each entry to a bounded shard worker queue.
	for i := uint32(0); i < entryCount; i++ {
		// Check minimum bytes for hash + action + length
		if offset+32+1+4 > len(data) {
			u.logger.Errorf("[txmetaHandler] truncated message at entry %d", i)
			return nil
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

		if offset+int(contentLen) > len(data) {
			u.logger.Errorf("[txmetaHandler] truncated content at entry %d", i)
			return nil
		}

		var content []byte
		if action == txmetaActionADD {
			content = make([]byte, contentLen)
			copy(content, data[offset:offset+int(contentLen)])
		}
		offset += int(contentLen)

		workItem := txmetaWorkItem{
			ctx:        ctx,
			hash:       hash,
			action:     action,
			content:    content,
			enqueuedAt: time.Now(),
		}
		ok, err := u.enqueueTxmetaWorkItem(ctx, workItem)
		if err != nil {
			return err
		}
		if !ok {
			// Caught-up (normal) mode: shard queue is full. The remainder of
			// the batch is abandoned; the cache will be repopulated from Kafka
			// on the next restart (best-effort by design).
			// enqueueTxmetaWorkItem has already emitted the Warn log.
			return nil
		}
	}

	// Latch from startup (blocking) to caught-up (drop-on-full) mode the first
	// time we observe a message at the partition's tail. One-way: never reverts.
	u.maybeMarkTxmetaCaughtUp(msg)

	return nil
}

// maybeMarkTxmetaCaughtUp flips the txmetaCaughtUp latch the first time a Kafka
// message is observed at the partition's high water mark. HighWaterMark is the
// next offset that will be produced; msg.Offset+1 == HighWaterMark means this
// message is the current tail of the partition.
//
// Latch semantics (deliberate trade-offs):
//
//   - One-way: once set it stays set even if the consumer falls behind later
//     (e.g., a long pause and re-catch-up). Live drop semantics persist.
//
//   - Any-partition: for multi-partition txmeta topics, the latch flips as
//     soon as ANY assigned partition reaches its tail, not when all do. This
//     is intentional — txmeta is sharded by tx hash, partitions are evenly
//     loaded under normal traffic, so seeing the tail on one partition is a
//     strong signal we're broadly caught up. A stricter per-partition gating
//     would extend cold-cache blocking unnecessarily if one partition is
//     temporarily empty or slow.
//
//   - Fail-closed on HighWaterMark<=0: an unset HWM (in-memory consumer,
//     hand-constructed messages) keeps the latch in startup (blocking) mode.
//     Smoke tests are low-throughput so the shard queues should never fill,
//     making the blocking mode effectively a no-op there.
func (u *Server) maybeMarkTxmetaCaughtUp(msg *kafka.KafkaMessage) {
	if u.txmetaCaughtUp.Load() {
		return
	}
	if msg.HighWaterMark <= 0 || msg.Offset+1 < msg.HighWaterMark {
		return
	}
	if u.txmetaCaughtUp.CompareAndSwap(false, true) {
		u.logger.Infof("[txmetaHandler] caught up on %s partition %d at offset %d (HWM %d); switching to drop-on-full mode", msg.Topic, msg.Partition, msg.Offset, msg.HighWaterMark)
	}
}

func (u *Server) ensureTxmetaWorkers(ctx context.Context) error {
	u.txmetaWorkerInitOnce.Do(func() {
		workerCtx, cancel := context.WithCancel(ctx)
		u.txmetaWorkerCancel = cancel

		u.txmetaWorkerQueues = make([]chan txmetaWorkItem, txmetaWorkerShardCount)
		for shard := 0; shard < txmetaWorkerShardCount; shard++ {
			ch := make(chan txmetaWorkItem, txmetaWorkerQueueSize)
			u.txmetaWorkerQueues[shard] = ch
			u.txmetaWorkerWg.Add(1)
			go u.runTxmetaWorker(workerCtx, ch)
		}
	})

	if len(u.txmetaWorkerQueues) == 0 {
		return errors.NewProcessingError("[txmetaHandler] txmeta worker queues not initialized")
	}

	return nil
}

func (u *Server) runTxmetaWorker(ctx context.Context, workQueue <-chan txmetaWorkItem) {
	defer u.txmetaWorkerWg.Done()

	// Workers exit immediately on context cancellation without draining remaining
	// queue items. This is intentional: in-flight txmeta updates are best-effort
	// and the cache will be repopulated from Kafka on restart.
	for {
		select {
		case <-ctx.Done():
			return
		case workItem := <-workQueue:
			u.processTxmetaWorkItem(workItem)
		}
	}
}

func (u *Server) processTxmetaWorkItem(workItem txmetaWorkItem) {
	switch workItem.action {
	case txmetaActionDELETE:
		if err := u.DelTxMetaCache(workItem.ctx, &workItem.hash); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Errorf("[txmetaHandler][%s] failed to delete tx meta data: %v", workItem.hash, err)
		}
		prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(float64(time.Since(workItem.enqueuedAt).Microseconds()) / 1_000_000)
	case txmetaActionADD:
		if err := u.SetTxMetaCacheFromBytes(workItem.ctx, workItem.hash[:], workItem.content); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Debugf("[txmetaHandler][%s] failed to set tx meta data: %v", workItem.hash, err)
		}
		prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(float64(time.Since(workItem.enqueuedAt).Microseconds()) / 1_000_000)
	default:
		prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
		u.logger.Errorf("[txmetaHandler][%s] unknown txmeta action: %d", workItem.hash, workItem.action)
	}
}

// enqueueTxmetaWorkItem dispatches a work item to the shard worker queue. Two
// modes:
//
//   - Startup mode (txmetaCaughtUp == false): block on send until the worker
//     accepts the item or ctx is cancelled. This applies natural backpressure
//     to the Kafka consumer during catch-up so no work is dropped while the
//     cache is cold.
//
//   - Caught-up mode (txmetaCaughtUp == true): non-blocking send; if the shard
//     queue is full, log a warning and signal the caller to abandon the
//     remainder of the batch. Live-traffic backpressure is by design
//     best-effort (the cache repopulates from Kafka on restart).
//
// Returns (true, nil) on successful enqueue, (false, nil) when an item was
// dropped in caught-up mode, and (false, ctx.Err()) when ctx is cancelled
// during a startup-mode blocking send.
func (u *Server) enqueueTxmetaWorkItem(ctx context.Context, workItem txmetaWorkItem) (bool, error) {
	shard := int(workItem.hash[0]) % len(u.txmetaWorkerQueues)
	ch := u.txmetaWorkerQueues[shard]

	if u.txmetaCaughtUp.Load() {
		select {
		case ch <- workItem:
			return true, nil
		default:
			u.logger.Warnf("[txmetaHandler] txmeta worker queue full for shard %d, dropping remainder of batch", shard)
			return false, nil
		}
	}

	select {
	case ch <- workItem:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
