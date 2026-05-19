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
	// txmetaWorkerQueueSize bounds the per-shard channel depth. Each slot holds
	// a whole shard batch (potentially thousands of entries), so this is
	// "Kafka messages per shard worth of headroom" rather than "individual
	// records".
	txmetaWorkerQueueSize = 256
)

// txmetaShardBatch is the unit dispatched to a per-shard worker: all entries
// from a single Kafka message that hashed to the same shard. Batching at the
// Kafka-message boundary (instead of one channel send per entry) collapses
// N channel hops into 1 and lets the worker bulk-write ADDs via
// SetCacheMulti, which takes the cache bucket lock once for the whole batch.
type txmetaShardBatch struct {
	ctx        context.Context
	adds       []txmetaAdd
	deletes    []chainhash.Hash
	enqueuedAt time.Time
}

type txmetaAdd struct {
	hash    chainhash.Hash
	content []byte // copied out of msg.Value at parse time
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
// Dispatch model: parse all entries into per-shard batches keyed by hash[0],
// then dispatch one batch per non-empty shard to its worker. Workers bulk-
// write ADDs via SetCacheMulti, which takes the cache bucket lock once per
// shard-batch instead of once per entry.
//
// Sharding by hash byte preserves per-key ordering: every operation for the
// same hash lands in the same shard and is therefore applied in arrival
// order. Per-shard parallelism gives 256-way concurrency.
//
// On full shard queues:
//
//   - During startup (before u.txmetaCaughtUp latches) the enqueue blocks,
//     propagating backpressure into the Kafka poll loop so the cold cache
//     is rebuilt without dropping entries.
//
//   - Once caught up (any partition reaches its tail) the enqueue is
//     drop-on-full and the remainder of the current Kafka batch is
//     abandoned. The cache will be repopulated from Kafka on the next
//     restart; live ADDs that are dropped fall through to the UTXO store on
//     the next BatchDecorate. enqueueTxmetaShardBatch logs a Warn.
//
// Memory: ADD content is COPIED out of msg.Value because the worker may run
// after the puller has advanced past this Kafka record. DELETE entries store
// only the 32-byte chainhash.Hash by value (no allocation).
//
// Errors:
//
//   - Truncated message: logged and acked (return nil) to avoid infinite
//     retry loops on corrupt input.
//
//   - Enqueue error (shard channel send fails for a reason other than
//     full): returned, so the Kafka offset stays uncommitted and the
//     message is re-delivered on restart.
func (u *Server) txmetaHandler(ctx context.Context, msg *kafka.KafkaMessage) error {
	if msg == nil || len(msg.Value) < 4 {
		return nil
	}

	if err := u.ensureTxmetaWorkers(ctx); err != nil {
		return err
	}

	data := msg.Value
	offset := 0

	entryCount := binary.LittleEndian.Uint32(data[offset:])
	offset += 4

	// Per-shard batches built up while walking the Kafka message. Nil slot
	// means no entries for that shard in this Kafka message — for small
	// Kafka messages most of the array stays nil, which keeps the dispatch
	// loop cheap.
	var shardBatches [txmetaWorkerShardCount]*txmetaShardBatch
	enqueuedAt := time.Now()

	var hash chainhash.Hash

	for i := uint32(0); i < entryCount; i++ {
		if offset+32+1+4 > len(data) {
			u.logger.Errorf("[txmetaHandler] truncated message at entry %d", i)
			break
		}

		copy(hash[:], data[offset:offset+32])
		offset += 32

		action := data[offset]
		offset++

		contentLen := binary.LittleEndian.Uint32(data[offset:])
		offset += 4

		if offset+int(contentLen) > len(data) {
			u.logger.Errorf("[txmetaHandler] truncated content at entry %d", i)
			break
		}

		shard := int(hash[0]) % txmetaWorkerShardCount
		b := shardBatches[shard]
		if b == nil {
			b = &txmetaShardBatch{ctx: ctx, enqueuedAt: enqueuedAt}
			shardBatches[shard] = b
		}

		switch action {
		case txmetaActionADD:
			content := make([]byte, contentLen)
			copy(content, data[offset:offset+int(contentLen)])
			b.adds = append(b.adds, txmetaAdd{hash: hash, content: content})
		case txmetaActionDELETE:
			b.deletes = append(b.deletes, hash)
		default:
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Errorf("[txmetaHandler][%s] unknown txmeta action: %d", hash, action)
		}
		offset += int(contentLen)
	}

	// Dispatch each non-empty shard batch. enqueueTxmetaShardBatch enforces
	// the two-mode (startup-block / caught-up-drop) contract per shard.
	for shard, b := range shardBatches {
		if b == nil {
			continue
		}
		ok, err := u.enqueueTxmetaShardBatch(ctx, shard, b)
		if err != nil {
			return err
		}
		if !ok {
			// Caught-up mode + full queue: abandon the remaining shard
			// batches in this Kafka message. enqueueTxmetaShardBatch
			// already emitted the Warn log.
			return nil
		}
	}

	// Latch from startup (blocking) to caught-up (drop-on-full) mode the
	// first time we observe a message at the partition's tail. One-way:
	// never reverts.
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

		u.txmetaWorkerQueues = make([]chan *txmetaShardBatch, txmetaWorkerShardCount)
		for shard := 0; shard < txmetaWorkerShardCount; shard++ {
			ch := make(chan *txmetaShardBatch, txmetaWorkerQueueSize)
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

func (u *Server) runTxmetaWorker(ctx context.Context, workQueue <-chan *txmetaShardBatch) {
	defer u.txmetaWorkerWg.Done()

	// Workers exit immediately on context cancellation without draining remaining
	// queue items. This is intentional: in-flight txmeta updates are best-effort
	// and the cache will be repopulated from Kafka on restart.
	for {
		select {
		case <-ctx.Done():
			return
		case batch := <-workQueue:
			u.processTxmetaShardBatch(batch)
		}
	}
}

func (u *Server) processTxmetaShardBatch(b *txmetaShardBatch) {
	// DELETEs are processed one-at-a-time: they're rare relative to ADDs
	// and the cache exposes no batch-delete API today.
	for i := range b.deletes {
		if err := u.DelTxMetaCache(b.ctx, &b.deletes[i]); err != nil {
			prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
			u.logger.Errorf("[txmetaHandler][%s] failed to delete tx meta data: %v", b.deletes[i], err)
		}
		prometheusSubtreeValidationDelTXMetaCacheKafka.Observe(float64(time.Since(b.enqueuedAt).Microseconds()) / 1_000_000)
	}

	if len(b.adds) == 0 {
		return
	}

	// Single SetCacheMulti call for the whole shard batch: the cache
	// bucket mutex is taken once instead of len(b.adds) times.
	keys := make([][]byte, len(b.adds))
	values := make([][]byte, len(b.adds))
	for i := range b.adds {
		keys[i] = b.adds[i].hash[:]
		values[i] = b.adds[i].content
	}

	if err := u.SetTxMetaCacheMulti(b.ctx, keys, values); err != nil {
		prometheusSubtreeValidationSetTXMetaCacheKafkaErrors.Inc()
		u.logger.Debugf("[txmetaHandler] failed to set tx meta data batch (%d items): %v", len(keys), err)
	}

	// Per-batch metric observation; histogram now records per-shard-batch
	// latency rather than per-item. Downstream dashboards that assumed one
	// observation == one record need adjustment.
	elapsed := float64(time.Since(b.enqueuedAt).Microseconds()) / 1_000_000
	prometheusSubtreeValidationSetTXMetaCacheKafka.Observe(elapsed)
}

// enqueueTxmetaShardBatch dispatches a shard batch to its worker queue. Two
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
// Returns (true, nil) on successful enqueue, (false, nil) when the batch was
// dropped in caught-up mode, and (false, ctx.Err()) when ctx is cancelled
// during a startup-mode blocking send.
func (u *Server) enqueueTxmetaShardBatch(ctx context.Context, shard int, b *txmetaShardBatch) (bool, error) {
	ch := u.txmetaWorkerQueues[shard]

	if u.txmetaCaughtUp.Load() {
		select {
		case ch <- b:
			return true, nil
		default:
			u.logger.Warnf("[txmetaHandler] txmeta worker queue full for shard %d, dropping remainder of batch", shard)
			return false, nil
		}
	}

	select {
	case ch <- b:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
