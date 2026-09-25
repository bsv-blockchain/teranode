package legacy

import (
	"container/list"
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/legacy/netsync"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
)

const (
	// maxRebroadcastInventory caps how many tx invs the rebroadcastHandler
	// holds at once. Beyond this, new adds are dropped: the existing
	// (older, already-retried) entries keep their retry budget instead of
	// being evicted by fresher adds that haven't yet failed. Mined txs are
	// pruned on every new block, so the cap only binds when that many txs
	// are genuinely stuck.
	maxRebroadcastInventory = 4096

	// maxRebroadcastTips is the per-entry retry budget, counted in new
	// blocks. SV Node clears its recent-rejects filter when its tip
	// changes, so one retry per block is the cadence at which a temporary
	// reject can clear. Six blocks is about an hour on mainnet.
	maxRebroadcastTips = 6

	// rebroadcastTipDelay is how long the handler waits after a new block
	// before retrying, so SV Node peers have connected the block and reset
	// their recent-rejects filter first. Blocks arriving during the wait
	// share the one retry.
	rebroadcastTipDelay = 30 * time.Second

	// rebroadcastPruneTimeout bounds the UTXO store lookup that drops mined
	// entries before each retry.
	rebroadcastPruneTimeout = 30 * time.Second

	// modifyRebroadcastInvBuffer is the channel buffer between
	// AddRebroadcastInventory callers and rebroadcastHandler. Sized to
	// absorb a short backlog while the handler is busy serving a retry;
	// AddRebroadcastInventory drops on full rather than blocking the
	// hot relay path, so this is best-effort, not lossless.
	modifyRebroadcastInvBuffer = 1024
)

// rebroadcastEntry is a pending rebroadcast inv: the original `data`
// payload handed to RelayInventory, the number of new blocks it has been
// retried on, and its parent tx hashes as last read from the UTXO store.
// Once tips reaches maxRebroadcastTips the entry ages out.
type rebroadcastEntry struct {
	iv      wire.InvVect
	data    interface{}
	tips    int
	parents []chainhash.Hash
}

// rebroadcastQueue is the set of pending rebroadcast entries in insertion
// order. Insertion order is the order txs were read from the txmeta Kafka
// topic, which is spread over partitions and so is not validation order: a
// child can be queued before its parent. retry therefore sorts each batch
// parents-first using the parent hashes pruneRebroadcastQueue records. Not
// safe for concurrent use; owned by rebroadcastHandler.
type rebroadcastQueue struct {
	capacity int
	order    *list.List
	index    map[wire.InvVect]*list.Element
}

func newRebroadcastQueue(capacity int) *rebroadcastQueue {
	return &rebroadcastQueue{
		capacity: capacity,
		order:    list.New(),
		index:    make(map[wire.InvVect]*list.Element),
	}
}

// add appends iv→data to the queue. If iv is already present, its data
// payload is refreshed but its position and tips counter are kept. Without
// this, a duplicate add (e.g. a Kafka replay) would reset the retry budget
// of a tx that should have aged out.
//
// Returns false (and does not mutate the queue) when iv is new and the queue
// is at capacity. The caller is responsible for any cap-hit telemetry.
func (q *rebroadcastQueue) add(iv wire.InvVect, data interface{}) bool {
	if el, ok := q.index[iv]; ok {
		el.Value.(*rebroadcastEntry).data = data
		return true
	}

	if len(q.index) >= q.capacity {
		return false
	}

	q.index[iv] = q.order.PushBack(&rebroadcastEntry{iv: iv, data: data})

	return true
}

// remove drops iv from the queue if present.
func (q *rebroadcastQueue) remove(iv wire.InvVect) {
	if el, ok := q.index[iv]; ok {
		q.order.Remove(el)
		delete(q.index, iv)
	}
}

func (q *rebroadcastQueue) len() int {
	return len(q.index)
}

// entries returns the pending entries in retry order.
func (q *rebroadcastQueue) entries() []*rebroadcastEntry {
	out := make([]*rebroadcastEntry, 0, len(q.index))
	for el := q.order.Front(); el != nil; el = el.Next() {
		out = append(out, el.Value.(*rebroadcastEntry))
	}

	return out
}

// retry hands every pending entry to relay as one batch with parents before
// their children, counts the retry against each entry's budget, and ages out
// entries that have reached maxTips. Entries whose parents are unknown keep
// their queue order. Returns the number of entries relayed and aged out.
func (q *rebroadcastQueue) retry(maxTips int, relay func([]relayMsg)) (relayed, agedOut int) {
	if q.len() == 0 {
		return 0, 0
	}

	entries := q.entries()

	hashes := make([]chainhash.Hash, len(entries))
	for i, entry := range entries {
		hashes[i] = entry.iv.Hash
	}

	batch := make([]relayMsg, 0, len(entries))

	for _, i := range netsync.ParentsFirst(hashes, func(i int) []chainhash.Hash { return entries[i].parents }) {
		iv := entries[i].iv
		batch = append(batch, relayMsg{invVect: &iv, data: entries[i].data, requeue: true})
	}

	for _, entry := range entries {
		entry.tips++
		if entry.tips >= maxTips {
			q.remove(entry.iv)

			agedOut++
		}
	}

	relay(batch)

	return len(batch), agedOut
}

// rebroadcastPruneResult counts the entries pruneRebroadcastQueue removed,
// by reason.
type rebroadcastPruneResult struct {
	mined       int
	conflicting int
	notFound    int
}

// pruneRebroadcastQueue drops entries that no longer need retrying: txs that
// have been mined, txs marked conflicting, and txs no longer in the UTXO
// store (which a peer could not fetch from us anyway). A lookup error keeps
// the entry: a failed prune costs one wasted retry, not a lost tx. Entries
// that stay record their parent tx hashes, which retry orders by.
//
// Looking the pending txs up is cheaper than walking the new block's
// subtrees: the queue holds at most maxRebroadcastInventory entries, while a
// block can hold millions of txs. It also catches txs mined in a block
// whose notification was missed.
//
// A tx whose only block was later orphaned by a reorg is dropped too early.
// Reorgs are rare enough that this is accepted.
func pruneRebroadcastQueue(ctx context.Context, store utxo.Store, q *rebroadcastQueue) (rebroadcastPruneResult, error) {
	var result rebroadcastPruneResult

	if store == nil || q.len() == 0 {
		return result, nil
	}

	entries := q.entries()
	lookupFields := []fields.FieldName{fields.BlockIDs, fields.Conflicting, fields.TxInpoints}

	items := make([]*utxo.UnresolvedMetaData, len(entries))
	for i, entry := range entries {
		items[i] = &utxo.UnresolvedMetaData{Hash: entry.iv.Hash, Idx: i, Fields: lookupFields}
	}

	if err := store.BatchDecorate(ctx, items, lookupFields...); err != nil {
		return result, err
	}

	for i, item := range items {
		switch {
		case item.Err != nil:
			if errors.Is(item.Err, errors.ErrTxNotFound) {
				q.remove(entries[i].iv)
				result.notFound++
			}
		case item.Data == nil:
		case len(item.Data.BlockIDs) > 0:
			q.remove(entries[i].iv)
			result.mined++
		case item.Data.Conflicting:
			q.remove(entries[i].iv)
			result.conflicting++
		default:
			entries[i].parents = item.Data.TxInpoints.ParentTxHashes
		}
	}

	return result, nil
}

// AddRebroadcastInventory adds 'iv' to the list of inventories to be
// rebroadcast after each new block until they are mined or run out of
// retries.
//
// Best-effort: sends are non-blocking. If the rebroadcastHandler is
// backlogged past the channel's buffer, the new add is dropped rather
// than blocking the hot relay path. Drops are an acceptable trade:
// the dropped tx still has its immediate RelayInventory dispatch, and
// the queue already contains older entries (which are more likely to
// actually be stuck) carrying their own retry budget.
func (s *server) AddRebroadcastInventory(iv *wire.InvVect, data interface{}) {
	// Ignore if shutting down.
	if atomic.LoadInt32(&s.shutdown) != 0 {
		return
	}

	select {
	case s.modifyRebroadcastInv <- broadcastInventoryAdd{invVect: iv, data: data}:
	default:
		// Drop on full, see doc comment above.
		s.droppedRebroadcastAdds.Add(1)
		prometheusLegacyRebroadcastAddDropped.Inc()
	}
}

// RemoveRebroadcastInventory removes 'iv' from the list of items to be
// rebroadcast if present.
func (s *server) RemoveRebroadcastInventory(iv *wire.InvVect) {
	// Ignore if shutting down.
	if atomic.LoadInt32(&s.shutdown) != 0 {
		return
	}

	select {
	case s.modifyRebroadcastInv <- broadcastInventoryDel(iv):
	default:
		// Drop on full. A missed delete only means the entry is pruned or
		// aged out on a later block. Not counted separately: a saturated
		// channel already surfaces via droppedRebroadcastAdds.
	}
}

// BlockConnected tells the rebroadcastHandler that a new block has been
// added, which schedules one retry of the pending queue. Non-blocking:
// notifications that arrive while one is already pending share its retry.
func (s *server) BlockConnected() {
	select {
	case s.rebroadcastTip <- struct{}{}:
	default:
	}
}

// RebroadcastDropCounts returns the cumulative non-blocking-send drop and
// queue-cap-hit counters for the rebroadcast queue. Never reset.
func (s *server) RebroadcastDropCounts() (adds, capHits uint64) {
	return s.droppedRebroadcastAdds.Load(), s.droppedRebroadcastCapHits.Load()
}

// retryRebroadcasts runs one retry of the pending queue: it prunes entries
// that no longer need retrying, then re-offers the rest to every connected
// peer, parents first. Skipped entirely, without spending any budget, while
// the node is not relaying txs.
func (s *server) retryRebroadcasts(q *rebroadcastQueue) {
	if !s.canRelayTx() {
		return
	}

	ctx, cancel := context.WithTimeout(s.ctx, rebroadcastPruneTimeout)
	pruned, err := pruneRebroadcastQueue(ctx, s.utxoStore, q)
	cancel()

	if err != nil {
		s.logger.Warnf("[rebroadcast] failed to prune mined txs from the rebroadcast queue, retrying all %d entries: %v", q.len(), err)
	}

	prometheusLegacyRebroadcastRemoved.WithLabelValues("mined").Add(float64(pruned.mined))
	prometheusLegacyRebroadcastRemoved.WithLabelValues("conflicting").Add(float64(pruned.conflicting))
	prometheusLegacyRebroadcastRemoved.WithLabelValues("not_found").Add(float64(pruned.notFound))

	relayed, agedOut := q.retry(maxRebroadcastTips, s.relayTxBatch)

	prometheusLegacyRebroadcastRetries.Add(float64(relayed))
	prometheusLegacyRebroadcastRemoved.WithLabelValues("aged_out").Add(float64(agedOut))

	s.logger.Debugf("[rebroadcast] retried %d txs, removed %d mined, %d conflicting, %d not found, %d aged out, %d pending", relayed, pruned.mined, pruned.conflicting, pruned.notFound, agedOut, q.len())
}

// rebroadcastHandler keeps track of inventories announced via
// AnnounceNewTransactions that have not yet been mined. After each new
// block it re-offers them to every connected peer, so a tx that hit a
// transient miss on first announce (peer not yet handshaken, brief
// disconnect, or a temporary reject by the peer) still reaches the network.
// Re-offers bypass each peer's known inventory, so peers that stayed
// connected are offered the tx again.
//
// Retries follow blocks rather than a timer because SV Node clears its
// recent-rejects filter when its tip changes: a retry before the next block
// is likely rejected again. Mined txs are pruned before each retry and every
// entry ages out after maxRebroadcastTips blocks, so the queue stays bounded.
func (s *server) rebroadcastHandler() {
	queue := newRebroadcastQueue(maxRebroadcastInventory)

	// Stopped until the first block arrives. retryPending tracks whether it
	// is armed, so blocks arriving during the delay share one retry.
	retryTimer := time.NewTimer(time.Hour)
	retryTimer.Stop()

	retryPending := false

out:
	for {
		select {
		case riv := <-s.modifyRebroadcastInv:
			switch msg := riv.(type) {
			// Incoming InvVects are added to our retry queue. Re-adds
			// of existing entries refresh the data payload but keep
			// their position and budget, see rebroadcastQueue.add.
			case broadcastInventoryAdd:
				if !queue.add(*msg.invVect, msg.data) {
					s.droppedRebroadcastCapHits.Add(1)
					prometheusLegacyRebroadcastCapHits.Inc()
				}

			// When an InvVect has been added to a block, we can
			// now remove it, if it was present.
			case broadcastInventoryDel:
				queue.remove(*msg)
			}

		case <-s.rebroadcastTip:
			if !retryPending {
				retryTimer.Reset(s.rebroadcastTipDelay)
				retryPending = true
			}

		case <-retryTimer.C:
			retryPending = false

			s.retryRebroadcasts(queue)

		case <-s.quit:
			break out
		}

		prometheusLegacyRebroadcastPending.Set(float64(queue.len()))
	}

	retryTimer.Stop()

	// Drain channels before exiting so nothing is left waiting around
	// to send.
cleanup:
	for {
		select {
		case <-s.modifyRebroadcastInv:
		default:
			break cleanup
		}
	}
	s.wg.Done()
}
