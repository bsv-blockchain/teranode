package netsync

import (
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/settings"
	utxostore "github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// The transaction announcement queue.
//
// A peer's inv message names up to wire.MaxInvPerMsg transactions. Each one
// used to get a goroutine and a store Get, and on 2026-10-19 that reached
// 223,118 goroutines on Hetzner mainnet before the container's memory limit
// killed the node. Now the peer's read loop copies a message's transaction
// items out of the decoded message in chunks and charges each chunk to the
// node cap before the message goes to the block handler (newInvMsg). The
// handler goroutine charges each chunk to the peer's share and pushes it onto
// one node-wide queue without waiting (queueTxChunks), and sixteen workers
// drain the queue into UTXO store batch reads.
//
// What the path holds, from the read loop until a worker finishes with a
// chunk, is the cap, legacy_invQueueMaxHashes, times about 40 bytes a hash;
// at most invHandlerSlots handler goroutines that carry only transaction items;
// and, for each peer, at most maxRequestedTxns hashes found missing and waiting
// in its request queue for a getdata.
const (
	// invLookupWorkers is the number of goroutines that take announcements
	// off invQueue and look them up in the UTXO store. It caps the store
	// reads in flight from this path, which the review of PR 2045 asked for,
	// and is not a memory bound (a batch coalesces to under two chunks, so
	// sixteen workers hold at most about 20 MB in flight),
	// so it is a constant rather than a setting.
	invLookupWorkers = 16

	// invHandlerSlots is the most handleInvMsg goroutines that carry only
	// transaction items at once. A handler waits on the blockchain service
	// (current(), and the cached FSM state), so a slow blockchain service
	// leaves one waiting per message; the node cap bounds the hashes they
	// carry but not their stacks, and a flood of one-hash messages costs a
	// goroutine a hash. A handler normally finishes in about a millisecond,
	// and the node receives a few hundred inv messages a second from twenty
	// peers, so 1,024 is far above what an honest load keeps busy, and costs
	// at most tens of megabytes of stacks. A message carrying a block item
	// does not need a slot and is never refused one.
	invHandlerSlots = 1024

	// invPeerShare is the most announced hashes one peer may have queued or
	// being looked up at once: two maximum-size announcements. It has to hold
	// at least one whole announcement, because queueTxChunks pushes all
	// thirteen chunks of a 50,000-hash message in microseconds and a worker
	// releases a chunk only after a store round trip of milliseconds; a share
	// of 32,768 would drop the tail of every maximum-size message from an
	// honest peer on an idle node. Two covers a peer sending two such messages
	// back to back; holding more than two in flight needs the peer to deliver
	// 7 MB of inv faster than the workers drain it, which no internet peer's
	// link does. It is clamped to the node cap when the cap is smaller.
	invPeerShare = 2 * wire.MaxInvPerMsg

	// defaultInvQueueMaxHashes is the fallback for a legacy_invQueueMaxHashes
	// below one chunk, and the same number as the setting's default in
	// settings/settings.go: eight maximum-size announcements.
	defaultInvQueueMaxHashes = 8 * wire.MaxInvPerMsg

	// The reasons announcements are dropped, as prometheusLegacyNetsyncInvDropped
	// labels them.
	invDropPeerShare    = "peer_share"
	invDropNodeCap      = "node_cap"
	invDropHandlersBusy = "handlers_busy"
	invDropRequestQueue = "request_queue"
	invDropLookupFailed = "lookup_failed"
)

// invDropReasons lists every drop reason so the metric has a zero row for
// each before the first drop.
var invDropReasons = []string{invDropPeerShare, invDropNodeCap, invDropHandlersBusy, invDropRequestQueue, invDropLookupFailed}

// invChunk is up to txInvLookupChunk transaction announcements from one peer.
// items are copies, in an array of the chunk's own, so a chunk never keeps the
// decoded message it came from alive. state is the peer's registration at the
// time of queueing, set by queueTxChunks. A worker compares it with the current
// one, so a peer that reconnected while the chunk waited is not asked for
// hashes it announced on its previous connection.
type invChunk struct {
	peer  *peerpkg.Peer
	state *peerSyncState
	items []wire.InvVect
}

// invQueueMaxHashes returns legacy_invQueueMaxHashes, or the default with a
// warning when the setting is below one chunk. Zero is deliberately not
// unbounded, unlike legacy_maxOrphanTxs: an unbounded queue is the incident.
func invQueueMaxHashes(logger ulogger.Logger, tSettings *settings.Settings) int {
	n := tSettings.Legacy.InvQueueMaxHashes
	if n < txInvLookupChunk {
		logger.Warnf("[legacy] legacy_invQueueMaxHashes=%d is below one chunk of %d, using the default %d", n, txInvLookupChunk, defaultInvQueueMaxHashes)

		return defaultInvQueueMaxHashes
	}

	return n
}

// initInvQueue builds the announcement queue for a cap of maxHashes. The
// channel holds one pointer per chunk and is sized to the cap in hashes, so
// the hash count, not the channel, is the limit that binds: chunks range from
// one hash to txInvLookupChunk, and a channel sized in chunks would bind first
// on the small announcements SV Node's trickle relay sends.
func (sm *SyncManager) initInvQueue(maxHashes int) {
	sm.invQueueCap = maxHashes
	sm.invQueue = make(chan *invChunk, maxHashes)
}

// invShare is the per-peer share in force: invPeerShare, or the cap when the
// cap is smaller, in which case one peer can fill the queue.
func (sm *SyncManager) invShare() int {
	return min(invPeerShare, sm.invQueueCap)
}

// newInvMsg turns a decoded inv message into what the block handler is sent:
// the block items, and the transaction items cut into chunks of
// txInvLookupChunk and charged to the node cap. It runs on the peer's read loop
// (QueueInv) or the Kafka listener, so it never waits.
//
// Both kinds of item are copied out of the decoded message. The decoder puts
// every item of a message in one backing array, up to 50,000 of them, and a
// pointer to any one item keeps the whole array alive; a chunk that kept such
// pointers would hold a whole message while the counters charged it for one
// chunk, and a dropped chunk's items would stay in memory under a counter that
// says they are gone. With the copies nothing downstream refers to the decoded
// message, and every transaction announcement the node holds from here until
// a lookup worker finishes with it is charged to invQueued.
//
// A chunk over the node cap is dropped here and counted. The peer's share is
// charged later, by queueTxChunks once the handler has resolved the peer's
// state, because a peer's first messages can reach QueueInv before the block
// handler has registered it. Transaction items are not charged in headers-first
// mode, where handleInvMsg ignores them, and every item type other than block
// and transaction is left out, as handleInvMsg ignores those in every state.
func (sm *SyncManager) newInvMsg(inv *wire.MsgInv, peer *peerpkg.Peer) *invMsg {
	var blocks, txs []*wire.InvVect

	for _, iv := range inv.InvList {
		switch iv.Type {
		case wire.InvTypeBlock:
			blocks = append(blocks, iv)
		case wire.InvTypeTx:
			txs = append(txs, iv)
		}
	}

	msg := &invMsg{inv: &wire.MsgInv{InvList: copyInvVects(blocks)}, peer: peer}

	if len(txs) > 0 && sm.invQueue != nil && !sm.headersFirstMode.Load() {
		msg.txChunks = sm.chargeTxInvs(peer, txs)
	}

	return msg
}

// copyInvVects returns copies of items in one fresh backing array, so the
// copies keep nothing alive of the array the items point into.
func copyInvVects(items []*wire.InvVect) []*wire.InvVect {
	if len(items) == 0 {
		return nil
	}

	values := make([]wire.InvVect, len(items))
	out := make([]*wire.InvVect, len(items))

	for i, iv := range items {
		values[i] = *iv
		out[i] = &values[i]
	}

	return out
}

// chargeTxInvs cuts transaction announcements into chunks of
// txInvLookupChunk and charges each to the node cap. A chunk that fits is
// copied into an array of its own; one that does not is dropped whole and
// counted by hashes, and nothing of it is kept. It never waits.
func (sm *SyncManager) chargeTxInvs(peer *peerpkg.Peer, txInvs []*wire.InvVect) []*invChunk {
	capHashes := int64(sm.invQueueCap)

	var chunks []*invChunk

	for start := 0; start < len(txInvs); start += txInvLookupChunk {
		items := txInvs[start:min(start+txInvLookupChunk, len(txInvs))]
		n := int64(len(items))

		if sm.invQueued.Add(n) > capHashes {
			sm.invQueued.Add(-n)
			sm.dropTxInvs(peer, n, invDropNodeCap)

			continue
		}

		values := make([]wire.InvVect, len(items))
		for i, iv := range items {
			values[i] = *iv
		}

		chunks = append(chunks, &invChunk{peer: peer, items: values})
	}

	prometheusLegacyNetsyncInvQueuedHashes.Set(float64(sm.invQueued.Load()))

	return chunks
}

// queueTxChunks marks every announcement in chunks as known for the peer,
// charges each chunk to the peer's share and queues it for the lookup workers.
// A chunk over the share is dropped and counted, and its node charge given
// back. It never waits, because it runs on the goroutine handling the peer's
// message. The counts it leaves raised are given back by releaseInvChunk once
// a worker has finished with the chunk, so they cover hashes being looked up
// as well as hashes waiting.
func (sm *SyncManager) queueTxChunks(peer *peerpkg.Peer, state *peerSyncState, chunks []*invChunk) {
	if len(chunks) == 0 {
		return
	}

	share := int64(sm.invShare())

	for _, c := range chunks {
		for i := range c.items {
			peer.AddKnownInventory(&c.items[i])
		}

		n := int64(len(c.items))

		if state.queuedInvs.Add(n) > share {
			state.queuedInvs.Add(-n)
			sm.giveBackTxChunk(c, invDropPeerShare)

			continue
		}

		c.peer, c.state = peer, state

		select {
		case sm.invQueue <- c:
		default:
			// The channel is sized to the cap, so this needs a queue narrower
			// than the cap, which production never builds.
			state.queuedInvs.Add(-n)
			sm.giveBackTxChunk(c, invDropNodeCap)
		}
	}

	prometheusLegacyNetsyncInvQueuedHashes.Set(float64(sm.invQueued.Load()))
}

// giveBackTxChunks gives back the node charge of chunks that will not be
// queued, and counts them as dropped for reason unless reason is empty. An
// empty reason is for announcements handleInvMsg ignores rather than drops:
// from a peer it does not know, from a peer other than the sync peer while
// the node is not current, outside RUNNING, or in headers-first mode. Those
// were never counted as drops.
func (sm *SyncManager) giveBackTxChunks(chunks []*invChunk, reason string) {
	if len(chunks) == 0 {
		return
	}

	for _, c := range chunks {
		sm.giveBackTxChunk(c, reason)
	}

	prometheusLegacyNetsyncInvQueuedHashes.Set(float64(sm.invQueued.Load()))
}

// giveBackTxChunk is giveBackTxChunks for one chunk, without the gauge update.
func (sm *SyncManager) giveBackTxChunk(c *invChunk, reason string) {
	n := int64(len(c.items))
	sm.invQueued.Add(-n)

	if reason != "" {
		sm.dropTxInvs(c.peer, n, reason)
	}
}

// dispatchInvMsg starts the goroutine that handles msg, as the block handler
// has always done for an inv message, so a slow blockchain lookup in
// handleInvMsg never holds the block handler up. A message carrying a block
// item always gets one, as on main: a block announcement is never dropped. A
// message carrying only transaction items gets one only while fewer than
// invHandlerSlots such handlers are running; otherwise its chunks are dropped
// under handlers_busy and their node charge given back.
func (sm *SyncManager) dispatchInvMsg(msg *invMsg) {
	if len(msg.inv.InvList) > 0 {
		go sm.handleInvMsg(msg)

		return
	}

	if len(msg.txChunks) == 0 {
		return
	}

	if sm.invHandlers.Add(1) > invHandlerSlots {
		sm.invHandlers.Add(-1)
		sm.giveBackTxChunks(msg.txChunks, invDropHandlersBusy)

		return
	}

	go func() {
		defer sm.invHandlers.Add(-1)

		sm.handleInvMsg(msg)
	}()
}

// dropTxInvs counts and logs announcements that were not, or will not be,
// looked up. Nothing is requested for them: the node relies on another peer
// announcing the transaction, or on fetching it with the block that mines it.
func (sm *SyncManager) dropTxInvs(peer *peerpkg.Peer, n int64, reason string) {
	prometheusLegacyNetsyncInvDropped.WithLabelValues(reason).Add(float64(n))
	sm.logger.Debugf("[invQueue] dropping %d transaction announcements from %s: %s", n, peer, reason)
}

// releaseInvChunk gives a chunk's hashes back to its peer's share and to the
// node count once a worker has finished with it.
func (sm *SyncManager) releaseInvChunk(c *invChunk) {
	n := int64(len(c.items))
	c.state.queuedInvs.Add(-n)
	prometheusLegacyNetsyncInvQueuedHashes.Set(float64(sm.invQueued.Add(-n)))
}

// takeInvBatch returns first plus, without waiting, further chunks from the
// queue until the batch holds at least txInvLookupChunk hashes or the queue
// is empty. Under load a batch spans many peers' small announcements, which
// is the cross-peer batching the shared get batcher gave, without its 10 ms
// timer; at a quiet tip a one-hash batch goes out at once.
func (sm *SyncManager) takeInvBatch(first *invChunk) []*invChunk {
	chunks := []*invChunk{first}
	total := len(first.items)

	for total < txInvLookupChunk {
		select {
		case c := <-sm.invQueue:
			chunks = append(chunks, c)
			total += len(c.items)
		default:
			return chunks
		}
	}

	return chunks
}

// lookupInvBatch is one worker step. It takes a batch starting with first,
// marks each hash it is going to read in requestedTxns, reads the batch from
// the UTXO store in slices of at most txInvLookupChunk, appends every hash
// the store lacks to its peer's request queue, and gives every chunk's hashes
// back to the counts. It returns the chunks whose peer's request queue gained
// items, for sendInvRequests.
//
// Before the read, in queue order, a hash is skipped when its peer is no
// longer registered under the same state (it left, or reconnected), when it
// has been rejected, or when SetIfAbsent finds it marked already: another
// peer's copy is being read, or was requested within the last ten seconds.
// Marking before the read, rather than after it in the drain, is what makes
// the copies from other peers that arrive during the read skip the store too;
// a transaction every peer announces is read about once instead of once per
// peer. A hash the store turns out to hold stays marked until it expires,
// which blocks nothing because the node holds it.
//
// A hash the store lacks goes to its peer's request queue unless that queue
// already holds maxRequestedTxns transactions, one getdata's worth: then it is
// unmarked, so another peer's announcement can fetch it, and counted under
// request_queue. The queue only grows past what one drain sends while a block
// at its head waits for room in the download ledger.
//
// A failed slice unmarks itself and every slice after it so the next
// announcement retries, and counts those hashes under lookup_failed; the
// slices read before it are still acted on. A per-item error other than
// not-found unmarks that hash and is counted in one warning for the batch.
func (sm *SyncManager) lookupInvBatch(first *invChunk) []*invChunk {
	chunks := sm.takeInvBatch(first)

	defer func() {
		for _, c := range chunks {
			sm.releaseInvChunk(c)
		}
	}()

	items := make([]*utxostore.UnresolvedMetaData, 0, txInvLookupChunk)
	invs := make([]*wire.InvVect, 0, txInvLookupChunk)
	owners := make([]*invChunk, 0, txInvLookupChunk)

	for _, c := range chunks {
		if current, ok := sm.peerStates.Get(c.peer); !ok || current != c.state {
			continue
		}

		for i := range c.items {
			iv := &c.items[i]

			if _, rejected := sm.rejectedTxns.Get(iv.Hash); rejected {
				continue
			}

			if !sm.requestedTxns.SetIfAbsent(iv.Hash, struct{}{}) {
				continue
			}

			items = append(items, &utxostore.UnresolvedMetaData{Hash: iv.Hash, Idx: len(items)})
			invs = append(invs, iv)
			owners = append(owners, c)
		}
	}

	if len(items) == 0 {
		return nil
	}

	// One store read per slice of at most txInvLookupChunk, whatever the batch
	// coalesced to: an Aerospike server refuses a batch request over its
	// batch-max-requests, 5,000 by default, and one refusal would drop every
	// hash in the batch under exactly the load that makes a batch coalesce.
	var (
		touched  []*invChunk
		seen     = make(map[*invChunk]struct{})
		failed   int
		full     int
		firstErr error
	)

	for start := 0; start < len(items); start += txInvLookupChunk {
		end := min(start+txInvLookupChunk, len(items))

		if err := sm.utxoStore.BatchDecorate(sm.ctx, items[start:end], fields.Fee); err != nil {
			for _, item := range items[start:] {
				sm.requestedTxns.Delete(item.Hash)
			}

			sm.logger.Warnf("[lookupInvBatch] Unexpected failure when checking %d announced transactions for existing inventory: %v", len(items)-start, err)
			prometheusLegacyNetsyncInvDropped.WithLabelValues(invDropLookupFailed).Add(float64(len(items) - start))

			break
		}

		for i := start; i < end; i++ {
			item := items[i]

			if item.Err == nil && item.Data != nil {
				// Held: processed completely at our end.
				continue
			}

			if item.Err != nil && !errors.Is(item.Err, errors.ErrTxNotFound) {
				sm.requestedTxns.Delete(item.Hash)

				failed++

				if firstErr == nil {
					firstErr = item.Err
				}

				continue
			}

			c := owners[i]

			if c.state.queuedTxRequests.Add(1) > maxRequestedTxns {
				c.state.queuedTxRequests.Add(-1)
				sm.requestedTxns.Delete(item.Hash)

				full++

				continue
			}

			// A copy of its own, so the request queue does not keep the
			// chunk's whole array alive after the worker is done with it.
			iv := *invs[i]
			c.state.requestQueue.Append(&iv)

			if _, done := seen[c]; !done {
				seen[c] = struct{}{}
				touched = append(touched, c)
			}
		}
	}

	if full > 0 {
		prometheusLegacyNetsyncInvDropped.WithLabelValues(invDropRequestQueue).Add(float64(full))
		sm.logger.Debugf("[lookupInvBatch] dropping %d missing transactions: their peers' request queues hold %d already", full, maxRequestedTxns)
	}

	if failed > 0 {
		sm.logger.Warnf("[lookupInvBatch] Unexpected failure when checking %d of %d announced transactions for existing inventory, first: %v", failed, len(items), firstErr)
	}

	return touched
}

// sendInvRequests drains the request queue of each distinct peer in touched
// into a getdata and queues it to the peer, as handleInvMsg does after its
// block pass.
func (sm *SyncManager) sendInvRequests(touched []*invChunk) {
	sent := make(map[*peerpkg.Peer]struct{}, len(touched))

	for _, c := range touched {
		if _, done := sent[c.peer]; done {
			continue
		}

		sent[c.peer] = struct{}{}

		gdmsg := sm.drainRequestQueue(c.peer, c.state)
		if len(gdmsg.InvList) > 0 {
			sm.logger.Debugf("[lookupInvBatch] Requesting %d transactions from %s", len(gdmsg.InvList), c.peer)
			c.peer.QueueMessage(gdmsg, nil)
		}
	}
}

// invLookupWorker looks up queued announcements until quit closes.
// blockHandler starts invLookupWorkers of them.
func (sm *SyncManager) invLookupWorker() {
	for {
		select {
		case <-sm.quit:
			return
		case c := <-sm.invQueue:
			sm.sendInvRequests(sm.lookupInvBatch(c))
		}
	}
}
