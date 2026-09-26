package netsync

import (
	"bytes"
	"context"
	"io"
	"math/big"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// Why this file exists.
//
// The wire layer can already read a block's body straight from the socket to a
// store without ever building it as a Go object, and every piece of that was
// written and left switched off: the handler is registered at startup, but it
// checks for a sink and a gate before streaming anything and neither was ever
// installed, so every block on every node has taken the decoding path. This is
// what installs them.
//
// It matters more than a saved allocation. A decoded block reserves download
// budget by its serialized size, and a block larger than that whole budget is
// clamped to it, so it can only proceed once the budget is completely empty.
// The semaphore behind that budget stops at the first waiter it cannot satisfy
// and leaves every waiter behind it blocked, which its own documentation says is
// deliberate. Each of those waiters is a peer's read loop, and a blocked read
// loop reads nothing further from its socket. Measured on Hetzner mainnet on
// 2026-09-10: one goroutine holding the queue head for two minutes against a
// 319 MB block, five peer read loops stacked behind it, and sixteen of the last
// two hundred blocks large enough to do the same thing. A streamed body never
// reserves that budget, because there is nothing in memory to bound.

// streamedBodyRequestWindow is how recently this node must have asked for a
// block before a peer may write its body to our disk.
//
// It is generous on purpose. The gate's job is to refuse bodies nobody asked
// for, not to enforce timeliness: a body that arrives late is still a body this
// node wanted, and refusing it means paying for the download again. The
// tracker's own assignment records expire well before this, so in practice the
// tracker is the tighter bound and this is the backstop.
const streamedBodyRequestWindow = 60 * 60 * 1000000000 // one hour, in nanoseconds

// installStreamingBlockPath wires the three functions the wire layer needs
// before it will stream a block body to disk instead of decoding it. Called
// once, from the sync manager's construction: the park is mandatory, so the
// park's store is always there for a streamed body to go to, and its entry
// table is what makes the body findable afterwards.
//
// Installing a sink without a gate is refused by the wire layer itself, which
// falls back to decoding rather than opening the door. This installs all three
// together for the same reason.
//
// The sink installed is always the pipeline sink: every block that reaches
// this path converts, whatever its size, so there is no second, decode-and-
// park sink left to choose between.
func (sm *SyncManager) installStreamingBlockPath(set func(
	sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error),
	gate func(chainhash.Hash, *wire.BlockHeader) error,
	del func(chainhash.Hash, bool) error,
)) {
	// admitPipelineSink wraps the download-admission budget around the
	// pipeline sink. See that method's doc comment for why it has to wrap
	// the sink itself rather than being charged in the on-disk message
	// handler that runs after the sink has already finished.
	//
	// del is pipelineBlockDelete, gated on the converted argument the wire
	// layer passes through from THIS delivery's own blockBodySink return,
	// never on whether a converted record merely exists for the hash (see
	// pipelineBlockDelete's own doc comment).
	sm.logger.Infof("[legacy] streaming block path installed")

	sink := sm.admitPipelineSink(sm.pipelineBlockSink)
	if sm.streams != nil {
		// Measured outermost, so a drained copy is timed too.
		sink = sm.trackBlockStreams(sink)
	}

	set(sink, sm.streamingBlockGate, sm.pipelineBlockDelete)
}

// admitPipelineSink wraps inner (the pipeline sink) with the download-admission
// budget AcquireBlockPrefetch/ReleaseBlockPrefetch already implement, charged
// one slot per block on this path (see AcquireBlockPrefetch's own doc
// comment, manager.go). That budget is sized for exactly this call site —
// blockPrefetchBudgetSlots is derived from MaxBlocksInTransitPerPeer — and this
// is its only caller: AcquireBlockPrefetch has no other route in, since every
// block arrives here as a streamed body, never a decoded *wire.MsgBlock.
//
// Charged here, wrapping the sink itself, NOT in handleBlockOnDiskMsg (the
// on-disk message handler that runs once the body is already fully on disk).
// By the time that handler's message even exists, the conversion this budget
// is meant to bound is already finished: inner has already streamed the whole
// body through the subtree builder and its ~50MB dedup map
// (newPipelineDedupMap, pipeline_sink.go) on this peer's own read-loop
// goroutine. A charge that only runs after that resident cost has already been
// paid bounds nothing real. Charging before inner runs instead blocks the read
// loop that would otherwise start that work.
//
// ctx passed to the acquire is sm.ctx bounded by pipelineAdmissionAcquireTimeout,
// not sm.ctx unbounded. Fix round 1 found a real self-inflicted
// disconnect in the first version of this function: it parked on sm.ctx with no
// timeout, and peer.inHandler's idle timer (peer/peer.go:2153) only calls
// idleTimer.Stop() AFTER readMessageStreaming — which is the call this sink runs
// inside of — returns. A park here longer than legacy_peerIdleTimeout (125s
// default) therefore tripped the SAME idle timer OnBlock's acquire was written
// to be safe from: shouldArmProcessingTimer (peer/peer.go:2193) disarms the
// separate processing watchdog for block messages under prefetch precisely so a
// budget park cannot kill a healthy connection, but that disarm only covers the
// timer armed AFTER a message is read, not the idle timer armed WHILE it is
// still being read — which is where this sink's park actually happens. Parking
// long enough here got a perfectly healthy peer disconnected with "No answer
// from peer", blamed for backpressure that was entirely this node's own.
//
// The better fix — thread a per-connection quit channel (and a way for
// peer.inHandler to know a read is blocked in application logic, not waiting on
// the peer) into the sink signature, the way peer_server.go:1322 hands OnBlock's
// acquire sp.quit — is not reachable from here without changing the external
// dependency go-wire itself. blockBodySink (services/legacy/peer/wire_streaming.go)
// is invoked from streamingBlockHandler, which is registered globally and
// peer-agnostically via wire.SetExternalHandler(wire.CmdBlock, ...); go-wire
// calls it with only (io.Reader, uint64, int) — no peer, no connection, no
// context of any kind — because that registration is process-wide, shared by
// every connected peer's read loop, not per-connection. There is no reader
// identity or type assertion that reliably recovers "which peer is calling
// this" from the io.Reader go-wire hands the external handler (it is go-wire's
// own internal wrapper around the socket, not the socket itself), so closing
// this gap for real means changing go-wire's SetExternalHandler/
// ReadMessageStreamingN to pass per-call context through — an upstream change,
// consistent with this codebase's own rule of fixing a dependency rather than
// working around it in the wrapper, and out of scope for a same-branch fix.
//
// So: a bounded wait instead of an unbounded park. pipelineAdmissionAcquireTimeout
// keeps the wait strictly below legacy_peerIdleTimeout; on that bound expiring,
// the copy is drained and the block is asked for again, rather than parking
// indefinitely into the idle timer's path.
func (sm *SyncManager) admitPipelineSink(inner func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error)) func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error) {
	return func(hash chainhash.Hash, header *wire.BlockHeader, r io.Reader, n int64) (bool, error) {
		acquireCtx, cancel := context.WithTimeout(sm.ctx, sm.pipelineAdmissionAcquireTimeout())
		defer cancel()

		acquireStart := time.Now()
		err := sm.AcquireBlockPrefetch(acquireCtx, hash)
		admitWait := time.Since(acquireStart)

		switch {
		case err == nil:
			sm.streams.noteAdmission(hash, admitWait, admitConverted)
		case errors.Is(err, ErrDuplicateBlockInFlight):
			sm.streams.noteAdmission(hash, admitWait, admitRawDuplicate)
		case errors.Is(err, context.DeadlineExceeded):
			sm.streams.noteAdmission(hash, admitWait, admitRawTimedOut)
		}

		if err != nil {
			if errors.Is(err, ErrDuplicateBlockInFlight) {
				// Never written as a raw block. A raw copy of a block another copy is
				// converting let the park take it whenever the converting copy then failed:
				// processing a raw copy after a conversion attempt failed subtree validation
				// at 707,178 and 708,115 on 2026-09-24 and stopped the chain each time. It is
				// kept in a side file instead, and converted only if it completes before the
				// copy converting now, after that copy has stopped and cleaned up.
				return sm.raceDuplicateCopy(hash, header, r, n, inner)
			}

			if errors.Is(err, context.DeadlineExceeded) {
				// pipelineAdmissionAcquireTimeout expired, not sm.ctx itself —
				// distinguished from the shutdown case below by which one a
				// context.WithTimeout-derived ctx reports. Falling back here
				// rather than returning an error keeps this peer connected:
				// any non-benign error from this sink disconnects the peer
				// (peer.shouldHandleReadError, see pipelineBlockSink's doc
				// comment on why it never errors for its own declines), so
				// erroring here would turn OUR admission pressure into a
				// disconnect blamed on the peer, exactly the failure mode
				// this bound exists to avoid.
				//
				// The copy is drained and the block is asked for again. It
				// used to be written whole to the park as a raw block, a
				// second route that nothing else used; at shipped settings the
				// wait cannot run out (pipelineBlockSlotPeerAllowance).
				return sm.drainDuplicate(hash, r)
			}

			// sm.ctx cancelled (daemon shutdown): nothing was reserved and
			// nothing productive is left to do with the bytes either.
			return false, err
		}
		defer sm.ReleaseBlockPrefetch(hash)

		return inner(hash, header, r, n)
	}
}

// pipelineAdmissionAcquireDivisor is how much smaller admitPipelineSink's
// acquire bound is than legacy_peerIdleTimeout: half, so a park that hits the
// bound still leaves a wide margin before the peer's own idle timer would have
// fired, rather than shaving it to the edge.
const pipelineAdmissionAcquireDivisor = 2

// pipelineAdmissionAcquireFallback is the acquire bound used when
// legacy_peerIdleTimeout is unset or non-positive, which should not happen in
// practice (its own settings doc says not to set it below 120s) but must still
// produce a bounded wait rather than an unbounded one.
const pipelineAdmissionAcquireFallback = 45 * time.Second

// pipelineAdmissionAcquireTimeout returns how long admitPipelineSink's acquire
// may block before falling back, strictly below legacy_peerIdleTimeout so a
// park here can never itself trip that timer. See admitPipelineSink's doc
// comment for why the bound exists instead of a per-connection quit channel.
func (sm *SyncManager) pipelineAdmissionAcquireTimeout() time.Duration {
	if sm.settings == nil || sm.settings.Legacy.PeerIdleTimeout <= 0 {
		return pipelineAdmissionAcquireFallback
	}

	return sm.settings.Legacy.PeerIdleTimeout / pipelineAdmissionAcquireDivisor
}

// streamingBlockGate answers whether a peer may write this block's body to our
// disk, and is the only check that runs before the bytes land. Nothing
// downstream can refuse a write that has already happened.
//
// Three questions, in the order that makes each of the later ones meaningful.
//
// First, did this node ask for this block. A peer that can choose what to write
// to our disk can fill it, and no amount of later verification gets the space
// back. The check is by hash rather than by peer because the wire handler is
// registered globally with go-wire and has no peer in scope; a body for a hash
// we asked somebody for is a body we wanted, and which peer answered is settled
// later on the same paths that already settle it for a decoded block.
//
// Second, is the header's declared target at least as hard as the chain's own
// limit. This is the check without which the third one gates nothing: a header
// carries the target it claims to meet, so a peer picks an easy one and always
// passes. This codebase's own hardening work measured 64 of 64 forged headers
// passing a target check that lacked this floor.
//
// Third, does the header actually meet that now-bounded target. This is the
// work that makes minting distinct block hashes expensive, which is what stops
// an attacker filling the park with unlimited fabrications.
//
// The body itself is not checked here and cannot be: it has not been read yet.
// A body that turns out to be rubbish is caught when the park reads it back and
// the block's transactions are parsed and its merkle root rebuilt, exactly as
// for a decoded block. What the gate buys is that the bytes on our disk are
// always bytes we asked for, under a hash somebody paid real work to produce.
func (sm *SyncManager) streamingBlockGate(hash chainhash.Hash, header *wire.BlockHeader) error {
	if header == nil {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] no header", hash)
	}

	// The body is filed under hash, so a hash the header does not produce would
	// put bytes on disk under a name that is not theirs — and every later reader
	// trusts the name.
	if got := header.BlockHash(); !got.IsEqual(&hash) {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] the header hashes to %s", hash, got)
	}

	if sm.blockDownloads == nil || !sm.blockDownloads.RequestedWithin(hash, streamedBodyRequestWindow) {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] this node did not ask for this block", hash)
	}

	// No chain means no floor to check against, and an unbounded write is the
	// wrong thing to default to.
	if sm.chainParams == nil || sm.chainParams.PowLimit == nil {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] no chain parameters, so there is no difficulty floor", hash)
	}

	// A larger target is easier work. Refusing a target above the chain's limit
	// is what bounds the header's own claim before it is tested against it.
	target := blockchain.CompactToBig(header.Bits)
	if target == nil || target.Sign() <= 0 || target.Cmp(sm.chainParams.PowLimit) > 0 {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] declared target %s is easier than the chain limit %s",
			hash, describeTarget(target), sm.chainParams.PowLimit)
	}

	var headerBytes bytes.Buffer
	if err := header.Serialize(&headerBytes); err != nil {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] could not serialize the header", hash, err)
	}

	modelHeader, err := model.NewBlockHeaderFromBytes(headerBytes.Bytes())
	if err != nil {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] could not read the header", hash, err)
	}

	if met, _, err := modelHeader.HasMetTargetDifficulty(); !met {
		return errors.NewBlockInvalidError("[streamingBlockGate][%s] the header does not meet its own target", hash, err)
	}

	return nil
}

// describeTarget renders a target for a refusal message, including the nil case
// CompactToBig can produce, so the log line never says "<nil> is easier than".
func describeTarget(t *big.Int) string {
	if t == nil {
		return "unreadable"
	}

	return t.String()
}

// blockOnDiskMsg tells the consumer that a block's body reached the park's store
// straight off the wire, so the park needs an entry for bytes that are already
// down.
//
// It carries no block and no reader. That is the point of the whole path: the
// body never existed as a Go object, so there is nothing here to charge against
// the download budget and nothing for a read loop to wait on.
type blockOnDiskMsg struct {
	body peerpkg.BlockBody
	peer *peerpkg.Peer
}

// QueueBlockOnDisk hands a streamed block to the consumer. It mirrors QueueBlock
// and deliberately takes no reply channel and no hand-off channel: the peer's
// read loop is already free by the time this is called, because the bytes went
// to disk as they arrived rather than into a buffer somebody has to release.
func (sm *SyncManager) QueueBlockOnDisk(body peerpkg.BlockBody, peer *peerpkg.Peer) {
	if atomic.LoadInt32(&sm.shutdown) != 0 {
		return
	}

	sm.msgChan <- &blockOnDiskMsg{body: body, peer: peer}
}

// handleBlockOnDiskMsg registers a streamed block with the park and asks for its
// parent's children to be looked at.
//
// Consumer goroutine only. The drain queue is that goroutine's own state, and
// the park's admission ordering is what the rest of this machinery assumes; five
// separate stalls this week came from one of those being touched from elsewhere.
//
// The drain request is what stops a streamed block waiting on the sweep. The
// sweep runs every thirty seconds, which would be added to every block large
// enough to stream whose parent is already in the chain — the common case, not a
// corner.
func (sm *SyncManager) handleBlockOnDiskMsg(msg *blockOnDiskMsg) {
	if msg == nil {
		return
	}

	// The streamed route never touched sm.blockDownloads, the map
	// handleBlockMsg releases via RemoveOwner/ForgiveOwners (manager.go) the
	// moment it dequeues a decoded block. Streaming is now unconditional
	// whenever the park is enabled, so this on-disk route is every block that
	// arrives on such a node, not a rare oversized one: without this release a
	// delivering peer's CountForPeer sticks at MaxBlocksInTransitPerPeer after
	// roughly sixteen deliveries and the scheduler (block_scheduler.go:144)
	// stops asking that peer for anything until the hour-long assignment TTL
	// expires.
	//
	// Released HERE — at message intake into this single consumer, the same
	// point handleBlockMsg releases it, NOT at the block's eventual commit.
	// By the time this message exists the peer has already fully answered: the
	// sink ran to completion on the peer's own read loop before
	// QueueBlockOnDisk was ever called, so there is no copy still being
	// converted for a duplicate to race against — releasing later, after
	// AdoptWritten or the eventual park commit (either of which an unreachable
	// parent or a full park can delay indefinitely), would only widen the
	// leak's own window instead of closing it. Unconditional on what happens
	// below: the peer earned the release by delivering the bytes, whether this
	// node ends up keeping them (AdoptWritten) or discarding them (unreachable
	// parent, full park).
	//
	// Unconditional here too: this handler only ever runs when the wire layer
	// dispatched a *peer.MsgBlockOnDisk, which only happens when the park is
	// enabled (installStreamingBlockPath installs the sink only then) — so by
	// the time this function is reached, the streaming route is the only route
	// this delivery could have taken.
	//
	// Resolve a stream sub-peer to its association primary, exactly as
	// handleBlockMsg does before its own RemoveOwner call (manager.go:3271-3276)
	// and as BlockRequested does before its HasOwner check (manager.go:6530-6538):
	// the download ledger records ownership under the primary, never under a
	// BlockPriority association's DATA1/DATA2 sub-peer. msg.peer is exactly
	// that sub-peer whenever the body arrived on its own stream — OnBlockOnDisk
	// (peer_server.go) hands QueueBlockOnDisk sp.Peer, which under a multistream
	// association is the sub-peer, not the primary. Sub-peers are never
	// registered in peerStates and so never own anything in blockDownloads;
	// passing msg.peer straight through here made RemoveOwner a silent no-op in
	// exactly that configuration. Fix round 1's own test caught only
	// ForgiveOwners actually working (it is peer-agnostic), never this.
	//
	// Guarded on msg.peer != nil: peerStateResolvingPrimary calls
	// AssociationRef on it, which dereferences a nil receiver.
	// QueueBlockOnDisk's production caller always hands a real peer, but a nil
	// one costs nothing extra to tolerate here.
	primary := msg.peer
	if msg.peer != nil {
		_, primary, _ = sm.peerStateResolvingPrimary(msg.peer)
	}

	sm.blockDownloads.RemoveOwner(primary, msg.body.Hash)

	// The delivering peer has room again, so ask it for more now. Only a commit
	// used to, and while download is the limit blocks arrive out of order and
	// park behind a missing one, so a peer whose block parked sat idle until a
	// later commit or the 30-second sweep. Deferred so it runs after the block
	// is parked or discarded, whichever path below is taken.
	defer sm.topUpHeaderBlocks(nil)

	if !msg.body.Converted && sm.takeDrainedDuplicate(msg.body.Hash) {
		// Only the sending peer is let off. The other owners are not: the copy being
		// converted is not here yet, and letting them off freed the block to be asked for
		// again while it was still arriving.
		sm.logger.Infof("[blockOnDisk][%s] a duplicate copy from %s was drained unwritten while another copy converted", msg.body.Hash, msg.peer)

		return
	}

	// The block is here, so whoever else was asked for it is let off; a copy still on the
	// wire from them is admitted when it lands.
	sm.blockDownloads.ForgiveOwners(msg.body.Hash, blockRequestRetryInterval)

	// Every delivery that wrote nothing is a drained copy, noted and consumed above. Anything
	// else unconverted cannot happen: the pipeline sink is the only sink, and the raw-body
	// route that once wrote whole blocks here is gone.
	if !msg.body.Converted {
		sm.logger.Warnf("[blockOnDisk][%s] a delivery from %s wrote no converted record and was not a drained copy; ignoring it", msg.body.Hash, msg.peer)

		return
	}

	entry := parkedBlock{
		hash:      msg.body.Hash,
		prevBlock: msg.body.Header.PrevBlock,
		size:      msg.body.Size,
		wireSize:  msg.body.Size,
		peer:      msg.peer,
	}

	// msg.body.Converted says whether THIS delivery's sink actually converted
	// the block, straight from the sink's own return value — see
	// BlockBody.Converted. It is deliberately NOT inferred by asking whether a
	// converted record happens to exist for this hash: a record surviving from
	// an unrelated earlier attempt, or from a racing duplicate delivery of the
	// same hash, would look identical to one this delivery produced, and
	// charging by that inference once misattributed a stale or foreign
	// record's size to a delivery that never wrote it.

	if msg.body.Converted {
		// The pipeline sink, when it is the one active, has already written
		// this hash's body as a converted record — a few hundred bytes under
		// FileTypeBlock — before the wire layer ever got here, so there is
		// nothing on disk shaped like the whole block msg.body.Size describes.
		// Charging that size against the park's budget would over-charge a
		// pipelined block by orders of magnitude and starve the park into
		// believing it is nearly full when it holds almost nothing, so this
		// charges the record's own length instead.
		if size, found, err := sm.blockPark.convertedRecordSize(sm.ctx, entry.hash); err != nil {
			sm.logger.Warnf("[blockOnDisk][%s] failed to check for a converted record, charging the whole block's wire size instead: %v", entry.hash, err)
		} else if found {
			entry.size = size
		} else {
			// The sink says it converted this hash, but the record is not
			// there. Something else already removed it — for example a racing
			// discard on the same hash — between the sink returning and this
			// handler running. Charging the whole block's wire size here would
			// be wrong the OTHER way for a genuinely converted block, but there
			// is no better number left to charge, so this falls back to it and
			// says so rather than silently mischarging.
			sm.logger.Warnf("[blockOnDisk][%s] the sink reports this delivery converted, but no converted record is on disk; charging the whole block's wire size instead", entry.hash)
		}
	}

	// A parked block is only ever committable if its parent is something this
	// node is going to get: already in the chain, or still ahead of us in the
	// header list because we asked for it. A block above a hole that is in
	// neither is unreachable, and the drain will keep offering it to the chain
	// for as long as it is held.
	//
	// Measured on mainnet on 2026-09-10, and caused by this path: three streamed
	// blocks whose parents were never in the header list produced 23,111 "the
	// parent is missing again" retries in two hours, against zero on each of the
	// three preceding days. Each retry is a store lookup on the goroutine that
	// commits blocks, so an unreachable block does not merely sit there, it
	// competes with the work the operator is waiting for.
	//
	// The decoded path never had to ask this question, because a block only
	// reaches its park call after handleBlockMsg has walked the header list for
	// it. Streaming skips that walk by design, which is the point of it, so the
	// question has to be asked here instead.
	if !sm.parentIsReachable(entry.prevBlock) {
		sm.logger.Infof("[blockOnDisk][%s] parent %s is neither in the chain nor in the header list, so this block is unreachable; discarding the body",
			entry.hash, entry.prevBlock)

		sm.blockPark.Delete(sm.ctx, entry)

		// Dropping the body is not the same as dropping the gap. Past the last
		// checkpoint the header cache is empty, and a peer that mined several
		// blocks at once announces only the newest, so this orphan is the only
		// sign that the blocks under it exist. The getblocks is what fetches
		// them, and it is the legacy protocol's batch-continuation signal as
		// well. parkOrphanBlock, the decoded path's twin of this branch, has
		// always sent it whether it parked the block or dropped it; leaving it
		// out here stalled every tip that moved by more than one block.
		// PushGetBlocksMsg drops a repeat of the same locator, so a run of
		// orphans behind one tip costs one request.
		if primary != nil && sm.blockchainClient != nil {
			sm.requestMissingBlocks(primary, entry.hash)
		}

		return
	}

	// Said at info, once per streamed block, because without it there is no way
	// to tell from a running node whether this path is carrying anything at all.
	// Both paths end with a body in the same store under the same name, so the
	// park's own files cannot answer it and neither can the block's size.
	sm.logger.Infof("[blockOnDisk][%s] body streamed to disk, %d bytes, %d txs, parent %s",
		entry.hash, entry.size, msg.body.TxCount, entry.prevBlock)

	if !sm.blockPark.AdoptWritten(entry) {
		// Either we already hold this block, in which case the body on disk is
		// the one the existing entry points at and there is nothing to do, or
		// the park is full, in which case the bytes are an orphan: on disk under
		// a well-formed hash with nothing pointing at them. Delete covers both,
		// because deleting a body we already hold would take the live one with
		// it — so only the second case may delete.
		if !sm.blockPark.Has(entry.hash) {
			sm.blockPark.Delete(sm.ctx, entry)
		} else if msg.body.Converted {
			sm.waste.dupConverted.Add(1)
			sm.logger.Infof("[blockOnDisk][%s] a duplicate copy from %s was converted in full for a block already parked", entry.hash, msg.peer)
		}

		return
	}

	// Hand the drain request to the consumer rather than queueing it here.
	//
	// This runs on blockHandler, and drainQueue is owned by the dispatchBlocks
	// consumer alone with no lock, on the stated invariant that every producer of
	// a drain request already runs on that goroutine. Calling scheduleDrain from
	// here broke that twice over: it raced the queue, and it could not wake a
	// consumer already asleep in its select, so a streamed block whose parent was
	// already committed parked and stayed there.
	//
	// Measured on mainnet on 2026-09-10: block 783,942 committed at 15:21:00, a
	// 177 MB block whose parent was that block streamed to disk at 15:21:17, and
	// nothing committed it. The operator's own probe read idle with 115 blocks
	// parked holding 4.9 GB.
	//
	// parkCommits is the existing route for precisely this, used by the sweep. Its
	// arm on the consumer restores the entry, which is a no-op for one already
	// registered, and then schedules the drain on the goroutine that owns the
	// queue. Sending also wakes a sleeping consumer, which queueing never could.
	// Only ask for a drain when the parent is actually IN THE CHAIN. A parent
	// that is merely parked means this block is not committable yet, and its own
	// commit will schedule the drain when it lands.
	//
	// parentIsReachable above is a different question and deliberately looser: a
	// parked parent makes the body worth keeping. Using that same answer to
	// trigger a drain sent the consumer after blocks whose parent was not
	// committed, and each attempt fails with the parent missing and costs a store
	// lookup on the goroutine that commits blocks. Measured on mainnet on
	// 2026-09-10 in one 45-second window with nothing committing: five blocks
	// streamed to disk and six of those failures.
	//
	// The sweep already applies this rule before it posts, which is why it does
	// not produce them.
	if !sm.parentIsInChain(entry.prevBlock) {
		// The parent still has to be asked for, and the getblocks is not an
		// alternative to keeping the block: it is the only thing that fetches the
		// gap, and the batch-continuation signal the legacy protocol runs on. A
		// peer pushes its tip after a batch and then sends nothing until the next
		// getblocks. Sent in both modes, as the decoded route always sent it:
		// inside headers-first mode the reply is dropped by processInvMsg and costs
		// one message, and outside it, on every node past the final checkpoint, it
		// is the whole of the recovery, because fetchMoreHeaderBlocks does nothing
		// there.
		if primary != nil {
			sm.requestMissingBlocks(primary, entry.hash)
		}

		return
	}

	sm.submitParkCommit(parkCommit{entry: entry})
}

// parentIsInChain reports whether this node has committed the parent, which is
// the condition for the block being committable now rather than merely worth
// keeping. Consumer goroutine only, like its caller.
func (sm *SyncManager) parentIsInChain(parent chainhash.Hash) bool {
	if sm.blockchainClient == nil {
		return false
	}

	_, _, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &parent)

	return err == nil
}

// parentIsReachable reports whether a parked block's parent is something this
// node will end up holding: in the chain now, or still ahead of us in the
// header cache because we asked for it.
//
// Consumer goroutine only. The cache holds its own lock for the check, released
// before the store call below it: a blocking client call behind that lock
// would serialise the whole sync path.
func (sm *SyncManager) parentIsReachable(parent chainhash.Hash) bool {
	// The park first, and this was the omission that made this function a hole
	// factory. A parked parent is a block we already hold on disk, waiting for
	// ITS own parent, so a child of it is exactly as reachable as a child of a
	// committed block. Leaving the park out meant discarding bodies whose parents
	// were sitting a few inches away.
	//
	// Measured on mainnet on 2026-09-10, hours after this function shipped:
	// sixteen of seventeen holes had their parent in the park at that moment, and
	// there was not one discard in four days of log before this deployed. It also
	// formed a loop with the frontier race, which re-requests the missing block,
	// receives it, and has it discarded again: one hash went round four times on a
	// thirty-second period, and 14 GB of the 28.7 GB streamed in the log is that
	// loop.
	if sm.blockPark.Has(parent) {
		return true
	}

	// A parent the dispatcher is committing right now. The drain takes a block
	// out of the park when it dispatches it, and the chain only has it once its
	// run settles, so for the whole of that run the parent is in neither place.
	// On a node with no checkpoints ahead, regtest being the everyday case, sync
	// runs on getblocks, the header cache stays empty, and the next block
	// arrives in exactly that window: discarding it left the node at height 1
	// for good, because nothing re-derives a block the cache never held.
	//
	// This closes the window for the dispatcher path because, short of the
	// shutdown drain, a frontier entry is popped only in complete, on this same
	// consumer goroutine, after its run has settled: a successful parent is in
	// the chain by then. A parent that fails leaves its already-kept child in the
	// park, which is the same position as a child of any parked block whose
	// parent never arrives, and the park's own reclaim handles it. Without a
	// dispatcher the drain commits on this goroutine, so no such window exists.
	// inFlight is nil-safe.
	if sm.dispatcher.inFlight(parent) {
		return true
	}

	if _, inCache := sm.headerCache.HeightOf(parent); inCache {
		return true
	}

	if sm.blockchainClient == nil {
		return true
	}

	// Not in the list, so the only way it is reachable is that we hold it
	// already. A store error reads as reachable: refusing a block because our
	// own storage was briefly unwell would throw away a completed download over
	// a condition that is over in seconds, which is the same judgement the
	// drain's retry-later disposition makes.
	_, _, err := sm.blockchainClient.GetBlockHeader(sm.ctx, &parent)
	if err == nil {
		return true
	}

	return !errors.Is(err, errors.ErrBlockNotFound) && !errors.Is(err, errors.ErrNotFound)
}

// noteDrainedDuplicate records one copy of hash drained off the wire unwritten.
func (sm *SyncManager) noteDrainedDuplicate(hash chainhash.Hash) {
	sm.drainedDuplicatesMu.Lock()
	defer sm.drainedDuplicatesMu.Unlock()

	if sm.drainedDuplicates == nil {
		sm.drainedDuplicates = make(map[chainhash.Hash]int)
	}

	sm.drainedDuplicates[hash]++
	sm.waste.dupDrained.Add(1)
}

// takeDrainedDuplicate consumes one drained copy of hash, reporting whether there was one.
func (sm *SyncManager) takeDrainedDuplicate(hash chainhash.Hash) bool {
	sm.drainedDuplicatesMu.Lock()
	defer sm.drainedDuplicatesMu.Unlock()

	n := sm.drainedDuplicates[hash]
	if n == 0 {
		return false
	}

	if n == 1 {
		delete(sm.drainedDuplicates, hash)
	} else {
		sm.drainedDuplicates[hash] = n - 1
	}

	return true
}
