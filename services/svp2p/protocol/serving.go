package protocol

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
)

// MaxGetBlocksResults is the `int nLimit = 500` ProcessGetBlocks spells inline
// (net_processing.cpp:2580): how many block hashes one getblocks reply may
// carry. wire.MaxBlocksPerMsg is the same 500, and is what the legacy service
// passed to LocateBlockHeaders (services/legacy/peer_server.go:1524).
const MaxGetBlocksResults = wire.MaxBlocksPerMsg

// Serving answers the two chain-query messages from the header index: the
// net_processing.cpp ProcessGetHeadersMessage (:2968) and ProcessGetBlocks
// (:2551) handlers. Like the other machines in this package it performs no
// I/O: each method reads the index and returns the messages the caller must
// send (spec §4.3).
//
// Both machines serve the ACTIVE chain — the tip the blockchain service has
// validated, handed in by the caller — and never the header index tip, which
// headers-first sync runs ahead to. Serving headers for blocks we cannot
// serve the bodies of would invite getdata we can only answer with notfound.
//
// Locking: Serving carries no lock of its own. Like HeaderSync and
// BlockDownloader, every method assumes the caller already holds
// PeerManager's shared sync-state mutex — this package's port of cs_main.
//
// Four guards ProcessGetHeadersMessage and ProcessGetBlocks carry are NOT
// ported here, each for a stated reason:
//
//   - The pendingResponses limit (net_processing.cpp:2974-2988), which
//     disconnects a non-whitelisted peer with too many getheaders replies
//     still queued. It measures the depth of the send queue per request type,
//     which this port's transport does not expose; it is a flood guard worth
//     having, and is carried as a Phase 3 hardening residual rather than
//     silently dropped.
//   - The AreOlderOrEqualUnvalidatedBlockIndexCandidates deferral
//     (net_processing.cpp:2564-2567), which parks a getblocks reply while a
//     block received before the request is still an unvalidated candidate. It
//     exists for the compact-block announcement race, and this port announces
//     no compact blocks (Phase 4).
//   - The pruning branch (net_processing.cpp:2593-2604). Teranode does not
//     prune blocks the way fPruneMode does, and it has no counterpart of
//     nPrunedBlocksLikelyToHave.
//   - state->pindexBestHeaderSent (net_processing.cpp:3044) is now WRITTEN
//     here — see the assignment at the end of OnGetHeaders below, and
//     peerSyncState's doc comment for the field. Its reader is the block
//     announcement relay (relay.go headerAlreadySent, Task 12), which is why
//     the field lives on peerSyncState and not on this machine: Serving
//     carries no lock and no per-peer state of its own beyond what the
//     caller hands it each call, and the relay runs on a different call path
//     entirely (a Kafka message, not a peer message).
type Serving struct {
	idx *HeaderIndex
	hs  *HeaderSync
}

// NewServing builds the serving machine over the shared header index and the
// headers-first machine, whose mode gates getheaders (see OnGetHeaders).
func NewServing(idx *HeaderIndex, hs *HeaderSync) (*Serving, error) {
	if idx == nil {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: header index is nil")
	}

	if hs == nil {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: header sync is nil")
	}

	return &Serving{idx: idx, hs: hs}, nil
}

// OnGetHeaders is the NetMsgType::GETHEADERS handler, net_processing.cpp
// ProcessGetHeadersMessage (:2968): locate the fork point from the locator and
// serve up to MaxHeadersResults headers after it, stopping at hashStop.
//
// refused reports the catch-up refusal, and is the ONE case that answers
// nothing at all rather than a possibly empty headers message. The rule is
// carried verbatim from the legacy service (services/legacy/peer_server.go:1574-1589):
// "Don't serve headers to other peers while we're actively syncing in
// headers-first mode", because serving 2000 headers per peer out of the store
// competes with our own header processing and costs the sync its
// three-minute window. SVNode refuses on IsInitialBlockDownload() instead
// (net_processing.cpp:2995-3000) — the same intent measured against the state
// this port does not carry — and exempts whitelisted peers, which this port
// has no notion of. The caller logs the refusal: the machines hold no logger.
//
// An empty locator is not a peer error. SVNode reads that case as "the peer
// named its start with hashStop" (GetFirstBlockIndexFromLocatorNL,
// net_processing.cpp:2845-2871) and serves from that header, which may sit off
// our chain — in which case it is the only header served, since CChain::Next
// of an off-chain index is nullptr. An empty locator whose hashStop we do not
// hold is the second case that answers nothing, matching the C++ early return
// on a nullopt locate.
func (s *Serving) OnGetHeaders(peer *SyncPeer, activeTip HeaderNode, msg *wire.MsgGetHeaders) (msgs []wire.Message, refused bool) {
	if peer == nil || msg == nil {
		return nil, false
	}

	if s.hs.IsHeadersFirstMode() {
		return nil, true
	}

	start, ok := s.locateStart(activeTip, msg.BlockLocatorHashes, msg.HashStop)
	if !ok {
		// Either the empty-locator hashStop is unknown (the C++ nullopt early
		// return), or the active tip is not in the index and there is no
		// chain to serve from.
		return nil, false
	}

	out := wire.NewMsgHeaders()

	// A locator that already names our tip leaves nothing to serve, and
	// SVNode answers it with an EMPTY headers message rather than with
	// silence: ProcessGetHeadersMessage builds vHeaders (possibly empty) and
	// PushMessage is unconditional. The legacy Go service diverges here,
	// sending nothing when it found no headers (peer_server.go:1636). SVNode's
	// answer is kept, because it is what tells a peer its getheaders was
	// received and its chain is not behind ours; a peer that gets silence
	// cannot tell that from a dropped message. Our own OnHeaders reads an
	// empty batch as "nothing interesting, stop asking" and neither scores nor
	// disconnects for it.
	if start.empty {
		// C++: pindex is nullptr here (chainActive.Next of the peer's own tip
		// is nullptr), and ProcessGetHeadersMessage's reset is unconditional on
		// pindex being null: "pindex ? pindex : chainActive.Tip()". An empty
		// reply still counts as telling this peer about our tip.
		if peer.State != nil {
			peer.State.pindexBestHeaderSent = &activeTip
		}

		return []wire.Message{out}, false
	}

	headers := s.idx.ActiveChainHeaders(activeTip.Hash, start.hash, MaxHeadersResults)
	stopping := msg.HashStop != chainhash.Hash{}

	for i := range headers {
		header := headers[i]

		// AddBlockHeader only refuses past MaxBlockHeadersPerMsg, which is
		// the cap the range was taken with, so this cannot fail. The error is
		// still not dropped: a future cap change must not silently serve a
		// message the wire encoder will refuse.
		if err := out.AddBlockHeader(&header); err != nil {
			break
		}

		// C++: `if(--nLimit <= 0 || pindex->GetBlockHash() == hashStop) break;`
		// — the cap is the range limit above, and hashStop is INCLUSIVE here,
		// unlike the getblocks loop which breaks before it pushes.
		//
		// The null-hashStop guard is not in the C++, which compares
		// unconditionally. It changes no answer — a null hashStop matches no
		// block hash — and it keeps the common case, a sync peer asking for
		// everything, from hashing all 2000 headers it is served.
		if stopping && header.BlockHash() == msg.HashStop {
			break
		}
	}

	// sent is the C++ "pindex" pindexBestHeaderSent is reset to: the last
	// header actually queued above, or our own tip if the loop pushed
	// nothing (the AddBlockHeader break on its very first iteration; see the
	// comment there for why that cannot happen in practice). Looked up ONCE
	// here rather than once per iteration inside the loop above (fix round
	// 1, review Minor 5): only the final value is ever read, and the loop
	// runs under syncMu, so up to MaxHeadersResults (2000) redundant
	// HeaderIndex.Lookup calls — each its own mutex acquisition — were paid
	// on this serving hot path for nothing.
	sent := activeTip

	if len(out.Headers) > 0 {
		if node, ok := s.idx.Lookup(out.Headers[len(out.Headers)-1].BlockHash()); ok {
			sent = node
		}
	}

	if peer.State != nil {
		peer.State.pindexBestHeaderSent = &sent
	}

	return []wire.Message{out}, false
}

// OnGetBlocks is the NetMsgType::GETBLOCKS handler, net_processing.cpp
// ProcessGetBlocks (:2551) and legacy OnGetBlocks (peer_server.go:1510): an
// inv of up to MaxGetBlocksResults block hashes following the locator's fork
// point, stopping BEFORE hashStop.
//
// Unlike getheaders there is no empty-locator special case: ProcessGetBlocks
// calls FindForkInGlobalIndex directly, so an empty locator takes the same
// genesis fallback an unknown one does. There is also no empty reply — the
// C++ loop pushes inventory or does nothing, and pushes no message of its own.
//
// SVNode queues the hashes into the peer's inventory-to-send set
// (PushBlockInventory) for its SendMessages pass to batch, which is the
// mechanism the relay path (Task 12) needs and this task does not: one
// getblocks answers with one inv, the shape the legacy service also has.
//
// The legacy service answers an empty inv with a notfound instead of silence
// (peer_server.go:1556-1568), for NODE_NETWORK_LIMITED, which SVNode does not
// do and this port does not advertise.
func (s *Serving) OnGetBlocks(peer *SyncPeer, activeTip HeaderNode, msg *wire.MsgGetBlocks) []wire.Message {
	if peer == nil || msg == nil {
		return nil
	}

	fork, ok := s.idx.ForkPoint(activeTip.Hash, derefHashes(msg.BlockLocatorHashes))
	if !ok {
		return nil
	}

	// C++: `if(pindex) pindex = chainActive.Next(pindex);` — serve the rest of
	// the chain, starting after what the peer already has. Nothing after the
	// fork point means the peer is level with us, and the loop below never
	// runs.
	next, ok := s.idx.ActiveChainNext(activeTip.Hash, fork.Hash)
	if !ok {
		return nil
	}

	hashes := s.idx.ActiveChainHashes(activeTip.Hash, next.Hash, MaxGetBlocksResults)

	out := wire.NewMsgInv()

	for _, hash := range hashes {
		// C++ breaks on hashStop BEFORE it pushes, so hashStop is excluded
		// from the inv — the opposite of the getheaders loop.
		if hash == msg.HashStop {
			break
		}

		if err := out.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &hash)); err != nil {
			break
		}
	}

	if len(out.InvList) == 0 {
		return nil
	}

	// C++: `pfrom->hashContinue = pindex->GetBlockHash();` on the nLimit
	// break, and the legacy service's own continueHash
	// (peer_server.go:1546-1553): a full inv means there is more chain than
	// fits, so when the peer requests this last hash we answer with an inv of
	// our tip, which triggers its next getblocks. Task 10's getdata path
	// consumes it.
	if len(out.InvList) == MaxGetBlocksResults {
		peer.hashContinue = out.InvList[len(out.InvList)-1].Hash
	}

	return []wire.Message{out}
}

// serveStart is where a serving walk begins: the header to serve first, or
// empty when the peer is level with our tip and there is nothing after the
// fork point.
type serveStart struct {
	hash  chainhash.Hash
	empty bool
}

// locateStart is net_processing.cpp GetFirstBlockIndexFromLocatorNL
// (:2845-2871), plus the CChain::Next step the C++ helper folds into its
// locator branch. ok is false for the two cases the C++ answers with a bare
// return: an empty locator naming a hashStop we do not hold, and (this port
// only) an active tip the index cannot place.
func (s *Serving) locateStart(activeTip HeaderNode, locator []*chainhash.Hash, hashStop chainhash.Hash) (serveStart, bool) {
	if len(locator) == 0 {
		// C++: `pindex = mapBlockIndex.Get(hashStop); if (!pindex) return {};`
		// The header may sit off our active chain, and the range walk answers
		// that with the one header, as CChain::Next does.
		if _, ok := s.idx.Lookup(hashStop); !ok {
			return serveStart{}, false
		}

		return serveStart{hash: hashStop}, true
	}

	fork, ok := s.idx.ForkPoint(activeTip.Hash, derefHashes(locator))
	if !ok {
		return serveStart{}, false
	}

	next, ok := s.idx.ActiveChainNext(activeTip.Hash, fork.Hash)
	if !ok {
		// C++ leaves pindex nullptr, which serves an empty headers message.
		return serveStart{empty: true}, true
	}

	return serveStart{hash: next.Hash}, true
}

// derefHashes flattens a wire locator into the value slice the index takes.
// A nil entry cannot come off the wire — the decoder always fills every slot —
// and is skipped rather than dereferenced so an in-process caller cannot panic
// the index.
func derefHashes(hashes []*chainhash.Hash) []chainhash.Hash {
	out := make([]chainhash.Hash, 0, len(hashes))

	for _, h := range hashes {
		if h == nil {
			continue
		}

		out = append(out, *h)
	}

	return out
}

// getDataKind is how ProcessGetData's branch structure appears here. The
// numbers are protocol.h:534-541 (MSG_TX 1, MSG_BLOCK 2, MSG_FILTERED_BLOCK 3,
// MSG_CMPCT_BLOCK 4, MSG_DATAREF_TX 5); go-wire's decoder only names 0-3
// (go-wire invvect.go:28-31, v1.2.10), so 4 and 5 reach us as unrecognised
// types.
type getDataKind int

const (
	// getDataTx is the MSG_TX branch (net_processing.cpp:1382-1419).
	getDataTx getDataKind = iota

	// getDataBlock is the MSG_BLOCK branch (net_processing.cpp:1189), the one
	// block type this port serves.
	getDataBlock

	// getDataFilteredBlock is MSG_FILTERED_BLOCK. SVNode answers it by
	// building a CMerkleBlock under pfrom->cs_filter
	// (net_processing.cpp:1281-1300); this port has no bloom filter support at
	// all, so there is nothing to build from.
	getDataFilteredBlock

	// getDataUnsupported is an inv type this port does not implement and which
	// is NOT a block type — MSG_CMPCT_BLOCK and MSG_DATAREF_TX included, since
	// go-wire decodes neither into a named constant.
	getDataUnsupported
)

// blockType is protocol.h:577-578 IsBlockType: MSG_BLOCK, MSG_FILTERED_BLOCK
// or MSG_CMPCT_BLOCK. It is what ends a serving pass — see the note on
// Serving.OnGetData.
//
// MSG_CMPCT_BLOCK cannot be recognised here, because go-wire has no InvType
// constant for it. Such an entry classifies as getDataUnsupported, so it is
// warn-logged, left unanswered, and does not end the pass.
//
// A peer CAN now ask for one. This port sends a version-1 sendcmpct after the
// handshake (net_processing.cpp ProcessVerAckMessage :1942-1953), which sets
// fSupportsDesiredCmpctVersion on SVNode's side. SVNode then rewrites a
// single-block headers direct fetch into MSG_CMPCT_BLOCK (:3553-3560). This
// port serves no compact block in either case — compact blocks are
// receive-only — so the only divergence is the missed break, and it is booked
// as a residual.
func (k getDataKind) blockType() bool {
	return k == getDataBlock || k == getDataFilteredBlock
}

// getDataItem is one classified inventory entry. Entries are classified once,
// when the request arrives, and held in the peer's pending queue in the order
// the peer asked for them.
type getDataItem struct {
	inv  *wire.InvVect
	kind getDataKind
}

// OnGetData is the NetMsgType::GETDATA handler, net_processing.cpp
// ProcessGetData (:1163) and legacy OnGetData (services/legacy/peer_server.go:1426).
// It classifies the inventory list and nothing else: the fetches and the sends
// are I/O, so they belong to the caller (spec §4.3), and a block send is the
// most blocking thing this service does — it must never run under the
// sync-state mutex this package's machines are called under (see the note on
// Serving).
//
// Classification is a pure function of inv.Type, so doing it here, once, on
// arrival is equivalent to the C++ doing it per entry at serve time.
//
// # What the caller must do with the result, and why it is not one flat loop
//
// ProcessGetData is PACED. Two mechanisms, both load-bearing, and the caller
// (peer.go servePass) implements both:
//
//   - At most ONE block-type entry per pass. After any entry whose type
//     IsBlockType (protocol.h:577), served or not, the C++ breaks out of its
//     while loop unconditionally (net_processing.cpp:1448-1452). Transactions
//     keep looping; the first block ends the pass. Use getDataKind.blockType.
//   - The unserved remainder is RETAINED. pfrom->vRecvGetData is a persistent
//     deque, and :1456 erases only the prefix actually consumed. Without this
//     a peer could getdata MaxInvPerMsg block hashes in one message and hold
//     us for hours, at two asset reads per block.
//
// The one pacing guard NOT ported is the GetPausedForSending break
// (net_processing.cpp:1176-1179), which stops a pass when the send buffer is
// already too full to answer. It needs the send-queue depth, which this port's
// transport does not expose — the same accessor the getheaders flood limit
// wants, and the same Phase 3 hardening residual.
//
// # notfound answers an unserved transaction AND an unserved block
//
// This is a DELIBERATE DIVERGENCE from SVNode, priced below.
//
// SVNode notfounds transactions only. vNotFound.push_back appears exactly
// twice in ProcessGetData: :1418 for MSG_TX when nothing was pushed, and :1442
// for MSG_DATAREF_TX when the dataref index had no entry. The block branch
// pushes it on NONE of its not-sent paths — unknown block, an old off-chain
// block (:1233-1237), the historical-serving limit (:1245-1257), or a block the
// store cannot stream. The C++ comment at :1458-1466 says why: notfound exists
// for SPV clients recursively walking the dependencies of unconfirmed
// transactions, so it was never meant as a block-serving mechanism.
//
// This port answers the block too, taking legacy's shape instead
// (services/legacy/peer_server.go:1491-1493, which notfounds every entry it
// could not push). Three reasons, in ascending order of weight:
//
//   - Precedent. Task 8 already ruled this exact shape the same way for
//     getheaders (Phase 3 execution ledger, Decision 5): silence is
//     indistinguishable from a dropped message.
//   - Legacy's behaviour is field-proven on this network. SVNode's
//     silence-for-blocks is proven for SVNode, which is not the same claim.
//   - What actually prices the trade: this port has an unconditional per-block
//     download timeout (blockdownload.go BlockDownloadTimeoutBase, 100 seconds,
//     600 during IBD). A peer we answer with silence therefore pays a full
//     timeout window before it re-requests the block elsewhere, while a peer we
//     answer with notfound can release its in-flight assignment at once. We
//     want our own peers to tell us; telling them is the symmetric choice.
//
// This port has no inbound notfound handler, and that is PARITY, not a gap:
// SVNode ignores an inbound notfound outright, with the explicit comment "We do
// not care about the NOTFOUND message, but logging an Unknown Command message
// would be undesirable as we transmit it ourselves"
// (net_processing.cpp:4847-4850), and the legacy service only logs one
// (OnNotFound, services/legacy/peer_server.go:1836-1843). Neither releases an
// in-flight assignment on it. Here an inbound notfound falls through
// dispatchSync's switch, which has no default, so it is ignored without a
// misbehavior score or a disconnect — the same outcome by a different route.
//
// So the reasoning above is asymmetric on purpose: we tell peers more than
// either reference tells us. CONSUMING a peer's notfound to release an
// assignment early would be a deliberate improvement over both, and is worth
// having precisely because the per-block timeouts are unconditional; it is
// booked as such rather than left implied here.
//
// The >4 GiB refusal is answered the same way and for a further reason of its
// own (OPEN QUESTION 5) — see serveBlock. What is NOT answered is anything
// below.
//
// # The three kinds we cannot answer, and why none drops the peer
//
// A filtered block, an inv type we do not implement, and a nil entry are all
// answered with a warn log and nothing else.
//
// Neither reference disconnects and neither replies. ProcessGetData has no
// else-clause: an unhandled type falls straight through to
// GetMainSignals().Inventory and the next iteration. Legacy logs "Unknown type
// in inventory request %d" and continues (peer_server.go:1471-1475). For a
// filtered block specifically, legacy's pushMerkleBlockMsg returns early and
// silently when the peer has no filter loaded.
//
// The "Got invalid inv type" -> fDisconnect rule is in the INV handler only
// (blockdownload.go OnInv), and the asymmetry is the point: an inv ASSERTS the
// peer holds data, so a bogus type there is misbehaviour, while a getdata only
// REQUESTS it, so an unimplemented type is a gap on our side. Nor may these
// answer notfound: notfound asserts "I do not have that item", and for a type
// we never parsed we cannot honestly name an item. A peer asking for a type we
// do not implement waits out its own request timeout, exactly as it would
// against SVNode.
//
// A nil entry cannot come off the wire but must not panic an in-process
// caller, the same rule as derefHashes.
//
// # Two ProcessGetData guards NOT ported, each for a stated reason
//
//   - The fingerprinting guard on off-chain blocks (:1215-1231), which serves a
//     block outside the active chain only if it is fully valid and less than a
//     month old in both time and equivalent work. It reads
//     BlockValidity::SCRIPTS and GetBlockProofEquivalentTime, neither of which
//     this port carries, and the deferral branch above it (:1194-1213) exists
//     for the same candidate machinery. Carried as a Phase 3 hardening
//     residual.
//   - The historical-block outbound limit (:1240-1256), which disconnects a
//     non-whitelisted peer once OutboundTargetReached. This port tracks no
//     outbound byte target and has no whitelist.
//
// One legacy guard is also not ported: the decaying ban score for oversized
// inventory queries (peer_server.go:1436-1442, `uint32(length)*99/wire.MaxInvPerMsg`).
// This package's misbehavior counter does not decay, so porting the number
// without the decay would ban a peer performing IBD — the exact case legacy's
// comment says the decay exists to protect. Carried as a residual, and note
// go-wire already caps an inbound getdata at MaxInvPerMsg entries.
func (s *Serving) OnGetData(msg *wire.MsgGetData) []getDataItem {
	if msg == nil {
		return nil
	}

	items := make([]getDataItem, 0, len(msg.InvList))

	for _, inv := range msg.InvList {
		if inv == nil {
			continue
		}

		item := getDataItem{inv: inv, kind: getDataUnsupported}

		switch inv.Type {
		case wire.InvTypeTx:
			item.kind = getDataTx
		case wire.InvTypeBlock:
			item.kind = getDataBlock
		case wire.InvTypeFilteredBlock:
			item.kind = getDataFilteredBlock
		}

		items = append(items, item)
	}

	return items
}

// ContinueInv is the ProcessGetData continuation (net_processing.cpp:1364-1377)
// and the legacy service's own continueHash handling
// (services/legacy/peer_server.go:2121-2144): once we have served the block
// that closed a full getblocks inv, we send an inv of our tip, which is what
// makes the peer send its next getblocks. It returns nothing unless hash is
// the armed trigger, and the trigger is one-shot — the C++ hashContinue.SetNull().
//
// The tip served here is the ACTIVE tip, never the header index tip, for the
// reason stated on Serving: an inv of a block whose body we cannot serve only
// invites a getdata we must answer with notfound. The C++ reads
// chainActive.Tip() at this same point.
//
// Called under PeerManager's sync-state mutex like every other method here,
// which is why it is separate from OnGetData: the check, the clear and the tip
// read must happen after the block is on the wire, and the block send itself
// must not hold that mutex.
func (s *Serving) ContinueInv(peer *SyncPeer, activeTip HeaderNode, hash chainhash.Hash) []wire.Message {
	if peer == nil {
		return nil
	}

	// The zero hash is the "no continuation pending" sentinel, so it must not
	// match a request for the zero hash either.
	if peer.hashContinue == (chainhash.Hash{}) || peer.hashContinue != hash {
		return nil
	}

	peer.hashContinue = chainhash.Hash{}

	out := wire.NewMsgInv()

	if err := out.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &activeTip.Hash)); err != nil {
		return nil
	}

	return []wire.Message{out}
}
