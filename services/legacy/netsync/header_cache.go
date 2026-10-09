package netsync

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
)

// headerCache holds the headers this node has been sent above its committed
// tip, as a small tree in SV Node's shape: the committed chain is the trunk,
// and each peer's best known run of headers is a branch of that tree, held by
// hash.
//
// SV Node keeps every header it accepts in one index (mapBlockIndex,
// AcceptBlockHeader and AddToBlockIndex in validation.cpp:5979-6045), branches
// coexist, and each peer's best known header (pindexBestKnownBlock) only ever
// moves to a header with at least as much chain work (UpdateBlockAvailability).
// A peer that feeds it a fake branch gets a branch of its own; it cannot take
// another peer's away. This cache is the same structure with two bounds SV Node
// does not need, because SV Node never discards a header: at most one branch per
// peer, and each branch at most branchCap headers above the committed tip.
//
// There is no bound over all branches, because any such bound has to evict
// someone, and below a checkpoint the honest branch cannot be told from a fake
// until it reaches the pinned hash: in mainnet's difficulty-1 era a fake header
// carries the same work as an honest one, so fakes that stop one short of the
// checkpoint outrank the honest branch and an eviction by rank removes the
// honest one. The number of branches is bounded instead by who may hold one:
// while headers-first mode is on, only a peer this node asked for headers and
// may still ask (SyncManager.handleHeadersMsg, requestHeaders and
// mayAskForHeaders): an outbound peer, addnode and connect peers included, a
// whitelisted inbound peer, or the one inbound fallback sync peer while no
// preferred peer is ahead.
//
// What that costs, measured on 2026-10-07 on darwin/arm64: a held header is
// 240 to 270 bytes of heap, its headerNode (176 bytes) and its entry in the
// index map, by TestHeaderBranches_MemoryStaysBoundedWithTwentyCappedPeers and
// by the same measurement at mainnet's cap of 52,000 headers a branch. One
// full mainnet branch is 12.8 MB. Nine distinct full branches, the default 8
// outbound peers and the inbound fallback, are 468,000 headers and 126 MB;
// thirteen, with 4 whitelisted or addnode peers more, are 676,000 headers and
// 170 MB. Each further outbound or whitelisted peer adds about 13 MB. Honest
// branches share their headers, so these are the bounds for distinct, fake
// branches.
//
// It is still a cache and not a work queue. Everything in it can be asked for
// again in one message, so a branch can be dropped at any instant: when its peer
// disconnects, when it falls behind the committed tip, or when it stops
// connecting to it.
//
// The branch every reader sees, the active branch, is the best branch by the
// same order SV Node's block-index comparator uses (most chain work, earliest
// received on a tie), with one rule placed in front of it: a branch with a
// higher matched checkpoint wins. Below a checkpoint a header that is not on the
// checkpointed chain can never be committed, so a branch that has matched the
// pinned hash beats any branch that has not, whatever work the other claims.
type headerCache struct {
	mu sync.Mutex

	// fillMu makes one change to the index at a time: Fill, Discard, the
	// physical sweep and the release of a dropped branch's headers. A fill
	// judges its headers against the index outside mu, because the trunk half
	// of that is a blockchain call, so it must know no header is added to or
	// removed from the index under it. Readers take mu alone, and so does
	// DropPeer, which only unlinks a branch and leaves its headers in released
	// for the next holder of fillMu to free: the peer-departure path runs on
	// the block handler's goroutine, which must not wait on a fill's store call.
	fillMu sync.Mutex

	// released holds the tips of branches DropPeer unlinked while it could not
	// take fillMu. Their headers stay in the index, still counted, until
	// reapLocked frees them. Guarded by mu; emptied only with fillMu held too.
	released []*headerNode

	// ownerLive reports whether an owner is still a connected peer this node
	// may ask for headers at the committed height it is given. A fill checks it
	// before installing a branch, so a fill that was running when its peer left
	// or lost the right to be asked, or that started after, never installs a
	// branch no DropPeer will come for. nil means every owner is live.
	ownerLive func(owner any, committed int32) bool

	// storeTimeout bounds every blockchain call one fill makes while it holds
	// fillMu (see headerStoreTimeout).
	storeTimeout time.Duration

	// index is every held header by hash. A header is held while some branch's
	// tip descends from it.
	index map[chainhash.Hash]*headerNode

	// branches is each peer's best known header, keyed by the peer.
	branches map[any]*headerBranch

	// floorHeight and floorHash are the committed tip as last reported, by
	// PruneTo or by Fill. Nothing at or below floorHeight is named. haveFloor is
	// false until the first report; floorHashKnown is false when the report
	// came without a hash (Prune), which skips the check that a branch still
	// connects to it.
	floorHeight    int32
	floorHash      chainhash.Hash
	haveFloor      bool
	floorHashKnown bool

	// sweptTo is the height at or below which nodes have been physically
	// removed. See sweepLocked.
	sweptTo int32

	// active is the branch the readers see, and activeNodes its headers in
	// height order from activeLow, so At is an index and not a walk.
	active      *headerBranch
	activeNodes []*headerNode
	activeLow   int32

	// seq numbers headers in the order they were first held, SV Node's
	// nSequenceId: on equal work the branch whose tip arrived first wins.
	seq uint64

	// checkpoints is the chain's pinned (height, hash) list. A header at a
	// checkpoint height must carry the pinned hash, and a match commits every
	// header below it on the same branch (see headerNode.proven). nil means no
	// proof is ever granted, which is the safe direction.
	checkpoints []chaincfg.Checkpoint

	// branchCap is how far above the committed tip a branch may reach, set from
	// the checkpoints (see capForCheckpoints).
	branchCap int32

	// powLimit is the easiest target a header may declare on this chain
	// (model.PowLimitCeiling). nil means no proof-of-work check, which is what
	// every test that only cares which heights the cache names gets.
	powLimit *big.Int

	// rules is ContextualCheckBlockHeader (see headerRules), run on every header
	// before it is held. nil means no contextual check and no trunk to read
	// chain work from, so chain work is counted from each branch's root.
	rules *headerRules

	// minChainWork is SV Node's nMinimumChainWork for this chain (see
	// minimumChainWork). nil means no gate.
	minChainWork *big.Int
}

// headerNode is one held header. Everything but refs and proven is fixed when
// the node is made; parent is cut to nil only by the sweep, which holds fillMu.
//
// The header is kept as its six wire fields, not as a wire.BlockHeader, whose
// time.Time timestamp costs 24 bytes against the wire's 4 and pads the struct.
// All six are kept, merkle root and nonce included, although no rule reads
// those two: the branchSource hands the difficulty calculator and the median
// time past walk model headers, and both check each header's Hash() against
// the hash they asked for (services/blockchain/Difficulty.go and
// medianTimePast), which needs every byte of the 80. Fields are ordered so
// nothing pads: 176 bytes, from 208.
type headerNode struct {
	hash       chainhash.Hash
	prevBlock  chainhash.Hash
	merkleRoot chainhash.Hash

	// chainWork is cumulative work to and including this header, 32 bytes
	// big-endian, the store's chain_work form.
	chainWork [32]byte

	// parent is the node this header builds on, or nil when that parent is
	// committed (prevBlock names it in the trunk).
	parent *headerNode

	seq uint64

	version   int32
	timestamp uint32
	bits      uint32
	nonce     uint32
	height    int32

	// cpHeight is the highest checkpoint height on the path from the trunk to
	// this node, 0 when none.
	cpHeight int32

	// refs counts the held children and branch tips that point at this node.
	// At zero the node is released.
	refs int32

	// proven is true for a checkpoint node and every node below it: linkage
	// commits backwards, so the pinned hash fixes all of them.
	proven bool
}

// newHeaderNode makes the node for header, whose hash is hash.
func newHeaderNode(hash chainhash.Hash, header *wire.BlockHeader, parent *headerNode, height, cpHeight int32) *headerNode {
	return &headerNode{
		hash:       hash,
		prevBlock:  header.PrevBlock,
		merkleRoot: header.MerkleRoot,
		parent:     parent,
		version:    header.Version,
		timestamp:  uint32(header.Timestamp.Unix()), //nolint:gosec // a header timestamp is 32 bits on the wire
		bits:       header.Bits,
		nonce:      header.Nonce,
		height:     height,
		cpHeight:   cpHeight,
	}
}

// modelHeader is the node's header in the model's form, the same 80 bytes the
// header arrived as, so its Hash() is the node's hash.
func (n *headerNode) modelHeader() *model.BlockHeader {
	prev, merkle := n.prevBlock, n.merkleRoot

	var bits model.NBit

	binary.LittleEndian.PutUint32(bits[:], n.bits)

	return &model.BlockHeader{
		Version:        uint32(n.version), //nolint:gosec // the same 32 bits either way
		HashPrevBlock:  &prev,
		HashMerkleRoot: &merkle,
		Timestamp:      n.timestamp,
		Bits:           bits,
		Nonce:          n.nonce,
	}
}

// headerBranch is one peer's best known header.
type headerBranch struct {
	owner any
	tip   *headerNode

	// diverged marks a branch that forked from the committed chain at or below
	// the committed tip, so it can never connect to it again while the tip only
	// moves up.
	diverged bool
}

// defaultHeaderOwner is the owner Fill, FillReporting and FillDetailed fill
// for: one anonymous peer, which is what a caller that has no peer to name
// gets, and which behaves as the single-run cache this replaced did for a
// single sender.
type defaultHeaderOwner struct{}

func newHeaderCache() *headerCache {
	return &headerCache{
		index:        make(map[chainhash.Hash]*headerNode),
		branches:     make(map[any]*headerBranch),
		branchCap:    capForCheckpoints(nil),
		storeTimeout: headerStoreTimeout,
	}
}

// headerStoreTimeout is the most one fill may spend in blockchain calls while
// it holds fillMu. A fill makes a handful of them (the committed tip's header
// and chain work, the ancestry the contextual rules read, and trunkFork's
// binary search, about eleven lookups for a 2,000-header reply), each normally
// a millisecond or two. A fill that runs out of time is refused as
// unjudgeable, which keeps the headers judged before it and costs the peer
// nothing; the next reply asks again.
const headerStoreTimeout = 30 * time.Second

// WithOwnerLive hands the cache the test a fill uses to decide whether its
// owner is still connected (see ownerLive) and returns it.
func (c *headerCache) WithOwnerLive(live func(owner any, committed int32) bool) *headerCache {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.ownerLive = live

	return c
}

// capForCheckpoints is the most headers above the committed tip a branch may
// hold: the widest gap between consecutive checkpoints (from genesis to the
// first included) plus one reply.
//
// The checkpoint walk is what needs the room. Below the last checkpoint nothing
// is downloaded until a branch matches the next checkpoint above the tip, and
// the walk toward it starts when the tip reaches the one before, so an honest
// branch must be able to reach across one whole gap. The reply that crosses the
// checkpoint can carry up to MaxBlockHeadersPerMsg-1 headers beyond it. That is
// 52,000 on mainnet (the 50,000 gaps from 600000) and 102,010 on testnet (its
// widest gap, 700000 to 800010). With no checkpoints there is no walk, and one reply's worth
// above two is room enough for the refill cadence (headerCacheRefillThreshold).
func capForCheckpoints(checkpoints []chaincfg.Checkpoint) int32 {
	gap := int32(wire.MaxBlockHeadersPerMsg)

	var prev int32

	for _, cp := range checkpoints {
		if cp.Height-prev > gap {
			gap = cp.Height - prev
		}

		prev = cp.Height
	}

	return gap + int32(wire.MaxBlockHeadersPerMsg)
}

// WithCheckpoints hands the cache the chain's pinned checkpoints and returns it,
// so a caller can write newHeaderCache().WithCheckpoints(params.Checkpoints) in
// one expression. It also sets the branch cap from them.
//
// A separate step rather than a constructor argument because the overwhelming
// majority of callers, every test that only cares which heights the cache
// names, has no checkpoint list to give and must not be made to invent one.
// Those callers never get proof, so the fast path is denied, which is the
// answer a harness with no checkpoints should get.
func (c *headerCache) WithCheckpoints(checkpoints []chaincfg.Checkpoint) *headerCache {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.checkpoints = checkpoints
	c.branchCap = capForCheckpoints(checkpoints)

	return c
}

// WithPowLimit hands the cache the chain's proof-of-work ceiling and returns it.
// With a ceiling, Fill refuses any batch containing a header whose declared
// target is easier than the ceiling or whose hash does not meet its own target.
// That is SV Node's CheckProofOfWork (pow.cpp:144-164), which CheckBlockHeader
// runs on every header (validation.cpp:5586-5596, high-hash, DoS 50) before
// AcceptBlockHeader does anything else.
func (c *headerCache) WithPowLimit(ceiling *big.Int) *headerCache {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.powLimit = ceiling

	return c
}

// WithHeaderRules hands the cache SV Node's contextual header rules and returns
// it. With rules, Fill refuses a header whose nBits is not the expected
// difficulty at its height, whose time is not after its parent's median time
// past or is more than two hours ahead, or whose version is obsolete at its
// height, and it counts chain work from the trunk's own chain work.
func (c *headerCache) WithHeaderRules(rules *headerRules) *headerCache {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.rules = rules

	return c
}

// WithMinimumChainWork hands the cache SV Node's nMinimumChainWork for the chain
// and returns it. See Wantable for how it is used.
func (c *headerCache) WithMinimumChainWork(work *big.Int) *headerCache {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.minChainWork = work

	return c
}

// PowLimit returns the proof-of-work ceiling Fill judges headers against, or
// nil when it judges linkage alone. fillHeaderCache reads it to classify a
// refusal with the same ceiling Fill used, never one re-derived elsewhere.
func (c *headerCache) PowLimit() *big.Int {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.powLimit
}

// fillResult is a fill's answer.
//
//   - accepted is true when the sender's branch moved to a new tip.
//   - extended is true when the batch connected to a header the cache already
//     held rather than to the committed tip.
//   - low and added say which heights this fill made the cache hold for the
//     first time: added of them, from low. Zero added is a sender catching up to
//     headers another peer already sent.
//   - top and topHash are the sender's branch tip after the fill.
//   - rejection, rejectedHeight and detail describe a header a rule refused.
type fillResult struct {
	accepted       bool
	extended       bool
	low            int32
	added          int
	top            int32
	topHash        chainhash.Hash
	rejection      headerRejection
	rejectedHeight int32
	detail         string
}

// Fill is FillFrom for the default owner, reporting only whether the branch moved.
func (c *headerCache) Fill(parent chainhash.Hash, baseHeight int32, headers []*wire.BlockHeader) bool {
	return c.FillFrom(defaultHeaderOwner{}, parent, baseHeight, headers).accepted
}

// FillReporting is FillFrom for the default owner, reporting whether the branch
// moved and whether the batch connected to a header already held.
func (c *headerCache) FillReporting(parent chainhash.Hash, baseHeight int32, headers []*wire.BlockHeader) (accepted, extended bool) {
	result := c.FillFrom(defaultHeaderOwner{}, parent, baseHeight, headers)

	return result.accepted, result.extended
}

// FillDetailed is FillFrom for the default owner.
func (c *headerCache) FillDetailed(parent chainhash.Hash, baseHeight int32, headers []*wire.BlockHeader) fillResult {
	return c.FillFrom(defaultHeaderOwner{}, parent, baseHeight, headers)
}

// FillFrom is SV Node's ProcessNewBlockHeaders and UpdateBlockAvailability for
// one headers message from owner. parent and baseHeight are the committed tip
// and the height above it, as this node read them when the message arrived.
//
// Every header must name the one before it, and meet proof of work when the
// cache has a ceiling, or the whole batch is refused before anything else
// (linkedHashes). Then the batch is placed in the tree: its new part starts
// after the last header in it that the tree already holds, or that is the
// committed tip. A batch that meets neither is refused, because it describes a
// chain this node is not on or one it has run past; the sender is not blamed for
// that, because an honest peer answering an older locator sends exactly that.
//
// Each new header is then judged, in order, by the rules SV Node's
// AcceptBlockHeader runs: the checkpoint at its height, then the contextual
// rules. A refusal SV Node scores DoS 100 refuses the whole batch. Any other
// keeps the headers before it, as SV Node keeps the headers it accepted before
// the first it did not.
//
// Last, owner's branch moves to the batch's last header if that has at least
// as much chain work as the branch's tip (SV Node's UpdateBlockAvailability,
// net_processing.cpp: pindexBestKnownBlock moves on nChainWork >=), and the
// active branch is chosen again. A batch that would not move the branch leaves
// the tree as it was.
func (c *headerCache) FillFrom(owner any, parent chainhash.Hash, baseHeight int32, headers []*wire.BlockHeader) fillResult {
	if c == nil || len(headers) == 0 {
		return fillResult{}
	}

	c.fillMu.Lock()
	defer c.fillMu.Unlock()

	// Whatever DropPeer unlinked while this fill waited is freed when it is
	// done, still under fillMu.
	defer c.reap()

	c.mu.Lock()
	ceiling, timeout := c.powLimit, c.storeTimeout
	c.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	hashes, ok := linkedHashes(headers, ceiling)
	if !ok {
		return fillResult{}
	}

	c.mu.Lock()
	plan, ok := c.placeLocked(parent, baseHeight, headers, hashes)
	rules, checkpoints := c.rules, c.checkpoints
	floor := baseHeight - 1

	if c.haveFloor && c.floorHeight > floor {
		floor = c.floorHeight
	}
	c.mu.Unlock()

	if !ok {
		if rules == nil {
			return fillResult{}
		}

		return trunkFork(ctx, rules, checkpoints, floor, headers, hashes)
	}

	nodes, result := c.judge(ctx, plan, headers, hashes)
	if result.rejection.disconnects() {
		return result
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.installLocked(owner, plan, nodes, result)
}

// fillPlan is where a batch meets the tree: anchor is the held node its new
// part builds on, or nil when it builds on the committed tip (parent at
// anchorHeight). start is the index of the first new header, and cut the index
// past the last one the branch cap allows.
type fillPlan struct {
	parent       chainhash.Hash
	anchor       *headerNode
	anchorHeight int32
	start        int
	cut          int

	// floorHeight is the committed tip this fill places against: the one it
	// was given, or the one already recorded when that is higher (a fill's
	// read of the tip can be older than PruneTo's).
	floorHeight int32

	// heldCheckpoint is the highest checkpoint height above the committed tip
	// whose pinned hash the tree holds on a path down to the tip, 0 when none:
	// SV Node's GetLastCheckpoint (checkpoints.cpp:24-36), the last checkpoint
	// in its block index, for the part of the index this cache is.
	heldCheckpoint int32
}

// placeLocked finds where a linked batch meets the tree. Called with mu held.
func (c *headerCache) placeLocked(parent chainhash.Hash, baseHeight int32, headers []*wire.BlockHeader, hashes []chainhash.Hash) (fillPlan, bool) {
	plan := fillPlan{parent: parent, start: -1, floorHeight: baseHeight - 1}
	if c.haveFloor && c.floorHeight > plan.floorHeight {
		plan.floorHeight = c.floorHeight
	}

	held := func(hash chainhash.Hash) *headerNode {
		node, ok := c.index[hash]
		if !ok || node.height <= plan.floorHeight {
			return nil
		}

		return node
	}

	// The last header the tree already holds, or the committed tip, whichever
	// is later in the batch: everything before it is either held or committed,
	// because the batch is one linked run.
	for i := len(hashes) - 1; i >= 0; i-- {
		if node := held(hashes[i]); node != nil {
			plan.anchor, plan.anchorHeight, plan.start = node, node.height, i+1

			break
		}

		if hashes[i] == parent {
			plan.anchorHeight, plan.start = baseHeight-1, i+1

			break
		}
	}

	if plan.start < 0 {
		switch node := held(headers[0].PrevBlock); {
		case node != nil:
			plan.anchor, plan.anchorHeight, plan.start = node, node.height, 0
		case headers[0].PrevBlock == parent:
			plan.anchorHeight, plan.start = baseHeight-1, 0
		default:
			return fillPlan{}, false
		}
	}

	// A fill reads the committed tip before it waits for fillMu, and PruneTo can
	// record a higher one in between. A batch built on that older tip then
	// starts at or below the recorded floor, and those headers are committed
	// history, not new ones: judged as new, they sit below any checkpoint the
	// tree holds and would read as a fork from it. So the batch is clipped to
	// the floor: it is placed on the floor when its header at the floor height
	// is the recorded floor hash, and otherwise it is not placed, which leaves
	// it to trunkFork to judge against the store, or to be dropped without
	// blame. A batch that ends at or below the floor has nothing new.
	if plan.anchor == nil && plan.anchorHeight < plan.floorHeight {
		atFloor := plan.start + int(plan.floorHeight-plan.anchorHeight) - 1
		if atFloor >= len(hashes)-1 || !c.floorHashKnown || hashes[atFloor] != c.floorHash {
			return fillPlan{}, false
		}

		plan.parent, plan.anchorHeight, plan.start = c.floorHash, plan.floorHeight, atFloor+1
	}

	plan.heldCheckpoint = c.heldCheckpointLocked(plan.floorHeight)

	// The cap counts from the committed tip: a branch may name at most
	// branchCap heights above it.
	plan.cut = len(headers)

	if limit := plan.floorHeight + c.branchCap; plan.anchorHeight+int32(len(headers)-plan.start) > limit { //nolint:gosec // a batch index, bounded by the wire limit
		plan.cut = plan.start + max(0, int(limit-plan.anchorHeight))
	}

	return plan, true
}

// heldCheckpointLocked returns the highest checkpoint height above floor whose
// pinned hash is a held node whose path reaches down to floor+1, or 0. Every
// height from floor+1 to that checkpoint is then held on the checkpoint's own
// chain, so a header at one of those heights that the tree does not already
// hold is not on that chain. The checkpoint list is short (35 on mainnet) and
// the walk is paid once per fill.
func (c *headerCache) heldCheckpointLocked(floor int32) int32 {
	for i := len(c.checkpoints) - 1; i >= 0; i-- {
		cp := c.checkpoints[i]
		if cp.Height <= floor || cp.Hash == nil {
			continue
		}

		node, ok := c.index[*cp.Hash]
		if !ok || node.height != cp.Height {
			continue
		}

		for node.parent != nil && node.height > floor+1 {
			node = node.parent
		}

		if node.height <= floor+1 {
			return cp.Height
		}
	}

	return 0
}

// committedCheckpoint returns the highest checkpoint height at or below the
// committed tip, or 0. The committed chain passed every checkpoint at or below
// its tip, so this is the last checkpoint SV Node would find in its index from
// the committed chain alone.
func committedCheckpoint(checkpoints []chaincfg.Checkpoint, tip int32) int32 {
	var height int32

	for _, cp := range checkpoints {
		if cp.Hash != nil && cp.Height <= tip && cp.Height > height {
			height = cp.Height
		}
	}

	return height
}

// heldLocked returns the held node for hash, or nil when the cache does not
// hold it above the committed tip.
func (c *headerCache) heldLocked(hash chainhash.Hash) *headerNode {
	node, ok := c.index[hash]
	if !ok || (c.haveFloor && node.height <= c.floorHeight) {
		return nil
	}

	return node
}

// judge makes the nodes for the new part of a placed batch, judging each header
// before the next. It returns the nodes it passed and the refusal that stopped
// it, if one did. It holds fillMu but not mu: the index and the nodes it reads
// cannot change, because every writer of them holds fillMu. Its store calls
// run under ctx, which carries the fill's deadline.
func (c *headerCache) judge(ctx context.Context, plan fillPlan, headers []*wire.BlockHeader, hashes []chainhash.Hash) ([]*headerNode, fillResult) {
	c.mu.Lock()
	rules := c.rules
	checkpoints := c.checkpoints
	c.mu.Unlock()

	var (
		parentWork [32]byte
		parentCP   int32
	)

	// Read without mu: see judge's doc. A committed tip the tree still holds
	// below the floor (not yet swept) already carries its chain work.
	committed, heldTip := c.index[plan.parent]

	switch {
	case plan.anchor != nil:
		parentWork, parentCP = plan.anchor.chainWork, plan.anchor.cpHeight
	case heldTip:
		parentWork, parentCP = committed.chainWork, committed.cpHeight
	case rules != nil && plan.start < plan.cut:
		_, meta, err := rules.trunk.GetBlockHeader(ctx, &plan.parent)
		if err != nil || meta == nil {
			return nil, fillResult{rejection: rejectUnjudgeable, rejectedHeight: plan.anchorHeight + 1, detail: fmt.Sprintf("chain work of the committed tip %s: %v", plan.parent, err)}
		}

		new(big.Int).SetBytes(meta.ChainWork).FillBytes(parentWork[:])
	}

	// Without rules there is no trunk to read chain work from, and a branch
	// rooted on a tip the tree does not hold counts its work from zero. Only
	// tests build a cache that way.

	nodes := make([]*headerNode, 0, plan.cut-plan.start)

	var (
		source  *branchSource
		pending map[chainhash.Hash]*headerNode
	)

	if rules != nil {
		pending = make(map[chainhash.Hash]*headerNode, plan.cut-plan.start)
		source = &branchSource{
			trunk: rules.trunk,
			lookup: func(hash chainhash.Hash) (*cachedHeader, bool) {
				node, ok := pending[hash]
				if !ok {
					// Read without mu: see judge's doc.
					node, ok = c.index[hash]
				}

				if !ok {
					return nil, false
				}

				return node.cached(), true
			},
		}
	}

	prev := plan.anchor
	prevHeight := plan.anchorHeight

	for i := plan.start; i < plan.cut; i++ {
		height := prevHeight + 1

		if cp := checkpointAt(checkpoints, height); cp != nil && !hashes[i].IsEqual(cp.Hash) {
			return nil, fillResult{rejection: rejectCheckpointMismatch, rejectedHeight: height, detail: fmt.Sprintf("height %d carries %s, the pinned checkpoint is %s", height, hashes[i], cp.Hash)}
		}

		// Every new header is one the tree does not hold (placeLocked starts
		// after the last held one), so below a held checkpoint it is a fork
		// from that checkpoint's chain.
		if height < plan.heldCheckpoint {
			return nil, fillResult{rejection: rejectForkBeforeCheckpoint, rejectedHeight: height, detail: fmt.Sprintf("height %d forks below the held checkpoint at %d", height, plan.heldCheckpoint)}
		}

		if rules != nil {
			var parentHeader *model.BlockHeader
			if prev != nil {
				parentHeader = prev.modelHeader()
			} else {
				header, _, err := rules.trunk.GetBlockHeader(ctx, &plan.parent)
				if err != nil || header == nil {
					return nodes, fillResult{rejection: rejectUnjudgeable, rejectedHeight: height, detail: fmt.Sprintf("committed tip %s: %v", plan.parent, err)}
				}

				parentHeader = header
			}

			if rejection, detail := rules.check(ctx, source, parentHeader, prevHeight, headers[i]); rejection != headerAccepted {
				return nodes, fillResult{rejection: rejection, rejectedHeight: height, detail: detail}
			}
		}

		node := newHeaderNode(hashes[i], headers[i], prev, height, parentCP)

		sum := new(big.Int).SetBytes(parentWork[:])
		sum.Add(sum, work.CalcBlockWork(headers[i].Bits))
		sum.FillBytes(node.chainWork[:])

		if checkpointAt(checkpoints, height) != nil {
			node.cpHeight = height
		}

		nodes = append(nodes, node)

		if pending != nil {
			pending[node.hash] = node
		}

		prev, prevHeight, parentWork, parentCP = node, height, node.chainWork, node.cpHeight
	}

	return nodes, fillResult{}
}

// trunkFork judges a batch that meets neither the committed tip nor a held
// header, which needs the store: SV Node's index holds the committed chain too,
// so a header forking from it below the last checkpoint is
// bad-fork-prior-to-checkpoint there (validation.cpp:5763-5772), and one at a
// checkpoint height with the wrong hash is checkpoint mismatch (5757-5761).
//
// The first header the store does not hold is found by binary search: a
// stored block's ancestors are all stored, so the stored headers in a linked
// batch are a prefix. A batch the store holds whole (an honest peer answering
// an older locator) costs one lookup and is dropped without blame, as is one
// whose fork point the store does not hold, and one forking above the last
// committed checkpoint: SV Node would keep that header as a side branch, and
// below the tip this cache holds none. Store lookups that fail say nothing
// about the peer: only a lookup the store answers with not found means a
// header is not stored, and any other failure abandons the batch without
// blame, because reading it as "not stored" would move the search and place
// the fork lower than it is.
func trunkFork(ctx context.Context, rules *headerRules, checkpoints []chaincfg.Checkpoint, floor int32, headers []*wire.BlockHeader, hashes []chainhash.Hash) fillResult {
	// stored reports whether hash is a stored block, and answered whether the
	// store gave an answer at all.
	stored := func(hash chainhash.Hash) (isStored, answered bool) {
		header, meta, err := rules.trunk.GetBlockHeader(ctx, &hash)

		switch {
		case err == nil && header != nil && meta != nil:
			return true, true
		case err != nil && errors.Is(err, errors.ErrNotFound):
			return false, true
		default:
			return false, false
		}
	}

	isStored, answered := stored(hashes[len(hashes)-1])
	if isStored || !answered {
		return fillResult{}
	}

	lo, hi := 0, len(hashes)-1 // hashes[hi] is not stored
	for lo < hi {
		mid := (lo + hi) / 2

		isStored, answered = stored(hashes[mid])
		if !answered {
			return fillResult{}
		}

		if isStored {
			lo = mid + 1
		} else {
			hi = mid
		}
	}

	parent := headers[0].PrevBlock
	if lo > 0 {
		parent = hashes[lo-1]
	}

	_, meta, err := rules.trunk.GetBlockHeader(ctx, &parent)
	if err != nil || meta == nil {
		return fillResult{}
	}

	height := int32(meta.Height) + 1 //nolint:gosec // a stored height fits a chain height

	if cp := checkpointAt(checkpoints, height); cp != nil && !hashes[lo].IsEqual(cp.Hash) {
		return fillResult{rejection: rejectCheckpointMismatch, rejectedHeight: height, detail: fmt.Sprintf("height %d carries %s, the pinned checkpoint is %s", height, hashes[lo], cp.Hash)}
	}

	if last := committedCheckpoint(checkpoints, floor); height < last {
		return fillResult{rejection: rejectForkBeforeCheckpoint, rejectedHeight: height, detail: fmt.Sprintf("height %d forks from the committed chain below its checkpoint at %d", height, last)}
	}

	return fillResult{}
}

// cached is the node in the form branchSource reads.
func (n *headerNode) cached() *cachedHeader {
	return &cachedHeader{header: n.modelHeader(), height: n.height, chainWork: n.chainWork}
}

// installLocked holds the judged nodes and moves owner's branch to the batch's
// last header, if that has at least as much chain work as owner's current tip.
// Called with mu and fillMu held.
func (c *headerCache) installLocked(owner any, plan fillPlan, nodes []*headerNode, result fillResult) fillResult {
	result.extended = plan.anchor != nil

	// A departed owner, or one this node may no longer ask for headers, gets
	// no branch: DropPeer has already run for it, or runs after this under mu
	// and finds the branch, because the peer is marked gone, or the sync peer
	// cleared, before DropPeer is called.
	if c.ownerLive != nil && !c.ownerLive(owner, plan.floorHeight) {
		return result
	}

	var tip *headerNode

	switch {
	case len(nodes) > 0:
		tip = nodes[len(nodes)-1]
	case plan.anchor != nil && plan.start > 0:
		// Every header in the batch was already held: the sender has caught up
		// to headers another peer sent. Its best known header is the last one.
		tip = plan.anchor
	default:
		return result
	}

	// The committed tip this fill was given is recorded only if the fill is
	// kept, so a refused fill changes nothing. It is judged first, because
	// whether owner's current branch still counts depends on it.
	saved := c.floorState()
	c.observeTipLocked(plan)

	if current := c.branches[owner]; current != nil && c.liveLocked(current) && workLess(tip, current.tip) {
		c.restoreFloor(saved)

		return result
	}

	c.trimActiveLocked()

	for _, node := range nodes {
		c.seq++
		node.seq = c.seq
		c.index[node.hash] = node

		if node.parent != nil {
			node.parent.refs++
		}

		if node.cpHeight == node.height && node.height > 0 {
			markProven(node)
		}
	}

	if len(nodes) > 0 {
		result.low, result.added = nodes[0].height, len(nodes)
	}

	c.setTipLocked(owner, tip)
	c.selectLocked()

	result.accepted = true
	result.top, result.topHash = tip.height, tip.hash

	return result
}

// floorSnapshot is the recorded committed tip, saved so a refused fill can
// put it back.
type floorSnapshot struct {
	height    int32
	hash      chainhash.Hash
	have      bool
	hashKnown bool
}

func (c *headerCache) floorState() floorSnapshot {
	return floorSnapshot{height: c.floorHeight, hash: c.floorHash, have: c.haveFloor, hashKnown: c.floorHashKnown}
}

func (c *headerCache) restoreFloor(s floorSnapshot) {
	c.floorHeight, c.floorHash, c.haveFloor, c.floorHashKnown = s.height, s.hash, s.have, s.hashKnown
}

// markProven sets proven on a checkpoint node and every node below it that is
// not already proven.
func markProven(node *headerNode) {
	for n := node; n != nil && !n.proven; n = n.parent {
		n.proven = true
	}
}

// setTipLocked points owner's branch at tip, releasing whatever only the old tip held.
func (c *headerCache) setTipLocked(owner any, tip *headerNode) {
	tip.refs++

	if current := c.branches[owner]; current != nil {
		old := current.tip
		current.tip, current.diverged = tip, false
		old.refs--
		c.releaseLocked(old)

		return
	}

	c.branches[owner] = &headerBranch{owner: owner, tip: tip}
}

// releaseLocked removes node, and then each parent left with nothing pointing
// at it, from the index.
func (c *headerCache) releaseLocked(node *headerNode) {
	for n := node; n != nil && n.refs <= 0; {
		if held, ok := c.index[n.hash]; ok && held == n {
			delete(c.index, n.hash)
		}

		p := n.parent
		if p != nil {
			p.refs--
		}

		n = p
	}
}

// workLess reports whether a has less chain work than b. Chain work is a fixed
// 32 bytes, big-endian, so byte order is numeric order.
func workLess(a, b *headerNode) bool {
	return bytes.Compare(a.chainWork[:], b.chainWork[:]) < 0
}

// observeTipLocked records the committed tip a kept fill was given. A fill's
// read of the tip can be older than one PruneTo has already recorded, so it
// only moves the floor up, or to a different hash at the same height (a
// same-height reorg). The hash is recorded as known only when the batch
// actually built on it: a batch that built on a held header never tested the
// hash it was handed.
func (c *headerCache) observeTipLocked(plan fillPlan) {
	height := plan.floorHeight
	known := plan.anchor == nil && height == plan.anchorHeight

	switch {
	case !c.haveFloor || height > c.floorHeight:
		c.floorHeight, c.floorHash, c.floorHashKnown, c.haveFloor = height, plan.parent, known, true
	case height == c.floorHeight && known && (!c.floorHashKnown || plan.parent != c.floorHash):
		c.floorHash, c.floorHashKnown = plan.parent, true

		for _, branch := range c.branches {
			branch.diverged = false
		}
	}
}

// trimActiveLocked drops the active layout's heights at or below the floor.
func (c *headerCache) trimActiveLocked() {
	if c.active == nil || len(c.activeNodes) == 0 || !c.haveFloor {
		return
	}

	if drop := int(c.floorHeight - c.activeLow + 1); drop > 0 {
		if drop >= len(c.activeNodes) {
			c.activeNodes, c.activeLow = nil, c.floorHeight+1
		} else {
			c.activeNodes, c.activeLow = c.activeNodes[drop:], c.floorHeight+1
		}
	}
}

// setFloorLocked records the committed tip and chooses the active branch again.
func (c *headerCache) setFloorLocked(height int32, hash chainhash.Hash, hashKnown bool) {
	if c.haveFloor && height < c.floorHeight {
		// A lower tip is a reorg: what forked below the old tip may connect to
		// the new one, and the active layout starts too high.
		for _, branch := range c.branches {
			branch.diverged = false
		}

		c.activeNodes = nil
	}

	c.floorHeight, c.floorHash, c.floorHashKnown, c.haveFloor = height, hash, hashKnown, true

	c.trimActiveLocked()

	c.selectLocked()
}

// liveLocked reports whether branch still names a height above the committed
// tip and still connects to it.
func (c *headerCache) liveLocked(branch *headerBranch) bool {
	if branch.diverged || (c.haveFloor && branch.tip.height <= c.floorHeight) {
		return false
	}

	return c.anchoredLocked(branch)
}

// anchoredLocked reports whether branch connects to the committed tip: its
// header one above the tip names the tip as its parent. A branch rooted above
// the tip (the tip reported lower than before, which a reorg does) does not.
func (c *headerCache) anchoredLocked(branch *headerBranch) bool {
	if !c.haveFloor || !c.floorHashKnown {
		return true
	}

	var first *headerNode

	if n := len(c.activeNodes); branch == c.active && n > 0 && c.activeNodes[n-1] == branch.tip && c.activeLow == c.floorHeight+1 {
		first = c.activeNodes[0]
	} else {
		first = branch.tip
		for first.height > c.floorHeight+1 && first.parent != nil {
			first = first.parent
		}
	}

	return first.height == c.floorHeight+1 && first.prevBlock == c.floorHash
}

// provenTo is the highest height on node's path that a matched checkpoint
// commits: node's own height when a checkpoint above it has been matched on
// some branch, else the highest checkpoint on its own path.
func provenTo(node *headerNode) int32 {
	if node.proven {
		return node.height
	}

	return node.cpHeight
}

// effectiveProofLocked is branch's proof for choosing between branches: proof
// at or below the committed tip says nothing about what is left to download.
func (c *headerCache) effectiveProofLocked(branch *headerBranch) int32 {
	p := provenTo(branch.tip)
	if c.haveFloor && p <= c.floorHeight {
		return 0
	}

	return p
}

// selectLocked chooses the active branch: the live branch with the highest
// effective proof, then the most chain work, then the current active branch,
// then the tip held first. That is SV Node's CBlockIndexWorkComparator
// (most work, then lowest nSequenceId) with checkpoint proof placed in front.
//
// It runs on every committed block (PruneTo), so it ranks first and checks
// connection to the committed tip only for the winner: the active branch's
// check is one index, another branch's is a walk, paid when the winner changes.
// A branch found to have forked from the committed chain is marked and skipped
// until the tip is reported lower again.
func (c *headerCache) selectLocked() {
	var waiting map[*headerBranch]bool

	for {
		var best *headerBranch

		for _, branch := range c.branches {
			if branch.diverged || waiting[branch] || (c.haveFloor && branch.tip.height <= c.floorHeight) {
				continue
			}

			if best == nil || c.betterLocked(branch, best) {
				best = branch
			}
		}

		if best == nil {
			c.active, c.activeNodes = nil, nil

			return
		}

		if !c.anchoredLocked(best) {
			if c.rootedAtOrBelowFloorLocked(best) {
				best.diverged = true

				continue
			}

			// Rooted on a tip reported higher than the one now: not a
			// candidate until the floor comes back up to it.
			if c.active == best {
				c.active, c.activeNodes = nil, nil
			}

			if waiting == nil {
				waiting = make(map[*headerBranch]bool)
			}

			waiting[best] = true

			continue
		}

		if best == c.active && len(c.activeNodes) > 0 && c.activeNodes[len(c.activeNodes)-1] == best.tip {
			return
		}

		c.active = best
		c.rebuildActiveLocked()

		return
	}
}

// betterLocked reports whether a should be active over b.
func (c *headerCache) betterLocked(a, b *headerBranch) bool {
	if pa, pb := c.effectiveProofLocked(a), c.effectiveProofLocked(b); pa != pb {
		return pa > pb
	}

	if cmp := bytes.Compare(a.tip.chainWork[:], b.tip.chainWork[:]); cmp != 0 {
		return cmp > 0
	}

	if a == c.active || b == c.active {
		return a == c.active
	}

	return a.tip.seq < b.tip.seq
}

// rebuildActiveLocked lays the active branch out by height, from one above the
// committed tip (or its root, if that is higher) to its tip. When the new tip
// descends from the old one, only the new part is walked.
func (c *headerCache) rebuildActiveLocked() {
	tip := c.active.tip

	if n := len(c.activeNodes); n > 0 && tip.height >= c.activeNodes[n-1].height {
		old := c.activeNodes[n-1]
		suffix := make([]*headerNode, 0, tip.height-old.height)

		walk := tip
		for walk != nil && walk.height > old.height {
			suffix = append(suffix, walk)
			walk = walk.parent
		}

		if walk == old {
			for i := len(suffix) - 1; i >= 0; i-- {
				c.activeNodes = append(c.activeNodes, suffix[i])
			}

			return
		}
	}

	low := tip.height
	if c.haveFloor {
		low = max(c.floorHeight+1, 0)
	}

	nodes := make([]*headerNode, 0, max(tip.height-low+1, 1))

	for walk := tip; walk != nil && walk.height >= low; walk = walk.parent {
		nodes = append(nodes, walk)
	}

	for i, j := 0, len(nodes)-1; i < j; i, j = i+1, j-1 {
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}

	c.activeNodes = nodes
	c.activeLow = nodes[0].height
}

// activeAtLocked returns the active branch's node at height, or nil.
func (c *headerCache) activeAtLocked(height int32) *headerNode {
	if c.active == nil || (c.haveFloor && height <= c.floorHeight) {
		return nil
	}

	i := int(height - c.activeLow)
	if i < 0 || i >= len(c.activeNodes) {
		return nil
	}

	return c.activeNodes[i]
}

// linkedHashes is Fill's walk: it returns each header's hash, or false when a
// header does not name the one before it or, with a ceiling, does not meet
// proof of work.
func linkedHashes(headers []*wire.BlockHeader, ceiling *big.Int) ([]chainhash.Hash, bool) {
	hashes := make([]chainhash.Hash, 0, len(headers))

	var prev chainhash.Hash

	for i, header := range headers {
		// i == 0 has nothing before it to link to.
		if i > 0 && !header.PrevBlock.IsEqual(&prev) {
			return nil, false
		}

		hash := header.BlockHash()

		if ceiling != nil && !headerMeetsWork(header.Bits, hash, ceiling) {
			return nil, false
		}

		hashes = append(hashes, hash)
		prev = hash
	}

	return hashes, true
}

// headerMeetsWork is CheckProofOfWork for one header: the declared target must
// be a positive number no easier than the chain's ceiling, and the header's hash,
// read as the 256-bit number the header's own difficulty is defined over, must
// not exceed that target. The range half comes first because a malformed or
// absurdly easy target makes the hash comparison meaningless.
//
// hash is a chainhash.Hash, whose bytes are the reverse of the number's
// big-endian form, so they are reversed before SetBytes, exactly as
// model.BlockHeader.HasMetTargetDifficulty does.
func headerMeetsWork(bits uint32, hash chainhash.Hash, ceiling *big.Int) bool {
	target := blockchain.CompactToBig(bits)
	if target == nil || target.Sign() <= 0 || target.Cmp(ceiling) > 0 {
		return false
	}

	return new(big.Int).SetBytes(bt.ReverseBytes(hash[:])).Cmp(target) <= 0
}

// firstHeaderWithoutWork returns the index of the first header in headers that
// headerMeetsWork refuses under ceiling, or -1 when every header passes or there
// is no ceiling. Fill reports a refusal as one false, so fillHeaderCache re-walks
// the batch with this to tell "does not meet proof of work" apart from the other
// reasons Fill says no, for the log line and the disconnect.
func firstHeaderWithoutWork(headers []*wire.BlockHeader, ceiling *big.Int) int {
	if ceiling == nil {
		return -1
	}

	for i, header := range headers {
		if !headerMeetsWork(header.Bits, header.BlockHash(), ceiling) {
			return i
		}
	}

	return -1
}

// findAnchor returns the index of the first header in headers usable once the
// batch is judged against anchor: either headers[0] already builds on anchor,
// so the whole batch is usable from index 0, or some headers[i] hashes to
// anchor, so headers[i+1] onward is usable. It returns -1 when neither shape
// holds.
func findAnchor(headers []*wire.BlockHeader, hashes []chainhash.Hash, anchor chainhash.Hash) int {
	if headers[0].PrevBlock.IsEqual(&anchor) {
		return 0
	}

	for i, hash := range hashes {
		if hash.IsEqual(&anchor) {
			return i + 1
		}
	}

	return -1
}

// checkpointAt returns the pinned checkpoint at height, or nil when height is
// not a checkpoint height. This is Checkpoints::CheckBlock's lookup
// (checkpoints.cpp:14-22).
func checkpointAt(checkpoints []chaincfg.Checkpoint, height int32) *chaincfg.Checkpoint {
	for i := range checkpoints {
		if checkpoints[i].Height == height && checkpoints[i].Hash != nil {
			return &checkpoints[i]
		}
	}

	return nil
}

// nextCheckpointAbove returns the lowest checkpoint in checkpoints whose
// height is greater than height, or nil when there is none — either because
// checkpoints is empty or height is already at or past the last one.
//
// Mirrors SyncManager.findNextHeaderCheckpoint exactly (the same >= cutoff
// against the final checkpoint, the same walk-backward-from-the-end search),
// because the two must agree on when a below-checkpoint walk is still owed a
// request.
func nextCheckpointAbove(checkpoints []chaincfg.Checkpoint, height int32) *chaincfg.Checkpoint {
	if len(checkpoints) == 0 {
		return nil
	}

	final := &checkpoints[len(checkpoints)-1]
	if height >= final.Height {
		return nil
	}

	next := final

	for i := len(checkpoints) - 2; i >= 0; i-- {
		if height >= checkpoints[i].Height {
			break
		}

		next = &checkpoints[i]
	}

	return next
}

// belowLastCheckpointLocked reports whether height sits below this cache's
// final checkpoint. Called with c.mu held.
func (c *headerCache) belowLastCheckpointLocked(height int32) bool {
	return nextCheckpointAbove(c.checkpoints, height) != nil
}

// NextCheckpointAbove returns the lowest pinned checkpoint above height (the
// committed tip, ordinarily) and whether one exists. maybeRequestMoreHeaders
// reads this to decide whether a below-checkpoint walk still owes a request.
func (c *headerCache) NextCheckpointAbove(height int32) (chaincfg.Checkpoint, bool) {
	if c == nil {
		return chaincfg.Checkpoint{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	cp := nextCheckpointAbove(c.checkpoints, height)
	if cp == nil {
		return chaincfg.Checkpoint{}, false
	}

	return *cp, true
}

// At returns the hash the active branch names for height, and whether it names one.
func (c *headerCache) At(height int32) (chainhash.Hash, bool) {
	if c == nil {
		return chainhash.Hash{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if node := c.activeAtLocked(height); node != nil {
		return node.hash, true
	}

	return chainhash.Hash{}, false
}

// Wantable is At with the gates a block request must pass: the hash the active
// branch names for height, and whether the node may ask a peer for that block
// yet.
//
// Below the last checkpoint every requested block is destined for the quick
// route, which runs no header or chain-membership rule of its own, so a block
// is wantable only once the active branch has matched a pinned checkpoint hash
// at or above it. That is the merge-base's checkpoint-gated fetch, and
// teranode's form of SV Node's bad-fork-prior-to-checkpoint
// (validation.cpp:5748-5775): SV Node holds the header index through the
// checkpoint before it downloads a body, and this cache holds the branch
// through it.
//
// Above the last checkpoint no proof can exist, and the gate is SV Node's own
// (net_processing.cpp:355-366, FindNextBlocksToDownload): no block is requested
// along a branch whose chain work is below nMinimumChainWork or below the
// committed tip's. The active branch connects to the committed tip, so its tip
// has more work than the committed tip whenever it holds a header; the minimum
// is the half that can refuse.
//
// Below the last checkpoint the minimum is not applied, and that is a deviation
// from SV Node, made because applying it would stall: SV Node reaches the
// minimum by holding every header to the network's tip before it downloads a
// body, while this cache holds one checkpoint gap. A branch that has matched a
// checkpoint is on the checkpointed chain, and on mainnet that chain passes the
// minimum below its last checkpoint (the chain work at 886000, pinned by
// services/blockchain's Difficulty_mainnet_test, already exceeds it, and the
// last checkpoint is 945000).
//
// Cost of the checkpoint gate, stated plainly: downloads between checkpoints C
// and C' begin only once the walk has matched C', and the walk to C' starts when
// the committed tip reaches C.
func (c *headerCache) Wantable(height int32) (chainhash.Hash, bool) {
	if c == nil {
		return chainhash.Hash{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	node := c.activeAtLocked(height)
	if node == nil {
		return chainhash.Hash{}, false
	}

	if c.belowLastCheckpointLocked(height - 1) {
		if height > provenTo(c.active.tip) {
			return chainhash.Hash{}, false
		}

		return node.hash, true
	}

	if c.minChainWork != nil && new(big.Int).SetBytes(c.active.tip.chainWork[:]).Cmp(c.minChainWork) < 0 {
		return chainhash.Hash{}, false
	}

	return node.hash, true
}

// HeightOf is At's reverse over every branch: the height of a held header,
// and whether one is held above the committed tip. Used by pipelineParentHeight
// to resolve a not-yet-committed parent's height from its hash alone. A hash's
// height is a fact about its ancestry, so it does not matter which branch holds
// it.
func (c *headerCache) HeightOf(hash chainhash.Hash) (int32, bool) {
	if c == nil {
		return 0, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if node := c.heldLocked(hash); node != nil {
		return node.height, true
	}

	return 0, false
}

// Proven reports whether this hash is a held header that a matched checkpoint
// commits: a checkpoint node or one below it on the same branch. That is the
// ancestry proof behind every below-checkpoint fast path.
//
// A false negative is possible and is deliberately tolerated: a branch dropped
// while a block is in flight takes the proof with it, and the answer then
// reverts to full validation, which costs time and nothing else.
func (c *headerCache) Proven(hash chainhash.Hash) bool {
	if c == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	node := c.heldLocked(hash)

	// height > 0 excludes genesis for the same reason model.BelowCheckpoint does:
	// a coinbase-only block has nothing to fast-path.
	return node != nil && node.height > 0 && node.proven
}

// ProvenTo returns the highest height above the committed tip that the active
// branch is committed to by a matched checkpoint hash, or 0 when none is.
func (c *headerCache) ProvenTo() int32 {
	if c == nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.active == nil {
		return 0
	}

	return c.effectiveProofLocked(c.active)
}

// Top returns the highest height the active branch names. A pass that has
// reached it has run off the end and needs another getheaders.
func (c *headerCache) Top() (int32, bool) {
	if c == nil {
		return 0, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.active == nil || len(c.activeNodes) == 0 {
		return 0, false
	}

	return c.active.tip.height, true
}

// PeerTop returns owner's own branch tip, and whether owner has one above the
// committed tip. A walk continued from it asks owner for what follows its own
// best known header, as SV Node's ProcessHeadersMessage asks for more from
// pindexLast (net_processing.cpp:3452-3464), never for what follows another
// peer's.
func (c *headerCache) PeerTop(owner any) (int32, chainhash.Hash, bool) {
	if c == nil {
		return 0, chainhash.Hash{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	branch := c.branches[owner]
	if branch == nil || !c.liveLocked(branch) {
		return 0, chainhash.Hash{}, false
	}

	return branch.tip.height, branch.tip.hash, true
}

// Len returns how many heights the active branch names above the committed tip.
func (c *headerCache) Len() int {
	if c == nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.activeNodes)
}

// heldHeaders returns how many headers the tree holds over all branches, for
// the memory bound's test.
func (c *headerCache) heldHeaders() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.index)
}

// Discard empties the cache. Nothing is lost that one message cannot replace.
func (c *headerCache) Discard() {
	if c == nil {
		return
	}

	c.fillMu.Lock()
	defer c.fillMu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.index = make(map[chainhash.Hash]*headerNode)
	c.branches = make(map[any]*headerBranch)
	c.active, c.activeNodes = nil, nil
	c.released = nil
}

// DropPeer drops owner's branch: SV Node forgets a peer's best known header
// when the peer goes (FinalizeNode), and here that is also what releases the
// headers only that peer sent.
//
// It takes mu alone, never waiting on fillMu: the branch is unlinked at once,
// so no reader sees it again, and its headers are freed now if no fill is
// running, or by the running fill when it finishes.
func (c *headerCache) DropPeer(owner any) {
	if c == nil {
		return
	}

	c.mu.Lock()
	c.dropBranchLocked(owner)
	c.selectLocked()
	c.mu.Unlock()

	if !c.fillMu.TryLock() {
		return
	}

	defer c.fillMu.Unlock()

	c.reap()
}

// dropBranchLocked unlinks owner's branch and queues its tip for reapLocked.
// Called with mu held.
func (c *headerCache) dropBranchLocked(owner any) {
	branch := c.branches[owner]
	if branch == nil {
		return
	}

	delete(c.branches, owner)

	if c.active == branch {
		c.active, c.activeNodes = nil, nil
	}

	c.released = append(c.released, branch.tip)
}

// reap takes mu and frees the dropped branches' headers. Called with fillMu held.
func (c *headerCache) reap() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.reapLocked()
}

// reapLocked frees what only the dropped branches held. Called with fillMu and
// mu held, because it removes headers from the index a fill reads without mu.
func (c *headerCache) reapLocked() {
	for _, tip := range c.released {
		tip.refs--
		c.releaseLocked(tip)
	}

	c.released = nil
}

// Prune is PruneTo without the committed tip's hash: it drops every height at
// or below height, and cannot check that a branch still connects there.
func (c *headerCache) Prune(height int32) {
	c.pruneTo(height, chainhash.Hash{}, false)
}

// PruneTo records the committed tip, height and hash, as assignWantedBlocks
// read it. Nothing at or below height is named from then on, a branch that no
// longer reaches above it or no longer connects to it stops being a candidate,
// and the active branch is chosen again. This is the authoritative report of
// the tip, so unlike a fill's it may move the floor down, which a reorg to a
// lower tip does.
//
// The headers below the tip are removed from memory in batches, by sweepLocked,
// when no fill is running; until then they are only unnamed.
func (c *headerCache) PruneTo(height int32, hash chainhash.Hash) {
	c.pruneTo(height, hash, true)
}

func (c *headerCache) pruneTo(height int32, hash chainhash.Hash, hashKnown bool) {
	if c == nil {
		return
	}

	c.mu.Lock()
	c.setFloorLocked(height, hash, hashKnown)
	due := height-c.sweptTo >= int32(wire.MaxBlockHeadersPerMsg)
	c.mu.Unlock()

	if !due || !c.fillMu.TryLock() {
		return
	}

	defer c.fillMu.Unlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	c.sweepLocked()
}

// sweepLocked removes from memory what the floor has left behind: every branch
// whose tip is at or below the committed tip, every branch that stopped
// connecting to it below its own root, and every node more than one reply below
// it. The margin keeps a tip reported a little lower (a short reorg) from
// finding its branches already gone. Called with fillMu and mu held; the cost is
// one pass over the index per MaxBlockHeadersPerMsg committed blocks.
func (c *headerCache) sweepLocked() {
	for owner, branch := range c.branches {
		if branch.tip.height <= c.floorHeight || !c.anchoredLocked(branch) && c.rootedAtOrBelowFloorLocked(branch) {
			c.dropBranchLocked(owner)
		}
	}

	c.reapLocked()

	cut := c.floorHeight - int32(wire.MaxBlockHeadersPerMsg)

	for hash, node := range c.index {
		switch {
		case node.height <= cut:
			delete(c.index, hash)
		case node.parent != nil && node.parent.height <= cut:
			node.parent = nil
		}
	}

	c.sweptTo = c.floorHeight
	c.selectLocked()
}

// rootedAtOrBelowFloorLocked reports whether branch's path reaches down to one
// above the committed tip, so a failed anchoredLocked means it forked from the
// committed chain rather than being rooted on a tip reported higher than now.
func (c *headerCache) rootedAtOrBelowFloorLocked(branch *headerBranch) bool {
	n := branch.tip
	for n.height > c.floorHeight+1 && n.parent != nil {
		n = n.parent
	}

	return n.height <= c.floorHeight+1
}
