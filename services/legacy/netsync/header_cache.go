package netsync

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
)

// headerCache holds the block hashes for a contiguous run of heights above the
// best block processed, taken from one getheaders reply.
//
// It is a cache and not a work queue, and the difference is the whole point. It
// can be thrown away at any instant with no consequence, because everything in
// it can be asked for again in one message. The structure it replaces, a linked
// list of every header ever received, could not be thrown away, so every bound
// on it was a decision to discard something, and every discard needed a
// mechanism somewhere else to put it back. On Hetzner mainnet that list reached
// 933,062 entries with a stale flag on its front, and only losing the memory it
// lived in ever cleared it.
//
// A fill replaces the contents outright. Merging would let a run from a chain
// this node has left name a height on the chain it is on.
type headerCache struct {
	mu       sync.Mutex
	byHeight map[int32]chainhash.Hash
	// byHash is the reverse of byHeight, maintained alongside it. It exists for
	// pipelineParentHeight, which is handed a block's parent hash — off the
	// wire, before that parent is committed — and needs the height that hash
	// names, not the other way round. The old header list answered this from
	// its own hash-keyed index; a cache keyed only by height could not, so this
	// is kept in step with byHeight at every Fill and Discard.
	byHash map[chainhash.Hash]int32
	top    int32
	filled bool

	// checkpoints is the chain's pinned (height, hash) list, held so that Fill
	// can decide, under the same lock that installs the contents, how much of
	// the run just accepted is committed by a checkpoint hash this node has
	// actually matched. nil means no proof is ever granted, which is the safe
	// direction: see provenTo.
	checkpoints []chaincfg.Checkpoint

	// provenTo is the highest height in the CURRENT contents that is committed
	// by a pinned checkpoint hash matched somewhere in this run.
	//
	// This is the replacement for upstream's SyncManager.verifiedCheckpointHeight
	// (PR 1390 / the fork security merge), which lived beside the header list this
	// branch deleted. The property it encodes is unchanged and is the reason the
	// below-checkpoint fast paths are allowed to exist at all: the hardcoded
	// checkpoints certify ONE CHAIN, not a height range, so a block's height says
	// nothing about whether it belongs to that chain. What certifies a block at
	// height h is a verified header chain running from h FORWARD to a pinned
	// checkpoint hash, because linkage only commits backwards.
	//
	// Fill has already proved that this run is one internally linked chain, so a
	// single hash comparison at a checkpoint height commits every height at or
	// below it in the same run. Nothing above it is committed by anything until a
	// LATER checkpoint is matched too — exactly the hole upstream's
	// headerNodeProven was written to close.
	//
	// Whether a fill may carry the previous value forward depends on whether it
	// replaced the contents or extended them, and this is the one place the two
	// are allowed to differ:
	//
	//   - A replace (replaceLocked) roots the run at whatever the committed tip is
	//     now, and a match in the PREVIOUS run says nothing about the heights in
	//     this one — two different runs can both link to the same tip and diverge
	//     above it. Carrying the old number forward here would be the
	//     forgeable-proof bug rather than a fix for it, so a replace always takes
	//     checkpointProof's answer for the new run alone.
	//   - An extend (extendLocked) appends onto this cache's own previous top,
	//     which Fill has already proved links backward, header by header, to the
	//     very content the earlier proof was computed over. A match already held
	//     is a match on a PREFIX of the same chain the appended suffix continues,
	//     not on some other run that merely happens to share a tip, so carrying it
	//     forward and keeping the higher of the two watermarks is sound. This is
	//     reachable only below the last checkpoint (see belowLastCheckpointLocked):
	//     above it the chain can still reorg, and every fill there replaces, for
	//     exactly the reason above.
	//
	// Zero means no proof, and every caller reads it as "deny the fast path".
	provenTo int32
}

func newHeaderCache() *headerCache {
	return &headerCache{
		byHeight: make(map[int32]chainhash.Hash),
		byHash:   make(map[chainhash.Hash]int32),
	}
}

// WithCheckpoints hands the cache the chain's pinned checkpoints and returns it,
// so a caller can write newHeaderCache().WithCheckpoints(params.Checkpoints) in
// one expression.
//
// A separate step rather than a constructor argument because the overwhelming
// majority of callers — every test that only cares which heights the cache names
// — has no checkpoint list to give and must not be made to invent one. Those
// callers get provenTo == 0 for ever, so the fast path is denied, which is the
// answer a harness with no checkpoints should get.
func (c *headerCache) WithCheckpoints(checkpoints []chaincfg.Checkpoint) *headerCache {
	if c == nil {
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.checkpoints = checkpoints

	return c
}

// Fill installs headers as either an extension of this cache's own previous
// contents or a fresh replacement of them, and reports whether anything was
// accepted. Which one happens is decided here, not by the caller: parent is
// always this node's actual committed tip and baseHeight is always tip+1,
// exactly as this always worked, but below the last checkpoint a later call
// may extend past what parent alone would justify — see
// belowLastCheckpointLocked and provenTo's own doc for why that is safe only
// in that one direction.
//
//   - Replace (replaceLocked): the ordinary case, and the ONLY case above the
//     last checkpoint, where the chain can still reorg. The batch is searched
//     for where it meets parent.
//   - Extend (extendLocked): below the last checkpoint, once a previous fill
//     has already left this cache naming heights above the tip. The batch is
//     searched for where it meets this cache's OWN top instead, and a match is
//     appended rather than replacing anything. A batch that reaches a
//     checkpoint height with the wrong hash in this mode drops the WHOLE list,
//     not merely this batch, because every entry already held was built as one
//     linked chain with the batch that just failed to agree — see
//     extendLocked's own doc.
//
// Both paths share one linkage walk, run once here: every header must name the
// one before it, or the whole batch is refused before either path is tried,
// because a break anywhere makes every height after it a guess, and a height
// is exactly what this structure exists to provide.
//
// Refusing changes nothing. The caller asks again, of the same peer or
// another.
func (c *headerCache) Fill(parent chainhash.Hash, baseHeight int32, headers []*wire.BlockHeader) bool {
	if c == nil || len(headers) == 0 {
		return false
	}

	// Walk the whole batch before touching anything, so a refusal leaves the
	// previous contents intact rather than half-replaced.
	hashes := make([]chainhash.Hash, 0, len(headers))

	var prev chainhash.Hash

	for i, header := range headers {
		// i == 0 has nothing before it to link to.
		if i > 0 && !header.PrevBlock.IsEqual(&prev) {
			return false
		}

		hash := header.BlockHash()
		hashes = append(hashes, hash)
		prev = hash
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.filled && c.top >= baseHeight && c.belowLastCheckpointLocked(baseHeight-1) {
		return c.extendLocked(headers, hashes)
	}

	return c.replaceLocked(parent, baseHeight, headers, hashes)
}

// findAnchor returns the index of the first header in headers usable once the
// batch is judged against anchor: either headers[0] already builds on anchor,
// so the whole batch is usable from index 0, or some headers[i] hashes to
// anchor, so headers[i+1] onward is usable. It returns -1 when neither shape
// holds — this batch does not meet anchor anywhere, describing a chain this
// node is not on, or one it has already run past.
//
// hashes is headers' own hashes, already computed and verified internally
// linked by the caller (Fill), so this need not touch header bytes again.
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

// replaceLocked installs headers as a fresh run rooted at parent, discarding
// whatever the cache held before. Called with c.mu held.
//
// parent is the node's committed tip, and the batch is a linked run answering a
// getheaders that was asked from that tip as it stood when the question went
// out. Those are not the same instant. On Hetzner mainnet the node commits
// around 18 blocks a second, so by the time a reply crosses the network the tip
// has moved perhaps 20 blocks into the batch. Requiring headers[0] to name the
// current tip as its parent therefore threw away the entire 2,000-header reply
// because its first 20 entries had gone behind us, and the cache could only be
// refilled once the node had already stopped committing — which is exactly the
// dead air between refills that this rule removes. Eleven getheaders went out
// on 2026-09-14 between 13:11:36 and 13:12:26, across eight peers, and not one
// of the replies was kept; the twelfth landed within a second of its own
// request, once commits had stopped.
//
// So the batch is searched for where it meets the tip rather than being judged
// on its front. Two shapes, and they are the same case:
//
//   - headers[0] names parent as its parent, so the whole batch is usable and
//     headers[0] sits at baseHeight. This is the cold-start shape, and the only
//     shape the rule this replaces would accept.
//   - some headers[i] hashes to parent, so headers[i+1] onward are usable and
//     headers[i+1] sits at baseHeight. The prefix up to and including i is
//     behind the committed tip and is dropped.
//
// This is not a weaker check than judging the front. The batch is verified to
// be one internally linked run before anything is kept, and parent is a
// specific 32-byte value: a header whose PrevBlock is that value builds on this
// node's tip, wherever in the run it happens to fall.
//
// It refuses a batch that does not link to parent. If parent appears nowhere in
// the run and is not headers[0]'s parent, the batch describes a chain this node
// is not on, or one it has run clean past. If parent is the run's own last
// header, the usable suffix is empty and there is nothing to cache.
func (c *headerCache) replaceLocked(parent chainhash.Hash, baseHeight int32, headers []*wire.BlockHeader, hashes []chainhash.Hash) bool {
	start := findAnchor(headers, hashes, parent)

	// start == len(hashes) is the tip being the batch's own last header: the run
	// is honest and connects, there is simply nothing above the tip in it. A
	// clean refusal, not an empty success — writing an empty map here would
	// throw away a perfectly good previous batch in exchange for nothing.
	if start < 0 || start >= len(hashes) {
		return false
	}

	kept := hashes[start:]

	// Checkpoint agreement is judged BEFORE anything is installed, so a run that
	// contradicts a pinned hash leaves the previous contents untouched rather than
	// replacing them with a batch this node has just proved is not its chain. The
	// caller treats a false here as "drop the batch"; for this particular reason it
	// also disconnects the sender (see fillHeaderCache), because a peer whose run
	// reaches a checkpoint height with the wrong hash is not answering about the
	// chain we asked about.
	proven, agrees := checkpointProof(c.checkpoints, baseHeight, kept)
	if !agrees {
		return false
	}

	c.byHeight = make(map[int32]chainhash.Hash, len(kept))
	c.byHash = make(map[chainhash.Hash]int32, len(kept))

	for i, hash := range kept {
		height := baseHeight + int32(i) //nolint:gosec // a batch index, bounded by the wire limit
		c.byHeight[height] = hash
		c.byHash[hash] = height
	}

	c.top = baseHeight + int32(len(kept)) - 1 //nolint:gosec // as above
	c.filled = true
	c.provenTo = proven

	return true
}

// extendLocked appends headers onto this cache's own top rather than replacing
// the cache, called with c.mu held once Fill has decided this run is below the
// last checkpoint and this cache already names heights above the tip. See
// Fill's own doc for the dispatch and provenTo's for why proof may be carried
// forward here and only here.
//
// The anchor is this cache's own top hash, not parent: below the last
// checkpoint every subsequent request this package sends is built from the
// list's own top (see manager.go's extendingHeadersLocator), so an honest reply
// connects there, not necessarily at the committed tip a block or two behind
// it. Searched with the same two-shape rule replaceLocked uses against parent,
// just aimed at a different hash.
//
// A batch that does not meet the top anywhere is refused and the list is left
// exactly as it stands — an honest answer about a point the walk has already
// moved past. It is deliberately never re-tried against parent instead: below
// the last checkpoint a reply that skips the list's own top would, if accepted,
// either duplicate what is already held or silently discard the run already
// proven, and the design settles that a below-checkpoint fill only ever grows
// this way.
func (c *headerCache) extendLocked(headers []*wire.BlockHeader, hashes []chainhash.Hash) bool {
	topHash := c.byHeight[c.top]

	start := findAnchor(headers, hashes, topHash)
	if start < 0 || start >= len(hashes) {
		return false
	}

	kept := hashes[start:]
	newBase := c.top + 1

	// Judged before anything is appended, and unlike replaceLocked's refusal, a
	// contradiction here does not leave the previous contents alone: every
	// entry already held was built as one linked chain with the batch that just
	// failed to agree with a pinned hash, so the checkpoint that would have
	// certified them has instead shown the whole run is wrong.
	proven, agrees := checkpointProof(c.checkpoints, newBase, kept)
	if !agrees {
		c.resetLocked()
		return false
	}

	for i, hash := range kept {
		height := newBase + int32(i) //nolint:gosec // a batch index, bounded by the wire limit
		c.byHeight[height] = hash
		c.byHash[hash] = height
	}

	c.top = newBase + int32(len(kept)) - 1 //nolint:gosec // as above

	// The higher of the two watermarks: a checkpoint matched in an earlier fill
	// is still matched, and the new suffix may have reached a further one.
	if proven > c.provenTo {
		c.provenTo = proven
	}

	return true
}

// checkpointProof walks a verified, internally linked run of hashes starting at
// baseHeight and answers two things: how far up the run is committed by a pinned
// checkpoint hash, and whether the run contradicts one.
//
// It returns (0, true) for a run that simply contains no checkpoint height — the
// common case on mainnet, where the checkpoints are tens of thousands of blocks
// apart and one getheaders reply covers two thousand. That is "no proof", not
// "bad run", and the caller keeps the batch and denies the fast path for it.
//
// It returns (0, false) only when the run reaches a checkpoint height and the
// hash there is NOT the pinned one. That is a lie about the certified chain, and
// the whole batch goes.
//
// The highest matching checkpoint wins, because linkage commits backwards: a
// match at height H commits every height in the run at or below H whether or not
// a lower checkpoint also appears.
func checkpointProof(checkpoints []chaincfg.Checkpoint, baseHeight int32, hashes []chainhash.Hash) (int32, bool) {
	if len(checkpoints) == 0 || len(hashes) == 0 {
		return 0, true
	}

	top := baseHeight + int32(len(hashes)) - 1 //nolint:gosec // a batch index, bounded by the wire limit

	var proven int32

	for i := range checkpoints {
		cp := checkpoints[i]
		if cp.Hash == nil || cp.Height < baseHeight || cp.Height > top {
			continue
		}

		if !hashes[cp.Height-baseHeight].IsEqual(cp.Hash) {
			return 0, false
		}

		if cp.Height > proven {
			proven = cp.Height
		}
	}

	return proven, true
}

// nextCheckpointAbove returns the lowest checkpoint in checkpoints whose
// height is greater than height, or nil when there is none — either because
// checkpoints is empty or height is already at or past the last one.
//
// Mirrors SyncManager.findNextHeaderCheckpoint exactly (the same >= cutoff
// against the final checkpoint, the same walk-backward-from-the-end search),
// because the two must agree on when a below-checkpoint walk is still owed a
// request. This package has no reference back to a SyncManager to call that
// method directly, so the rule is duplicated here as a pure function rather
// than guessed at independently.
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
// final checkpoint, i.e. whether a below-checkpoint walk is still meaningful
// at all. Called with c.mu held; see Fill's dispatch and provenTo's doc for
// what this decides.
func (c *headerCache) belowLastCheckpointLocked(height int32) bool {
	return nextCheckpointAbove(c.checkpoints, height) != nil
}

// NextCheckpointAbove returns the lowest pinned checkpoint above height — the
// committed tip, ordinarily — and whether one exists. maybeRequestMoreHeaders
// reads this to decide whether a below-checkpoint walk still owes a request
// regardless of how much runway the cache already holds for downloads, which
// headerCacheRefillThreshold answers a different question about.
//
// Returned by value rather than pointer: c.checkpoints is only ever replaced
// wholesale (WithCheckpoints, or a test rebuilding the cache), never mutated
// element-wise, but handing back a value keeps that guarantee local to this
// file rather than resting on every caller never keeping a pointer past c.mu.
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

// At returns the hash this cache names for height, and whether it names one.
func (c *headerCache) At(height int32) (chainhash.Hash, bool) {
	if c == nil {
		return chainhash.Hash{}, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	hash, ok := c.byHeight[height]

	return hash, ok
}

// HeightOf is At's reverse: the height this cache names for hash, and whether
// it names one. Used by pipelineParentHeight to resolve a not-yet-committed
// parent's height from its hash alone, which is all a block streaming off the
// wire ever hands over.
func (c *headerCache) HeightOf(hash chainhash.Hash) (int32, bool) {
	if c == nil {
		return 0, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	height, ok := c.byHash[hash]

	return height, ok
}

// Proven reports whether this hash is one the cache names inside the prefix a
// pinned checkpoint hash has actually committed — the ancestry proof behind every
// below-checkpoint fast path. See provenTo for why linkage to the committed tip is
// not that proof on its own.
//
// A false negative is possible and is deliberately tolerated: a fill that lands
// while a block is in flight replaces the contents, and if the fresh run does not
// reach a checkpoint the proof for a block still on the wire is gone. The answer
// then reverts to full validation, which costs time and nothing else. The
// alternative — caching the answer at request time so it survives a refill — is
// what upstream tried first with an expiring map, and it is only safe in the
// direction that loses a proof, never in the direction that keeps one.
func (c *headerCache) Proven(hash chainhash.Hash) bool {
	if c == nil {
		return false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.provenTo <= 0 {
		return false
	}

	height, ok := c.byHash[hash]

	// height > 0 excludes genesis for the same reason model.BelowCheckpoint does:
	// a coinbase-only block has nothing to fast-path, and every gate treats it as
	// not fast-pathable.
	return ok && height > 0 && height <= c.provenTo
}

// ProvenTo returns the highest height the current contents are committed to by a
// matched checkpoint hash, or 0 when nothing in them is.
func (c *headerCache) ProvenTo() int32 {
	if c == nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return c.provenTo
}

// Top returns the highest height the cache names. A pass that has reached it has
// run off the end and needs another getheaders.
func (c *headerCache) Top() (int32, bool) {
	if c == nil {
		return 0, false
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.filled || len(c.byHeight) == 0 {
		return 0, false
	}

	return c.top, true
}

// Len returns how many heights are named.
func (c *headerCache) Len() int {
	if c == nil {
		return 0
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.byHeight)
}

// Discard empties the cache. Nothing is lost that one message cannot replace.
func (c *headerCache) Discard() {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	c.resetLocked()
}

// resetLocked is Discard's body, factored out so extendLocked can drop the
// whole list under the lock it is already holding, on the same terms: see
// extendLocked's own doc for why a checkpoint contradiction reached by
// extending taints everything already held, not merely the batch that just
// failed.
func (c *headerCache) resetLocked() {
	c.byHeight = make(map[int32]chainhash.Hash)
	c.byHash = make(map[chainhash.Hash]int32)
	c.top = 0
	c.filled = false
	// The proof belongs to the contents, so it goes with them. Leaving it behind
	// would have an empty cache claim a proven prefix it can no longer name.
	c.provenTo = 0
}

// Prune drops every entry at or below height, the committed tip as of the
// caller's read. Below the last checkpoint a fill only ever appends
// (extendLocked), so nothing else shrinks the list as the tip advances past
// what it already names; without this it would grow for the whole gap between
// checkpoints instead of staying bounded to roughly one checkpoint interval's
// worth of headers plus whatever the tip has not yet caught up to.
//
// Pruning height itself, not merely below it, is safe because a block's parent
// hash exactly at the committed tip still resolves once it is gone from here:
// pipelineParentHeight falls back to sm.blockchainClient.GetBlockHeader for a
// hash this cache no longer names, and the tip is by definition committed, so
// that call always answers for it.
//
// Above the last checkpoint this is a no-op in practice, because Fill there
// already replaces the whole map from the tip on every reply; it is still safe
// to call unconditionally rather than asking every caller to know which regime
// it is in.
func (c *headerCache) Prune(height int32) {
	if c == nil {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if !c.filled {
		return
	}

	// Walk up from the lowest height held rather than ranging over the map.
	// This runs on every assignment pass, which is once per committed block on
	// the serial commit path, and below a checkpoint the list holds up to a
	// whole checkpoint interval, about 43,000 heights at the widest mainnet
	// gap. Ranging over all of that on every pass, under the cache lock that
	// block delivery reads Proven through, is exactly the per-block bookkeeping
	// already crowding that path. The list is always one contiguous run of
	// heights: replaceLocked writes baseHeight up to top, extendLocked appends
	// from top+1, and this only ever removes a prefix. So the lowest height held
	// is top minus the count plus one, and the work is proportional to what
	// actually fell below the tip, usually a single height.
	low := c.top - int32(len(c.byHeight)) + 1 //nolint:gosec // bounded by one checkpoint interval

	for h := low; h <= height && h <= c.top; h++ {
		if hash, ok := c.byHeight[h]; ok {
			delete(c.byHeight, h)
			delete(c.byHash, hash)
		}
	}

	if len(c.byHeight) == 0 {
		// Nothing left above height: the same state Fill leaves an empty cache
		// in, so top and filled must not go on describing a run that is gone —
		// left stale, extendLocked's next call would look up c.byHeight[c.top],
		// find nothing there any more, and search the batch for a zero hash.
		// provenTo is deliberately left alone: pruning is bookkeeping about what
		// the cache still NAMES, not a verdict on the chain it named, and the
		// next Fill governs provenTo on its own terms either way.
		c.top = 0
		c.filled = false
	}
}
