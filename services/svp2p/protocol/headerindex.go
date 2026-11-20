package protocol

import (
	"math/big"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockchain/work"
)

// node mirrors block_index.h CBlockIndex: pprev links to the parent, pskip
// is the skiplist shortcut set once at insert time, nHeight is the height
// above genesis, and nChainWork is the cumulative proof of work of the whole
// branch ending here. Teranode's blockchain service stays the authoritative
// header store (spec §11); this tree is a rebuildable, in-memory cache scoped
// to protocol decisions (locators, best-known-block, download scheduling)
// that must not sit on a per-message gRPC call.
type node struct {
	hash   chainhash.Hash
	header wire.BlockHeader
	prev   *node // CBlockIndex::pprev
	skip   *node // CBlockIndex::pskip
	height int32 // CBlockIndex::nHeight

	// chainWork is CBlockIndex::nChainWork. It is set once, under idx.mu,
	// before the node is published into idx.nodes, and is never written
	// again — which is what lets exportNode hand the same *big.Int out to
	// every caller instead of copying it. See the note on HeaderNode.
	chainWork *big.Int
}

// blockProof is block_index.cpp GetBlockProof (block_index.cpp:114-125): the
// work one header contributes, 2**256 / (target + 1), computed there as
// (~bnTarget / (bnTarget + 1)) + 1 because arith_uint256 cannot hold 2**256.
// math/big has no such limit, so the quotient is taken directly; the two
// forms are the same value, and TestBlockProof pins it against hand-derived
// vectors.
//
// work.CalcBlockWork is Teranode's existing port of that function and stays
// the single implementation of the arithmetic. It answers zero for the two
// targets arith_uint256::SetCompact rejects — negative-encoded (fNegative)
// and past 2**256 (fOverflow) — matching GetBlockProof's own zero return for
// them.
func blockProof(bits uint32) *big.Int {
	return work.CalcBlockWork(bits)
}

// invertLowestOne mirrors block_index.cpp InvertLowestOne (block_index.cpp:18-20):
// turn the lowest '1' bit in the binary representation of n into a '0'.
func invertLowestOne(n int32) int32 {
	return n & (n - 1)
}

// getSkipHeight mirrors block_index.cpp GetSkipHeight (block_index.cpp:22-33):
// compute what height to jump back to with the node's skip pointer
// (CBlockIndex::pskip). Any number strictly lower than height is acceptable,
// but this expression performs well in simulations (max 110 steps to go back
// up to 2**18 blocks).
func getSkipHeight(height int32) int32 {
	if height < 2 {
		return 0
	}

	if height&1 != 0 {
		return invertLowestOne(invertLowestOne(height-1)) + 1
	}

	return invertLowestOne(height)
}

// HeaderNode is the exported, immutable snapshot of a node returned by
// Lookup and Ancestor. It carries the CBlockIndex fields callers in other
// packages need: the hash, its height, its cumulative chain work, and its
// parent's hash for walking the tree one step at a time via another Lookup
// call. ParentHash is the zero chainhash.Hash at genesis, which has no
// parent.
//
// ChainWork is CBlockIndex::nChainWork, and it is a pointer INTO the index:
// every snapshot of the same node shares one *big.Int with the tree, so a
// caller must treat it as read-only. Mutating it would corrupt tip selection
// for every other reader. Sharing rather than copying is what keeps Lookup
// allocation-free on the download walk, which calls it once per block of the
// window; the value is safe to share because a node's chainWork is written
// once at insert and never again.
//
// ChainWork is nil on a HeaderNode that did not come out of the index (a
// zero value, or one a caller built). Read it through chainWorkOf, which
// answers zero work for that case, the same state a C++ CBlockIndex is in
// before SetChainWork runs.
//
// Time and Bits are CBlockIndex::GetBlockTime() and CBlockIndex::GetBits():
// the two header fields the contextual difficulty check reads off an
// ancestor. They come from the wire.BlockHeader the index has stored since
// Phase 2 Task 2, and Phase 3 Task 2 is what first reads it.
type HeaderNode struct {
	Hash       chainhash.Hash
	ParentHash chainhash.Hash
	Height     int32
	ChainWork  *big.Int

	// Time is CBlockHeader::nTime in Unix seconds.
	Time int64

	// Bits is CBlockHeader::nBits, the compact target the header claims.
	Bits uint32
}

func exportNode(n *node) HeaderNode {
	var parentHash chainhash.Hash
	if n.prev != nil {
		parentHash = n.prev.hash
	}

	return HeaderNode{
		Hash:       n.hash,
		ParentHash: parentHash,
		Height:     n.height,
		ChainWork:  n.chainWork,
		Time:       n.header.Timestamp.Unix(),
		Bits:       n.header.Bits,
	}
}

// chainWorkOf reads a HeaderNode's cumulative work, answering zero for a
// snapshot that never came from the index. It is the only way the comparison
// sites below read ChainWork, so a caller-built HeaderNode compares as the
// zero-work node it is instead of panicking on a nil *big.Int.
func chainWorkOf(n HeaderNode) *big.Int {
	if n.ChainWork == nil {
		return new(big.Int)
	}

	return n.ChainWork
}

// HeaderIndex is the net_processing.cpp mapBlockIndex counterpart: a small
// header tree hydrated from the blockchain client at startup and kept
// current from its subscription. It is safe for concurrent use under one
// mutex and holds no reference to any Teranode client.
//
// The tree is genesis-rooted: NewHeaderIndex seeds a single height-0 node
// with no parent, and AddHeader only ever attaches a new node to a parent
// already in the tree, so every node's prev chain terminates at that
// height-0 node. Locator and Ancestor walk that chain and assume the
// height-0 terminus holds; there is no API to seed a partial, non-genesis
// tree.
type HeaderIndex struct {
	mu    sync.Mutex
	nodes map[chainhash.Hash]*node
	tip   *node
}

// NewHeaderIndex seeds the tree with the chain's genesis header at height 0.
// Callers hydrate the rest by feeding headers fetched from the blockchain
// client into AddHeader, in order, before serving any peer.
func NewHeaderIndex(genesis *wire.BlockHeader) (*HeaderIndex, error) {
	if genesis == nil {
		return nil, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: genesis header is nil")
	}

	root := &node{
		hash:   genesis.BlockHash(),
		header: *genesis,
		prev:   nil,
		height: 0,
		// block_index.h SetChainWork: nChainWork = (pprev ? pprev->nChainWork
		// : 0) + GetBlockProof(*this). Genesis has no pprev, so it carries
		// only its own proof.
		chainWork: blockProof(genesis.Bits),
	}

	return &HeaderIndex{
		nodes: map[chainhash.Hash]*node{root.hash: root},
		tip:   root,
	}, nil
}

// AddHeader links header onto its parent, mirroring only the mapBlockIndex
// insert portion of net_processing.cpp AcceptBlockHeader. It performs no
// header validation: CheckBlockHeader's proof-of-work check and any
// contextual checks are deliberately the caller's responsibility (the
// headers-first sync layer), not this passive index's. connected is false
// when the parent is not yet known (an orphan header the caller must fill
// in with more getheaders before this one can attach); the tip is left
// unmutated in that case. A header already present returns
// connected == true without mutating anything, matching a mapBlockIndex
// lookup that finds the existing entry.
func (idx *HeaderIndex) AddHeader(header *wire.BlockHeader) (connected bool, err error) {
	if header == nil {
		return false, errors.New(errors.ERR_INVALID_ARGUMENT, "svp2p: header is nil")
	}

	hash := header.BlockHash()

	idx.mu.Lock()
	defer idx.mu.Unlock()

	if _, ok := idx.nodes[hash]; ok {
		return true, nil
	}

	parent, ok := idx.nodes[header.PrevBlock]
	if !ok {
		return false, nil
	}

	n := &node{
		hash:   hash,
		header: *header,
		prev:   parent,
		height: parent.height + 1,
		// block_index.h SetChainWork: nChainWork = (pprev ? pprev->nChainWork
		// : 0) + GetBlockProof(*this).
		chainWork: new(big.Int).Add(parent.chainWork, blockProof(header.Bits)),
	}
	n.skip = buildSkipLocked(n)

	idx.nodes[hash] = n

	// block_index_store.h SetBestHeader, whose ordering is
	// CBlockIndexWorkComparator (block_index.h:1225-1260): "First sort by
	// most total work", then by validation-completion time and sequence id —
	// tails this index has no counterpart for, and which both leave the
	// first-seen node ahead of a later arrival. A strictly-greater test is
	// that whole ordering here: more work takes the tip, an equal-work
	// branch does not displace the branch already on it.
	//
	// This replaces the Phase 2 height rule (spec §6, header index), which
	// left Tip() on the taller branch after a shorter, heavier one arrived.
	// Upgraded by Phase 3 Task 1.
	//
	// SetBestHeader also gates on bestHeaderCandidate.IsValid(TREE), which
	// this index has no status field for. The counterpart is upstream:
	// HeaderSync.acceptHeader (headersync.go) runs CheckBlockHeader's
	// proof-of-work check, the checkpoint fence and the contextual difficulty
	// rule before OnHeaders calls AddHeader, and headers from the blockchain
	// subscription come from Teranode's own validated store. The
	// proof-of-work half of that gate is also what keeps a node's work
	// non-zero — see the longer note on the availability compares in
	// syncstate.go.
	if n.chainWork.Cmp(idx.tip.chainWork) > 0 {
		idx.tip = n
	}

	return true, nil
}

// Tip returns the current best-known header.
func (idx *HeaderIndex) Tip() (hash chainhash.Hash, height int32) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return idx.tip.hash, idx.tip.height
}

// Lookup returns the node for hash, mirroring a mapBlockIndex find.
func (idx *HeaderIndex) Lookup(hash chainhash.Hash) (HeaderNode, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	n, ok := idx.nodes[hash]
	if !ok {
		return HeaderNode{}, false
	}

	return exportNode(n), true
}

// Ancestor returns the predecessor of hash at height, mirroring
// CBlockIndex::GetAncestor (block_index.cpp:81-105) via the skiplist descent
// in ancestorLocked.
func (idx *HeaderIndex) Ancestor(hash chainhash.Hash, height int32) (HeaderNode, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	n, ok := idx.nodes[hash]
	if !ok {
		return HeaderNode{}, false
	}

	anc, ok := ancestorLocked(n, height)
	if !ok {
		return HeaderNode{}, false
	}

	return exportNode(anc), true
}

// ancestorLocked mirrors block_index.cpp CBlockIndex::GetAncestor
// (block_index.cpp:81-105): descend via the skip pointer whenever it lands
// at-or-after the target height and isn't worse than falling back to pprev,
// otherwise step pprev once. This is the same decision order and heuristic
// condition as the source, in the same order. The nil guard on walk.prev
// replaces the source's `assert(pindexWalk->pprev)`: given the
// genesis-rooted invariant documented on HeaderIndex, every node's chain
// reaches the height-0 node before prev ever goes nil, so this should be
// unreachable. It exists so a violated invariant fails the call (ok ==
// false) instead of panicking on a nil dereference.
func ancestorLocked(n *node, height int32) (*node, bool) {
	if height < 0 || height > n.height {
		return nil, false
	}

	walk := n
	heightWalk := n.height

	for heightWalk > height {
		heightSkip := getSkipHeight(heightWalk)
		heightSkipPrev := getSkipHeight(heightWalk - 1)

		if walk.skip != nil &&
			(heightSkip == height ||
				(heightSkip > height &&
					!(heightSkipPrev < heightSkip-2 && heightSkipPrev >= height))) {
			// Only follow pskip if pprev->pskip isn't better than pskip->pprev.
			walk = walk.skip
			heightWalk = heightSkip
		} else {
			if walk.prev == nil {
				return nil, false
			}

			walk = walk.prev
			heightWalk--
		}
	}

	return walk, true
}

// buildSkipLocked mirrors block_index.cpp CBlockIndex::BuildSkipNL
// (block_index.cpp:107-112): pskip = pprev->GetAncestor(GetSkipHeight(nHeight)).
// Called once at insert time, under idx.mu, after n.prev and n.height are
// set and before n is published into idx.nodes; genesis has no prev, so it
// keeps pskip nil, matching the source's `if (pprev)` guard.
func buildSkipLocked(n *node) *node {
	if n.prev == nil {
		return nil
	}

	skip, ok := ancestorLocked(n.prev, getSkipHeight(n.height))
	if !ok {
		return nil
	}

	return skip
}

// Locator builds a block locator for the current tip.
func (idx *HeaderIndex) Locator() []chainhash.Hash {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	return locatorFromLocked(idx.tip)
}

// LocatorFrom builds a block locator for hash instead of the tip, used when
// answering a peer whose best-known-block is behind ours. It returns nil if
// hash is not in the index.
func (idx *HeaderIndex) LocatorFrom(hash chainhash.Hash) []chainhash.Hash {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	n, ok := idx.nodes[hash]
	if !ok {
		return nil
	}

	return locatorFromLocked(n)
}

// locatorFromLocked mirrors chain.cpp CChain::GetLocator: walk back from n,
// appending each visited hash. The back-step starts at 1 and doubles again
// every time more than 10 hashes have already been collected — the literal
// `if (vHave.size() > 10) nStep *= 2` check, re-evaluated after each append,
// so it keeps doubling as the walk continues. Genesis (height 0) is always
// the final entry appended, and ends the walk. Called with idx.mu held.
func locatorFromLocked(n *node) []chainhash.Hash {
	have := make([]chainhash.Hash, 0, 32)
	step := int32(1)

	for {
		have = append(have, n.hash)

		if n.height == 0 {
			break
		}

		nextHeight := n.height - step
		if nextHeight < 0 {
			nextHeight = 0
		}

		next, ok := ancestorLocked(n, nextHeight)
		if !ok {
			// Defensive: unreachable given the genesis-rooted invariant
			// documented on HeaderIndex (every chain terminates at height
			// 0). Stop rather than continue with a nil node.
			break
		}

		n = next

		if len(have) > 10 {
			step *= 2
		}
	}

	return have
}

// containsLocked mirrors chain.h CChain::Contains (chain.h:53-56): "Efficiently
// check whether a block is present in this chain." The C++ tests
// (*this)[pindex->nHeight] == pindex against the vector of the active chain;
// this index has no such vector, so the same question is asked of the branch
// ending at tip — the node at start's own height on that branch is start
// itself, or start sits on another branch. Called with idx.mu held.
func containsLocked(tip, n *node) bool {
	anc, ok := ancestorLocked(tip, n.height)

	return ok && anc == n
}

// ForkPoint mirrors validation.cpp FindForkInGlobalIndex (validation.cpp:202-217):
// "Find the first block the caller has in the main chain." It walks the
// locator in the order the peer sent it (newest first, by the locator
// convention) and answers the first hash we hold that sits on the branch
// ending at tip. A hash we hold that instead DESCENDS from tip answers tip
// itself — the peer is ahead of us on our own chain, so our tip is the fork
// point. A locator naming nothing we hold answers genesis, which is what
// makes an unknown locator restart the peer from the bottom of the chain.
//
// tip is our own active chain tip (PeerManager.activeTip), not the header
// index tip: this port serves blocks and headers from the chain the
// blockchain service has validated, not from the header tree that headers-first
// sync has run ahead to.
//
// ok is false only when tip is not in the index, in which case there is no
// active chain to locate anything on and the caller must serve nothing.
func (idx *HeaderIndex) ForkPoint(tip chainhash.Hash, locator []chainhash.Hash) (HeaderNode, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	tipNode, ok := idx.nodes[tip]
	if !ok {
		return HeaderNode{}, false
	}

	for _, hash := range locator {
		n, ok := idx.nodes[hash]
		if !ok {
			continue
		}

		if containsLocked(tipNode, n) {
			return exportNode(n), true
		}

		// C++: `if (pindex->GetAncestor(chain.Height()) == chain.Tip())
		// return chain.Tip();` — the peer named a block that descends from
		// our tip, so everything we have is already common with it.
		if anc, ok := ancestorLocked(n, tipNode.height); ok && anc == tipNode {
			return exportNode(tipNode), true
		}
	}

	// C++: `return chain.Genesis();`
	genesis, ok := ancestorLocked(tipNode, 0)
	if !ok {
		return HeaderNode{}, false
	}

	return exportNode(genesis), true
}

// ActiveChainNext mirrors chain.h CChain::Next (chain.h:58-68): the block
// that follows hash on the branch ending at tip, or nothing when hash is the
// tip or sits off that branch. It is what both serving machines apply to a
// fork point before they start serving, the C++ `pindex = chainActive.Next(pindex)`.
func (idx *HeaderIndex) ActiveChainNext(tip, hash chainhash.Hash) (HeaderNode, bool) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	tipNode, ok := idx.nodes[tip]
	if !ok {
		return HeaderNode{}, false
	}

	n, ok := idx.nodes[hash]
	if !ok {
		return HeaderNode{}, false
	}

	if !containsLocked(tipNode, n) {
		return HeaderNode{}, false
	}

	next, ok := ancestorLocked(tipNode, n.height+1)
	if !ok {
		return HeaderNode{}, false
	}

	return exportNode(next), true
}

// activeChainRangeLocked returns at most limit nodes beginning at start and
// following the branch that ends at tip, start first. It is the walk both
// serving loops run — the C++ `for(; pindex; pindex = chainActive.Next(pindex))`
// bounded by that loop's own nLimit.
//
// A start that sits OFF the branch yields start alone, because CChain::Next
// answers nullptr for an index the active chain does not hold, which ends the
// C++ loop after its first iteration. That case is reachable: a getheaders
// with an empty locator names its start by hashStop, and the peer may name a
// header on a branch we did not take.
//
// The walk goes backwards from the far end rather than forwards from start:
// one skiplist descent places the last node, then prev links collect the
// range in O(limit) steps. Walking forwards would need one descent per step.
// Called with idx.mu held.
func activeChainRangeLocked(tip, start *node, limit int) []*node {
	if limit <= 0 {
		return nil
	}

	if !containsLocked(tip, start) {
		return []*node{start}
	}

	// int64 throughout: start.height + limit is not representable in int32
	// near the top of the height range, and the clamp below is what brings it
	// back into it.
	end := int64(start.height) + int64(limit) - 1
	if end > int64(tip.height) {
		end = int64(tip.height)
	}

	last, ok := ancestorLocked(tip, int32(end))
	if !ok {
		return []*node{start}
	}

	out := make([]*node, end-int64(start.height)+1)

	n := last

	for i := len(out) - 1; i >= 0; i-- {
		out[i] = n
		n = n.prev
	}

	return out
}

// ActiveChainHeaders returns at most limit headers beginning at start and
// following the active chain that ends at tip. It is the getheaders serving
// range, and the one place a stored wire.BlockHeader leaves the index.
// It returns nil when either hash is unknown.
func (idx *HeaderIndex) ActiveChainHeaders(tip, start chainhash.Hash, limit int) []wire.BlockHeader {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	tipNode, startNode, ok := rangeEndsLocked(idx, tip, start)
	if !ok {
		return nil
	}

	nodes := activeChainRangeLocked(tipNode, startNode, limit)

	out := make([]wire.BlockHeader, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.header)
	}

	return out
}

// ActiveChainHashes is ActiveChainHeaders returning the hashes instead, for
// the getblocks inv. The hashes are already in the index, so this exists to
// keep the inv path from re-hashing every header it serves.
func (idx *HeaderIndex) ActiveChainHashes(tip, start chainhash.Hash, limit int) []chainhash.Hash {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	tipNode, startNode, ok := rangeEndsLocked(idx, tip, start)
	if !ok {
		return nil
	}

	nodes := activeChainRangeLocked(tipNode, startNode, limit)

	out := make([]chainhash.Hash, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.hash)
	}

	return out
}

// rangeEndsLocked resolves the two hashes the range accessors take. Called
// with idx.mu held.
func rangeEndsLocked(idx *HeaderIndex, tip, start chainhash.Hash) (tipNode, startNode *node, ok bool) {
	tipNode, ok = idx.nodes[tip]
	if !ok {
		return nil, nil, false
	}

	startNode, ok = idx.nodes[start]
	if !ok {
		return nil, nil, false
	}

	return tipNode, startNode, true
}
