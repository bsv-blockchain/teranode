package netsync

import (
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

// THE SIMPLE SCHEDULE (PR 1699, 2026-10-08).
// A block size is not known before the download: the header has no size, and go-wire has no
// hdrsen. So the scheduler uses no size, no pace and no deadline. In height sequence, each block
// goes to the measured peer with the lowest (blocks owed + 1) / rate, the peer with more room on a
// tie: a fast peer gets more blocks and the nearer ones, and each peer with room gets work. No
// block waits for a full peer. A late block is the watcher's (THE WATCHER, watcher.go), which knows
// a block's size from its first bytes.
//
// It replaces the deadline rule. With a 4 GB block after each 5 to 10 blocks of 254 bytes, that
// rule counted each block at 4 GB and took its pace from the small blocks, so on 2026-10-08 five
// of seven peers were idle, and at 12:26 the next block waited for a full peer and was never asked
// for.

// anyMeasured reports whether one or more peers of the pass have a rate.
func (a *downloadAssigner) anyMeasured() bool {
	for _, p := range a.peers {
		if p.measured {
			return true
		}
	}

	for _, p := range a.full {
		if p.measured {
			return true
		}
	}

	return false
}

// rank is p's place for its next block: (blocks owed + 1) / rate. Lower is earlier.
func (p *assignerPeer) rank() float64 {
	return float64(p.owed+1) / p.rate
}

// schedulePick is the peer for the block at height; see THE SIMPLE SCHEDULE. With no peer
// measured, each peer takes the next block it can (takeAvoiding). A peer with no rate takes only a
// far block (placeUnmeasured) once one or more peers are measured. A peer avoid marks (it owes the
// block already) is picked only when no other measured peer can serve the block.
func (a *downloadAssigner) schedulePick(height int32, avoid func(*peerpkg.Peer) bool) (*assignerPeer, bool) {
	if a == nil || a.remaining <= 0 {
		return nil, false
	}

	if !a.anyMeasured() {
		return a.takeAvoiding(height, avoid)
	}

	var best *assignerPeer

	for _, p := range a.peers {
		if p.budget <= 0 || !p.measured || p.rate <= 0 || !p.canServe(height) || (avoid != nil && avoid(p.peer)) {
			continue
		}

		if best == nil || p.rank() < best.rank() || (p.rank() == best.rank() && p.budget > best.budget) {
			best = p
		}
	}

	if best != nil {
		return best, true
	}

	return a.takeWhere(height, avoid, func(p *assignerPeer) bool { return p.measured })
}

// overBackstop reports whether the bytes held in front of the chain, with each block this pass
// gave counted at the recent mean size, have reached parkBackstopBytes. On 2026-10-08 the
// backstop was read once for each pass, and one pass took the node to 136 GB against 107 GB.
func (a *downloadAssigner) overBackstop() bool {
	return a.held+int64(a.given)*a.mean >= parkBackstopBytes
}

// placeUnmeasured gives each peer with no rate and room one of the highest unowned blocks of the
// full window (a.far), before the schedule places the others, so the node measures it on a block
// the chain does not need soon. It gives nothing when no peer has a rate: then each peer takes one
// block in height sequence. It returns the candidates left. On 2026-10-08 after a restart, blocks
// near the tip given to unmeasured peers at 0.0 MB/s let the chain apply 4 blocks in 10 minutes.
func (sm *SyncManager) placeUnmeasured(a *downloadAssigner, candidates []wantedBlock, highestHeld int32) []wantedBlock {
	if a == nil || !a.anyMeasured() {
		return candidates
	}

	far := a.far
	placedHashes := make(map[chainhash.Hash]struct{})

	for _, p := range a.peers {
		if len(far) == 0 || a.remaining <= 0 {
			break
		}

		if p.measured || p.budget <= 0 {
			continue
		}

		block := far[0]
		if block.height > highestHeld && a.overBackstop() {
			break
		}

		if !p.canServe(block.height) || sm.blockDownloads.HasOwner(p.peer, block.hash) || !sm.blockDownloads.Add(p.peer, block.hash) {
			continue
		}

		hash := block.hash
		if err := a.recordRequest(p, &hash, false); err != nil {
			sm.blockDownloads.RemoveOwner(p.peer, block.hash)

			continue
		}

		far = far[1:]
		placedHashes[block.hash] = struct{}{}
		sm.farProbes.add(block.hash)

		sm.logger.Infof("[schedule] %s has no rate yet: asked it for block %d, %d above the chain", p.peer, block.height, block.height-a.tip)
	}

	if len(placedHashes) == 0 {
		return candidates
	}

	left := candidates[:0:0]

	for _, c := range candidates {
		if _, ok := placedHashes[c.hash]; !ok {
			left = append(left, c)
		}
	}

	return left
}

// farProbeSet is the blocks placeUnmeasured gave a peer with no rate, from the top of the window.
// highestHeld does not count them: once such a block arrives or parks, each block below it would
// count as a gap, and the disk backstop would stop nothing in the whole window. The mark is in
// memory only, so a probe parked before a restart counts again until the chain reaches it.
type farProbeSet struct {
	mu     sync.Mutex
	hashes map[chainhash.Hash]struct{}
}

func (f *farProbeSet) add(h chainhash.Hash) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.hashes == nil {
		f.hashes = make(map[chainhash.Hash]struct{})
	}

	f.hashes[h] = struct{}{}
}

// within returns the probes among wanted, and forgets each probe not in wanted: a block the chain
// has reached, or one that has left the window.
func (f *farProbeSet) within(wanted []wantedBlock) map[chainhash.Hash]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	if len(f.hashes) == 0 {
		return nil
	}

	in := make(map[chainhash.Hash]struct{}, len(f.hashes))

	for _, b := range wanted {
		if _, ok := f.hashes[b.hash]; ok {
			in[b.hash] = struct{}{}
		}
	}

	f.hashes = in

	out := make(map[chainhash.Hash]struct{}, len(in))
	for h := range in {
		out[h] = struct{}{}
	}

	return out
}

// unmeasuredWithRoom is how many peers of the pass have no rate and room, when one or more peers
// have a rate: the number of far blocks placeUnmeasured can give.
func (a *downloadAssigner) unmeasuredWithRoom() int {
	if a == nil || !a.anyMeasured() {
		return 0
	}

	n := 0

	for _, p := range a.peers {
		if !p.measured && p.budget > 0 {
			n++
		}
	}

	return n
}
