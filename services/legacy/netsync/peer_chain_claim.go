package netsync

import (
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
)

// Why this file exists.
//
// This node's record of what a peer has was seeded from the height that peer
// advertised about itself during the handshake, and it only ever rises. A
// self-report that can never be contradicted means every peer permanently claims
// to have every block, so canServe — the scheduler's "can this peer serve this
// height" test — has always been a no-op, and the assigner's own comment
// describes it as "not a veto" for that reason.
//
// SV Node keeps the same handshake number, nStartingHeight, and never writes it
// into its availability record. It reads it for a relay heuristic and a log line
// and nothing else. What fills pindexBestKnownBlock there is headers and
// announcements the peer actually sent, credited on the code path that accepts
// them.
//
// The consequence, measured on Hetzner mainnet on 2026-09-10: the node is dead
// 39.7% of the day. Of the 27 gaps over two minutes, 20 had a frontier published
// and no headers round inside, worth 64% of the long-gap time. The node knew
// exactly which block it wanted and had asked a peer that never sent it, because
// nothing could tell a peer that has the block from a peer that merely said so
// at handshake.
//
// So a claim here is graded by how it was learned, and an ungraded assertion
// cannot be made at all.

// claimProof grades how this node came to believe a peer has a chain.
//
// The grades are ordered by strength and the ordering is load-bearing: a weaker
// grade never overwrites a stronger one, which is what stops a peer's own word
// displacing something we verified.
type claimProof uint8

const (
	// proofNone is a peer that has told us nothing we could check. It is the
	// zero value, and it is what a peer is worth at the end of a handshake.
	proofNone claimProof = iota

	// proofPending is a peer that named a block we cannot place. Below a
	// checkpoint the header list stops at that checkpoint, so a peer announcing
	// its own tip far above the walk cannot be resolved and will not be for
	// days. SV Node calls this hashLastUnknownBlock and treats it the same way:
	// worth remembering, worth nothing as permission.
	proofPending

	// proofProven is a chain this node has placed. Either we put the header in
	// the list ourselves from a batch this peer sent, or this peer delivered a
	// block we committed. The height is ours, not the peer's word for it.
	proofProven
)

// peerChainClaim is what a peer has demonstrated it holds. Stored by value and
// replaced whole, so a reader never sees a hash from one claim beside a height
// from another.
type peerChainClaim struct {
	hash   chainhash.Hash
	height int32
	proof  claimProof
}

// peerChainClaimState is embedded in peerSyncState. It is a separate type so the
// whole mechanism can be read, and removed, as one piece.
type peerChainClaimState struct {
	// claim is the peer's graded chain claim, published by whole-value swap.
	//
	// An atomic pointer rather than a mutex for the same reason
	// noteBestKnownHeight beside it is atomic: a *peerSyncState is shared by
	// pointer across the block handler goroutine and the per-message inv and
	// headers handlers, each on its own goroutine.
	claim atomic.Pointer[peerChainClaim]
}

// noteProvenClaim records a chain this node has placed at a height it worked out
// for itself, and never lowers a proven height.
//
// Monotone in the same direction as the record it replaces, and for the same
// reason: a peer that had block N a minute ago still has it. What changes is that
// the starting point is zero rather than whatever the peer said about itself.
func (s *peerSyncState) noteProvenClaim(hash chainhash.Hash, height int32) {
	if s == nil || height <= 0 {
		return
	}

	next := &peerChainClaim{hash: hash, height: height, proof: proofProven}

	for {
		cur := s.claim.Load()
		if cur != nil && cur.proof == proofProven && cur.height >= height {
			return
		}

		if s.claim.CompareAndSwap(cur, next) {
			return
		}
	}
}

// notePendingClaim records that a peer named a block we cannot place. It never
// displaces a proven claim, because an unresolvable hash is strictly less
// informative than a height we established.
//
// Worth recording at all because it is the only trace a non-sync peer's
// announcement leaves during a deep sync, and because it becomes proof the moment
// the walk reaches that block.
func (s *peerSyncState) notePendingClaim(hash chainhash.Hash) {
	if s == nil {
		return
	}

	next := &peerChainClaim{hash: hash, proof: proofPending}

	for {
		cur := s.claim.Load()
		if cur != nil && cur.proof == proofProven {
			return
		}

		if cur != nil && cur.proof == proofPending && cur.hash.IsEqual(&hash) {
			return
		}

		if s.claim.CompareAndSwap(cur, next) {
			return
		}
	}
}

// Claim returns what this peer has demonstrated. Never nil, so callers do not
// have to distinguish "no claim" from "no peer state".
func (s *peerSyncState) Claim() peerChainClaim {
	if s == nil {
		return peerChainClaim{}
	}

	if c := s.claim.Load(); c != nil {
		return *c
	}

	return peerChainClaim{}
}

// HasProvenTo reports whether this peer has demonstrated a chain reaching height.
//
// False for a peer that has proven nothing, which is the whole point: it is the
// question the old integer could not answer, because a handshake seed made every
// peer answer yes to every height forever.
func (s *peerSyncState) HasProvenTo(height int32) bool {
	c := s.Claim()

	return c.proof == proofProven && c.height >= height
}
