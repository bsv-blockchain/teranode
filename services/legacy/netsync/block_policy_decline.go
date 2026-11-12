package netsync

import (
	"net"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
)

const (
	// policyDeclinePeers is how many different hosts, each asked for the block, must declare a block's body above this
	// node's block policy before the block is written off for recentlyFailedBlocksTTL. One peer's
	// declared length is only its claim, and one liar must not stop this node asking anyone for a
	// block (streamingBlockGate); every honest peer declares the same length for the same block.
	policyDeclinePeers = 2

	// maxTrackedPolicyDeclines bounds the blocks whose declines are counted. At the bound a new
	// block's decline is not counted, and the block is drained on each delivery as before.
	maxTrackedPolicyDeclines = 1024
)

// policyDecline is the hosts that declared one block's body above this node's block policy,
// within recentlyFailedBlocksTTL of the first.
type policyDecline struct {
	first time.Time
	hosts map[string]struct{}
}

// declineHost is the remote host a decline is counted under: the address without its port, so
// two connections from one host are one vote.
func declineHost(p *peerpkg.Peer) string {
	addr := p.Addr()

	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}

	return addr
}

// notePolicyDecline counts a decline of hash's body on this node's block policy, declared by
// from. The gate declines such a body before reading it and the wire layer drains it, with the
// connection kept, and nothing marked: a block declined that way was asked for again and drained
// again on every pass that wanted it, gigabytes each time for the oversize block an operator who
// lowered excessiveblocksize cannot commit (review of 76be891d6). Once policyDeclinePeers
// different hosts that owe the block have declared it, the block is marked in recentlyFailedBlocks, as the park's
// policy_declined row marks it, so the wanted-range pass does not ask for it while the mark
// stands.
func (sm *SyncManager) notePolicyDecline(hash chainhash.Hash, from *peerpkg.Peer, now time.Time) {
	if from == nil || sm.recentlyFailedBlocks == nil {
		return
	}

	_, primary, _ := sm.peerStateResolvingPrimary(from)

	// Only a peer this node asked for the block counts. The gate admits a body for any
	// requested hash whoever sends it, so without this any peer could vote on a block it was
	// never asked for (review of c1f4e9202). This node chooses whom it asks.
	if sm.blockDownloads == nil || !sm.blockDownloads.HasOwner(primary, hash) {
		return
	}

	host := declineHost(primary)

	sm.policyDeclinesMu.Lock()

	rec, ok := sm.policyDeclines[hash]
	if ok && now.Sub(rec.first) >= recentlyFailedBlocksTTL {
		delete(sm.policyDeclines, hash)

		ok = false
	}

	if !ok {
		if len(sm.policyDeclines) >= maxTrackedPolicyDeclines {
			sm.policyDeclinesMu.Unlock()

			return
		}

		if sm.policyDeclines == nil {
			sm.policyDeclines = make(map[chainhash.Hash]*policyDecline)
		}

		rec = &policyDecline{first: now, hosts: make(map[string]struct{})}
		sm.policyDeclines[hash] = rec
	}

	rec.hosts[host] = struct{}{}

	declared := len(rec.hosts)
	if declared >= policyDeclinePeers {
		delete(sm.policyDeclines, hash)
	}

	sm.policyDeclinesMu.Unlock()

	if declared < policyDeclinePeers {
		return
	}

	sm.recentlyFailedBlocks.Set(hash, struct{}{})

	sm.logger.Warnf("[streamingBlockGate][%s] %d hosts asked for the block declared it above excessiveblocksize; not asking for it again for %s. This node cannot sync past it unless excessiveblocksize is raised", hash, declared, recentlyFailedBlocksTTL)
}
