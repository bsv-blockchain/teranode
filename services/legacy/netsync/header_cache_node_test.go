package netsync

import (
	"testing"
	"unsafe"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// mainnetFloor is a committed tip above mainnet's last checkpoint (945,000), so
// a branch of synthetic headers above it meets no pinned hash and runs into
// the real 52,000 branch cap rather than a checkpoint mismatch.
const mainnetFloor = int32(950000)

// taggedRun is linkedRun with the merkle root set to tag, so two runs from one
// parent are different headers.
func taggedRun(parent chainhash.Hash, count int, tag byte) ([]*wire.BlockHeader, []chainhash.Hash) {
	headers := make([]*wire.BlockHeader, 0, count)
	hashes := make([]chainhash.Hash, 0, count)

	prev := parent

	for i := 0; i < count; i++ {
		header := wire.NewBlockHeader(1, &prev, &chainhash.Hash{tag}, 0x207fffff, uint32(i)) //nolint:gosec // a small test nonce
		hash := header.BlockHash()

		headers = append(headers, header)
		hashes = append(hashes, hash)
		prev = hash
	}

	return headers, hashes
}

// fillInReplies feeds owner's run to the cache one wire reply at a time, as a
// peer's replies arrive, and returns how many of the replies moved its branch.
func fillInReplies(cache *headerCache, owner any, tip chainhash.Hash, headers []*wire.BlockHeader) int {
	moved := 0

	for i := 0; i < len(headers); i += wire.MaxBlockHeadersPerMsg {
		end := min(i+wire.MaxBlockHeadersPerMsg, len(headers))
		if cache.FillFrom(owner, tip, mainnetFloor+1, headers[i:end]).accepted {
			moved++
		}
	}

	return moved
}

func mainnetCapCache(t *testing.T) (*headerCache, chainhash.Hash) {
	t.Helper()

	cache := newHeaderCache().WithCheckpoints(chaincfg.MainNetParams.Checkpoints)
	require.Equal(t, int32(52000), cache.branchCap, "sanity: the real mainnet branch cap")

	tip := chainhash.Hash{0xc1}
	cache.PruneTo(mainnetFloor, tip)

	return cache, tip
}

// Twenty honest peers on one chain, each holding the whole 52,000-header branch
// the mainnet cap allows, share their headers: the tree holds one branch's
// worth, every peer keeps its branch, and the header one past the branch cap is
// cut.
func TestHeaderCacheBranchCap_TwentyHonestPeersAtTheMainnetBranchCapShareOneBranch(t *testing.T) {
	cache, tip := mainnetCapCache(t)

	honest, hashes := taggedRun(tip, 52000+wire.MaxBlockHeadersPerMsg, 0x01)

	for peer := 0; peer < 20; peer++ {
		fillInReplies(cache, peer, tip, honest[:52000])
	}

	require.Equal(t, 52000, cache.heldHeaders(), "twenty peers on one chain hold one branch's worth")

	for peer := 0; peer < 20; peer++ {
		top, topHash, ok := cache.PeerTop(peer)
		require.True(t, ok, "peer %d keeps its branch", peer)
		require.Equal(t, mainnetFloor+52000, top)
		require.Equal(t, hashes[51999], topHash)
	}

	// The next reply reaches past the branch cap and is cut to nothing.
	result := cache.FillFrom(0, tip, mainnetFloor+1, honest[52000:])
	require.False(t, result.accepted)

	top, ok := cache.Top()
	require.True(t, ok)
	require.Equal(t, mainnetFloor+52000, top, "the active branch stops at the branch cap")
	require.Equal(t, 52000, cache.heldHeaders())
}

// A held header is kept as its six wire fields, 176 bytes a node where a node
// holding a wire.BlockHeader was 208, and it still hashes to itself: the
// difficulty calculator and the median time past walk check every cached
// header's Hash() against the hash they asked for, which needs all 80 bytes.
func TestHeaderNode_IsCompactAndStillHashesToItself(t *testing.T) {
	require.Equal(t, uintptr(176), unsafe.Sizeof(headerNode{}))

	for _, header := range mainnetFixture(t)[:50] {
		hash := header.BlockHash()
		node := newHeaderNode(hash, header, nil, 1, 0)

		require.Equal(t, hash, *node.modelHeader().Hash(), "the node's header is the 80 bytes it arrived as")
		require.Equal(t, header.PrevBlock, node.prevBlock)
	}
}
