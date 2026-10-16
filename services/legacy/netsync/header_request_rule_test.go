package netsync

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/legacy/blockchain"
	peerpkg "github.com/bsv-blockchain/teranode/services/legacy/peer"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// mainnetFirstCheckpointHeaders reads testdata/mainnet_headers_1_11111.bin, the
// real mainnet headers from height 1 to mainnet's first checkpoint at 11111,
// and checks that they link to the genesis block and end at the pinned hash.
func mainnetFirstCheckpointHeaders(t *testing.T) []*wire.BlockHeader {
	t.Helper()

	const count = 11111

	data, err := os.ReadFile("testdata/mainnet_headers_1_11111.bin")
	require.NoError(t, err)
	require.Len(t, data, count*80)

	digest := sha256.Sum256(data)
	require.Equal(t, "031de0ade9e449252be2ac8863357501515f47ac8cdb04af2137bc4adef30b3e", hex.EncodeToString(digest[:]))

	headers := make([]*wire.BlockHeader, 0, count)
	prev := *chaincfg.MainNetParams.GenesisHash

	for i := 0; i < len(data); i += 80 {
		var header wire.BlockHeader
		require.NoError(t, header.Deserialize(bytes.NewReader(data[i:i+80])))
		require.Equal(t, prev, header.PrevBlock, "fixture linkage at height %d", len(headers)+1)

		headers = append(headers, &header)
		prev = header.BlockHash()
	}

	checkpoint := chaincfg.MainNetParams.Checkpoints[0]
	require.Equal(t, int32(count), checkpoint.Height)
	require.Equal(t, *checkpoint.Hash, prev, "the fixture ends at mainnet's first checkpoint")

	return headers
}

// sybilBranch is honest[:len-1] with each merkle root changed and the run
// relinked: a branch from genesis that stops one short of the checkpoint. Every
// header keeps the honest version, time and bits, so each carries the same work
// as the honest header at its height and passes the same contextual rules. The
// hashes no longer meet their targets, which is why these tests build the cache
// without the proof-of-work ceiling (see headerRuleManager).
func sybilBranch(honest []*wire.BlockHeader, tag byte) []*wire.BlockHeader {
	fake := copyHeaders(honest[:len(honest)-1])

	for _, header := range fake {
		header.MerkleRoot[31] ^= tag
	}

	relink(fake)

	return fake
}

// headerRuleManager is a manager in headers-first mode over a real sqlitememory
// mainnet store holding genesis, with the header cache New builds: mainnet's
// real checkpoints, SV Node's contextual header rules read through the real
// blockchain client, the minimum chain work and the live-owner check. It leaves
// out the proof-of-work ceiling and nothing else. A difficulty-1 header costs
// about 2^32 hashes, seconds for one mining ASIC and out of reach of a test, so
// the fakes here are the attack with the mining step skipped.
func headerRuleManager(t *testing.T) *SyncManager {
	t.Helper()

	params := chaincfg.MainNetParams
	trunk := newRealTrunk(t, &params, 0, nil)

	sm := newHeaderCacheManager(t)
	sm.blockchainClient = trunk.client
	sm.chainParams = &params
	sm.headerCache = newHeaderCache().
		WithCheckpoints(params.Checkpoints).
		WithHeaderRules(trunk.rules(t, time.Now())).
		WithMinimumChainWork(minimumChainWork(&params)).
		WithOwnerLive(sm.headerOwnerLive)

	best, tip, ok := sm.committedTip()
	require.True(t, ok)
	require.Zero(t, best)
	require.Equal(t, *params.GenesisHash, tip)

	return sm
}

// inboundHeaderPeer connects a peer pair and registers the INBOUND side with the
// manager as a sync candidate: a peer that connected to this node, the only
// kind an attacker can add at will.
func inboundHeaderPeer(t *testing.T, sm *SyncManager, idx uint8) *peerpkg.Peer {
	t.Helper()

	cfg := peerpkg.Config{
		UserAgentName:    "btcdtest",
		UserAgentVersion: "1.0",
		ChainParams:      &chaincfg.MainNetParams,
	}

	inbound, outbound, err := MakeConnectedPeers(t, cfg, cfg, idx)
	require.NoError(t, err)
	require.True(t, inbound.Inbound())

	t.Cleanup(func() {
		inbound.DisconnectWithInfo("test over")
		outbound.DisconnectWithInfo("test over")
	})

	state := &peerSyncState{
		syncCandidate: true,
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](10 * time.Second),
	}
	t.Cleanup(state.requestedTxns.Stop)

	sm.peerStates.Set(inbound, state)

	return inbound
}

// askForHeaders sends p a getheaders from the committed tip through
// requestHeaders, the one place this node records that it asked a peer.
func askForHeaders(t *testing.T, sm *SyncManager, p *peerpkg.Peer) {
	t.Helper()

	best, tip, ok := sm.committedTip()
	require.True(t, ok)
	require.NoError(t, sm.requestHeaders(p, best, blockchain.BlockLocator{&tip}, &zeroHash))
}

// sendHeadersInReplies hands the manager p's run one wire reply at a time
// through the production handler, as a peer's replies arrive.
func sendHeadersInReplies(t *testing.T, sm *SyncManager, p *peerpkg.Peer, headers []*wire.BlockHeader) {
	t.Helper()

	for i := 0; i < len(headers); i += wire.MaxBlockHeadersPerMsg {
		end := min(i+wire.MaxBlockHeadersPerMsg, len(headers))
		sm.handleHeadersMsg(&headersMsg{headers: headersMsgOf(t, headers[i:end]), peer: p})
	}
}

// requireHonestWalkReachedTheCheckpoint checks that p's branch is the honest
// run to 11111, that it proved mainnet's first checkpoint, and that the wanted
// range names the honest blocks from height 1.
func requireHonestWalkReachedTheCheckpoint(t *testing.T, sm *SyncManager, p *peerpkg.Peer, honest []*wire.BlockHeader) {
	t.Helper()

	top, topHash, ok := sm.headerCache.PeerTop(p)
	require.True(t, ok, "the honest peer holds its branch")
	require.Equal(t, int32(11111), top)
	require.Equal(t, honest[11110].BlockHash(), topHash)
	require.Equal(t, int32(11111), sm.headerCache.ProvenTo(), "the walk proved mainnet's first checkpoint")

	wanted := sm.wantedBlocksFromCache(0, 100)
	require.Len(t, wanted, 100, "the honest blocks are wanted")

	for i, w := range wanted {
		require.Equal(t, int32(i+1), w.height) //nolint:gosec // a wanted index below 100
		require.Equal(t, honest[i].BlockHash(), w.hash, "wanted height %d is the honest block", w.height)
	}
}

// Ten peers that connected to this node send fake branches from genesis to one
// short of mainnet's first checkpoint, 111,100 headers between them, more than
// the 108,000 the removed global node cap allowed. None was asked for headers,
// so none gets a branch and none is disconnected. The honest outbound peer this
// node asked then walks to the checkpoint and its blocks become wanted.
func TestHeaderRequestRule_UnaskedSybilBranchesAreDiscardedAndTheHonestWalkCompletes(t *testing.T) {
	sm := headerRuleManager(t)
	honest := mainnetFirstCheckpointHeaders(t)

	sybils := make([]*peerpkg.Peer, 0, 10)

	for i := 0; i < 10; i++ {
		sybil := inboundHeaderPeer(t, sm, uint8(10+i)) //nolint:gosec // a small peer index
		sybils = append(sybils, sybil)

		sendHeadersInReplies(t, sm, sybil, sybilBranch(honest, byte(i+1))) //nolint:gosec // a small tag
	}

	require.Zero(t, sm.headerCache.heldHeaders(), "no unasked peer's headers are held")

	honestPeer, _, _ := demotionPeer(t, sm, 40, 20000)
	askForHeaders(t, sm, honestPeer)
	sendHeadersInReplies(t, sm, honestPeer, honest)

	requireHonestWalkReachedTheCheckpoint(t, sm, honestPeer, honest)
	require.Equal(t, 11111, sm.headerCache.heldHeaders(), "the honest branch is all the cache holds")

	for i, sybil := range sybils {
		_, _, ok := sm.headerCache.PeerTop(sybil)
		require.False(t, ok, "sybil %d has no branch", i)
		require.True(t, sybil.Connected(), "sybil %d is not disconnected for unsolicited headers", i)

		state, exists := sm.peerStates.Get(sybil)
		require.True(t, exists)
		require.False(t, state.headersAsked.Load(), "no path asked inbound sybil %d for headers", i)
	}
}

// Ten sybils this node did ask, because they are its outbound peers, each send
// a fake branch to one short of the checkpoint. Each keeps its own branch, and
// the honest outbound peer's branch still grows to the checkpoint: the tree
// holds 122,211 headers, past the 108,000 at which the removed node cap evicted
// the lowest ranked branch, which was the honest one until it matched the
// checkpoint.
func TestHeaderRequestRule_AskedSybilsKeepOnlyTheirOwnBranchesAndTheHonestWalkCompletes(t *testing.T) {
	sm := headerRuleManager(t)
	honest := mainnetFirstCheckpointHeaders(t)

	sybils := make([]*peerpkg.Peer, 0, 10)

	for i := 0; i < 10; i++ {
		sybil, _, _ := demotionPeer(t, sm, uint8(10+i), 20000) //nolint:gosec // a small peer index
		sybils = append(sybils, sybil)

		askForHeaders(t, sm, sybil)
		sendHeadersInReplies(t, sm, sybil, sybilBranch(honest, byte(i+1))) //nolint:gosec // a small tag
	}

	honestPeer, _, _ := demotionPeer(t, sm, 40, 20000)
	askForHeaders(t, sm, honestPeer)
	sendHeadersInReplies(t, sm, honestPeer, honest)

	requireHonestWalkReachedTheCheckpoint(t, sm, honestPeer, honest)

	for i, sybil := range sybils {
		top, _, ok := sm.headerCache.PeerTop(sybil)
		require.True(t, ok, "sybil %d still holds its own branch", i)
		require.Equal(t, int32(11110), top)
	}

	require.Equal(t, 10*11110+11111, sm.headerCache.heldHeaders(), "eleven branches, one per asked peer, nothing evicted")
}

// An outbound peer this node never asked sends headers below the checkpoint:
// the batch creates no branch and the peer stays connected. SV Node never
// scores unsolicited headers.
func TestHeaderRequestRule_AnUnsolicitedBatchBelowTheCheckpointIsDiscardedWithoutDisconnect(t *testing.T) {
	sm := headerRuleManager(t)
	honest := mainnetFirstCheckpointHeaders(t)

	peer, _, _ := demotionPeer(t, sm, 41, 20000)
	require.False(t, peer.Inbound())

	sm.handleHeadersMsg(&headersMsg{headers: headersMsgOf(t, honest[:wire.MaxBlockHeadersPerMsg]), peer: peer})

	_, _, ok := sm.headerCache.PeerTop(peer)
	require.False(t, ok, "an unsolicited batch creates no branch")
	require.Zero(t, sm.headerCache.heldHeaders())
	require.True(t, peer.Connected(), "and costs the sender nothing")

	// The same batch after a getheaders is kept.
	askForHeaders(t, sm, peer)
	sm.handleHeadersMsg(&headersMsg{headers: headersMsgOf(t, honest[:wire.MaxBlockHeadersPerMsg]), peer: peer})

	top, _, ok := sm.headerCache.PeerTop(peer)
	require.True(t, ok, "a batch from a peer this node asked makes a branch")
	require.Equal(t, int32(wire.MaxBlockHeadersPerMsg), top)
}

// Outside headers-first mode a headers message is not read, and its sender is
// no longer disconnected for it.
func TestHeaderRequestRule_HeadersOutsideHeadersFirstModeKeepTheConnection(t *testing.T) {
	sm := headerRuleManager(t)
	sm.headersFirstMode.Store(false)

	honest := mainnetFirstCheckpointHeaders(t)
	peer, _, _ := demotionPeer(t, sm, 42, 20000)

	sm.handleHeadersMsg(&headersMsg{headers: headersMsgOf(t, honest[:10]), peer: peer})

	require.Zero(t, sm.headerCache.heldHeaders())
	require.True(t, peer.Connected())
}

// Below the last checkpoint an inbound peer that is not the sync peer is never
// asked for headers: the request is refused before anything is sent or
// recorded. At the last checkpoint the rule still applies while headers-first
// mode is on, and not once the mode is left.
func TestHeaderRequestRule_AnInboundPeerIsNeverAskedBelowTheCheckpoint(t *testing.T) {
	sm := headerRuleManager(t)
	inbound := inboundHeaderPeer(t, sm, 43)

	_, tip, ok := sm.committedTip()
	require.True(t, ok)

	require.False(t, sm.mayAskForHeaders(inbound, 0))
	require.Error(t, sm.requestHeaders(inbound, 0, blockchain.BlockLocator{&tip}, &zeroHash))

	state, exists := sm.peerStates.Get(inbound)
	require.True(t, exists)
	require.False(t, state.headersAsked.Load(), "a refused request records nothing")

	last := chaincfg.MainNetParams.Checkpoints[len(chaincfg.MainNetParams.Checkpoints)-1].Height
	require.False(t, sm.mayAskForHeaders(inbound, last), "at the last checkpoint the rule holds while headers-first mode is on")

	sm.headersFirstMode.Store(false)
	require.True(t, sm.mayAskForHeaders(inbound, last), "at the last checkpoint, outside headers-first mode, the rule no longer applies")

	outbound, _, _ := demotionPeer(t, sm, 44, 20000)
	require.True(t, sm.mayAskForHeaders(outbound, 0))
}
