package protocol

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-chaincfg"
	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// serveFixture is a chain in the index plus the serving machine over it. The
// headers are not mined: nothing on the serving path checks proof of work, so
// grinding targets here would only slow the 2001-header cap case down.
type serveFixture struct {
	genesis *wire.BlockHeader
	chain   []*wire.BlockHeader
	idx     *HeaderIndex
	hs      *HeaderSync
	srv     *Serving
	nc      *nonceCounter
}

func newServeFixture(t *testing.T, height int, checkpoints ...chaincfg.Checkpoint) *serveFixture {
	t.Helper()

	genesis := testGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	nc := &nonceCounter{}
	chain := buildChain(t, idx, nc, genesis, height)

	hs, err := NewHeaderSync(HeaderSyncConfig{Index: idx, Params: syncTestParams(checkpoints)})
	require.NoError(t, err)

	srv, err := NewServing(idx, hs)
	require.NoError(t, err)

	return &serveFixture{genesis: genesis, chain: chain, idx: idx, hs: hs, srv: srv, nc: nc}
}

// at returns the header at height, counting genesis as height 0.
func (f *serveFixture) at(height int) *wire.BlockHeader {
	if height == 0 {
		return f.genesis
	}

	return f.chain[height-1]
}

// node returns the HeaderNode the serving methods take as the active tip.
func (f *serveFixture) node(t *testing.T, height int) HeaderNode {
	t.Helper()

	n, ok := f.idx.Lookup(f.at(height).BlockHash())
	require.True(t, ok)

	return n
}

// tip is the active tip at the top of the fixture's chain.
func (f *serveFixture) tip(t *testing.T) HeaderNode {
	t.Helper()

	return f.node(t, len(f.chain))
}

func getHeadersFor(locator []chainhash.Hash, hashStop chainhash.Hash) *wire.MsgGetHeaders {
	msg := wire.NewMsgGetHeaders()

	for i := range locator {
		hash := locator[i]
		msg.BlockLocatorHashes = append(msg.BlockLocatorHashes, &hash)
	}

	msg.HashStop = hashStop

	return msg
}

func getBlocksFor(locator []chainhash.Hash, hashStop chainhash.Hash) *wire.MsgGetBlocks {
	msg := wire.NewMsgGetBlocks(&hashStop)

	for i := range locator {
		hash := locator[i]
		msg.BlockLocatorHashes = append(msg.BlockLocatorHashes, &hash)
	}

	return msg
}

// requireHeadersReply unwraps the single headers message a served getheaders
// answers with and returns the hashes it carries, in order.
func requireHeadersReply(t *testing.T, msgs []wire.Message) []chainhash.Hash {
	t.Helper()

	require.Len(t, msgs, 1)

	headers, ok := msgs[0].(*wire.MsgHeaders)
	require.True(t, ok, "expected a headers message, got %s", msgs[0].Command())

	out := make([]chainhash.Hash, 0, len(headers.Headers))
	for _, h := range headers.Headers {
		out = append(out, h.BlockHash())
	}

	return out
}

// requireInvReply unwraps the single inv message a served getblocks answers
// with and returns the block hashes it carries, in order.
func requireInvReply(t *testing.T, msgs []wire.Message) []chainhash.Hash {
	t.Helper()

	require.Len(t, msgs, 1)

	inv, ok := msgs[0].(*wire.MsgInv)
	require.True(t, ok, "expected an inv message, got %s", msgs[0].Command())

	out := make([]chainhash.Hash, 0, len(inv.InvList))

	for _, iv := range inv.InvList {
		require.Equal(t, wire.InvTypeBlock, iv.Type)
		out = append(out, iv.Hash)
	}

	return out
}

// hashRange is the hashes of the fixture's headers from height first to last
// inclusive, the answer every serving assertion below is written against.
func (f *serveFixture) hashRange(first, last int) []chainhash.Hash {
	out := make([]chainhash.Hash, 0, last-first+1)

	for h := first; h <= last; h++ {
		out = append(out, f.at(h).BlockHash())
	}

	return out
}

// TestOnGetHeaders pins net_processing.cpp ProcessGetHeadersMessage
// (net_processing.cpp:2968) rule by rule.
func TestOnGetHeaders(t *testing.T) {
	t.Run("a locator hit mid-chain serves every header after the fork point", func(t *testing.T) {
		f := newServeFixture(t, 10)
		peer := fullNodePeer("1.2.3.4:8333")

		// The peer's locator names the block at height 4, so the fork point is
		// there and the reply starts at 5 — C++ chainActive.Next(pindex).
		msgs, refused := f.srv.OnGetHeaders(peer,
			f.tip(t), getHeadersFor([]chainhash.Hash{f.at(4).BlockHash()}, chainhash.Hash{}))

		require.False(t, refused)
		require.Equal(t, f.hashRange(5, 10), requireHeadersReply(t, msgs))
	})

	t.Run("an unknown locator starts after genesis", func(t *testing.T) {
		f := newServeFixture(t, 6)
		peer := fullNodePeer("1.2.3.4:8333")

		// FindForkInGlobalIndex returns chain.Genesis() when it knows nothing
		// in the locator, so the peer restarts from height 1.
		msgs, refused := f.srv.OnGetHeaders(peer,
			f.tip(t), getHeadersFor([]chainhash.Hash{{0xAB}}, chainhash.Hash{}))

		require.False(t, refused)
		require.Equal(t, f.hashRange(1, 6), requireHeadersReply(t, msgs))
	})

	t.Run("a locator on a side branch falls back to genesis", func(t *testing.T) {
		f := newServeFixture(t, 6)
		peer := fullNodePeer("1.2.3.4:8333")

		// A branch off height 2 that we hold but never took: it is neither on
		// the active chain nor a descendant of our tip, so the locator walk
		// falls through to the genesis fallback.
		side := buildChain(t, f.idx, f.nc, f.at(2), 2)

		msgs, refused := f.srv.OnGetHeaders(peer,
			f.tip(t), getHeadersFor([]chainhash.Hash{side[1].BlockHash()}, chainhash.Hash{}))

		require.False(t, refused)
		require.Equal(t, f.hashRange(1, 6), requireHeadersReply(t, msgs))
	})

	t.Run("hashStop is served and ends the batch", func(t *testing.T) {
		f := newServeFixture(t, 10)
		peer := fullNodePeer("1.2.3.4:8333")

		// C++ pushes the header first and breaks on hashStop after, so
		// hashStop is INCLUDED.
		msgs, refused := f.srv.OnGetHeaders(peer,
			f.tip(t), getHeadersFor([]chainhash.Hash{f.at(3).BlockHash()}, f.at(7).BlockHash()))

		require.False(t, refused)
		require.Equal(t, f.hashRange(4, 7), requireHeadersReply(t, msgs))
	})

	t.Run("the batch is capped at MaxHeadersResults", func(t *testing.T) {
		f := newServeFixture(t, MaxHeadersResults+5)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs, refused := f.srv.OnGetHeaders(peer,
			f.tip(t), getHeadersFor([]chainhash.Hash{f.genesis.BlockHash()}, chainhash.Hash{}))

		require.False(t, refused)

		got := requireHeadersReply(t, msgs)
		require.Len(t, got, MaxHeadersResults)
		require.Equal(t, f.hashRange(1, MaxHeadersResults), got)
	})

	t.Run("headers-first catch-up refuses to serve", func(t *testing.T) {
		// A checkpoint above our tip puts the machine in headers-first mode as
		// soon as a sync peer starts a round.
		cp := chainhash.Hash{0xC0}
		f := newServeFixture(t, 5, chaincfg.Checkpoint{Height: 1000, Hash: &cp})

		require.Len(t, f.hs.PeerEstablished(fullNodePeer("9.9.9.9:8333")), 1)
		require.True(t, f.hs.IsHeadersFirstMode())

		msgs, refused := f.srv.OnGetHeaders(fullNodePeer("1.2.3.4:8333"),
			f.tip(t), getHeadersFor([]chainhash.Hash{f.genesis.BlockHash()}, chainhash.Hash{}))

		require.True(t, refused)
		require.Nil(t, msgs)
	})

	t.Run("a peer level with our tip gets an empty headers message", func(t *testing.T) {
		f := newServeFixture(t, 4)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs, refused := f.srv.OnGetHeaders(peer,
			f.tip(t), getHeadersFor([]chainhash.Hash{f.at(4).BlockHash()}, chainhash.Hash{}))

		require.False(t, refused)
		require.Empty(t, requireHeadersReply(t, msgs))
	})

	t.Run("a peer ahead of our tip gets an empty headers message", func(t *testing.T) {
		// Our active tip is at height 4 while the index holds headers up to 8:
		// the peer's locator names height 8, which descends from our tip, so
		// FindForkInGlobalIndex answers the tip and there is nothing after it.
		f := newServeFixture(t, 8)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs, refused := f.srv.OnGetHeaders(peer,
			f.node(t, 4), getHeadersFor([]chainhash.Hash{f.at(8).BlockHash()}, chainhash.Hash{}))

		require.False(t, refused)
		require.Empty(t, requireHeadersReply(t, msgs))
	})

	t.Run("serving stops at the active tip, not the header index tip", func(t *testing.T) {
		// The Teranode-specific half of the rule: headers-first sync runs the
		// index ahead of the blocks the node actually holds, and we must not
		// offer headers whose bodies we cannot serve.
		f := newServeFixture(t, 9)
		peer := fullNodePeer("1.2.3.4:8333")

		_, indexHeight := f.idx.Tip()
		require.Equal(t, int32(9), indexHeight)

		msgs, refused := f.srv.OnGetHeaders(peer,
			f.node(t, 5), getHeadersFor([]chainhash.Hash{f.genesis.BlockHash()}, chainhash.Hash{}))

		require.False(t, refused)
		require.Equal(t, f.hashRange(1, 5), requireHeadersReply(t, msgs))
	})

	t.Run("an empty locator serves the single header hashStop names", func(t *testing.T) {
		f := newServeFixture(t, 6)
		peer := fullNodePeer("1.2.3.4:8333")

		// GetFirstBlockIndexFromLocatorNL reads a null locator as "start at
		// hashStop", and the loop breaks on hashStop right after pushing it.
		msgs, refused := f.srv.OnGetHeaders(peer, f.tip(t), getHeadersFor(nil, f.at(3).BlockHash()))

		require.False(t, refused)
		require.Equal(t, f.hashRange(3, 3), requireHeadersReply(t, msgs))
	})

	t.Run("an empty locator naming a header off our chain serves that header alone", func(t *testing.T) {
		f := newServeFixture(t, 6)
		peer := fullNodePeer("1.2.3.4:8333")

		side := buildChain(t, f.idx, f.nc, f.at(2), 3)

		msgs, refused := f.srv.OnGetHeaders(peer, f.tip(t), getHeadersFor(nil, side[0].BlockHash()))

		require.False(t, refused)
		require.Equal(t, []chainhash.Hash{side[0].BlockHash()}, requireHeadersReply(t, msgs))
	})

	t.Run("an empty locator with an unknown hashStop answers nothing", func(t *testing.T) {
		f := newServeFixture(t, 6)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs, refused := f.srv.OnGetHeaders(peer, f.tip(t), getHeadersFor(nil, chainhash.Hash{0xEE}))

		require.False(t, refused)
		require.Nil(t, msgs)
	})

	t.Run("an active tip the index cannot place answers nothing", func(t *testing.T) {
		f := newServeFixture(t, 3)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs, refused := f.srv.OnGetHeaders(peer, HeaderNode{Hash: chainhash.Hash{0x77}},
			getHeadersFor([]chainhash.Hash{f.genesis.BlockHash()}, chainhash.Hash{}))

		require.False(t, refused)
		require.Nil(t, msgs)
	})

	t.Run("a nil peer or message answers nothing", func(t *testing.T) {
		f := newServeFixture(t, 3)

		msgs, refused := f.srv.OnGetHeaders(nil, f.tip(t), getHeadersFor(nil, chainhash.Hash{}))
		require.Nil(t, msgs)
		require.False(t, refused)

		msgs, refused = f.srv.OnGetHeaders(fullNodePeer("1.2.3.4:8333"), f.tip(t), nil)
		require.Nil(t, msgs)
		require.False(t, refused)
	})
}

// TestOnGetBlocks pins net_processing.cpp ProcessGetBlocks
// (net_processing.cpp:2551) rule by rule.
func TestOnGetBlocks(t *testing.T) {
	t.Run("an inv carries every block after the fork point", func(t *testing.T) {
		f := newServeFixture(t, 10)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs := f.srv.OnGetBlocks(peer,
			f.tip(t), getBlocksFor([]chainhash.Hash{f.at(6).BlockHash()}, chainhash.Hash{}))

		require.Equal(t, f.hashRange(7, 10), requireInvReply(t, msgs))
		require.Equal(t, chainhash.Hash{}, peer.hashContinue)
	})

	t.Run("hashStop is excluded from the inv", func(t *testing.T) {
		f := newServeFixture(t, 10)
		peer := fullNodePeer("1.2.3.4:8333")

		// C++ breaks BEFORE it pushes hashStop, the opposite of the getheaders
		// loop, so height 8 is the last hash served.
		msgs := f.srv.OnGetBlocks(peer,
			f.tip(t), getBlocksFor([]chainhash.Hash{f.at(3).BlockHash()}, f.at(9).BlockHash()))

		require.Equal(t, f.hashRange(4, 8), requireInvReply(t, msgs))
	})

	t.Run("an unknown locator starts after genesis", func(t *testing.T) {
		f := newServeFixture(t, 5)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs := f.srv.OnGetBlocks(peer,
			f.tip(t), getBlocksFor([]chainhash.Hash{{0xAB}}, chainhash.Hash{}))

		require.Equal(t, f.hashRange(1, 5), requireInvReply(t, msgs))
	})

	t.Run("an empty locator takes the same genesis fallback", func(t *testing.T) {
		// ProcessGetBlocks calls FindForkInGlobalIndex directly, so unlike
		// getheaders it has no empty-locator hashStop branch.
		f := newServeFixture(t, 5)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs := f.srv.OnGetBlocks(peer, f.tip(t), getBlocksFor(nil, chainhash.Hash{}))

		require.Equal(t, f.hashRange(1, 5), requireInvReply(t, msgs))
	})

	t.Run("a full inv sets the continue hash", func(t *testing.T) {
		f := newServeFixture(t, MaxGetBlocksResults+10)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs := f.srv.OnGetBlocks(peer,
			f.tip(t), getBlocksFor([]chainhash.Hash{f.genesis.BlockHash()}, chainhash.Hash{}))

		got := requireInvReply(t, msgs)
		require.Len(t, got, MaxGetBlocksResults)
		require.Equal(t, f.hashRange(1, MaxGetBlocksResults), got)

		// pfrom->hashContinue: the last hash of a full inv, which the getdata
		// path answers with an inv of our tip.
		require.Equal(t, f.at(MaxGetBlocksResults).BlockHash(), peer.hashContinue)
	})

	t.Run("an inv one short of the cap sets no continue hash", func(t *testing.T) {
		f := newServeFixture(t, MaxGetBlocksResults+10)
		peer := fullNodePeer("1.2.3.4:8333")

		// hashStop at the height that would have been the 500th hash, so the
		// loop breaks on hashStop before the cap is ever reached.
		msgs := f.srv.OnGetBlocks(peer, f.tip(t),
			getBlocksFor([]chainhash.Hash{f.genesis.BlockHash()}, f.at(MaxGetBlocksResults).BlockHash()))

		require.Len(t, requireInvReply(t, msgs), MaxGetBlocksResults-1)
		require.Equal(t, chainhash.Hash{}, peer.hashContinue)
	})

	t.Run("a peer level with our tip gets no inv at all", func(t *testing.T) {
		f := newServeFixture(t, 4)
		peer := fullNodePeer("1.2.3.4:8333")

		// Unlike getheaders, ProcessGetBlocks pushes no message when it has
		// nothing to offer.
		msgs := f.srv.OnGetBlocks(peer,
			f.tip(t), getBlocksFor([]chainhash.Hash{f.at(4).BlockHash()}, chainhash.Hash{}))

		require.Nil(t, msgs)
	})

	t.Run("serving stops at the active tip, not the header index tip", func(t *testing.T) {
		f := newServeFixture(t, 9)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs := f.srv.OnGetBlocks(peer,
			f.node(t, 5), getBlocksFor([]chainhash.Hash{f.genesis.BlockHash()}, chainhash.Hash{}))

		require.Equal(t, f.hashRange(1, 5), requireInvReply(t, msgs))
	})

	t.Run("headers-first catch-up does not refuse getblocks", func(t *testing.T) {
		// The refusal is about the cost of serving headers out of the store
		// during a round (legacy peer_server.go:1574-1589). SVNode's own IBD
		// gate is on getheaders alone too, and ProcessGetBlocks has none.
		cp := chainhash.Hash{0xC0}
		f := newServeFixture(t, 5, chaincfg.Checkpoint{Height: 1000, Hash: &cp})

		require.Len(t, f.hs.PeerEstablished(fullNodePeer("9.9.9.9:8333")), 1)
		require.True(t, f.hs.IsHeadersFirstMode())

		msgs := f.srv.OnGetBlocks(fullNodePeer("1.2.3.4:8333"),
			f.tip(t), getBlocksFor([]chainhash.Hash{f.genesis.BlockHash()}, chainhash.Hash{}))

		require.Equal(t, f.hashRange(1, 5), requireInvReply(t, msgs))
	})

	t.Run("an active tip the index cannot place answers nothing", func(t *testing.T) {
		f := newServeFixture(t, 3)
		peer := fullNodePeer("1.2.3.4:8333")

		msgs := f.srv.OnGetBlocks(peer, HeaderNode{Hash: chainhash.Hash{0x77}},
			getBlocksFor([]chainhash.Hash{f.genesis.BlockHash()}, chainhash.Hash{}))

		require.Nil(t, msgs)
	})

	t.Run("a nil peer or message answers nothing", func(t *testing.T) {
		f := newServeFixture(t, 3)

		require.Nil(t, f.srv.OnGetBlocks(nil, f.tip(t), getBlocksFor(nil, chainhash.Hash{})))
		require.Nil(t, f.srv.OnGetBlocks(fullNodePeer("1.2.3.4:8333"), f.tip(t), nil))
	})
}

func TestNewServing_RefusesMissingDependencies(t *testing.T) {
	f := newServeFixture(t, 1)

	_, err := NewServing(nil, f.hs)
	require.Error(t, err)

	_, err = NewServing(f.idx, nil)
	require.Error(t, err)
}
