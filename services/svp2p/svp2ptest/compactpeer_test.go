package svp2ptest

import (
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// multiTxBlock replaces the fixture chain's tip block with one carrying its
// coinbase plus extra transactions, and returns the block. The fixture chain
// mines coinbase-only blocks, and a compact block exercise needs slots a
// getblocktxn can ask for.
//
// The extra transactions are not valid spends and the block's merkle root is
// not recomputed: nothing in this file validates either. The scripted peer
// answers a getblocktxn from Chain.Blocks by index, and AnnounceCompact hashes
// each transaction, so distinct transactions are the whole requirement.
func multiTxBlock(t *testing.T, chain *FixtureChain, extra int) *wire.MsgBlock {
	t.Helper()

	block := chain.Blocks[chain.Tip()]
	require.NotNil(t, block)
	require.Len(t, block.Transactions, 1)

	for i := 0; i < extra; i++ {
		tx := wire.NewMsgTx(1)
		tx.LockTime = uint32(i + 1) //nolint:gosec // small test values

		require.NoError(t, block.AddTransaction(tx))
	}

	require.Len(t, block.Transactions, extra+1)

	return block
}

func getBlockTxnFor(hash chainhash.Hash, indexes []uint32) *wire.MsgGetBlockTxn {
	return wire.NewMsgGetBlockTxn(&hash, indexes)
}

// pingRoundTrip sends a ping and waits for its pong. The peer serves one
// connection from one goroutine, so a pong proves every message written before
// the ping has already been handled — which is how a test asserts that the
// peer answered NOTHING without waiting on a timeout.
func (c *rawClient) pingRoundTrip(nonce uint64) {
	c.t.Helper()

	c.write(wire.NewMsgPing(nonce))

	pong := c.readUntil("pong", 5*time.Second).(*wire.MsgPong)
	require.Equal(c.t, nonce, pong.Nonce)
}

func TestScriptedPeer_AnswersGetBlockTxnWithExactIndexes(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 5)
	hash := block.BlockHash()

	c := dialScripted(t, peer)
	c.write(getBlockTxnFor(hash, []uint32{1, 3, 4}))

	reply := c.readUntil(wire.CmdBlockTxn, 5*time.Second).(*wire.MsgBlockTxn)

	require.Equal(t, hash, reply.BlockHash)
	require.Len(t, reply.Transactions, 3)

	for i, index := range []uint32{1, 3, 4} {
		require.Equal(t, block.Transactions[index].TxHash(), reply.Transactions[i].TxHash(),
			"reply slot %d must carry block index %d", i, index)
	}

	require.Equal(t, 1, peer.Transcript.Count(In, wire.CmdGetBlockTxn))
	require.Equal(t, 1, peer.Transcript.Count(Out, wire.CmdBlockTxn))
}

// A getblocktxn cannot ask out of block order: the wire form stores each index
// as the difference from the previous one, so go-wire refuses to encode a
// sequence that does not increase. A scenario that wants to put an unordered
// request on the wire must frame it with Raw.
func TestScriptedPeer_GetBlockTxnCannotEncodeUnorderedIndexes(t *testing.T) {
	msg := getBlockTxnFor(chainhash.Hash{0xcc}, []uint32{4, 0, 2})

	err := wire.WriteMessage(io.Discard, msg, wire.ProtocolVersion, wire.TestNet)
	require.Error(t, err)
	require.Contains(t, err.Error(), "non-strictly-monotonic index")
}

// BlockTxnFor answers positionally: slot i of the reply is the transaction at
// the index the request listed at position i. The wire form only ever carries
// increasing indexes (see above), so this is asserted on the helper directly.
func TestScriptedPeer_BlockTxnForKeepsRequestOrder(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 4)

	hash := block.BlockHash()
	reply := peer.BlockTxnFor(getBlockTxnFor(hash, []uint32{4, 0, 2}))

	require.NotNil(t, reply)
	require.Equal(t, hash, reply.BlockHash)
	require.Len(t, reply.Transactions, 3)

	for i, index := range []uint32{4, 0, 2} {
		require.Equal(t, block.Transactions[index].TxHash(), reply.Transactions[i].TxHash(),
			"reply slot %d must carry block index %d", i, index)
	}

	require.Nil(t, peer.BlockTxnFor(getBlockTxnFor(chainhash.Hash{0xdd}, []uint32{0})))
	require.Nil(t, peer.BlockTxnFor(getBlockTxnFor(hash, []uint32{0, 99})))
}

func TestScriptedPeer_GetBlockTxnUnknownHashAnswersNothing(t *testing.T) {
	peer, _ := newTestPeer(t, 3, Script{})

	c := dialScripted(t, peer)
	c.write(getBlockTxnFor(chainhash.Hash{0xaa}, []uint32{0}))
	c.pingRoundTrip(7)

	require.Equal(t, 1, peer.Transcript.Count(In, wire.CmdGetBlockTxn))
	require.Equal(t, 0, peer.Transcript.Count(Out, wire.CmdBlockTxn))
}

func TestScriptedPeer_GetBlockTxnOutOfRangeIndexAnswersNothing(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 2)

	c := dialScripted(t, peer)
	c.write(getBlockTxnFor(block.BlockHash(), []uint32{1, 99}))
	c.pingRoundTrip(9)

	require.Equal(t, 1, peer.Transcript.Count(In, wire.CmdGetBlockTxn))
	require.Equal(t, 0, peer.Transcript.Count(Out, wire.CmdBlockTxn))
}

func TestScriptedPeer_ScriptOverridesGetBlockTxn(t *testing.T) {
	// The reply is built HERE, on the test goroutine, and the hook only hands
	// it back. A require inside the hook would run on the peer's serve
	// goroutine, where a failure calls runtime.Goexit off the test goroutine:
	// that kills the serve loop and turns a clear assertion failure into a
	// five second readUntil timeout.
	//
	// A peer that answers a gap request with one transaction too few is the
	// READ_STATUS_INVALID offence a parity scenario wants to script.
	short := wire.NewMsgBlockTxn(&chainhash.Hash{})
	require.NoError(t, short.AddTransaction(wire.NewMsgTx(1)))

	script := Script{OnGetBlockTxn: func(p *ScriptedPeer, conn net.Conn, m *wire.MsgGetBlockTxn) []wire.Message {
		short.BlockHash = m.BlockHash

		return []wire.Message{short}
	}}

	peer, chain := newTestPeer(t, 3, script)
	block := multiTxBlock(t, chain, 3)

	c := dialScripted(t, peer)
	c.write(getBlockTxnFor(block.BlockHash(), []uint32{1, 2}))

	reply := c.readUntil(wire.CmdBlockTxn, 5*time.Second).(*wire.MsgBlockTxn)
	require.Len(t, reply.Transactions, 1)
	require.Equal(t, block.BlockHash(), reply.BlockHash)

	// Read the request back from the Transcript rather than from a variable the
	// hook wrote on another goroutine: the transcript is mutex-guarded, so this
	// declares the happens-before edge instead of relying on the socket round
	// trip to provide one.
	entry, ok := peer.Transcript.FirstOn(In, wire.CmdGetBlockTxn)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 2}, entry.Msg.(*wire.MsgGetBlockTxn).Indexes)
}

// The wire form of getblocktxn stores each index as the difference from the
// previous one minus one, so a sparse request only survives if go-wire's
// differential codec is symmetric. The hash is deliberately unknown: this
// asserts on what the peer DECODED, not on what it answered.
func TestScriptedPeer_GetBlockTxnDifferentialIndexesRoundTrip(t *testing.T) {
	peer, _ := newTestPeer(t, 3, Script{})

	want := []uint32{0, 1, 5, 7, 1000, 65536}

	c := dialScripted(t, peer)
	c.write(getBlockTxnFor(chainhash.Hash{0xbb}, want))
	c.pingRoundTrip(11)

	entry, ok := peer.Transcript.FirstOn(In, wire.CmdGetBlockTxn)
	require.True(t, ok)

	decoded, ok := entry.Msg.(*wire.MsgGetBlockTxn)
	require.True(t, ok)
	require.Equal(t, want, decoded.Indexes)
}

// The whole value of keeping a second short ID derivation in svp2ptest is that
// the two are INDEPENDENT: the harness must not share the code under test. This
// is what holds them together. It compares the peer's derivation against the
// node's (services/svp2p/protocol) over real fixture headers and real
// transaction hashes — the SipHash key from SHA256(header || nonce) and the
// BIP152 short IDs it produces.
//
// If either copy drifts, this fails before any scenario built on
// AnnounceCompact can quietly agree with a wrong node.
func TestScriptedPeer_ShortIDDerivationAgreesWithProtocol(t *testing.T) {
	chain := BuildFixtureChain(t, test.CreateBaseTestSettings(t), 4)
	block := multiTxBlock(t, chain, 6)

	nonces := []uint64{0, 1, 0x0123456789abcdef, ^uint64(0)}

	for _, header := range chain.Headers {
		for _, nonce := range nonces {
			k0, k1 := shortIDKeys(header, nonce)
			wantK0, wantK1 := protocol.ShortIDKeys(header, nonce)

			require.Equal(t, wantK0, k0, "k0 must agree for header %s nonce %d", header.BlockHash(), nonce)
			require.Equal(t, wantK1, k1, "k1 must agree for header %s nonce %d", header.BlockHash(), nonce)

			for _, tx := range block.Transactions {
				hash := tx.TxHash()

				require.Equal(t, protocol.ShortID(k0, k1, hash), shortID(k0, k1, hash),
					"short id must agree for tx %s nonce %d", hash, nonce)
			}
		}
	}

	// A short ID is masked to the low 48 bits (BIP152: SHORTTXIDS_LENGTH is 6
	// bytes), so it must always fit the 6 byte wire field.
	k0, k1 := shortIDKeys(chain.Headers[0], 7)
	require.Less(t, shortID(k0, k1, block.Transactions[0].TxHash()), uint64(1)<<48)

}

func TestScriptedPeer_AnnounceCompactCarriesShortIDsAndPrefilled(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 4)

	c := dialScripted(t, peer)

	const nonce = uint64(0x0123456789abcdef)

	require.NoError(t, peer.AnnounceCompact(block, nonce, []int{0, 2}))

	cmpct := c.readUntil(wire.CmdCmpctBlock, 5*time.Second).(*wire.MsgCmpctBlock)

	require.Equal(t, block.BlockHash(), cmpct.Header.BlockHash())
	require.Equal(t, nonce, cmpct.Nonce)
	require.Equal(t, len(block.Transactions), cmpct.BlockTxCount())

	require.Len(t, cmpct.PrefilledTxn, 2)
	require.Equal(t, uint32(0), cmpct.PrefilledTxn[0].Index)
	require.Equal(t, uint32(2), cmpct.PrefilledTxn[1].Index)
	require.Equal(t, block.Transactions[0].TxHash(), cmpct.PrefilledTxn[0].Tx.TxHash())
	require.Equal(t, block.Transactions[2].TxHash(), cmpct.PrefilledTxn[1].Tx.TxHash())

	// The short IDs are checked against the node's own derivation
	// (protocol.ShortIDKeys/ShortID), not against the peer's: svp2ptest
	// carries its own copy so the harness never shares the code under test,
	// and this is what makes the two copies agree.
	k0, k1 := protocol.ShortIDKeys(&cmpct.Header, nonce)

	require.Len(t, cmpct.ShortIDs, 3)

	for i, index := range []int{1, 3, 4} {
		require.Equal(t, protocol.ShortID(k0, k1, block.Transactions[index].TxHash()), cmpct.ShortIDs[i],
			"short id %d must be the id of block index %d", i, index)
	}

	require.Equal(t, 1, peer.Transcript.Count(Out, wire.CmdCmpctBlock))
}

// Prefilled indexes are written differentially and must be strictly
// increasing, so AnnounceCompact sorts and de-duplicates what it is handed
// rather than letting go-wire refuse the message.
func TestScriptedPeer_AnnounceCompactNormalisesPrefilledIndexes(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 4)

	c := dialScripted(t, peer)

	require.NoError(t, peer.AnnounceCompact(block, 1, []int{3, 0, 3}))

	cmpct := c.readUntil(wire.CmdCmpctBlock, 5*time.Second).(*wire.MsgCmpctBlock)

	require.Len(t, cmpct.PrefilledTxn, 2)
	require.Equal(t, uint32(0), cmpct.PrefilledTxn[0].Index)
	require.Equal(t, uint32(3), cmpct.PrefilledTxn[1].Index)
	require.Equal(t, len(block.Transactions), cmpct.BlockTxCount())
}

func TestScriptedPeer_AnnounceCompactRejectsOutOfRangePrefilled(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 2)

	_ = dialScripted(t, peer)

	require.Error(t, peer.AnnounceCompact(block, 1, []int{0, 9}))
	require.Equal(t, 0, peer.Transcript.Count(Out, wire.CmdCmpctBlock))
}

// AnnounceCompact is a Send, so it reaches every GENERAL connection this peer
// holds — the node may have more than one association open.
func TestScriptedPeer_AnnounceCompactReachesEveryGeneralConn(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 2)

	first := dialScripted(t, peer)
	second := dialScripted(t, peer)

	require.NoError(t, peer.AnnounceCompact(block, 4, []int{0}))

	for _, c := range []*rawClient{first, second} {
		cmpct := c.readUntil(wire.CmdCmpctBlock, 5*time.Second).(*wire.MsgCmpctBlock)
		require.Equal(t, block.BlockHash(), cmpct.Header.BlockHash())
	}

	require.Equal(t, 2, peer.Transcript.Count(Out, wire.CmdCmpctBlock))
}

func TestScriptedPeer_RecordsSendCmpctAndAnswersNothingByDefault(t *testing.T) {
	peer, _ := newTestPeer(t, 3, Script{})

	c := dialScripted(t, peer)
	c.write(wire.NewMsgSendcmpct(true))
	c.pingRoundTrip(13)

	entry, ok := peer.Transcript.FirstOn(In, wire.CmdSendcmpct)
	require.True(t, ok)

	decoded, ok := entry.Msg.(*wire.MsgSendcmpct)
	require.True(t, ok)
	require.True(t, decoded.SendCmpct)
	require.Equal(t, uint64(1), decoded.Version)

	require.Equal(t, 0, peer.Transcript.Count(Out, wire.CmdSendcmpct))
}

func TestScriptedPeer_ScriptOverridesSendCmpct(t *testing.T) {
	script := Script{OnSendCmpct: func(p *ScriptedPeer, conn net.Conn, m *wire.MsgSendcmpct) []wire.Message {
		return []wire.Message{wire.NewMsgSendcmpct(false)}
	}}

	peer, _ := newTestPeer(t, 3, script)

	c := dialScripted(t, peer)
	c.write(wire.NewMsgSendcmpct(true))

	reply := c.readUntil(wire.CmdSendcmpct, 5*time.Second).(*wire.MsgSendcmpct)
	require.False(t, reply.SendCmpct)

	// Read the request from the Transcript, not from a variable written on the
	// serve goroutine. See TestScriptedPeer_ScriptOverridesGetBlockTxn.
	entry, ok := peer.Transcript.FirstOn(In, wire.CmdSendcmpct)
	require.True(t, ok)
	require.True(t, entry.Msg.(*wire.MsgSendcmpct).SendCmpct)
}

// Every scripted answer names the request it answers, so a scenario can judge
// a reply by when its request arrived rather than by transcript position.
func TestScriptedPeer_CompactRepliesNameTheirRequest(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 3)

	c := dialScripted(t, peer)
	c.write(getBlockTxnFor(block.BlockHash(), []uint32{1}))
	c.readUntil(wire.CmdBlockTxn, 5*time.Second)

	request, ok := peer.Transcript.FirstOn(In, wire.CmdGetBlockTxn)
	require.True(t, ok)

	reply, ok := peer.Transcript.FirstOn(Out, wire.CmdBlockTxn)
	require.True(t, ok)

	require.Equal(t, request.Seq, reply.ReplyTo)
}

// Announcing to nobody is an error. A scenario that races the node's connect
// would otherwise pass here and then block waiting for a getblocktxn that no
// peer was ever told to expect.
func TestScriptedPeer_AnnounceCompactWithNoGeneralConnIsAnError(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 2)

	err := peer.AnnounceCompact(block, 1, []int{0})

	require.Error(t, err)
	require.True(t, errors.Is(err, errors.ErrProcessing), "want a teranode processing error, got %v", err)
	require.Contains(t, err.Error(), "no general connection")
	require.Equal(t, 0, peer.Transcript.Count(Out, wire.CmdCmpctBlock))

	// It starts working the moment a connection exists, so the error is about
	// having no audience and nothing else.
	_ = dialScripted(t, peer)
	require.NoError(t, peer.AnnounceCompact(block, 1, []int{0}))
}

// A getblocktxn asking for nothing is answered with an empty blocktxn, which is
// what serving exactly the requested indexes means when there are none.
func TestScriptedPeer_GetBlockTxnEmptyIndexListAnswersEmptyBlockTxn(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 2)

	hash := block.BlockHash()

	reply := peer.BlockTxnFor(getBlockTxnFor(hash, nil))
	require.NotNil(t, reply)
	require.Equal(t, hash, reply.BlockHash)
	require.Empty(t, reply.Transactions)

	c := dialScripted(t, peer)
	c.write(getBlockTxnFor(hash, nil))

	onWire := c.readUntil(wire.CmdBlockTxn, 5*time.Second).(*wire.MsgBlockTxn)
	require.Equal(t, hash, onWire.BlockHash)
	require.Empty(t, onWire.Transactions)
}

// Writes on one connection are serialised, so the Transcript records messages
// in the order their bytes reached the socket.
//
// The bytes themselves never interleave even without the lock: go-wire writes
// header and payload in a single w.Write, and net.TCPConn.Write holds the
// connection's own write lock for the whole call. The TRANSCRIPT is what needs
// serialising — recording happens after the write returns, so two unsynchronised
// writers can record in the opposite order to the order they wrote, which breaks
// the promise Entry.Seq makes in its doc comment.
func TestScriptedPeer_ConcurrentWritesKeepTranscriptInWireOrder(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 4)

	c := dialScripted(t, peer)

	conns := peer.Conns()
	require.Len(t, conns, 1)

	conn := conns[0]

	const (
		writers = 16
		each    = 60
	)

	var wg sync.WaitGroup

	for w := 0; w < writers; w++ {
		wg.Add(1)

		go func(w int) {
			defer wg.Done()

			for i := 0; i < each; i++ {
				// The nonce is this message's identity, so the two orders can
				// be compared element by element.
				msg, err := peer.CompactBlockFor(block, uint64(w*each+i), []int{0, 2}) //nolint:gosec // small test values
				if err != nil {
					return
				}

				_ = peer.Write(conn, msg)
			}
		}(w)
	}

	var onWire []uint64

	for len(onWire) < writers*each {
		require.NoError(t, c.conn.SetReadDeadline(time.Now().Add(20*time.Second)))

		msg, _, err := wire.ReadMessage(c.conn, wire.ProtocolVersion, c.net)
		require.NoError(t, err, "frame %d failed to decode", len(onWire))

		if cmpct, ok := msg.(*wire.MsgCmpctBlock); ok {
			onWire = append(onWire, cmpct.Nonce)
		}
	}

	wg.Wait()

	var recorded []uint64

	for _, e := range peer.Transcript.Snapshot() {
		if e.Dir == Out && e.Cmd == wire.CmdCmpctBlock {
			recorded = append(recorded, e.Msg.(*wire.MsgCmpctBlock).Nonce)
		}
	}

	require.Equal(t, onWire, recorded, "transcript order must equal wire order")
}

// AnnounceCompact runs on the scenario goroutine while the peer's serve
// goroutine answers requests on the same connection. Every frame must still
// decode, and every reply must still be recorded.
func TestScriptedPeer_AnnounceRacingRepliesOnOneConn(t *testing.T) {
	peer, chain := newTestPeer(t, 3, Script{})
	block := multiTxBlock(t, chain, 4)

	c := dialScripted(t, peer)
	hash := block.BlockHash()

	const rounds = 60

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < rounds; i++ {
			_ = peer.AnnounceCompact(block, uint64(i), []int{0, 2}) //nolint:gosec // small test values
		}
	}()

	for i := 0; i < rounds; i++ {
		c.write(getBlockTxnFor(hash, []uint32{1, 3}))
	}

	announced, replied := 0, 0

	for announced < rounds || replied < rounds {
		require.NoError(t, c.conn.SetReadDeadline(time.Now().Add(20*time.Second)))

		msg, _, err := wire.ReadMessage(c.conn, wire.ProtocolVersion, c.net)
		require.NoError(t, err, "a frame failed to decode after %d announcements and %d replies", announced, replied)

		switch m := msg.(type) {
		case *wire.MsgCmpctBlock:
			require.Equal(t, hash, m.Header.BlockHash())

			announced++

		case *wire.MsgBlockTxn:
			require.Len(t, m.Transactions, 2)
			require.Equal(t, block.Transactions[1].TxHash(), m.Transactions[0].TxHash())
			require.Equal(t, block.Transactions[3].TxHash(), m.Transactions[1].TxHash())

			replied++
		}
	}

	wg.Wait()

	require.Equal(t, rounds, peer.Transcript.Count(Out, wire.CmdCmpctBlock))
	require.Equal(t, rounds, peer.Transcript.Count(Out, wire.CmdBlockTxn))
}
