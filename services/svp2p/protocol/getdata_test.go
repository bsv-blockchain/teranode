package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// --- fixtures -------------------------------------------------------------

// stubFetcher stands in for the Teranode read path. It is a test double only
// for the store behind the narrow interface, never for the getdata logic under
// test: every classification, ordering and notfound decision below is made by
// the real Serving machine and the real peer serve loop.
type stubFetcher struct {
	// mu guards every map: FetchBlock is called from TWO goroutines for one
	// served block — the serve loop hashes the first pass, the transport writer
	// opens the second — and a test observes the counter while both run.
	mu sync.Mutex

	blocks map[chainhash.Hash][]byte
	txs    map[chainhash.Hash][]byte

	// blockErr and txErr override a present entry with a lookup outcome.
	blockErr map[chainhash.Hash]error
	txErr    map[chainhash.Hash]error

	// declared overrides the length FetchBlock reports, so a test can make the
	// store lie about a block's size.
	declared map[chainhash.Hash]uint64

	// blockOpens counts FetchBlock calls, which is what pins the cost of the
	// two-pass checksum.
	blockOpens map[chainhash.Hash]int
}

func newStubFetcher() *stubFetcher {
	return &stubFetcher{
		blocks:     map[chainhash.Hash][]byte{},
		txs:        map[chainhash.Hash][]byte{},
		blockErr:   map[chainhash.Hash]error{},
		txErr:      map[chainhash.Hash]error{},
		declared:   map[chainhash.Hash]uint64{},
		blockOpens: map[chainhash.Hash]int{},
	}
}

func (s *stubFetcher) FetchBlock(_ context.Context, hash *chainhash.Hash) (io.ReadCloser, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.blockOpens[*hash]++

	if err := s.blockErr[*hash]; err != nil {
		return nil, 0, err
	}

	raw, ok := s.blocks[*hash]
	if !ok {
		return nil, 0, errors.NewBlockNotFoundError("block %s not held", hash.String())
	}

	length := uint64(len(raw))
	if d, ok := s.declared[*hash]; ok {
		length = d
	}

	return io.NopCloser(bytes.NewReader(raw)), length, nil
}

// opens reports how many times a block has been fetched, safely.
func (s *stubFetcher) opens(hash chainhash.Hash) int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.blockOpens[hash]
}

func (s *stubFetcher) FetchTx(_ context.Context, hash *chainhash.Hash) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.txErr[*hash]; err != nil {
		return nil, err
	}

	raw, ok := s.txs[*hash]
	if !ok {
		return nil, errors.NewTxNotFoundError("tx %s not held", hash.String())
	}

	return raw, nil
}

// servingDispatcher is the peer loop's sync dispatch reduced to what the
// getdata path uses, driven by a REAL Serving machine over a real header
// index. Everything else is a no-op, so a test peer can be run without the
// whole PeerManager.
type servingDispatcher struct {
	srv       *Serving
	sp        *SyncPeer
	activeTip HeaderNode
}

func (d *servingDispatcher) Established(*SyncPeer, wire.ServiceFlag) []wire.Message { return nil }

func (d *servingDispatcher) Headers(*SyncPeer, *wire.MsgHeaders) ([]wire.Message, int, error) {
	return nil, 0, nil
}

func (d *servingDispatcher) Inv(*SyncPeer, *wire.MsgInv) ([]wire.Message, error) { return nil, nil }

func (d *servingDispatcher) GetHeaders(*SyncPeer, *wire.MsgGetHeaders) []wire.Message { return nil }

func (d *servingDispatcher) GetBlocks(*SyncPeer, *wire.MsgGetBlocks) []wire.Message { return nil }

func (d *servingDispatcher) GetData(_ *SyncPeer, msg *wire.MsgGetData) []getDataItem {
	return d.srv.OnGetData(msg)
}

func (d *servingDispatcher) ContinueInv(sp *SyncPeer, hash chainhash.Hash) []wire.Message {
	return d.srv.ContinueInv(sp, d.activeTip, hash)
}

func (d *servingDispatcher) BlockExpected(*SyncPeer, chainhash.Hash) bool { return false }

func (d *servingDispatcher) BlockDone(*SyncPeer, chainhash.Hash, IngestOutcome) (int, error) {
	return 0, nil
}

// testWireBlock builds a small but real legacy-wire block: 80 byte header,
// transaction count varint, transactions. Those are exactly the bytes
// FetchBlock streams.
func testWireBlock(t *testing.T, seed byte, txCount int) (*wire.MsgBlock, []byte) {
	t.Helper()

	hdr := wire.NewBlockHeader(1, &chainhash.Hash{seed}, &chainhash.Hash{seed + 1}, 0x1d00ffff, uint32(seed))
	hdr.Timestamp = time.Unix(1700000000, 0)

	blk := wire.NewMsgBlock(hdr)

	for i := 0; i < txCount; i++ {
		blk.AddTransaction(testWireTx(byte(i))) //nolint:errcheck,gosec // small test loop
	}

	var buf bytes.Buffer
	require.NoError(t, blk.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding))

	return blk, buf.Bytes()
}

func testWireTx(seed byte) *wire.MsgTx {
	tx := wire.NewMsgTx(1)
	tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&chainhash.Hash{seed}, uint32(seed)), []byte{0x51, seed}))
	tx.AddTxOut(wire.NewTxOut(int64(1000)+int64(seed), []byte{0x76, 0xa9, seed}))

	return tx
}

func txBytes(t *testing.T, tx *wire.MsgTx) []byte {
	t.Helper()

	var buf bytes.Buffer
	require.NoError(t, tx.Serialize(&buf))

	return buf.Bytes()
}

func getDataFor(entries ...*wire.InvVect) *wire.MsgGetData {
	msg := wire.NewMsgGetData()
	msg.InvList = entries

	return msg
}

// newServingTestPeer runs a peer whose getdata path is fully wired: the real
// Serving machine over the fixture's index, and the stub read path.
func newServingTestPeer(t *testing.T, f *serveFixture, fetch *stubFetcher) (*Peer, *scriptedPeer, *SyncPeer) {
	t.Helper()

	return newServingTestPeerWithIdle(t, f, fetch, time.Hour)
}

func newServingTestPeerWithIdle(t *testing.T, f *serveFixture, fetch *stubFetcher, idle time.Duration) (*Peer, *scriptedPeer, *SyncPeer) {
	t.Helper()

	p, far := newTestPeer(t, idle, time.Hour)

	sp := NewSyncPeer("127.0.0.1:8333", 0, newPeerSyncState())

	p.cfg.Sync = &servingDispatcher{srv: f.srv, sp: sp, activeTip: f.tip(t)}
	p.cfg.SyncPeer = sp
	p.cfg.Fetcher = fetch

	return p, far, sp
}

// newServingTestPeerWithConnVersion is newServingTestPeer with the transport
// connection's OWN initial protocol version pinned to connVersion, instead of
// wire.ProtocolVersion. Production always constructs the Conn at our own
// wire.ProtocolVersion (manager.go:753), which already sits at
// transport.ExtendedPayloadVersion — so a test built on that default cannot
// tell "the handshake wired the negotiated version onto the Conn" apart from
// "the Conn's untouched construction default happened to be high enough".
// Pinning connVersion below the extended floor here makes an extended send
// succeed ONLY if something moves the Conn's version up after the handshake.
func newServingTestPeerWithConnVersion(t *testing.T, f *serveFixture, fetch *stubFetcher, connVersion uint32) (*Peer, *scriptedPeer, *SyncPeer) {
	t.Helper()

	a, b := net.Pipe()
	conn := transport.New(a, transport.Config{
		Net: wire.MainNet, ProtocolVersion: connVersion,
		SendBudgetBytes: 1 << 20, RecvQueueLen: 32, WriteTimeout: 5 * time.Second,
	})

	cfg := PeerConfig{
		Handshake: HandshakeConfig{
			Inbound: false, Nonce: 7777, UserAgent: "/teranode-svp2p:0.1.0/",
			StartingHeight: 0, MaxRecvPayloadLength: wire.DefaultMaxRecvPayloadLength,
			AllowBlockPriority: true,
			LocalAddr:          wire.NewNetAddressIPPort(nil, 8333, 0),
			RemoteAddr:         wire.NewNetAddressIPPort(nil, 8333, 0),
		},
		Conn: conn, Logger: ulogger.TestLogger{},
		IdleTimeout: time.Hour, PingInterval: time.Hour, BanThreshold: 100,
	}

	p := NewPeer(cfg)
	far := &scriptedPeer{nc: b}

	sp := NewSyncPeer("127.0.0.1:8333", 0, newPeerSyncState())

	p.cfg.Sync = &servingDispatcher{srv: f.srv, sp: sp, activeTip: f.tip(t)}
	p.cfg.SyncPeer = sp
	p.cfg.Fetcher = fetch

	return p, far, sp
}

// --- Serving.OnGetData: the ProcessGetData branch table -------------------

func TestOnGetData_Classification(t *testing.T) {
	f := newServeFixture(t, 3)

	txHash := chainhash.Hash{0xaa}
	blockHash := chainhash.Hash{0xbb}

	t.Run("request order is preserved across kinds", func(t *testing.T) {
		items := f.srv.OnGetData(getDataFor(
			wire.NewInvVect(wire.InvTypeTx, &txHash),
			wire.NewInvVect(wire.InvTypeBlock, &blockHash),
			wire.NewInvVect(wire.InvTypeTx, &txHash),
		))

		require.Len(t, items, 3)
		require.Equal(t, getDataTx, items[0].kind)
		require.Equal(t, getDataBlock, items[1].kind)
		require.Equal(t, getDataTx, items[2].kind)
	})

	t.Run("a filtered block is its own kind, and is a block type", func(t *testing.T) {
		items := f.srv.OnGetData(getDataFor(wire.NewInvVect(wire.InvTypeFilteredBlock, &blockHash)))

		require.Len(t, items, 1)
		require.Equal(t, getDataFilteredBlock, items[0].kind)

		// protocol.h:577 IsBlockType covers MSG_FILTERED_BLOCK, and it is
		// IsBlockType that ends a serving pass.
		require.True(t, items[0].kind.blockType())
	})

	t.Run("an inv type we do not implement is unsupported and not a block type", func(t *testing.T) {
		items := f.srv.OnGetData(getDataFor(&wire.InvVect{Type: wire.InvType(99), Hash: blockHash}))

		require.Len(t, items, 1)
		require.Equal(t, getDataUnsupported, items[0].kind)
		require.False(t, items[0].kind.blockType())
	})

	t.Run("only MSG_BLOCK and MSG_FILTERED_BLOCK are block types", func(t *testing.T) {
		require.True(t, getDataBlock.blockType())
		require.True(t, getDataFilteredBlock.blockType())
		require.False(t, getDataTx.blockType())
		require.False(t, getDataUnsupported.blockType())
	})

	t.Run("nil entries and a nil message are dropped, never dereferenced", func(t *testing.T) {
		items := f.srv.OnGetData(getDataFor(nil, wire.NewInvVect(wire.InvTypeTx, &txHash), nil))
		require.Len(t, items, 1)

		require.Empty(t, f.srv.OnGetData(nil))
	})
}

// TestContinueInv pins the net_processing.cpp ProcessGetData continuation
// (:1364-1377): the hash that closed a full getblocks inv is answered with the
// block and then an inv of the ACTIVE tip, and the trigger is one-shot.
func TestContinueInv(t *testing.T) {
	f := newServeFixture(t, 4)
	tip := f.tip(t)

	t.Run("no continuation pending", func(t *testing.T) {
		sp := NewSyncPeer("a", 0, newPeerSyncState())
		require.Empty(t, f.srv.ContinueInv(sp, tip, f.at(2).BlockHash()))
	})

	t.Run("a zero hashContinue never matches, including a zero request", func(t *testing.T) {
		sp := NewSyncPeer("a", 0, newPeerSyncState())
		require.Empty(t, f.srv.ContinueInv(sp, tip, chainhash.Hash{}))
	})

	t.Run("the matching hash yields an inv of the active tip, once", func(t *testing.T) {
		sp := NewSyncPeer("a", 0, newPeerSyncState())
		trigger := f.at(2).BlockHash()
		sp.hashContinue = trigger

		msgs := f.srv.ContinueInv(sp, tip, trigger)

		require.Len(t, msgs, 1)

		inv, ok := msgs[0].(*wire.MsgInv)
		require.True(t, ok, "expected an inv, got %s", msgs[0].Command())
		require.Len(t, inv.InvList, 1)
		require.Equal(t, wire.InvTypeBlock, inv.InvList[0].Type)
		require.Equal(t, tip.Hash, inv.InvList[0].Hash)

		require.Equal(t, chainhash.Hash{}, sp.hashContinue, "the trigger must be cleared")
		require.Empty(t, f.srv.ContinueInv(sp, tip, trigger), "the trigger must be one-shot")
	})

	t.Run("a different hash leaves the trigger armed", func(t *testing.T) {
		sp := NewSyncPeer("a", 0, newPeerSyncState())
		sp.hashContinue = f.at(2).BlockHash()

		require.Empty(t, f.srv.ContinueInv(sp, tip, f.at(3).BlockHash()))
		require.Equal(t, f.at(2).BlockHash(), sp.hashContinue)
	})
}

// --- the served leg, end to end over a real socket ------------------------

// TestServeGetData_MixedListInRequestOrder is the brief's protocol table plus
// its end-to-end leg. The far side is go-wire's own reader, so every frame's
// checksum is verified against its payload: a block streamed with a wrong
// checksum, or a lane that misaligned the socket, fails here.
func TestServeGetData_MixedListInRequestOrder(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	blk, raw := testWireBlock(t, 0x40, 3)
	blockHash := blk.BlockHash()
	fetch.blocks[blockHash] = raw

	tx := testWireTx(9)
	txHash := tx.TxHash()
	fetch.txs[txHash] = txBytes(t, tx)

	missing := chainhash.Hash{0xde, 0xad}

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = p.Run(ctx) }()

	completeHandshake(t, far)

	far.write(t, getDataFor(
		wire.NewInvVect(wire.InvTypeTx, &txHash),
		wire.NewInvVect(wire.InvTypeBlock, &blockHash),
		wire.NewInvVect(wire.InvTypeTx, &missing),
	))

	gotTx, ok := far.read(t).(*wire.MsgTx)
	require.True(t, ok, "the tx must come first, in request order")
	require.Equal(t, txHash, gotTx.TxHash())

	gotBlock, ok := far.read(t).(*wire.MsgBlock)
	require.True(t, ok, "the block must come second")
	require.Equal(t, blockHash, gotBlock.BlockHash())

	// Step 3 of the brief: the bytes the peer received are the bytes we hold.
	var back bytes.Buffer
	require.NoError(t, gotBlock.BsvEncode(&back, wire.ProtocolVersion, wire.BaseEncoding))
	require.Equal(t, raw, back.Bytes())

	nf, ok := far.read(t).(*wire.MsgNotFound)
	require.True(t, ok, "one trailing notfound must close the reply")
	require.Len(t, nf.InvList, 1)
	require.Equal(t, missing, nf.InvList[0].Hash)
	require.Equal(t, wire.InvTypeTx, nf.InvList[0].Type)

	// B7(2): the measured cost of the two-pass checksum on this path.
	require.Equal(t, 2, fetch.opens(blockHash), "a served block is read exactly twice")
}

// TestServeGetData_ContinueInvFollowsTheBlock is the continuation on the wire:
// the inv of our tip must arrive right after the block, with nothing between.
func TestServeGetData_ContinueInvFollowsTheBlock(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	blk, raw := testWireBlock(t, 0x50, 2)
	blockHash := blk.BlockHash()
	fetch.blocks[blockHash] = raw

	p, far, sp := newServingTestPeer(t, f, fetch)
	sp.hashContinue = blockHash

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = p.Run(ctx) }()

	completeHandshake(t, far)

	far.write(t, getDataFor(wire.NewInvVect(wire.InvTypeBlock, &blockHash)))

	gotBlock, ok := far.read(t).(*wire.MsgBlock)
	require.True(t, ok)
	require.Equal(t, blockHash, gotBlock.BlockHash())

	inv, ok := far.read(t).(*wire.MsgInv)
	require.True(t, ok, "the continuation inv must follow the block immediately")
	require.Len(t, inv.InvList, 1)
	require.Equal(t, f.tip(t).Hash, inv.InvList[0].Hash)
}

// TestServeGetData_BlockAboveTheFramingLimit_OldPeer is the OPEN QUESTION 5
// answer for a peer that never negotiated the extended header, and the ONE
// case left where a block enters a notfound. It is a deliberate divergence,
// not an instance of the missing-block rule: SVNode frames a payload this
// large with an extended header for any peer (protocol.cpp:220-237), but this
// service only extends it to a peer whose negotiated version reaches
// EXTENDED_PAYLOAD_VERSION (version.h:51) — SVNode's own
// CMessageHeader::GetMaxPayloadLength(version) draws the same floor. Nothing
// of the block reaches the socket.
func TestServeGetData_BlockAboveTheFramingLimit_OldPeer(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	blk, raw := testWireBlock(t, 0x60, 1)
	blockHash := blk.BlockHash()
	fetch.blocks[blockHash] = raw
	fetch.declared[blockHash] = transport.MaxBlockFrameBytes + 1

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = p.Run(ctx) }()

	completeHandshakeWithProtocolVersion(t, far, int32(transport.ExtendedPayloadVersion-1)) //nolint:gosec // fixed test constant, fits int32

	far.write(t, getDataFor(wire.NewInvVect(wire.InvTypeBlock, &blockHash)))

	nf, ok := far.read(t).(*wire.MsgNotFound)
	require.True(t, ok, "an unframeable block must be answered with notfound")
	require.Len(t, nf.InvList, 1)
	require.Equal(t, blockHash, nf.InvList[0].Hash)

	require.Equal(t, 1, fetch.opens(blockHash), "the refusal must not pay for a second read")
}

// TestServeGetData_BlockAboveTheFramingLimit_ExtendedPeer is the OPEN
// QUESTION 5 answer's mirror: a peer that negotiated
// transport.ExtendedPayloadVersion gets the block framed with the extended
// header instead of notfound.
//
// The Conn under test is built at a version BELOW the extended floor
// (newServingTestPeerWithConnVersion), so the extended frame below can only
// appear on the wire if completing the handshake pushed the negotiated
// version onto the Conn — proving peer.go's post-handshake
// SetProtocolVersion call, not merely getdata.go's own version check.
//
// The payload itself is never streamed: MaxBlockFrameBytes+1 declared bytes
// is over 4 GiB, and proving the frame started is exactly what "a SendBlock
// happened" needs. Only the 44 byte extended header (extheader.go) is read
// off the wire; the connection is then torn down without draining the body
// SendBlock is still trying to write.
func TestServeGetData_BlockAboveTheFramingLimit_ExtendedPeer(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	blk, raw := testWireBlock(t, 0x60, 1)
	blockHash := blk.BlockHash()
	fetch.blocks[blockHash] = raw
	fetch.declared[blockHash] = transport.MaxBlockFrameBytes + 1

	p, far, _ := newServingTestPeerWithConnVersion(t, f, fetch, transport.ExtendedPayloadVersion-1)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = p.Run(ctx) }()

	completeHandshake(t, far)

	far.write(t, getDataFor(wire.NewInvVect(wire.InvTypeBlock, &blockHash)))

	require.NoError(t, far.nc.SetReadDeadline(time.Now().Add(5*time.Second)))

	hdr := make([]byte, wire.MessageHeaderSize+wire.CommandSize+8)
	_, err := io.ReadFull(far.nc, hdr)
	require.NoError(t, err, "the extended header must reach the wire")

	require.Equal(t, uint32(wire.MainNet), binary.LittleEndian.Uint32(hdr[0:4]), "network magic")
	require.Equal(t, wire.CmdExtMsg, cmdString(hdr[4:16]), "extmsg marker command")
	require.Equal(t, uint32(0xffffffff), binary.LittleEndian.Uint32(hdr[16:20]), "basic length field pinned to the reserved marker")
	require.Equal(t, [4]byte{}, [4]byte(hdr[20:24]), "the extended path carries a zero checksum")
	require.Equal(t, wire.CmdBlock, cmdString(hdr[24:36]), "the real command in the extension")
	require.Equal(t, transport.MaxBlockFrameBytes+1, binary.LittleEndian.Uint64(hdr[36:44]), "the declared payload length")

	require.NoError(t, far.nc.Close())
}

// cmdString trims a fixed-width, NUL-padded wire command field down to its
// text, the same convention go-wire's own header reader applies.
func cmdString(field []byte) string {
	i := bytes.IndexByte(field, 0)
	if i < 0 {
		i = len(field)
	}

	return string(field[:i])
}

// TestServeGetData_LookupFailureIsNotNotFound is the B4 split. A store that
// failed is not a store that does not hold the data: claiming notfound would
// tell the peer to stop asking for something we may well have.
func TestServeGetData_LookupFailureIsNotNotFound(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	broken := chainhash.Hash{0x71}
	fetch.txErr[broken] = errors.NewStorageError("utxo store is unreachable")

	absent := chainhash.Hash{0x72}

	present := testWireTx(4)
	presentHash := present.TxHash()
	fetch.txs[presentHash] = txBytes(t, present)

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = p.Run(ctx) }()

	completeHandshake(t, far)

	// A genuinely ABSENT entry rides along, and it is what makes this test
	// deterministic: it guarantees a notfound is sent, so the notfound's own
	// arrival is the barrier and no wall-clock window is needed. Every entry
	// here is a transaction, so all three are answered in ONE pass and share
	// one notfound.
	far.write(t, getDataFor(
		wire.NewInvVect(wire.InvTypeTx, &presentHash),
		wire.NewInvVect(wire.InvTypeTx, &broken),
		wire.NewInvVect(wire.InvTypeTx, &absent),
	))

	gotTx, ok := far.read(t).(*wire.MsgTx)
	require.True(t, ok, "the entry we hold must still be served")
	require.Equal(t, presentHash, gotTx.TxHash())

	// The rule stated positively: the notfound names the absent entry and NOT
	// the failed one. Claiming notfound for a failed lookup would tell the peer
	// to stop asking for a transaction we may well hold.
	nf, ok := far.read(t).(*wire.MsgNotFound)
	require.True(t, ok, "the absent entry must be reported, got %T", nf)
	require.Len(t, nf.InvList, 1, "a failed lookup must not be added to the notfound")
	require.Equal(t, absent, nf.InvList[0].Hash)
}

// TestServeGetData_UnsupportedTypesAreAnsweredWithNothing covers the two kinds
// we cannot answer: a filtered block (SVNode builds a CMerkleBlock at
// net_processing.cpp:1281-1300; this port has no bloom filter) and an inv type
// we do not implement.
//
// Neither gets a reply and neither drops the peer. ProcessGetData has no
// else-clause for an unhandled type — it falls through to the next iteration —
// and legacy warns and continues (peer_server.go:1471-1475). Crucially neither
// enters a notfound: notfound asserts "I do not have that item", and for a type
// we never parsed we cannot honestly name one.
func TestServeGetData_UnsupportedTypesAreAnsweredWithNothing(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	filtered := chainhash.Hash{0x81}
	unknown := chainhash.Hash{0x82}
	absent := chainhash.Hash{0x83}

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(ctx) }()

	completeHandshake(t, far)

	// ORDER IS LOAD-BEARING, and invisible if you do not know servePass. A
	// filtered block IS a block type (protocol.h:577 IsBlockType), so it ENDS
	// the pass. Everything whose answer must share one notfound therefore has
	// to precede it: the absent transaction first so a notfound is guaranteed
	// at all, then the unknown type which does not end a pass, then the
	// filtered block last. Reshuffling this list for tidiness would split the
	// answers across passes and silently disarm the assertion below.
	far.write(t, getDataFor(
		wire.NewInvVect(wire.InvTypeTx, &absent),
		&wire.InvVect{Type: wire.InvType(99), Hash: unknown},
		wire.NewInvVect(wire.InvTypeFilteredBlock, &filtered),
	))

	// The notfound is guaranteed by the absent transaction, so its arrival is
	// the barrier — no wall-clock window. It must name that entry and ONLY
	// that entry: neither unsupported kind may appear.
	nf, ok := far.read(t).(*wire.MsgNotFound)
	require.True(t, ok, "the absent entry must be reported, got %T", nf)
	require.Len(t, nf.InvList, 1, "an unsupported inventory type must not enter a notfound")
	require.Equal(t, absent, nf.InvList[0].Hash)

	select {
	case err := <-errCh:
		t.Fatalf("the peer must not be dropped for a getdata we cannot answer: %v", err)
	default:
	}
}

// TestServeGetData_MissingBlockAndTxBothDrawNotFound pins the deliberate
// divergence from SVNode. vNotFound.push_back appears twice in ProcessGetData —
// :1418 for MSG_TX and :1442 for MSG_DATAREF_TX — and never in the block
// branch, so SVNode answers an unserved block with silence. This port answers
// it, taking legacy's shape (peer_server.go:1491-1493), because a peer given
// silence pays the full per-block download timeout
// (blockdownload.BlockDownloadTimeoutBase, 100 seconds and 600 during IBD)
// before it re-requests elsewhere, while a notfound lets it release the
// assignment at once.
//
// Asking for a missing block and a missing transaction together pins that they
// share one notfound, in request order, rather than the block being dropped.
func TestServeGetData_MissingBlockAndTxBothDrawNotFound(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	missingBlock := chainhash.Hash{0xc1}
	missingTx := chainhash.Hash{0xc2}

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(ctx) }()

	completeHandshake(t, far)

	// The block is FIRST, so it also ends its pass (any block-type entry does),
	// which is why the transaction is answered by a second pass and a second
	// notfound.
	far.write(t, getDataFor(
		wire.NewInvVect(wire.InvTypeBlock, &missingBlock),
		wire.NewInvVect(wire.InvTypeTx, &missingTx),
	))

	blockNF, ok := far.read(t).(*wire.MsgNotFound)
	require.True(t, ok, "an unserved block must be answered, not met with silence")
	require.Len(t, blockNF.InvList, 1)
	require.Equal(t, missingBlock, blockNF.InvList[0].Hash)
	require.Equal(t, wire.InvTypeBlock, blockNF.InvList[0].Type)

	txNF, ok := far.read(t).(*wire.MsgNotFound)
	require.True(t, ok, "the transaction after the block must be answered in a later pass")
	require.Len(t, txNF.InvList, 1)
	require.Equal(t, missingTx, txNF.InvList[0].Hash)
	require.Equal(t, wire.InvTypeTx, txNF.InvList[0].Type)

	select {
	case err := <-errCh:
		t.Fatalf("a missing block must not drop the peer: %v", err)
	default:
	}
}

// TestServeGetData_BlockLookupFailureDrawsNoNotFound is the other half of the
// rule above, and the boundary that matters: a block we FAILED to look up is
// not a block we do not have. Claiming notfound for it would tell the peer to
// stop asking for a block we may well hold.
func TestServeGetData_BlockLookupFailureDrawsNoNotFound(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	broken := chainhash.Hash{0xc3}
	fetch.blockErr[broken] = errors.NewServiceError("asset service is unreachable")

	absent := chainhash.Hash{0xc4}

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = p.Run(ctx) }()

	completeHandshake(t, far)

	// ORDER IS LOAD-BEARING. The broken BLOCK ends the pass, so the absent
	// transaction has to come first or the two answers land in different
	// passes and different notfounds. With this order one pass records the
	// absent entry, reaches the block, correctly records nothing for it, and
	// breaks — so exactly one notfound of exactly one entry is sent.
	far.write(t, getDataFor(
		wire.NewInvVect(wire.InvTypeTx, &absent),
		wire.NewInvVect(wire.InvTypeBlock, &broken),
	))

	nf, ok := far.read(t).(*wire.MsgNotFound)
	require.True(t, ok, "the absent entry must be reported, got %T", nf)
	require.Len(t, nf.InvList, 1, "a failed block lookup must not be added to the notfound")
	require.Equal(t, absent, nf.InvList[0].Hash)
	require.Equal(t, wire.InvTypeTx, nf.InvList[0].Type)
}

// TestServeGetData_DeclaredLengthLieAbortsTheSend is the between-pass and A1
// guard seen from the protocol side: a store whose declared length disagrees
// with the bytes it streams must never produce a frame. The peer gets nothing
// for that block rather than a frame whose checksum cannot match.
func TestServeGetData_DeclaredLengthLieAbortsTheSend(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	blk, raw := testWireBlock(t, 0x90, 2)
	blockHash := blk.BlockHash()
	fetch.blocks[blockHash] = raw
	fetch.declared[blockHash] = uint64(len(raw)) + 7

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(ctx) }()

	completeHandshake(t, far)

	far.write(t, getDataFor(wire.NewInvVect(wire.InvTypeBlock, &blockHash)))

	// Nothing was written for the block, and the connection still works.
	far.write(t, wire.NewMsgPing(77))

	pong, ok := far.read(t).(*wire.MsgPong)
	require.True(t, ok, "expected a pong and no block frame, got %T", pong)
	require.Equal(t, uint64(77), pong.Nonce)

	select {
	case err := <-errCh:
		t.Fatalf("a store-side length lie must not drop the peer: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestServeGetData_LongBlockSendKeepsTheLoopResponsive is why the getdata
// answerer is a goroutine of its own rather than part of Peer.Run.
//
// Legacy answered a getdata inline and had to stop processing that peer for
// the whole reply, which it knew was a problem: "We don't process anything
// else by them in this time … else the idle timeout could fire when we were
// only half done sending the blocks" (services/legacy/peer_server.go:1496-1500).
// SVNode has no such tradeoff — it services both directions of the socket
// throughout, and judges send and receive inactivity independently
// (net.cpp:1054-1073).
//
// This test holds a block send stuck on a peer that never drains it, then
// sends a message on the same connection and requires the peer loop to have
// PROCESSED it. An inline reply would leave Run parked inside the send and
// lastRecv frozen at the getdata.
//
// The discriminator rests on lastRecv having exactly ONE writer, and that is a
// checkable fact rather than a claim: in non-test protocol code the field
// appears three times — declared at peer.go:212, written once in
// handleMessage at peer.go:415, read once in Info at peer.go:741. So it
// advances only if the Run goroutine drained its inbound channel. A second
// writer would silently invalidate this test, which is why the grep is
// recorded here instead of being left for the next reader to repeat.
func TestServeGetData_LongBlockSendKeepsTheLoopResponsive(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	blk, raw := testWireBlock(t, 0xa0, 2)
	blockHash := blk.BlockHash()
	fetch.blocks[blockHash] = raw

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(ctx) }()

	completeHandshake(t, far)

	// The far side asks for the block and then never reads a byte of it, so
	// the writer stays stuck on that frame for the rest of the test.
	far.write(t, getDataFor(wire.NewInvVect(wire.InvTypeBlock, &blockHash)))

	// Wait for the send to be genuinely in flight: the block has been hashed
	// and handed to the writer, which is now blocked on the socket.
	require.Eventually(t, func() bool {
		return fetch.opens(blockHash) == 2
	}, 5*time.Second, 5*time.Millisecond, "the block send never reached its second pass")

	mark := p.Info().LastRecv
	require.False(t, mark.IsZero())

	far.write(t, wire.NewMsgPing(1))

	require.Eventually(t, func() bool {
		return p.Info().LastRecv.After(mark)
	}, 5*time.Second, 5*time.Millisecond,
		"the peer loop stopped processing inbound messages while a block send was in flight")

	select {
	case err := <-errCh:
		t.Fatalf("the peer was dropped while a block send was in flight: %v", err)
	default:
	}
}

// TestServeGetData_OneBlockPerPassAndTheRemainderIsRetained is the pacing port
// (RULING 4). ProcessGetData breaks out of its loop after ANY block-type entry
// (net_processing.cpp:1448-1452) and erases only the prefix it consumed
// (:1456), leaving the rest of vRecvGetData for a later pass. Without the
// retention half, the second block would simply be lost.
//
// Three blocks in one request must therefore be served across three passes, in
// request order, all three arriving.
func TestServeGetData_OneBlockPerPassAndTheRemainderIsRetained(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	invs := make([]*wire.InvVect, 0, 3)
	want := make([]chainhash.Hash, 0, 3)

	for i := 0; i < 3; i++ {
		blk, raw := testWireBlock(t, byte(0xb0+i), 2)
		hash := blk.BlockHash()
		fetch.blocks[hash] = raw

		invs = append(invs, wire.NewInvVect(wire.InvTypeBlock, &hash))
		want = append(want, hash)
	}

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = p.Run(ctx) }()

	completeHandshake(t, far)

	far.write(t, getDataFor(invs...))

	got := make([]chainhash.Hash, 0, 3)

	for i := 0; i < 3; i++ {
		blk, ok := far.read(t).(*wire.MsgBlock)
		require.True(t, ok, "block %d of the request never arrived", i)

		got = append(got, blk.BlockHash())
	}

	require.Equal(t, want, got, "blocks must be served in request order across passes")
}

// TestServeGetData_NotFoundClosesEachPassNotTheRequest follows from the pass
// structure: vNotFound is local to one ProcessGetData call, so a request whose
// misses straddle a block boundary draws one notfound PER PASS.
//
// The request is [missing tx, block, missing tx]. Pass one answers the first
// miss and the block, then ends on the block type; pass two answers the second
// miss. So the wire carries block, notfound, notfound — and never one notfound
// holding both.
func TestServeGetData_NotFoundClosesEachPassNotTheRequest(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	blk, raw := testWireBlock(t, 0xd0, 2)
	blockHash := blk.BlockHash()
	fetch.blocks[blockHash] = raw

	firstMiss := chainhash.Hash{0xd1}
	secondMiss := chainhash.Hash{0xd2}

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() { _ = p.Run(ctx) }()

	completeHandshake(t, far)

	far.write(t, getDataFor(
		wire.NewInvVect(wire.InvTypeTx, &firstMiss),
		wire.NewInvVect(wire.InvTypeBlock, &blockHash),
		wire.NewInvVect(wire.InvTypeTx, &secondMiss),
	))

	// The block goes out before its own pass's notfound, because the notfound
	// closes the pass.
	gotBlock, ok := far.read(t).(*wire.MsgBlock)
	require.True(t, ok, "the block must precede the notfound that closes its pass")
	require.Equal(t, blockHash, gotBlock.BlockHash())

	firstNF, ok := far.read(t).(*wire.MsgNotFound)
	require.True(t, ok)
	require.Len(t, firstNF.InvList, 1, "one notfound per pass, not one per request")
	require.Equal(t, firstMiss, firstNF.InvList[0].Hash)

	secondNF, ok := far.read(t).(*wire.MsgNotFound)
	require.True(t, ok, "the entry after the block must be served in a later pass")
	require.Len(t, secondNF.InvList, 1)
	require.Equal(t, secondMiss, secondNF.InvList[0].Hash)
}

// TestQueueGetData_CapRefusesOnlyTheExcess pins the per-peer memory bound
// SVNode's own unbounded vRecvGetData does not have. One whole legal request —
// MaxInvPerMsg entries, the cap go-wire's decoder applies — must always be
// admitted in full; only what a peer piles on beyond that is refused.
func TestQueueGetData_CapRefusesOnlyTheExcess(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	p, _, _ := newServingTestPeer(t, f, fetch)

	require.Equal(t, wire.MaxInvPerMsg, maxPendingGetData,
		"the cap must admit a whole legal getdata, whose size go-wire caps at MaxInvPerMsg")

	full := make([]*wire.InvVect, 0, maxPendingGetData)
	hash := chainhash.Hash{0xe0}

	for i := 0; i < maxPendingGetData; i++ {
		full = append(full, wire.NewInvVect(wire.InvTypeTx, &hash))
	}

	p.queueGetData(getDataFor(full...))

	p.getDataMu.Lock()
	admitted := len(p.getData)
	p.getDataMu.Unlock()

	require.Equal(t, maxPendingGetData, admitted, "a full legal request must be admitted entirely")

	// A second request on top of a full queue is refused entirely, and refusal
	// costs the peer its excess entries, not the connection.
	p.queueGetData(getDataFor(wire.NewInvVect(wire.InvTypeTx, &hash)))

	p.getDataMu.Lock()
	after := len(p.getData)
	p.getDataMu.Unlock()

	require.Equal(t, maxPendingGetData, after, "the cap must hold")
}

// TestQueueGetData_CapAdmitsPartially exercises the branch the test above does
// not: `items = items[:room]`, where a request STRADDLES the cap. That is the
// novel half of the rule — "refuse only the excess" rather than "refuse the
// request" — and refusing a whole getdata would lose entries SVNode would have
// honoured.
func TestQueueGetData_CapAdmitsPartially(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	p, _, _ := newServingTestPeer(t, f, fetch)

	hash := chainhash.Hash{0xe1}

	// Fill to two short of the cap.
	prefill := make([]*wire.InvVect, 0, maxPendingGetData-2)
	for i := 0; i < maxPendingGetData-2; i++ {
		prefill = append(prefill, wire.NewInvVect(wire.InvTypeTx, &hash))
	}

	p.queueGetData(getDataFor(prefill...))

	p.getDataMu.Lock()
	require.Len(t, p.getData, maxPendingGetData-2)
	p.getDataMu.Unlock()

	// Five more against two slots: two admitted, three refused.
	five := make([]*wire.InvVect, 0, 5)
	for i := 0; i < 5; i++ {
		five = append(five, wire.NewInvVect(wire.InvTypeBlock, &hash))
	}

	p.queueGetData(getDataFor(five...))

	p.getDataMu.Lock()
	defer p.getDataMu.Unlock()

	require.Len(t, p.getData, maxPendingGetData, "the straddling request must fill the queue exactly, not overflow it")

	// The two admitted are the FIRST two of the five, in the peer's order, not
	// an arbitrary two: truncation keeps the prefix.
	require.Equal(t, getDataBlock, p.getData[maxPendingGetData-2].kind)
	require.Equal(t, getDataBlock, p.getData[maxPendingGetData-1].kind)
	require.Equal(t, getDataTx, p.getData[maxPendingGetData-3].kind, "the prefill must not be displaced")
}

// TestInboundNotFoundIsIgnoredWithoutPenalty pins the claim made in
// Serving.OnGetData's notfound section: an inbound notfound is ignored, and
// ignored CHEAPLY — no misbehavior score, no disconnect.
//
// That is parity with both references. SVNode handles NOTFOUND with an empty
// branch whose comment is "We do not care about the NOTFOUND message, but
// logging an Unknown Command message would be undesirable as we transmit it
// ourselves" (net_processing.cpp:4847-4850). The legacy service only logs one
// (OnNotFound, services/legacy/peer_server.go:1836-1843).
//
// It is worth a test rather than a reading of the code, because "falls through
// a switch with no default" is only half the path: the message also passes
// through the handshake machine, which DOES score some unexpected messages, and
// the claim is false if it scores this one.
func TestInboundNotFoundIsIgnoredWithoutPenalty(t *testing.T) {
	f := newServeFixture(t, 3)
	fetch := newStubFetcher()

	p, far, _ := newServingTestPeer(t, f, fetch)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(ctx) }()

	completeHandshake(t, far)

	before := p.Info().MisbehaviorScore

	nf := wire.NewMsgNotFound()
	require.NoError(t, nf.AddInvVect(wire.NewInvVect(wire.InvTypeBlock, &chainhash.Hash{0xf1})))
	require.NoError(t, nf.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &chainhash.Hash{0xf2})))

	far.write(t, nf)

	// A ping after it proves the loop processed the notfound and carried on,
	// rather than the test merely racing ahead of it.
	far.write(t, wire.NewMsgPing(55))

	pong, ok := far.read(t).(*wire.MsgPong)
	require.True(t, ok, "the peer loop must carry on after an inbound notfound, got %T", pong)
	require.Equal(t, uint64(55), pong.Nonce)

	require.Equal(t, before, p.Info().MisbehaviorScore, "an inbound notfound must not be scored")

	select {
	case err := <-errCh:
		t.Fatalf("an inbound notfound must not drop the peer: %v", err)
	default:
	}
}
