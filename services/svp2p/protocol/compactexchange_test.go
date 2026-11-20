package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// testTxIndex is a TxIndex over a fixed set of transactions, keyed by hash.
// Unlike fakeTxIndex (txindex_test.go), which only proves the manager seam
// wires through, this one answers Match and Open honestly, so a test can
// choose exactly which of a block's transactions the node already holds.
type testTxIndex struct {
	mu sync.Mutex

	txs map[chainhash.Hash][]byte

	// collide makes Match report the BIP152 collision flag, so a test can
	// drive the index-side collision rule.
	collide bool

	// openErr, when set, is what Open returns instead of the bytes.
	openErr error
}

func newTestTxIndex(txs ...*wire.MsgTx) *testTxIndex {
	idx := &testTxIndex{txs: make(map[chainhash.Hash][]byte, len(txs))}

	for _, tx := range txs {
		idx.add(tx)
	}

	return idx
}

func (i *testTxIndex) add(tx *wire.MsgTx) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.txs[tx.TxHash()] = rawTx(tx)
}

func (i *testTxIndex) Match(k0, k1 uint64, shortIDs []uint64) ([]*chainhash.Hash, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()

	byID := make(map[uint64]chainhash.Hash, len(i.txs))

	for hash := range i.txs {
		byID[ShortID(k0, k1, hash)] = hash
	}

	out := make([]*chainhash.Hash, len(shortIDs))

	for n, id := range shortIDs {
		if hash, held := byID[id]; held {
			h := hash
			out[n] = &h
		}
	}

	return out, i.collide
}

func (i *testTxIndex) Open(_ context.Context, hash chainhash.Hash) (io.ReadCloser, uint64, error) {
	i.mu.Lock()
	defer i.mu.Unlock()

	if i.openErr != nil {
		return nil, 0, i.openErr
	}

	raw, held := i.txs[hash]
	if !held {
		return nil, 0, ErrTxUnknown
	}

	return io.NopCloser(bytes.NewReader(raw)), uint64(len(raw)), nil
}

// rawTx is a transaction's wire encoding, which is what a compact block
// carries prefilled and what the assembled stream must reproduce.
func rawTx(tx *wire.MsgTx) []byte {
	var buf bytes.Buffer

	_ = tx.BsvEncode(&buf, wire.ProtocolVersion, wire.BaseEncoding)

	return buf.Bytes()
}

// testTxs returns count distinct, syntactically valid transactions. Only the
// bytes matter here: the fake ingestor copies the assembled stream and never
// validates it, exactly as recordingIngestor does for a plain block.
func testTxs(count int, salt byte) []*wire.MsgTx {
	out := make([]*wire.MsgTx, 0, count)

	for i := 0; i < count; i++ {
		tx := wire.NewMsgTx(1)

		prev := chainhash.Hash{salt, byte(i)}
		tx.AddTxIn(wire.NewTxIn(wire.NewOutPoint(&prev, uint32(i)), []byte{0x51, salt, byte(i)})) //nolint:gosec // test index is small
		tx.AddTxOut(wire.NewTxOut(int64(1000+i), []byte{0x51}))

		out = append(out, tx)
	}

	return out
}

// compactBlockFor builds a cmpctblock announcing header with txs, prefilling
// exactly the indexes in prefilled and carrying a short ID for every other
// slot. It is the message shape SVNode's SendCompactBlock produces.
func compactBlockFor(t *testing.T, header *wire.BlockHeader, txs []*wire.MsgTx, prefilled ...int) *wire.MsgCmpctBlock {
	t.Helper()

	const nonce = 0x0102030405060708

	msg := wire.NewMsgCmpctBlock(header, nonce)

	isPrefilled := make(map[int]bool, len(prefilled))
	for _, i := range prefilled {
		isPrefilled[i] = true
	}

	k0, k1 := ShortIDKeys(header, nonce)

	for i, tx := range txs {
		if isPrefilled[i] {
			require.NoError(t, msg.AddPrefilledTransaction(uint32(i), tx)) //nolint:gosec // test index is small

			continue
		}

		msg.ShortIDs = append(msg.ShortIDs, ShortID(k0, k1, tx.TxHash()))
	}

	return msg
}

// compactSyncManager is syncTestManager plus the two things compact-block
// reception is gated on: legacy_compactBlocks and a wired TxIndex
// (SetTxIndex's own doc comment).
func compactSyncManager(t *testing.T, idx *HeaderIndex, ingestor BlockIngestor, txIdx TxIndex) *PeerManager {
	t.Helper()

	tSettings := managerSettings()
	tSettings.ChainCfgParams = syncTestParams(nil)
	tSettings.Legacy.CompactBlocks = txIdx != nil

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	if txIdx != nil {
		m.SetTxIndex(txIdx)
	}

	require.NoError(t, m.ConfigureSync(SyncConfig{
		Index:        idx,
		Ingestor:     ingestor,
		TickInterval: 20 * time.Millisecond,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))
	t.Cleanup(func() { require.NoError(t, m.Stop()) })

	return m
}

// connectCompactPeer completes the handshake as a block-serving peer and
// answers the node's opening getheaders with an empty batch, so headers-first
// sync is quiet and the only thing that can move the node is the cmpctblock
// the test sends next.
func connectCompactPeer(t *testing.T, m *PeerManager) *scriptedPeer {
	t.Helper()

	far := dialScripted(t, m.ListenAddrs()[0])
	t.Cleanup(func() { _ = far.nc.Close() })

	version := remoteVersion(4321)
	version.Services = wire.SFNodeNetwork
	far.completeOutboundHandshakeAs(t, version)

	far.readUntil(t, wire.CmdGetHeaders)
	far.write(t, wire.NewMsgHeaders())

	return far
}

// blockTxnFor is the blocktxn reply a peer sends for the gaps a getblocktxn
// named, in the order the request listed them.
func blockTxnFor(t *testing.T, hash chainhash.Hash, txs []*wire.MsgTx, indexes []uint32) *wire.MsgBlockTxn {
	t.Helper()

	msg := wire.NewMsgBlockTxn(&hash)

	for _, i := range indexes {
		require.Less(t, int(i), len(txs))
		require.NoError(t, msg.AddTransaction(txs[i]))
	}

	return msg
}

// assembledBody is what the ingest must have read off the assembled stream:
// every transaction of the block, wire-encoded back to back, in block order.
func assembledBody(txs []*wire.MsgTx) []byte {
	var buf bytes.Buffer

	for _, tx := range txs {
		buf.Write(rawTx(tx))
	}

	return buf.Bytes()
}

// TestCompactBlock_AllTransactionsKnownIngestsWithoutAGapRequest is the
// no-round-trip case of net_processing.cpp:3870-3877: every slot is filled
// from the index or prefilled, req.indices is empty, and SVNode jumps
// straight to the blocktxn completion path with an empty message.
func TestCompactBlock_AllTransactionsKnownIngestsWithoutAGapRequest(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 11)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(4, 0x10)
	txIdx := newTestTxIndex(txs[1], txs[2], txs[3])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	require.Eventually(t, func() bool { return ingestor.count() == 1 }, 10*time.Second, 20*time.Millisecond,
		"the compact block never reached the ingest interface")

	require.Equal(t, []chainhash.Hash{header.BlockHash()}, ingestor.hashes())
	require.Equal(t, uint64(len(txs)), ingestor.txCount(0))
	require.Equal(t, assembledBody(txs), ingestor.body(0),
		"the assembled stream must carry every transaction of the block in block order")

	// The peer is never asked for anything: no getblocktxn, and no getdata
	// fallback either.
	far.mustNotReceive(t, 300*time.Millisecond, wire.CmdGetBlockTxn, wire.CmdGetData)
}

// TestCompactBlock_GapsAreFetchedWithGetBlockTxn is the round-trip case:
// net_processing.cpp:3864-3881 collects every slot the index could not fill
// into a getblocktxn, and the peer's blocktxn completes the block
// (ProcessBlockTxnMessage :3608-3609, :3646).
func TestCompactBlock_GapsAreFetchedWithGetBlockTxn(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 12)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(5, 0x20)

	// The node holds slots 1 and 3; slots 2 and 4 are the gaps, and slot 0 is
	// prefilled.
	txIdx := newTestTxIndex(txs[1], txs[3])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	req, ok := far.readUntil(t, wire.CmdGetBlockTxn).(*wire.MsgGetBlockTxn)
	require.True(t, ok)
	require.Equal(t, header.BlockHash(), req.BlockHash)
	require.Equal(t, []uint32{2, 4}, req.Indexes, "the gaps must be requested in increasing slot order")

	far.write(t, blockTxnFor(t, header.BlockHash(), txs, req.Indexes))

	require.Eventually(t, func() bool { return ingestor.count() == 1 }, 10*time.Second, 20*time.Millisecond,
		"the filled compact block never reached the ingest interface")

	require.Equal(t, []chainhash.Hash{header.BlockHash()}, ingestor.hashes())
	require.Equal(t, assembledBody(txs), ingestor.body(0),
		"the gap transactions must land in the slots that asked for them")
}

// TestCompactBlock_BlockTxnWithTheWrongCountIsPeerFault is FillBlock's arity
// check reaching net_processing.cpp:3610-3616: READ_STATUS_INVALID earns
// Misbehaving(pfrom, 100, "invalid-cmpctblk-txns") and the block is failed.
func TestCompactBlock_BlockTxnWithTheWrongCountIsPeerFault(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 13)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(4, 0x30)
	txIdx := newTestTxIndex(txs[1])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	req, ok := far.readUntil(t, wire.CmdGetBlockTxn).(*wire.MsgGetBlockTxn)
	require.True(t, ok)
	require.Equal(t, []uint32{2, 3}, req.Indexes)

	// Two were asked for; one is sent.
	far.write(t, blockTxnFor(t, header.BlockHash(), txs, req.Indexes[:1]))

	require.Eventually(t, func() bool { return m.ConnectedCount() == 0 }, 10*time.Second, 20*time.Millisecond,
		"a blocktxn with the wrong transaction count must disconnect the peer")

	require.Equal(t, 0, ingestor.count(), "a rejected compact block must never be ingested")
}

// TestCompactBlock_BlockWeAlreadyHoldIsIgnored is
// net_processing.cpp:3795-3799: pindex->getStatus().hasData() means "nothing
// to do here" — the claim is released and the message goes no further.
func TestCompactBlock_BlockWeAlreadyHoldIsIgnored(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 14)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	connected, err := idx.AddHeader(header)
	require.NoError(t, err)
	require.True(t, connected)

	txs := testTxs(3, 0x40)
	txIdx := newTestTxIndex(txs[1], txs[2])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	// Our own chain still stands at genesis, so every OTHER guard wantCompact
	// applies passes and the have-data rule is the only thing that can stop
	// this announcement. Without this the header would be the index tip, and
	// the chainwork guard would refuse it for a different reason.
	require.True(t, m.SetActiveTip(genesis.BlockHash()))

	// The node already holds this block's data, which is what hasData reports.
	m.syncMu.Lock()
	m.blockDownloader.haveData[header.BlockHash()] = 1
	m.syncMu.Unlock()

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	far.mustNotReceive(t, 500*time.Millisecond, wire.CmdGetBlockTxn)

	require.Equal(t, 0, ingestor.count(), "a block we already hold must not be reconstructed")
	require.Equal(t, int32(1), m.ConnectedCount(), "ignoring a block must not cost the peer its connection")
}

// TestCompactBlock_SecondAnnouncementWhileOneIsOutstandingIsIgnored is
// net_processing.cpp:3839-3844, "Peer sent us compact block we were already
// syncing!": one partial block per peer, and the outstanding one is kept.
func TestCompactBlock_SecondAnnouncementWhileOneIsOutstandingIsIgnored(t *testing.T) {
	genesis := syncGenesis()
	first := minedRun(genesis, 1, 15)[0]
	second := minedRun(genesis, 1, 16)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	firstTxs := testTxs(3, 0x50)
	secondTxs := testTxs(3, 0x60)

	txIdx := newTestTxIndex(firstTxs[1], secondTxs[1], secondTxs[2])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)

	// The first announcement leaves slot 2 outstanding.
	far.write(t, compactBlockFor(t, first, firstTxs, 0))

	req, ok := far.readUntil(t, wire.CmdGetBlockTxn).(*wire.MsgGetBlockTxn)
	require.True(t, ok)
	require.Equal(t, first.BlockHash(), req.BlockHash)
	require.Equal(t, []uint32{2}, req.Indexes)

	// The second would need no round trip at all, so if it were accepted it
	// would ingest on its own and overwrite the outstanding state.
	far.write(t, compactBlockFor(t, second, secondTxs, 0))

	far.mustNotReceive(t, 500*time.Millisecond, wire.CmdGetBlockTxn)
	require.Equal(t, 0, ingestor.count(), "the second compact block must be ignored while one is outstanding")

	// The outstanding block still completes, which is what proves its state
	// survived the second announcement.
	far.write(t, blockTxnFor(t, first.BlockHash(), firstTxs, req.Indexes))

	require.Eventually(t, func() bool { return ingestor.count() == 1 }, 10*time.Second, 20*time.Millisecond,
		"the outstanding compact block must still complete")

	require.Equal(t, []chainhash.Hash{first.BlockHash()}, ingestor.hashes())
}

// TestCompactBlock_IgnoredWhenTheFlagIsOff is spec §8's flag-off half: no
// sendcmpct is ever sent, and an unsolicited cmpctblock is ignored rather
// than acted on or punished.
func TestCompactBlock_IgnoredWhenTheFlagIsOff(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 17)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(3, 0x70)

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, nil)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	far.mustNotReceive(t, 500*time.Millisecond, wire.CmdSendcmpct, wire.CmdGetBlockTxn, wire.CmdGetData)

	require.Equal(t, 0, ingestor.count(), "with the flag off no compact block may be reconstructed")
	require.Equal(t, int32(1), m.ConnectedCount(), "an ignored cmpctblock must not cost the peer its connection")
}

// TestCompactBlock_HeaderThatDoesNotConnectAsksForHeaders is
// net_processing.cpp:3721-3733: a cmpctblock whose parent we do not hold is
// NOT run through AcceptBlockHeader — "Doesn't connect (or is genesis),
// instead of DoSing in AcceptBlockHeader, request deeper headers". The peer
// pays nothing for announcing a block we are behind on.
func TestCompactBlock_HeaderThatDoesNotConnectAsksForHeaders(t *testing.T) {
	genesis := syncGenesis()

	// The announced header is two above genesis, so its parent is a header
	// the node has never seen.
	run := minedRun(genesis, 2, 21)
	header := run[1]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(3, 0xa0)
	txIdx := newTestTxIndex(txs[1], txs[2])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	ask, ok := far.readUntil(t, wire.CmdGetHeaders).(*wire.MsgGetHeaders)
	require.True(t, ok)
	require.NotEmpty(t, ask.BlockLocatorHashes, "the getheaders must carry our own locator")

	require.Equal(t, 0, ingestor.count(), "a block whose header does not connect must not be reconstructed")
	require.Equal(t, int32(1), m.ConnectedCount())

	snaps := m.Snapshots()
	require.Len(t, snaps, 1)
	require.Equal(t, 0, snaps[0].MisbehaviorScore,
		"a header that does not connect must not be scored (net_processing.cpp:3723-3724)")
}

// TestCompactBlock_TooHighAboveOurTipIsIgnored is net_processing.cpp:3823-3825,
// "We want to be a bit conservative just to be extra careful about DoS
// possibilities in compact block processing": a block more than two heights
// above our own tip is not reconstructed. Its header still reaches the index,
// which is what the :3913-3921 revert-to-headers branch is for — see
// CompactBlock's own doc comment.
func TestCompactBlock_TooHighAboveOurTipIsIgnored(t *testing.T) {
	genesis := syncGenesis()
	run := minedRun(genesis, 3, 22)
	header := run[2]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	// The two headers below the announced one are held, so it connects; only
	// its height above our own chain can refuse it.
	for _, h := range run[:2] {
		connected, addErr := idx.AddHeader(h)
		require.NoError(t, addErr)
		require.True(t, connected)
	}

	txs := testTxs(3, 0xb0)
	txIdx := newTestTxIndex(txs[1], txs[2])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	// Our own chain stands at genesis while the header index runs ahead, which
	// is exactly the headers-first shape this rule guards.
	require.True(t, m.SetActiveTip(genesis.BlockHash()))

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	far.mustNotReceive(t, 500*time.Millisecond, wire.CmdGetBlockTxn)

	require.Equal(t, 0, ingestor.count(), "a block %d above our tip must not be reconstructed", 3)
	require.Equal(t, int32(1), m.ConnectedCount())

	// The header itself IS accepted, which is what makes the :3913-3921
	// revert-to-headers branch unnecessary here: the announcement has already
	// had the effect a plain headers message would have had.
	// The peer's state is snapshotted under m.mu BEFORE syncMu is taken: this
	// package never holds the two together (see the note on syncMu).
	state := onlySyncPeerState(t, m)

	m.syncMu.Lock()
	_, indexed := m.headerIndex.Lookup(header.BlockHash())
	best := state.pindexBestKnownBlock
	m.syncMu.Unlock()

	require.True(t, indexed, "a refused compact block must still leave its header in the index")
	require.NotNil(t, best, "the announcement must still count as block availability")
	require.Equal(t, header.BlockHash(), best.Hash)
}

// TestBlockTxn_WithoutAnOutstandingRequestIsDroppedUnscored is
// net_processing.cpp:3595-3606: GetBlockDetails throws for a block this peer
// does not owe us, and the catch block logs "Peer %d sent us block
// transactions for block we weren't expecting" and returns. There is no
// Misbehaving call on that path — the only score in ProcessBlockTxnMessage is
// the READ_STATUS_INVALID branch at :3610-3616.
func TestBlockTxn_WithoutAnOutstandingRequestIsDroppedUnscored(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 23)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(3, 0xc0)
	txIdx := newTestTxIndex(txs[1], txs[2])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)

	// Nothing was ever announced, so nothing is outstanding.
	far.write(t, blockTxnFor(t, header.BlockHash(), txs, []uint32{1, 2}))

	// The connection survives, and a later ping proves the stream was released
	// rather than left holding the transport read loop.
	far.write(t, wire.NewMsgPing(99))

	pong, ok := far.readUntil(t, wire.CmdPong).(*wire.MsgPong)
	require.True(t, ok)
	require.Equal(t, uint64(99), pong.Nonce, "the connection must still be serving after an unsolicited blocktxn")

	require.Equal(t, int32(1), m.ConnectedCount())

	snaps := m.Snapshots()
	require.Len(t, snaps, 1)
	require.Equal(t, 0, snaps[0].MisbehaviorScore, "an unsolicited blocktxn must not be scored")

	require.Equal(t, 0, ingestor.count())
}

// TestCompactBlock_IndexCollisionJoinsTheGaps is blockencodings.cpp:174-183
// as the Task 5 ruling carries it: a short ID two indexed hashes share leaves
// the slot empty, so it is requested by getblocktxn like any other gap rather
// than failing reconstruction.
func TestCompactBlock_IndexCollisionJoinsTheGaps(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 18)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(3, 0x80)

	// The index reports the collision flag and matches nothing, which is what
	// an index that cannot name a slot's transaction answers.
	txIdx := newTestTxIndex()
	txIdx.collide = true

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	req, ok := far.readUntil(t, wire.CmdGetBlockTxn).(*wire.MsgGetBlockTxn)
	require.True(t, ok)
	require.Equal(t, []uint32{1, 2}, req.Indexes, "a collided slot must be requested, not fail the block")

	far.write(t, blockTxnFor(t, header.BlockHash(), txs, req.Indexes))

	require.Eventually(t, func() bool { return ingestor.count() == 1 }, 10*time.Second, 20*time.Millisecond)
	require.Equal(t, assembledBody(txs), ingestor.body(0))
}

// TestCompactBlock_UnknownHeldTransactionFallsBackWithoutAScore is the Task 7
// ruling's READ_STATUS_FAILED path: the index named a transaction whose bytes
// it can no longer open, which SVNode treats as a collision rather than
// malice (net_processing.cpp:3618-3623, "Might have collided, fall back to
// getdata now"). No score, and the block is offered again.
func TestCompactBlock_UnknownHeldTransactionFallsBackWithoutAScore(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 19)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(3, 0x90)

	txIdx := newTestTxIndex(txs[1], txs[2])
	txIdx.openErr = ErrTxUnknown

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	// The scheduler offers the block again, which reaches the wire as the
	// ordinary getdata this node's download tick sends.
	getData, ok := far.readUntil(t, wire.CmdGetData).(*wire.MsgGetData)
	require.True(t, ok)
	require.Len(t, getData.InvList, 1)
	require.Equal(t, wire.InvTypeBlock, getData.InvList[0].Type)
	require.Equal(t, header.BlockHash(), getData.InvList[0].Hash)

	require.Equal(t, int32(1), m.ConnectedCount(), "a failed reconstruction must not disconnect the peer")

	snaps := m.Snapshots()
	require.Len(t, snaps, 1)
	require.Equal(t, 0, snaps[0].MisbehaviorScore, "a failed reconstruction must score nothing")
}

// ---------------------------------------------------------------------------

// recordingBodyIngestor is recordingIngestor plus the assembled bytes, which
// is the only way to prove a reconstructed block carries the right
// transactions in the right slots.
type recordingBodyIngestor struct {
	outcome IngestOutcome

	mu       sync.Mutex
	ingested []chainhash.Hash
	bodies   [][]byte
	counts   []uint64
}

func (r *recordingBodyIngestor) WatchProgress(rd io.ReadCloser) IngestProgress {
	return newTestProgress(rd)
}

func (r *recordingBodyIngestor) Ingest(_ context.Context, req BlockIngestRequest) IngestOutcome {
	var body bytes.Buffer

	_, err := io.Copy(&body, req.TxReader)

	if closeErr := req.TxReader.Close(); closeErr != nil && err == nil {
		err = closeErr
	}

	if err != nil {
		return IngestOutcome{Err: err}
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.ingested = append(r.ingested, req.Header.BlockHash())
	r.bodies = append(r.bodies, body.Bytes())
	r.counts = append(r.counts, req.TxCount)

	return r.outcome
}

func (r *recordingBodyIngestor) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.ingested)
}

func (r *recordingBodyIngestor) hashes() []chainhash.Hash {
	r.mu.Lock()
	defer r.mu.Unlock()

	return append([]chainhash.Hash(nil), r.ingested...)
}

func (r *recordingBodyIngestor) body(i int) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.bodies[i]
}

func (r *recordingBodyIngestor) txCount(i int) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.counts[i]
}

// mustNotReceive reads for the given window and fails if any of the named
// commands arrives. It is the negative half of readUntil.
func (s *scriptedPeer) mustNotReceive(t *testing.T, window time.Duration, commands ...string) {
	t.Helper()

	banned := make(map[string]struct{}, len(commands))
	for _, cmd := range commands {
		banned[cmd] = struct{}{}
	}

	deadline := time.Now().Add(window)

	for time.Now().Before(deadline) {
		require.NoError(t, s.nc.SetReadDeadline(time.Now().Add(50*time.Millisecond)))

		_, msg, _, err := wire.ReadMessageWithEncodingN(s.nc, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
		if err != nil {
			continue
		}

		_, forbidden := banned[msg.Command()]
		require.False(t, forbidden, "the node must not send %s", msg.Command())
	}
}

// ---------------------------------------------------------------------------
// Fix round 1
// ---------------------------------------------------------------------------

// minedChildAt is minedChild with the timestamp chosen rather than taken from
// the clock, so a test can put the chain's tip far enough in the past to reach
// the initial-block-download and direct-fetch guards.
func minedChildAt(parent *wire.BlockHeader, bits, salt uint32, unixTime int64) *wire.BlockHeader {
	prevHash := parent.BlockHash()

	merkle := chainhash.Hash{}
	merkle[0] = byte(salt)
	merkle[1] = byte(salt >> 8)

	h := wire.NewBlockHeader(1, &prevHash, &merkle, bits, 0)
	h.Timestamp = time.Unix(unixTime, 0)

	for !testMeetsTarget(h) {
		h.Nonce++
	}

	return h
}

// agedGenesis is syncGenesis with its timestamp pushed back, which is what
// makes tipIsNearAdjustedTime (the port's initial-block-download predicate)
// and canDirectFetch answer false.
func agedGenesis(age time.Duration) *wire.BlockHeader {
	zero := chainhash.Hash{}

	h := wire.NewBlockHeader(1, &zero, &zero, testEasyBits, 0)
	h.Timestamp = time.Unix(time.Now().Add(-age).Unix(), 0)

	for !testMeetsTarget(h) {
		h.Nonce++
	}

	return h
}

// writeRawBlockTxn frames a blocktxn message by hand, so a test can send a
// declared transaction count with a body that does not decode. go-wire's
// MsgBlockTxn can only carry transactions that already encode, so the
// malformed case is unreachable through it.
func writeRawBlockTxn(t *testing.T, s *scriptedPeer, hash chainhash.Hash, count uint64, body []byte) {
	t.Helper()

	var payload bytes.Buffer

	payload.Write(hash[:])
	require.NoError(t, wire.WriteVarInt(&payload, wire.ProtocolVersion, count))
	payload.Write(body)

	frame := make([]byte, wire.MessageHeaderSize)
	binary.LittleEndian.PutUint32(frame[0:4], uint32(wire.MainNet))
	copy(frame[4:4+wire.CommandSize], wire.CmdBlockTxn)
	binary.LittleEndian.PutUint32(frame[16:20], uint32(payload.Len())) //nolint:gosec // test payload is small
	// Bytes 20:24 are the checksum, which the streaming path does not verify
	// (transport.TxnStream's own note).

	_, err := s.nc.Write(frame)
	require.NoError(t, err)

	_, err = s.nc.Write(payload.Bytes())
	require.NoError(t, err)
}

// tailIngestor models what the real bridge does and what recordingBodyIngestor
// does not: it releases the transaction stream as soon as the stream is drained
// (BlockIngestor.Ingest's own contract, "It must release req.TxReader on every
// exit path"), and only THEN runs its post-stream pipeline tail. On a real
// block that tail — extendTransactions, createUtxos, createSubtrees,
// ProcessBlock — is where most of the wall clock goes.
//
// That gap matters here. Releasing the stream unparks the transport read loop
// (transport/conn.go serveStream waits on the stream's done channel), so the
// peer's NEXT message is dispatched while this ingest is still running and long
// before BlockDone reports it. A fake that closes and returns in the same breath
// hides the window almost completely.
type tailIngestor struct {
	release chan struct{}

	mu       sync.Mutex
	ingested []chainhash.Hash
	firstOut chan struct{}
}

func newTailIngestor() *tailIngestor {
	return &tailIngestor{release: make(chan struct{}), firstOut: make(chan struct{})}
}

func (r *tailIngestor) WatchProgress(rd io.ReadCloser) IngestProgress { return newTestProgress(rd) }

func (r *tailIngestor) Ingest(_ context.Context, req BlockIngestRequest) IngestOutcome {
	_, err := io.Copy(io.Discard, req.TxReader)

	if closeErr := req.TxReader.Close(); closeErr != nil && err == nil {
		err = closeErr
	}

	r.mu.Lock()
	r.ingested = append(r.ingested, req.Header.BlockHash())
	first := len(r.ingested) == 1
	r.mu.Unlock()

	if first {
		close(r.firstOut)
	}

	// The pipeline tail, held open by the test.
	<-r.release

	if err != nil {
		return IngestOutcome{Err: err}
	}

	return IngestOutcome{}
}

func (r *tailIngestor) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.ingested)
}

// TestBlockTxn_ReplayAfterTheHandoffIsDroppedUnscored is the B1 regression.
//
// Once FillBlock succeeds, SVNode has already called MarkBlockAsReceived
// (net_processing.cpp:3646), which destroys the QueuedBlock the partial block
// hangs off. A second copy of the same blocktxn therefore throws in
// GetBlockDetails and takes the unsolicited path at :3595-3606 — logged,
// dropped, unscored. It must never start a second ingest of the same block.
//
// The window this drives is the real one: the ingestor releases the stream,
// which unparks the transport read loop, and the partial block stays on the
// peer until BlockDone runs at the far end of the pipeline tail. The replay is
// written INSIDE that window, so the outcome does not depend on scheduling.
func TestBlockTxn_ReplayAfterTheHandoffIsDroppedUnscored(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 31)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(4, 0xd0)
	txIdx := newTestTxIndex(txs[1], txs[2])

	ingestor := newTailIngestor()
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	req, ok := far.readUntil(t, wire.CmdGetBlockTxn).(*wire.MsgGetBlockTxn)
	require.True(t, ok)
	require.Equal(t, []uint32{3}, req.Indexes)

	reply := blockTxnFor(t, header.BlockHash(), txs, req.Indexes)
	far.write(t, reply)

	// The first ingest has drained and released the stream. BlockDone has not
	// run and cannot run until the tail is released below.
	select {
	case <-ingestor.firstOut:
	case <-time.After(10 * time.Second):
		t.Fatal("the compact block never reached the ingest interface")
	}

	// The byte-identical reply, replayed squarely inside the window.
	far.write(t, reply)

	require.Never(t, func() bool { return ingestor.count() > 1 }, 2*time.Second, 50*time.Millisecond,
		"a replayed blocktxn must never start a second ingest of the same block")

	close(ingestor.release)

	require.Equal(t, int32(1), m.ConnectedCount(), "a replayed blocktxn must not disconnect the peer")

	snaps := m.Snapshots()
	require.Len(t, snaps, 1)
	require.Equal(t, 0, snaps[0].MisbehaviorScore, "a replayed blocktxn must not be scored")
}

// TestCompactBlock_NonConnectingHeaderSendsNoGetHeadersDuringInitialSync is the
// gate this port dropped in round 1: net_processing.cpp:3725 wraps the
// GETHEADERS push in `if(!IsInitialBlockDownload())`. Without it a peer can
// make the node emit a full locator per malformed announcement while it is
// still catching up.
func TestCompactBlock_NonConnectingHeaderSendsNoGetHeadersDuringInitialSync(t *testing.T) {
	// A tip a day old is what this port reads as initial block download
	// (HeaderSync.tipIsNearAdjustedTime, NearTipHeaderSyncWindow).
	genesis := agedGenesis(48 * time.Hour)

	// Two above genesis, so its parent is a header the node has never seen.
	orphanParent := minedChildAt(genesis, testEasyBits, 32, time.Now().Add(-47*time.Hour).Unix())
	header := minedChildAt(orphanParent, testEasyBits, 33, time.Now().Add(-46*time.Hour).Unix())

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(3, 0xe0)
	txIdx := newTestTxIndex(txs[1], txs[2])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	far.mustNotReceive(t, 1*time.Second, wire.CmdGetHeaders, wire.CmdGetBlockTxn)

	require.Equal(t, 0, ingestor.count())
	require.Equal(t, int32(1), m.ConnectedCount())

	snaps := m.Snapshots()
	require.Len(t, snaps, 1)
	require.Equal(t, 0, snaps[0].MisbehaviorScore)
}

// TestCompactBlock_NotCloseToTipIsIgnored is net_processing.cpp:3817-3820,
// "If we're not close to tip yet, give up and let parallel block fetch work its
// magic": CanDirectFetch (:308-311) is the active tip's own time inside
// nPowTargetSpacing * 20 of now.
func TestCompactBlock_NotCloseToTipIsIgnored(t *testing.T) {
	// Four hours is outside the 20 * 10-minute direct-fetch window and inside
	// the 24 hour initial-sync window, so this guard is the one under test and
	// not the header path's.
	genesis := agedGenesis(4 * time.Hour)
	header := minedChildAt(genesis, testEasyBits, 34, time.Now().Add(-3*time.Hour).Unix())

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(3, 0xf0)
	txIdx := newTestTxIndex(txs[1], txs[2])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	far.mustNotReceive(t, 1*time.Second, wire.CmdGetBlockTxn)

	require.Equal(t, 0, ingestor.count(), "a block announced while we are far from the tip must not be reconstructed")
	require.Equal(t, int32(1), m.ConnectedCount())
}

// TestCompactBlock_PeerAtTheInFlightCapIsIgnored is net_processing.cpp:3826-3828:
// the claim is taken only when `(!fAlreadyInFlight && nodestate->nBlocksInFlight
// < MAX_BLOCKS_IN_TRANSIT_PER_PEER) || fAlreadyInFlightFromThisPeer`. Without
// it the compact path walks straight past the 16-block per-peer cap the getdata
// scheduler enforces.
func TestCompactBlock_PeerAtTheInFlightCapIsIgnored(t *testing.T) {
	genesis := syncGenesis()

	// Siblings, so every one of them sits at height 1 and the announced block
	// stays inside the tip+2 ceiling however many are already in flight.
	siblings := make([]*wire.BlockHeader, 0, MaxBlocksInTransitPerPeer+1)
	for i := 0; i <= MaxBlocksInTransitPerPeer; i++ {
		siblings = append(siblings, minedChild(genesis, testEasyBits, uint32(100+i))) //nolint:gosec // test salt is small
	}

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	// Every sibling but the last is already in the index and claimed.
	for _, h := range siblings[:MaxBlocksInTransitPerPeer] {
		connected, addErr := idx.AddHeader(h)
		require.NoError(t, addErr)
		require.True(t, connected)
	}

	txs := testTxs(3, 0x11)
	txIdx := newTestTxIndex(txs[1], txs[2])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	require.True(t, m.SetActiveTip(genesis.BlockHash()))

	far := connectCompactPeer(t, m)

	state := onlySyncPeerState(t, m)
	sp := onlySyncPeer(t, m)

	m.syncMu.Lock()
	for _, h := range siblings[:MaxBlocksInTransitPerPeer] {
		node, known := m.headerIndex.Lookup(h.BlockHash())
		require.True(t, known)
		require.True(t, m.blockDownloader.MarkBlockAsInFlight(sp, node, time.Now().UnixMicro()))
	}

	atCap := state.nBlocksInFlight
	m.syncMu.Unlock()

	require.Equal(t, MaxBlocksInTransitPerPeer, atCap, "the peer must start the announcement at the cap")

	far.write(t, compactBlockFor(t, siblings[MaxBlocksInTransitPerPeer], txs, 0))

	far.mustNotReceive(t, 1*time.Second, wire.CmdGetBlockTxn)

	require.Equal(t, 0, ingestor.count(), "a peer already at the in-flight cap must not claim another block")
	require.Equal(t, int32(1), m.ConnectedCount())
}

// TestCompactBlock_UndecodableGapTransactionIsPeerFault is the
// readInvalid-via-BlockDone branch, the one the round-1 review found untested.
// The peer answers with the DECLARED number of transactions but with bytes
// that do not decode, which compactAssembler.readGap reaches as a.invalid.
// That surfaces only while the assembled stream is read, so the verdict is
// BlockDone's: net_processing.cpp:3610-3616, Misbehaving(pfrom, 100,
// "invalid-cmpctblk-txns").
func TestCompactBlock_UndecodableGapTransactionIsPeerFault(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 35)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(3, 0x12)

	// Slots 0 and 1 are covered, so slot 2 is the only gap and the reply
	// declares exactly the one transaction the request asked for.
	txIdx := newTestTxIndex(txs[1])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	req, ok := far.readUntil(t, wire.CmdGetBlockTxn).(*wire.MsgGetBlockTxn)
	require.True(t, ok)
	require.Equal(t, []uint32{2}, req.Indexes)

	// Twelve bytes that cannot be a transaction: the version reads, then the
	// input-count varint claims an eight byte length the payload cannot hold.
	garbage := bytes.Repeat([]byte{0xff}, 12)
	writeRawBlockTxn(t, far, header.BlockHash(), 1, garbage)

	require.Eventually(t, func() bool { return m.ConnectedCount() == 0 }, 10*time.Second, 20*time.Millisecond,
		"a blocktxn whose transactions do not decode must disconnect the peer")

	require.Equal(t, 0, ingestor.count(), "a block that failed to assemble must never count as ingested")
}

// onlySyncPeer returns the sole connected peer's SyncPeer.
func onlySyncPeer(t *testing.T, m *PeerManager) *SyncPeer {
	t.Helper()

	m.mu.Lock()
	defer m.mu.Unlock()

	require.Len(t, m.peers, 1)

	for _, sp := range m.peers {
		return sp
	}

	return nil
}

// ---------------------------------------------------------------------------
// Final-review fix round
// ---------------------------------------------------------------------------

// writeRawBlockTxnFrame writes a blocktxn frame whose DECLARED payload length
// is chosen independently of the bytes actually sent, so a test can model a
// peer that promises a large payload and then dribbles.
func writeRawBlockTxnFrame(t *testing.T, s *scriptedPeer, hash chainhash.Hash, count uint64, declared uint32) {
	t.Helper()

	var payload bytes.Buffer

	payload.Write(hash[:])
	require.NoError(t, wire.WriteVarInt(&payload, wire.ProtocolVersion, count))

	require.Greater(t, declared, uint32(payload.Len()), "the frame must declare more than it sends")

	frame := make([]byte, wire.MessageHeaderSize)
	binary.LittleEndian.PutUint32(frame[0:4], uint32(wire.MainNet))
	copy(frame[4:4+wire.CommandSize], wire.CmdBlockTxn)
	binary.LittleEndian.PutUint32(frame[16:20], declared)

	_, err := s.nc.Write(frame)
	require.NoError(t, err)

	_, err = s.nc.Write(payload.Bytes())
	require.NoError(t, err)
}

// idleBoundedManager is a manager whose idle timeout is short enough to observe
// in a test, so a peer loop that is still servicing its own select can be told
// apart from one that is wedged.
func idleBoundedManager(t *testing.T, idle time.Duration) *PeerManager {
	t.Helper()

	genesis := syncGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	tSettings := managerSettings()
	tSettings.ChainCfgParams = syncTestParams(nil)
	tSettings.Legacy.PeerIdleTimeout = idle

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	require.NoError(t, m.ConfigureSync(SyncConfig{
		Index:        idx,
		Ingestor:     &recordingBodyIngestor{},
		TickInterval: 20 * time.Millisecond,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))
	t.Cleanup(func() { require.NoError(t, m.Stop()) })

	return m
}

// TestBlockTxn_DribbledUnsolicitedPayloadDoesNotWedgeThePeerLoop is F1.
//
// payloadStream.Close drains the whole declared payload with io.Copy, and the
// transport sets no read deadline. Running that drain on Peer.Run's goroutine
// hands any peer a way to pin the loop for as long as it cares to dribble: the
// idle timer, the ping ticker and the ctx select are all serviced by that one
// goroutine.
//
// startIngest states the rule for exactly this reason (peer.go): "A drain runs
// io.Copy over up to MaxBlockPayload bytes ON THIS GOROUTINE, which is the one
// servicing the idle timer and ctx cancellation — a peer that declares a huge
// payload and then dribbles would hold the loop for as long as it liked."
//
// The flag is OFF here deliberately. Nothing about this needs compact blocks
// negotiated or enabled: blocktxn is decoded and streamed by the transport
// either way, so the exposure is flag-independent.
func TestBlockTxn_DribbledUnsolicitedPayloadDoesNotWedgeThePeerLoop(t *testing.T) {
	const idle = 3 * time.Second

	m := idleBoundedManager(t, idle)

	far := dialScripted(t, m.ListenAddrs()[0])
	t.Cleanup(func() { _ = far.nc.Close() })

	version := remoteVersion(4321)
	version.Services = wire.SFNodeNetwork
	far.completeOutboundHandshakeAs(t, version)

	require.Eventually(t, func() bool { return m.ConnectedCount() == 1 }, 5*time.Second, 20*time.Millisecond)

	// 100 MB promised, 33 bytes sent, nothing after that. No peer asked for
	// these transactions, so this takes the unsolicited path.
	writeRawBlockTxnFrame(t, far, chainhash.Hash{0x77}, 0, 100_000_000)

	// The loop must still be its own master: the idle timer is what ends a peer
	// that has gone quiet, and it can only fire if Run is servicing its select.
	require.Eventually(t, func() bool { return m.ConnectedCount() == 0 }, 20*time.Second, 50*time.Millisecond,
		"the peer loop must stay responsive while an undelivered blocktxn payload is released")
}

// TestCompactBlock_ClaimEndingByDeliveryReleasesThePartialBlock is F3.
//
// A partial block hangs off the QueuedBlock in C++ (node_state.h:80) and dies
// with it, so every path that ends the claim must end the reconstruction. Round
// 1 released it only through the dispatcher's own paths and clearPeer, which
// misses the claim ending by DELIVERY: removeFromFlight runs, the peer stays
// connected, and the partial block is stranded.
//
// The sequence here is entirely peer-driven and needs no parallel fetch: the
// peer announces a compact block, we ask for its gaps, and the peer then sends
// the same block whole. That is a legal thing for it to do, and BlockReceived
// releases the claim while the partial block is still waiting on a blocktxn.
//
// Stranded, it costs two things, both asserted below: every later cmpctblock
// from that peer is refused as "already reconstructing" for the life of the
// connection, and a late blocktxn is accepted and assembles a block we hold.
func TestCompactBlock_ClaimEndingByDeliveryReleasesThePartialBlock(t *testing.T) {
	genesis := syncGenesis()
	first := minedRun(genesis, 1, 41)[0]
	second := minedRun(genesis, 1, 42)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	firstTxs := testTxs(4, 0x21)
	secondTxs := testTxs(3, 0x22)

	txIdx := newTestTxIndex(firstTxs[1], secondTxs[1], secondTxs[2])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, first, firstTxs, 0))

	req, ok := far.readUntil(t, wire.CmdGetBlockTxn).(*wire.MsgGetBlockTxn)
	require.True(t, ok)
	require.Equal(t, first.BlockHash(), req.BlockHash)

	state := onlySyncPeerState(t, m)

	m.syncMu.Lock()
	held := state.compact != nil
	m.syncMu.Unlock()
	require.True(t, held, "the partial block must be outstanding before the claim ends")

	// The peer answers the whole block instead. It is in flight from this peer,
	// so the ingest is accepted and BlockDone releases the claim.
	far.write(t, blockFor(first))

	require.Eventually(t, func() bool { return ingestor.count() == 1 }, 10*time.Second, 20*time.Millisecond,
		"the whole block must be ingested")

	require.Eventually(t, func() bool {
		m.syncMu.Lock()
		defer m.syncMu.Unlock()

		return state.compact == nil && state.compactIngest == nil
	}, 10*time.Second, 20*time.Millisecond,
		"a claim that ends by delivery must release the partial block with it")

	// Consequence one: the compact path must still work for this peer.
	far.write(t, compactBlockFor(t, second, secondTxs, 0))

	require.Eventually(t, func() bool { return ingestor.count() == 2 }, 10*time.Second, 20*time.Millisecond,
		"a later compact block from the same peer must still be reconstructed")

	require.Equal(t, []chainhash.Hash{first.BlockHash(), second.BlockHash()}, ingestor.hashes())

	// Consequence two: the blocktxn we asked for, arriving late, is now
	// unsolicited — it must not assemble a block we already hold.
	far.write(t, blockTxnFor(t, first.BlockHash(), firstTxs, req.Indexes))

	require.Never(t, func() bool { return ingestor.count() > 2 }, 2*time.Second, 50*time.Millisecond,
		"a blocktxn for a claim that has ended must be treated as unsolicited")

	require.Equal(t, int32(1), m.ConnectedCount())

	snaps := m.Snapshots()
	require.Len(t, snaps, 1)
	require.Equal(t, 0, snaps[0].MisbehaviorScore, "none of this is the peer's fault")
}

// TestCompactBlock_HoldIsRefusedWhenTheClaimIsGone is F3's second half.
//
// CompactBlock releases syncMu to run newCompactState, because TxIndex.Match
// hashes every entry the index holds and must not run under the lock. A
// disconnect, a rotation or another peer's delivery inside that window ends the
// claim, and the write that follows would otherwise resurrect a partial block
// for a claim that no longer exists — stranding the peer exactly as F3
// describes, but with no delivery involved.
func TestCompactBlock_HoldIsRefusedWhenTheClaimIsGone(t *testing.T) {
	genesis := syncGenesis()
	header := minedRun(genesis, 1, 43)[0]

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	txs := testTxs(3, 0x23)
	txIdx := newTestTxIndex(txs[1])

	ingestor := &recordingBodyIngestor{}
	m := compactSyncManager(t, idx, ingestor, txIdx)

	far := connectCompactPeer(t, m)
	far.write(t, compactBlockFor(t, header, txs, 0))

	_, ok := far.readUntil(t, wire.CmdGetBlockTxn).(*wire.MsgGetBlockTxn)
	require.True(t, ok)

	sp := onlySyncPeer(t, m)
	state := onlySyncPeerState(t, m)

	hash := header.BlockHash()

	// Build a second partial block for the same announcement, standing in for
	// the one CompactBlock holds while the lock is released.
	msg := compactBlockFor(t, header, txs, 0)

	fresh, status, err := newCompactState(msg, txIdx)
	require.NoError(t, err)
	require.Equal(t, readOK, status)

	// The claim ends while that work is in flight.
	m.syncMu.Lock()
	m.blockDownloader.BlockFailed(sp, hash, time.Now().UnixMicro())
	state.compact = nil
	m.syncMu.Unlock()

	require.False(t, m.holdCompactBlock(sp, fresh),
		"a partial block must not be held for a claim that has ended")

	m.syncMu.Lock()
	stranded := state.compact
	m.syncMu.Unlock()

	require.Nil(t, stranded, "nothing may be left on the peer after a refused hold")
}
