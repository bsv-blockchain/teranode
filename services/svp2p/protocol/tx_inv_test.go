package protocol

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/expiringmap"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// BlockDownloader.RequestTxs: the CONSUME half of Task 16's tx-inv round
// trip, unit level (no Kafka, no sockets — fullNodePeer gives a *SyncPeer
// with a real peerSyncState but no live connection).
// ---------------------------------------------------------------------------

func TestRequestTxs_BuildsGetDataForAFreshHash(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	hash := chainhash.Hash{0x01}

	gdmsg := f.bd.RequestTxs(peer, []chainhash.Hash{hash}, nil)
	require.NotNil(t, gdmsg)
	require.Len(t, gdmsg.InvList, 1)
	require.Equal(t, wire.InvTypeTx, gdmsg.InvList[0].Type)
	require.Equal(t, hash, gdmsg.InvList[0].Hash)
}

// TestRequestTxs_RejectedTxSkippedButOtherRequested pairs the rejected-tx
// negative with a second, distinguishable hash that MUST be requested in
// the same call (controller addendum H6): a getdata missing the rejected
// hash and holding exactly the other one proves suppression actually ran,
// rather than nothing having run at all.
func TestRequestTxs_RejectedTxSkippedButOtherRequested(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	rejected := chainhash.Hash{0xAA}
	wanted := chainhash.Hash{0xBB}

	rejectedFn := func(h chainhash.Hash) bool { return h == rejected }

	gdmsg := f.bd.RequestTxs(peer, []chainhash.Hash{rejected, wanted}, rejectedFn)
	require.NotNil(t, gdmsg)
	require.Len(t, gdmsg.InvList, 1, "the already-rejected hash must never reach the getdata")
	require.Equal(t, wanted, gdmsg.InvList[0].Hash)
}

// TestRequestTxs_DuplicateNotDoubleRequested is H6 again: a hash already in
// the node-wide requestedTxns map is skipped on a SECOND call, paired with a
// fresh hash in that same call that must still be requested.
func TestRequestTxs_DuplicateNotDoubleRequested(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	already := chainhash.Hash{0x01}
	fresh := chainhash.Hash{0x02}

	first := f.bd.RequestTxs(peer, []chainhash.Hash{already}, nil)
	require.NotNil(t, first)
	require.Len(t, first.InvList, 1)

	second := f.bd.RequestTxs(peer, []chainhash.Hash{already, fresh}, nil)
	require.NotNil(t, second)
	require.Len(t, second.InvList, 1, "the already-requested hash must not draw a second getdata")
	require.Equal(t, fresh, second.InvList[0].Hash)
}

// TestRequestTxs_PerPeerDedupBlocksACrossPeerRerequest proves the node-wide
// half of the dedup independently of the per-peer half: a hash requested for
// peerA must not be re-requested for peerB either, while a hash peerB has
// not yet been asked for still goes out.
func TestRequestTxs_PerPeerDedupBlocksACrossPeerRerequest(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peerA := fullNodePeer("1.2.3.4:8333")
	peerB := fullNodePeer("5.6.7.8:8333")

	shared := chainhash.Hash{0x01}
	onlyB := chainhash.Hash{0x02}

	gdA := f.bd.RequestTxs(peerA, []chainhash.Hash{shared}, nil)
	require.NotNil(t, gdA)
	require.Len(t, gdA.InvList, 1)

	gdB := f.bd.RequestTxs(peerB, []chainhash.Hash{shared, onlyB}, nil)
	require.NotNil(t, gdB)
	require.Len(t, gdB.InvList, 1, "a hash already requested from peerA must not be re-requested for peerB")
	require.Equal(t, onlyB, gdB.InvList[0].Hash)
}

// TestRequestTxs_HeadersFirstSuppressesThenResumes is H3 and H6 together:
// nothing is requested while headers-first mode is on, and the SAME hashes
// ARE requested once it clears — the positive arrival that proves the
// suppression really fired rather than the call having done nothing at all.
func TestRequestTxs_HeadersFirstSuppressesThenResumes(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	hashes := []chainhash.Hash{{0x01}, {0x02}}

	f.hs.headersFirstMode = true
	require.Nil(t, f.bd.RequestTxs(peer, hashes, nil), "nothing may be requested while downloading headers up to a checkpoint")

	f.hs.headersFirstMode = false
	gdmsg := f.bd.RequestTxs(peer, hashes, nil)
	require.NotNil(t, gdmsg)
	require.Len(t, gdmsg.InvList, 2, "the same hashes must reach getdata once headers-first mode clears")
}

// TestRequestTxs_MarksKnownTxsEvenWhenHeadersFirstSuppressesGetData is the
// ordering the controller's ruling made load-bearing: legacy's
// peer.AddKnownInventory (manager.go:2371) runs BEFORE the headers-first
// check (manager.go:2373-2376), so a tx this peer announced during
// headers-first sync is still recorded as known to them — the tx
// announcement relay (relay.go selectTxRelayTargets) must never re-offer
// it back — even though no getdata goes out for it. Marking AFTER the
// headers-first return, or not at all, would silently lose that fact for
// exactly the hashes this suppression is about.
func TestRequestTxs_MarksKnownTxsEvenWhenHeadersFirstSuppressesGetData(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	hash := chainhash.Hash{0x01}

	f.hs.headersFirstMode = true
	require.Nil(t, f.bd.RequestTxs(peer, []chainhash.Hash{hash}, nil), "no getdata while headers-first is on")
	require.True(t, peer.State.knownTxs.has(hash), "the hash must still be marked known, even though it was not requested")
}

// TestRequestTxs_MarksKnownTxsBeforeCheckingRejectedOrDedup proves the
// marking is unconditional on the OTHER suppressions too, not just
// headers-first: a rejected hash and an already-requested hash both still
// get marked known, exactly as legacy's unconditional AddKnownInventory
// does before the have-inventory/rejected checks that follow it
// (manager.go:2380-2408 run only after the mark at :2371).
func TestRequestTxs_MarksKnownTxsBeforeCheckingRejectedOrDedup(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	rejected := chainhash.Hash{0xAA}
	duplicate := chainhash.Hash{0xBB}

	// Pre-seed the global dedup map so `duplicate` is already "requested".
	first := f.bd.RequestTxs(peer, []chainhash.Hash{duplicate}, nil)
	require.NotNil(t, first)

	rejectedFn := func(h chainhash.Hash) bool { return h == rejected }

	gdmsg := f.bd.RequestTxs(peer, []chainhash.Hash{rejected, duplicate}, rejectedFn)
	require.Nil(t, gdmsg, "both entries are suppressed, one by rejection and one by dedup")
	require.True(t, peer.State.knownTxs.has(rejected), "a rejected hash must still be marked known")
	require.True(t, peer.State.knownTxs.has(duplicate), "an already-requested hash must still be marked known")
}

func TestRequestTxs_NoHashesOrNilPeerIsANoOp(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := fullNodePeer("1.2.3.4:8333")

	require.Nil(t, f.bd.RequestTxs(peer, nil, nil))
	require.Nil(t, f.bd.RequestTxs(nil, []chainhash.Hash{{0x01}}, nil))
}

// ---------------------------------------------------------------------------
// PeerManager.InvFromKafka: the same consume half, wired end to end over a
// real PeerManager and a real socket, standing in for
// bridge.StartLegacyInvConsumer's decoded callback.
// ---------------------------------------------------------------------------

// recordingRejectedIngestor is a TxIngestor whose Rejected reports true for
// exactly the hashes it is built with — enough to drive
// PeerManager.InvFromKafka's rejectedTxns suppression (H1) without a real
// bridge.
type recordingRejectedIngestor struct {
	mu       sync.Mutex
	rejected map[chainhash.Hash]bool
}

func newRecordingRejectedIngestor(rejected ...chainhash.Hash) *recordingRejectedIngestor {
	m := make(map[chainhash.Hash]bool, len(rejected))
	for _, h := range rejected {
		m[h] = true
	}

	return &recordingRejectedIngestor{rejected: m}
}

func (r *recordingRejectedIngestor) Ingest(context.Context, *wire.MsgTx, string) TxIngestOutcome {
	return TxIngestOutcome{}
}

func (r *recordingRejectedIngestor) Rejected(h chainhash.Hash) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.rejected[h]
}

// invKafkaTestFixture is a real, running PeerManager with block sync wired
// (so blockDownloader exists), used to test InvFromKafka against real
// sockets and real peer registration.
type invKafkaTestFixture struct {
	m *PeerManager
}

func newInvKafkaTestFixture(t *testing.T, txIngestor TxIngestor) *invKafkaTestFixture {
	t.Helper()

	genesis := syncGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	tSettings := managerSettings()
	tSettings.ChainCfgParams = syncTestParams(nil)

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	require.NoError(t, m.ConfigureSync(SyncConfig{
		Index:      idx,
		Ingestor:   &recordingIngestor{},
		TxIngestor: txIngestor,
	}))

	ctx, cancel := context.WithCancel(context.Background())
	require.NoError(t, m.Start(ctx, []string{"127.0.0.1:0"}))

	t.Cleanup(func() {
		cancel()
		_ = m.Stop()
	})

	return &invKafkaTestFixture{m: m}
}

// peersSnapshot mirrors relayTestFixture's own helper (unexported, this
// file's own copy — kept local rather than shared, matching this package's
// existing per-file fixture convention).
func (f *invKafkaTestFixture) peersSnapshot() map[*Peer]*SyncPeer {
	f.m.mu.Lock()
	defer f.m.mu.Unlock()

	out := make(map[*Peer]*SyncPeer, len(f.m.peers))
	for p, s := range f.m.peers {
		out[p] = s
	}

	return out
}

// connect dials the manager and completes the handshake, returning the
// scripted peer and the manager's own *SyncPeer (for its Addr, which
// InvFromKafka matches on).
func (f *invKafkaTestFixture) connect(t *testing.T) (*scriptedPeer, *SyncPeer) {
	t.Helper()

	before := f.peersSnapshot()

	far := dialScripted(t, f.m.ListenAddrs()[0])

	version := remoteVersion(uint64(time.Now().UnixNano())) //nolint:gosec // test-only nonce
	version.Services = wire.SFNodeNetwork
	far.completeOutboundHandshakeAs(t, version)

	_, ok := far.readUntil(t, wire.CmdGetHeaders).(*wire.MsgGetHeaders)
	require.True(t, ok)
	far.write(t, wire.NewMsgHeaders())

	var syncPeer *SyncPeer

	require.Eventually(t, func() bool {
		f.m.mu.Lock()
		defer f.m.mu.Unlock()

		for p, s := range f.m.peers {
			if _, existed := before[p]; !existed {
				syncPeer = s
				return true
			}
		}

		return false
	}, 2*time.Second, 10*time.Millisecond, "the manager never registered the new connection")

	return far, syncPeer
}

// TestInvFromKafka_RoundTripsToGetData is the produce/consume seam's own
// consume half, proved over a real socket: a hash handed to InvFromKafka for
// a connected peer's address comes back out as a getdata to that same peer.
func TestInvFromKafka_RoundTripsToGetData(t *testing.T) {
	f := newInvKafkaTestFixture(t, nil)

	peer, syncPeer := f.connect(t)

	hash := chainhash.Hash{0xCD}
	f.m.InvFromKafka(syncPeer.Addr, []chainhash.Hash{hash})

	gdmsg, ok := peer.readUntil(t, wire.CmdGetData).(*wire.MsgGetData)
	require.True(t, ok)
	require.Len(t, gdmsg.InvList, 1)
	require.Equal(t, hash, gdmsg.InvList[0].Hash)
	require.Equal(t, wire.InvTypeTx, gdmsg.InvList[0].Type)
}

// TestInvFromKafka_DepartedPeerDroppedQuietlyAlongsideALivePeer is H5 and H6
// together: a message for an address nobody holds is dropped with no panic
// and no getdata anywhere, proved alongside a SECOND, live peer whose own
// hash IS answered in the same test — so "nothing happened for the departed
// address" cannot be mistaken for "nothing happened at all".
func TestInvFromKafka_DepartedPeerDroppedQuietlyAlongsideALivePeer(t *testing.T) {
	f := newInvKafkaTestFixture(t, nil)

	livePeer, liveSync := f.connect(t)

	departedHash := chainhash.Hash{0x01}
	liveHash := chainhash.Hash{0x02}

	require.NotPanics(t, func() {
		f.m.InvFromKafka("10.0.0.1:9999", []chainhash.Hash{departedHash})
	})

	f.m.InvFromKafka(liveSync.Addr, []chainhash.Hash{liveHash})

	gdmsg, ok := livePeer.readUntil(t, wire.CmdGetData).(*wire.MsgGetData)
	require.True(t, ok)
	require.Len(t, gdmsg.InvList, 1)
	require.Equal(t, liveHash, gdmsg.InvList[0].Hash, "the live peer's own hash must still be requested")
}

// TestInvFromKafka_RejectedTxSuppressedButOtherRequested is H1's seam,
// exercised end to end through the manager: a hash the TxIngestor reports
// as rejected never reaches getdata, while a second hash in the same call
// does.
func TestInvFromKafka_RejectedTxSuppressedButOtherRequested(t *testing.T) {
	rejectedHash := chainhash.Hash{0xAA}
	wantedHash := chainhash.Hash{0xBB}

	f := newInvKafkaTestFixture(t, newRecordingRejectedIngestor(rejectedHash))

	peer, syncPeer := f.connect(t)

	f.m.InvFromKafka(syncPeer.Addr, []chainhash.Hash{rejectedHash, wantedHash})

	gdmsg, ok := peer.readUntil(t, wire.CmdGetData).(*wire.MsgGetData)
	require.True(t, ok)
	require.Len(t, gdmsg.InvList, 1, "the rejected hash must never reach getdata")
	require.Equal(t, wantedHash, gdmsg.InvList[0].Hash)
}

func TestInvFromKafka_NoHashesIsANoOp(t *testing.T) {
	f := newInvKafkaTestFixture(t, nil)

	require.NotPanics(t, func() {
		f.m.InvFromKafka("1.2.3.4:8333", nil)
	})
}

// ---------------------------------------------------------------------------
// PeerManager.Inv: the PRODUCE half, proved with a fake TxInvProducer.
// ---------------------------------------------------------------------------

type recordingTxInvProducer struct {
	mu    sync.Mutex
	calls []recordedProduceCall
}

type recordedProduceCall struct {
	peerAddr string
	hashes   []chainhash.Hash
}

func (p *recordingTxInvProducer) Produce(peerAddr string, hashes []chainhash.Hash) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.calls = append(p.calls, recordedProduceCall{peerAddr: peerAddr, hashes: append([]chainhash.Hash(nil), hashes...)})
}

func (p *recordingTxInvProducer) len() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return len(p.calls)
}

func (p *recordingTxInvProducer) at(i int) recordedProduceCall {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.calls[i]
}

// TestInv_ProducesTxHashesToKafkaAfterUnlockingSyncMu proves the produce
// side end to end: a real inv message carrying tx entries reaches the
// TxInvProducer with the peer's address and every tx hash, and reaching it
// does not deadlock — which it would if Produce were ever called while
// syncMu is still held, since the fixture's fake Produce takes no lock at
// all and returning at all proves the call happened outside one.
func TestInv_ProducesTxHashesToKafkaAfterUnlockingSyncMu(t *testing.T) {
	genesis := syncGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	tSettings := managerSettings()
	tSettings.ChainCfgParams = syncTestParams(nil)

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	producer := &recordingTxInvProducer{}

	require.NoError(t, m.ConfigureSync(SyncConfig{
		Index:         idx,
		Ingestor:      &recordingIngestor{},
		TxInvProducer: producer,
	}))

	peer := fullNodePeer("1.2.3.4:8333")

	hash1 := chainhash.Hash{0x01}
	hash2 := chainhash.Hash{0x02}

	msg := wire.NewMsgInv()
	require.NoError(t, msg.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &hash1)))
	require.NoError(t, msg.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &hash2)))

	_, err = m.Inv(peer, msg)
	require.NoError(t, err)

	require.Equal(t, 1, producer.len())
	call := producer.at(0)
	require.Equal(t, peer.Addr, call.peerAddr)
	require.ElementsMatch(t, []chainhash.Hash{hash1, hash2}, call.hashes)
}

// TestInv_NoProducerConfiguredDropsTxHashesWithoutError mirrors the
// "no ingestor wired" pattern this package uses everywhere else: a manager
// with no TxInvProducer still answers Inv without error, it just has
// nowhere to send the collected tx hashes.
func TestInv_NoProducerConfiguredDropsTxHashesWithoutError(t *testing.T) {
	genesis := syncGenesis()

	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	tSettings := managerSettings()
	tSettings.ChainCfgParams = syncTestParams(nil)

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	require.NoError(t, m.ConfigureSync(SyncConfig{
		Index:    idx,
		Ingestor: &recordingIngestor{},
	}))

	peer := fullNodePeer("1.2.3.4:8333")

	hash := chainhash.Hash{0x01}
	msg := wire.NewMsgInv()
	require.NoError(t, msg.AddInvVect(wire.NewInvVect(wire.InvTypeTx, &hash)))

	msgs, err := m.Inv(peer, msg)
	require.NoError(t, err)
	require.Empty(t, msgs)
}

// ---------------------------------------------------------------------------
// requestedTxns lifecycle: fix round 2, Important 3/4. Both tests use a
// short-TTL requestedTxns map built directly (bypassing newPeerSyncState's
// fixed 10s constant) so the observable signal — an entry surviving past
// its own TTL — resolves in milliseconds rather than seconds. The signal
// itself is ExpiringMap.Len(), not Get(): Get filters expired entries
// inline regardless of whether the background ticker is still running
// (see that method's own doc comment), so only Len() — which reports the
// raw map size, touched only by the ticker's own clean() — can actually
// distinguish "the TTL goroutine is still evicting" from "it is not".
// ---------------------------------------------------------------------------

const lifecycleTestTTL = 30 * time.Millisecond

// syncPeerWithShortTTL builds a *SyncPeer whose requestedTxns map expires
// almost immediately, for the two lifecycle tests below.
func syncPeerWithShortTTL(addr string) *SyncPeer {
	return NewSyncPeer(addr, wire.SFNodeNetwork, &peerSyncState{
		knownBlocks:   newKnownBlockSet(knownBlockCap),
		knownTxs:      newKnownBlockSet(knownTxCap),
		requestedTxns: expiringmap.New[chainhash.Hash, struct{}](lifecycleTestTTL),
	})
}

// TestClearPeer_RotationLeavesRequestedTxnsTickerRunning is Important 3:
// clearPeer (reached by a ROTATION, where the peer stays connected — see
// its own doc comment) must NOT stop requestedTxns' TTL goroutine. Proved
// positively: an entry set before the rotation is still auto-evicted by
// the still-running ticker after the TTL elapses — if clearPeer had
// stopped it (the bug this test would have caught), the entry would still
// be sitting in the map, uncollected, forever.
func TestClearPeer_RotationLeavesRequestedTxnsTickerRunning(t *testing.T) {
	f := newDownloadFixture(t, 3)
	peer := syncPeerWithShortTTL("1.2.3.4:8333")

	peer.State.requestedTxns.Set(chainhash.Hash{0x01}, struct{}{})
	require.Equal(t, 1, peer.State.requestedTxns.Len())

	// The rotate branch's own call (blockdownload.go CheckStall): the peer
	// stays connected, only its download bookkeeping is cleared.
	f.bd.clearPeer(peer)

	require.Eventually(t, func() bool {
		return peer.State.requestedTxns.Len() == 0
	}, 2*time.Second, 5*time.Millisecond, "a rotation must not stop requestedTxns' TTL goroutine — the entry should still be auto-evicted")
}

// TestPeerGone_StopsRequestedTxnsTickerEvenWithNoBlockSyncConfigured is
// Important 4: peerGone — the actual, one-time disconnect path — must stop
// requestedTxns' TTL goroutine unconditionally, even when m.headerSync is
// nil (no Ingestor injected, exactly Server.startSync's own depless-server
// shape). Proved positively: an entry set before the disconnect is NEVER
// auto-evicted afterward, even long past its own TTL — proving the ticker
// actually stopped, not merely that the test did not wait long enough.
func TestPeerGone_StopsRequestedTxnsTickerEvenWithNoBlockSyncConfigured(t *testing.T) {
	tSettings := managerSettings()
	tSettings.ChainCfgParams = syncTestParams(nil)

	banList, err := NewBanList("")
	require.NoError(t, err)

	m := NewPeerManager(ulogger.TestLogger{}, tSettings, banList)

	genesis := syncGenesis()
	idx, err := NewHeaderIndex(genesis)
	require.NoError(t, err)

	// Index alone, deliberately no Ingestor: ConfigureSync leaves
	// m.headerSync/m.blockDownloader nil, matching Server.startSync's own
	// shape when the bridge dependencies are not injected (a depless
	// server) — exactly the configuration Important 4 was filed against.
	require.NoError(t, m.ConfigureSync(SyncConfig{Index: idx}))

	peer := syncPeerWithShortTTL("1.2.3.4:8333")

	peer.State.requestedTxns.Set(chainhash.Hash{0x01}, struct{}{})
	require.Equal(t, 1, peer.State.requestedTxns.Len())

	m.peerGone(peer)

	// A fixed wait, not require.Eventually: this test's whole claim is that
	// NOTHING happens to the entry, ever — waiting several TTLs past the
	// point eviction would have occurred (were the ticker still running)
	// and finding the entry still there is the actual proof, not an
	// impatient poll that gave up too early.
	time.Sleep(10 * lifecycleTestTTL)

	require.Equal(t, 1, peer.State.requestedTxns.Len(), "an actual disconnect must stop requestedTxns' TTL goroutine — with no block sync configured, this was the ONLY path that ever could")
}
