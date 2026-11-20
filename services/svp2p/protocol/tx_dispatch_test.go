package protocol

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// blockingTxIngestor is the tx-side counterpart to blockingIngestor: it lets
// a test hold Ingest open long enough to prove the peer loop stays
// responsive while validation is in flight (F5's "dispatch off the peer
// loop" requirement), without a real validator.
type blockingTxIngestor struct {
	mu       sync.Mutex
	calls    []*wire.MsgTx
	peerAddr string

	started chan struct{}
	release chan struct{}
	outcome TxIngestOutcome
}

func newBlockingTxIngestor(outcome TxIngestOutcome) *blockingTxIngestor {
	return &blockingTxIngestor{
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
		outcome: outcome,
	}
}

func (b *blockingTxIngestor) Ingest(ctx context.Context, msg *wire.MsgTx, peerAddr string) TxIngestOutcome {
	b.mu.Lock()
	b.calls = append(b.calls, msg)
	b.peerAddr = peerAddr
	b.mu.Unlock()

	select {
	case b.started <- struct{}{}:
	default:
	}

	select {
	case <-b.release:
	case <-ctx.Done():
	}

	return b.outcome
}

func (b *blockingTxIngestor) Rejected(chainhash.Hash) bool { return false }

func (b *blockingTxIngestor) callCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.calls)
}

// debugCaptureLogger records Warnf lines — the level queueTx logs a dropped
// tx at (review round 1, Important 3/Minor 5: matches this file's own
// convention of Warn for a drop, e.g. Peer.send). Named for its original
// level; kept rather than renamed to avoid a needless diff across every
// existing call site.
type debugCaptureLogger struct {
	ulogger.TestLogger

	mu   sync.Mutex
	logs []string
}

func (l *debugCaptureLogger) Warnf(format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.logs = append(l.logs, fmt.Sprintf(format, args...))
}

func (l *debugCaptureLogger) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, line := range l.logs {
		if strings.Contains(line, substr) {
			return true
		}
	}

	return false
}

func newTxIngestingTestPeer(t *testing.T, ingestor TxIngestor) (*Peer, *scriptedPeer) {
	t.Helper()

	return newTxIngestingTestPeerWithLogger(t, ingestor, ulogger.TestLogger{})
}

func newTxIngestingTestPeerWithLogger(t *testing.T, ingestor TxIngestor, logger ulogger.Logger) (*Peer, *scriptedPeer) {
	t.Helper()

	a, b := net.Pipe()
	conn := transport.New(a, transport.Config{
		Net: wire.MainNet, ProtocolVersion: wire.ProtocolVersion,
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
		Conn: conn, Logger: logger,
		IdleTimeout: 5 * time.Second, PingInterval: 5 * time.Second, BanThreshold: 100,
		TxIngestor: ingestor,
	}

	return NewPeer(cfg), &scriptedPeer{nc: b}
}

// TestPeerDispatchesInboundTxOffTheLoop is F5's "must not stall the loop"
// requirement, proved rather than asserted: TxIngestor.Ingest is held open
// by blockingTxIngestor, and the peer is still required to answer a ping
// sent to it while that call is in flight. If dispatchSync called Ingest
// synchronously on the Run goroutine, this ping would never be answered
// because Run would be parked inside handleMessage.
func TestPeerDispatchesInboundTxOffTheLoop(t *testing.T) {
	ing := newBlockingTxIngestor(TxIngestOutcome{Accepted: true})
	peer, sp := newTxIngestingTestPeer(t, ing)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- peer.Run(ctx) }()

	completeHandshake(t, sp)

	tx := wire.NewMsgTx(1)
	sp.write(t, tx)

	select {
	case <-ing.started:
	case <-time.After(5 * time.Second):
		t.Fatal("tx ingestor was never called")
	}

	// The Run loop must still be alive and answering pings while Ingest is
	// blocked: this is the payoff of dispatching off the loop.
	sp.write(t, wire.NewMsgPing(4242))
	pong := sp.readUntil(t, wire.CmdPong)
	require.Equal(t, uint64(4242), pong.(*wire.MsgPong).Nonce)

	close(ing.release)

	require.Equal(t, 1, ing.callCount())

	peer.Disconnect("test done")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after disconnect")
	}
}

// TestPeerSendsTxRejectFromIngestor proves the other half of the seam: a
// rejected outcome's wire.MsgReject actually reaches the peer. bridge stays
// I/O-free (it only returns the message, per ingest_tx.go); this is where
// that returned message is actually sent.
func TestPeerSendsTxRejectFromIngestor(t *testing.T) {
	tx := wire.NewMsgTx(1)
	txHash := tx.TxHash()

	reject := wire.NewMsgReject(wire.CmdTx, wire.RejectInvalid, "rejected")
	reject.Hash = txHash

	ing := newBlockingTxIngestor(TxIngestOutcome{Reject: reject})
	// Release immediately: this test is about the message that comes back,
	// not about blocking behavior.
	close(ing.release)

	peer, sp := newTxIngestingTestPeer(t, ing)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() { _ = peer.Run(ctx) }()

	completeHandshake(t, sp)
	sp.write(t, tx)

	msg := sp.readUntil(t, wire.CmdReject)
	got, ok := msg.(*wire.MsgReject)
	require.True(t, ok)
	require.Equal(t, wire.CmdTx, got.Cmd)
	require.Equal(t, wire.RejectInvalid, got.Code)
	require.Equal(t, txHash, got.Hash)

	peer.Disconnect("test done")
}

// TestPeerDropsTxWhenIngestQueueFull is queueTx's own backpressure contract:
// with the single worker parked inside a blocked Ingest call, a peer that
// keeps sending txs past txIngestQueueDepth must have the excess dropped
// (logged at Warn, and counted in PeerSnapshot.TxDropped — review round 1,
// Important 3/Minor 5) rather than have dispatchSync (and so the whole Run
// loop) block waiting for room in txMsgCh.
func TestPeerDropsTxWhenIngestQueueFull(t *testing.T) {
	ing := newBlockingTxIngestor(TxIngestOutcome{})
	logger := &debugCaptureLogger{}
	peer, sp := newTxIngestingTestPeerWithLogger(t, ing, logger)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	go func() { _ = peer.Run(ctx) }()

	completeHandshake(t, sp)

	// The first tx is taken by the single worker and held there by
	// blockingTxIngestor until release, which this test never signals: every
	// subsequent tx queues in txMsgCh (capacity txIngestQueueDepth) behind
	// it. Sending depth+1 more therefore guarantees exactly one drop,
	// without a race against how fast the worker drains — it can't drain at
	// all until release.
	sp.write(t, wire.NewMsgTx(1))

	select {
	case <-ing.started:
	case <-time.After(5 * time.Second):
		t.Fatal("tx ingestor was never called for the first tx")
	}

	for i := 0; i < txIngestQueueDepth+1; i++ {
		tx := wire.NewMsgTx(1)
		tx.LockTime = uint32(i) //nolint:gosec // test data, distinguishes each tx's hash
		sp.write(t, tx)
	}

	require.Eventually(t, func() bool {
		return logger.contains("tx ingest queue full")
	}, 5*time.Second, 10*time.Millisecond, "queueTx must drop and log once the queue is full")

	require.Equal(t, uint64(1), peer.Info().TxDropped, "exactly one of the depth+1 sends must have been dropped and counted")

	peer.Disconnect("test done")
}
