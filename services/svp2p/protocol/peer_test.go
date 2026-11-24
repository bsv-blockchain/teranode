package protocol

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/transport"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

type scriptedPeer struct {
	nc net.Conn
}

func (s *scriptedPeer) read(t *testing.T) wire.Message {
	t.Helper()

	require.NoError(t, s.nc.SetReadDeadline(time.Now().Add(5*time.Second)))

	_, msg, _, err := wire.ReadMessageWithEncodingN(s.nc, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)

	return msg
}

func (s *scriptedPeer) write(t *testing.T, msg wire.Message) {
	t.Helper()

	_, err := wire.WriteMessageWithEncodingN(s.nc, msg, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	require.NoError(t, err)
}

// writeAsync writes on its own goroutine and ignores the outcome. A block
// message is bigger than net.Pipe's zero buffer, so the write only completes
// once the consumer has taken the whole payload — which is exactly what a
// stalled-ingest test is holding up.
func (s *scriptedPeer) writeAsync(msg wire.Message) {
	go func() {
		_, _ = wire.WriteMessageWithEncodingN(s.nc, msg, wire.ProtocolVersion, wire.MainNet, wire.BaseEncoding)
	}()
}

// writeStalledBlockFrame writes a "block" message that declares `declared`
// payload bytes but carries only what the streaming transport reads before it
// hands the stream to its consumer: the 80 byte block header and the
// transaction count. The rest never arrives, so anything that tries to read
// the remaining payload blocks for ever.
func (s *scriptedPeer) writeStalledBlockFrame(t *testing.T, header *wire.BlockHeader, declared uint32) {
	t.Helper()

	var payload bytes.Buffer

	require.NoError(t, header.Serialize(&payload))
	require.NoError(t, wire.WriteVarInt(&payload, wire.ProtocolVersion, 1))

	frame := make([]byte, wire.MessageHeaderSize)
	binary.LittleEndian.PutUint32(frame[0:4], uint32(wire.MainNet))
	copy(frame[4:4+wire.CommandSize], wire.CmdBlock)
	binary.LittleEndian.PutUint32(frame[16:20], declared)
	// Bytes 20:24 are the payload checksum, which the streaming path does not
	// verify (see the note on transport.BlockStream).

	_, err := s.nc.Write(frame)
	require.NoError(t, err)

	_, err = s.nc.Write(payload.Bytes())
	require.NoError(t, err)
}

// readUntil reads messages until one carries the wanted command, so a test
// can assert on a sync message without scripting every ping and sendheaders
// that shares the lane with it.
func (s *scriptedPeer) readUntil(t *testing.T, want string) wire.Message {
	t.Helper()

	for i := 0; i < 64; i++ {
		msg := s.read(t)
		if msg.Command() == want {
			return msg
		}
	}

	t.Fatalf("no %s message received", want)

	return nil
}

func newTestPeer(t *testing.T, idle, ping time.Duration) (*Peer, *scriptedPeer) {
	t.Helper()

	return newIngestingTestPeer(t, idle, ping, nil)
}

func newIngestingTestPeer(t *testing.T, idle, ping time.Duration, ingestor BlockIngestor) (*Peer, *scriptedPeer) {
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
		Conn: conn, Logger: ulogger.TestLogger{},
		IdleTimeout: idle, PingInterval: ping, BanThreshold: 100,
		Ingestor: ingestor,
	}

	return NewPeer(cfg), &scriptedPeer{nc: b}
}

// ingestPrefix is how much of the block stream a fake ingest consumes before
// it stalls. It is the only thing that separates the three states the idle
// timer has to tell apart.
type ingestPrefix int

const (
	// ingestReadsNothing is IngestBlock parked in its LOCAL pre-read waits
	// (WaitForBlockAssemblyReady, waitForPreviousBlockMined). The peer is not
	// at fault.
	ingestReadsNothing ingestPrefix = iota

	// ingestReadsOneByte is a peer that started delivering the payload and
	// then went silent mid-stream. The peer IS at fault.
	ingestReadsOneByte

	// ingestReadsAll is the whole payload delivered, with the pipeline's
	// post-stream validation tail still to run. The peer is not at fault, and
	// its progress stamp can never move again.
	ingestReadsAll
)

// blockingIngestor holds a block stream open the way a real ingest of a fat
// block does, so a test can drive the peer's idle timer against it.
type blockingIngestor struct {
	prefix ingestPrefix

	started chan struct{}
	release chan struct{}
}

func newBlockingIngestor(prefix ingestPrefix) *blockingIngestor {
	return &blockingIngestor{
		prefix:  prefix,
		started: make(chan struct{}, 1),
		release: make(chan struct{}),
	}
}

func (b *blockingIngestor) WatchProgress(r io.ReadCloser) IngestProgress {
	return newTestProgress(r)
}

func (b *blockingIngestor) Ingest(ctx context.Context, req BlockIngestRequest) IngestOutcome {
	defer func() { _ = req.TxReader.Close() }()

	switch b.prefix {
	case ingestReadsOneByte:
		if _, err := io.ReadFull(req.TxReader, make([]byte, 1)); err != nil {
			return IngestOutcome{Err: err}
		}

	case ingestReadsAll:
		if _, err := io.Copy(io.Discard, req.TxReader); err != nil {
			return IngestOutcome{Err: err}
		}

	case ingestReadsNothing:
	}

	select {
	case b.started <- struct{}{}:
	default:
	}

	select {
	case <-b.release:
	case <-req.Quit:
	case <-ctx.Done():
	}

	return IngestOutcome{}
}

// TestPeerIdleTimerToleratesLocalIngestWait is the ProgressReader rule the
// peer loop must honour: an ingest that has read no payload byte yet is
// waiting on OUR services (WaitForBlockAssemblyReady,
// waitForPreviousBlockMined), and the peer must not be dropped for it.
func TestPeerIdleTimerToleratesLocalIngestWait(t *testing.T) {
	const idle = 150 * time.Millisecond

	ingestor := newBlockingIngestor(ingestReadsNothing)

	p, far := newIngestingTestPeer(t, idle, time.Hour, ingestor)

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(context.Background()) }()

	defer func() {
		p.Disconnect("test teardown")
		close(ingestor.release)
	}()

	completeHandshake(t, far)

	genesis := syncGenesis()
	far.writeAsync(blockFor(minedChild(genesis, testEasyBits, 9)))

	select {
	case <-ingestor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the block never reached the ingestor")
	}

	select {
	case err := <-errCh:
		t.Fatalf("peer disconnected during a local ingest wait: %v", err)
	case <-time.After(4 * idle):
	}
}

// TestPeerIdleTimerDropsStalledIngest is the other half of the rule: once
// payload bytes have started moving, they have to keep moving.
func TestPeerIdleTimerDropsStalledIngest(t *testing.T) {
	const idle = 150 * time.Millisecond

	ingestor := newBlockingIngestor(ingestReadsOneByte)

	p, far := newIngestingTestPeer(t, idle, time.Hour, ingestor)

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(context.Background()) }()

	defer close(ingestor.release)

	completeHandshake(t, far)

	genesis := syncGenesis()
	far.writeAsync(blockFor(minedChild(genesis, testEasyBits, 10)))

	select {
	case <-ingestor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the block never reached the ingestor")
	}

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.Contains(t, err.Error(), "idle")
	case <-time.After(5 * time.Second):
		t.Fatal("peer did not disconnect after the ingest stopped making progress")
	}
}

// TestPeerIdleTimerToleratesValidationTail is the third state, and the one the
// byte-silence window alone gets wrong. Once the ingest has taken every
// declared payload byte the peer owes us nothing more, and ProgressReader's
// stamp can never move again — yet the pipeline's post-stream tail
// (extendTransactions, createUtxos, createSubtrees, ProcessBlock) still has
// minutes to run on a fat block. Judged on byte silence the peer is dropped
// mid-validation, Run's deferred cancel aborts the ingest, the block is
// re-offered, and the same tail stalls the same way for ever.
func TestPeerIdleTimerToleratesValidationTail(t *testing.T) {
	const idle = 150 * time.Millisecond

	ingestor := newBlockingIngestor(ingestReadsAll)

	p, far := newIngestingTestPeer(t, idle, time.Hour, ingestor)

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(context.Background()) }()

	defer func() {
		p.Disconnect("test teardown")
		close(ingestor.release)
	}()

	completeHandshake(t, far)

	genesis := syncGenesis()
	far.writeAsync(blockFor(minedChild(genesis, testEasyBits, 11)))

	select {
	case <-ingestor.started:
	case <-time.After(5 * time.Second):
		t.Fatal("the block never reached the ingestor")
	}

	select {
	case err := <-errCh:
		t.Fatalf("peer disconnected during the post-stream validation tail: %v", err)
	case <-time.After(4 * idle):
	}
}

// fixedProgress is an IngestProgress frozen at one observation, so a test can
// put the idle timer in front of an exact ingest state — including ages no
// test can wait out, such as the MaxBlockDownloadTime ceiling.
type fixedProgress struct {
	read uint64
	last time.Time
}

func (f fixedProgress) Read([]byte) (int, error) { return 0, io.EOF }
func (f fixedProgress) Close() error             { return nil }
func (f fixedProgress) BytesRead() uint64        { return f.read }
func (f fixedProgress) LastProgress() time.Time  { return f.last }

func TestIngestAlive(t *testing.T) {
	const idle = 30 * time.Second

	// txBytes is the whole transaction payload of the ingest under test; read
	// is how much of it has arrived; lastAge and startedAge are how long ago
	// the stamp last moved and the ingest began.
	tests := []struct {
		name       string
		txBytes    uint64
		read       uint64
		lastAge    time.Duration
		startedAge time.Duration
		want       bool
	}{{
		name:       "consumed stream survives its validation tail",
		txBytes:    1000,
		read:       1000,
		lastAge:    4 * idle,
		startedAge: 4 * idle,
		want:       true,
	}, {
		name:       "consumed stream still ends at MaxBlockDownloadTime",
		txBytes:    1000,
		read:       1000,
		lastAge:    MaxBlockDownloadTime + time.Second,
		startedAge: MaxBlockDownloadTime + time.Second,
		want:       false,
	}, {
		name:       "mid-stream silence past the idle window disconnects",
		txBytes:    1000,
		read:       500,
		lastAge:    2 * idle,
		startedAge: 2 * idle,
		want:       false,
	}, {
		name:       "mid-stream progress inside the idle window survives",
		txBytes:    1000,
		read:       500,
		lastAge:    idle / 2,
		startedAge: 2 * idle,
		want:       true,
	}, {
		name:       "local pre-read wait is never the peer's fault",
		txBytes:    1000,
		read:       0,
		lastAge:    10 * idle,
		startedAge: 10 * idle,
		want:       true,
	}}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			now := time.Now()

			p := &Peer{cfg: PeerConfig{IdleTimeout: idle}}
			p.ingest = fixedProgress{read: tc.read, last: now.Add(-tc.lastAge)}
			p.ingestStarted = now.Add(-tc.startedAge)
			p.ingestTxBytes = tc.txBytes
			p.ingestActive = 1

			require.Equal(t, tc.want, p.ingestAlive())
		})
	}

	t.Run("no ingest running", func(t *testing.T) {
		p := &Peer{cfg: PeerConfig{IdleTimeout: idle}}

		require.False(t, p.ingestAlive())
	})
}

// TestTxPayloadBytes pins the count BytesRead converges on: the declared
// payload length less the header and transaction-count varint the transport
// consumed before the stream was handed over.
func TestTxPayloadBytes(t *testing.T) {
	require.Equal(t, uint64(919), txPayloadBytes(1000, 1),
		"80 byte header plus a one byte count varint")
	require.Equal(t, uint64(915), txPayloadBytes(1000, 70000),
		"a count above 65535 takes a five byte varint")
	require.Zero(t, txPayloadBytes(81, 0),
		"a payload no longer than its own prefix yields no transaction bytes")
	require.Zero(t, txPayloadBytes(10, 1),
		"an under-length declaration must floor at zero, not wrap")
}

func completeHandshake(t *testing.T, far *scriptedPeer) {
	t.Helper()

	completeHandshakeWithProtocolVersion(t, far, int32(wire.ProtocolVersion)) //nolint:gosec // fixed protocol constant, fits int32
}

// completeHandshakeWithProtocolVersion runs the same handshake as
// completeHandshake, but with the remote peer advertising protocolVersion
// instead of our own wire.ProtocolVersion — the lever a test needs to land
// the negotiated version on either side of transport.ExtendedPayloadVersion
// (handshake.go:170, NegotiatedVersion = min(wire.ProtocolVersion, advertised)).
func completeHandshakeWithProtocolVersion(t *testing.T, far *scriptedPeer, protocolVersion int32) {
	t.Helper()

	require.IsType(t, &wire.MsgVersion{}, far.read(t))
	v := remoteVersion(1234)
	v.ProtocolVersion = protocolVersion
	far.write(t, v)
	require.IsType(t, &wire.MsgVerAck{}, far.read(t))
	require.IsType(t, &wire.MsgProtoconf{}, far.read(t))
	far.write(t, wire.NewMsgVerAck())
	require.IsType(t, &wire.MsgSendHeaders{}, far.read(t))
}

func TestPeerCompletesHandshake(t *testing.T) {
	p, far := newTestPeer(t, 30*time.Second, 30*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(ctx) }()

	completeHandshake(t, far)

	select {
	case <-p.Established():
	case <-time.After(5 * time.Second):
		t.Fatal("handshake did not complete")
	}

	snap := p.Info()
	require.Equal(t, "/sv:1.1.0/", snap.UserAgent)
	require.False(t, snap.Inbound)
	require.Equal(t, int32(850000), snap.StartingHeight)
	require.Positive(t, snap.BytesSent)
	require.Positive(t, snap.BytesReceived)
}

func TestPeerIdleTimeoutDisconnects(t *testing.T) {
	p, far := newTestPeer(t, 200*time.Millisecond, time.Hour)

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(context.Background()) }()

	require.IsType(t, &wire.MsgVersion{}, far.read(t)) // drain, then go silent

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.Contains(t, err.Error(), "idle")
	case <-time.After(5 * time.Second):
		t.Fatal("peer did not disconnect on idle timeout")
	}
}

func TestPeerSendsPings(t *testing.T) {
	p, far := newTestPeer(t, time.Hour, 200*time.Millisecond)

	go func() { _ = p.Run(context.Background()) }()

	completeHandshake(t, far)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, ok := far.read(t).(*wire.MsgPing); ok {
			return
		}
	}

	t.Fatal("no ping observed")
}

func TestPeerSelfConnectionTerminatesRun(t *testing.T) {
	p, far := newTestPeer(t, time.Hour, time.Hour)

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(context.Background()) }()

	require.IsType(t, &wire.MsgVersion{}, far.read(t))
	far.write(t, remoteVersion(7777)) // our own nonce

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, ErrSelfConnection)
	case <-time.After(5 * time.Second):
		t.Fatal("self-connection not detected")
	}
}

func TestPeerDisconnectStopsRun(t *testing.T) {
	p, far := newTestPeer(t, time.Hour, time.Hour)

	errCh := make(chan error, 1)

	go func() { errCh <- p.Run(context.Background()) }()

	require.IsType(t, &wire.MsgVersion{}, far.read(t))

	p.Disconnect("test teardown")

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Disconnect did not stop Run")
	}
}

// scoringDispatcher is a syncDispatcher whose only interesting method is
// BlockDone: it returns the misbehavior delta and the disconnect verdict a
// test wants, so the peer loop's score-then-disconnect ordering can be driven
// without a PeerManager.
type scoringDispatcher struct {
	delta int
	err   error
}

func (d *scoringDispatcher) Established(*SyncPeer, wire.ServiceFlag) []wire.Message { return nil }

func (d *scoringDispatcher) Headers(*SyncPeer, *wire.MsgHeaders) ([]wire.Message, int, error) {
	return nil, 0, nil
}

func (d *scoringDispatcher) Inv(*SyncPeer, *wire.MsgInv) ([]wire.Message, error) { return nil, nil }

func (d *scoringDispatcher) GetHeaders(*SyncPeer, *wire.MsgGetHeaders) []wire.Message { return nil }

func (d *scoringDispatcher) GetBlocks(*SyncPeer, *wire.MsgGetBlocks) []wire.Message { return nil }

func (d *scoringDispatcher) GetData(*SyncPeer, *wire.MsgGetData) []getDataItem { return nil }

func (d *scoringDispatcher) ContinueInv(*SyncPeer, chainhash.Hash) []wire.Message { return nil }

// BlockExpected answers true so the block under test is solicited: an
// unsolicited block is refused before it ever reaches the ingestor
// (Peer.startIngest).
func (d *scoringDispatcher) BlockExpected(*SyncPeer, chainhash.Hash) bool { return true }

func (d *scoringDispatcher) BlockDone(*SyncPeer, chainhash.Hash, IngestOutcome) (int, error) {
	return d.delta, d.err
}

// completingIngestor finishes an ingest immediately with a fixed outcome, so a
// test can drive the peer loop's ingest-report handling rather than its idle
// timer.
type completingIngestor struct {
	outcome IngestOutcome

	started chan struct{}
}

func newCompletingIngestor(outcome IngestOutcome) *completingIngestor {
	return &completingIngestor{outcome: outcome, started: make(chan struct{}, 1)}
}

func (c *completingIngestor) WatchProgress(r io.ReadCloser) IngestProgress {
	return newTestProgress(r)
}

func (c *completingIngestor) Ingest(_ context.Context, req BlockIngestRequest) IngestOutcome {
	select {
	case c.started <- struct{}{}:
	default:
	}

	_, _ = io.Copy(io.Discard, req.TxReader)
	_ = req.TxReader.Close()

	return c.outcome
}

// TestPeerScoresAPeerFaultBlockBeforeDisconnecting is Task 20 part (b): a
// block reject that IS the peer's fault must reach the ban counter, not only
// the disconnect.
//
// SVNode scores it. PeerLogicValidation::BlockChecked (net/net_processing.cpp:903)
// hands the validation state to BlockDownloadTracker::BlockChecked
// (net/block_download_tracker.cpp:87), which at :113-127 reads the DoS level out
// of the state and calls Misbehaving(node, nDoS, state.GetRejectReason()) for
// every node that sourced the block. Misbehaving (net_processing.cpp:609-633)
// adds to state->nMisbehavior and raises fShouldBan at the threshold. Before
// this task svp2p disconnected on the reject and recorded nothing, so a peer
// that reconnected arrived with a clean counter.
//
// The score is asserted even on the rows that also disconnect, because the
// order is the point: SVNode scores first and acts second, the same order
// dispatchAddr already keeps for oversized-addr (peer.go, net_processing.cpp
// :2285-2286). A disconnect that skipped the score would lose the evidence the
// ban threshold is counting.
func TestPeerScoresAPeerFaultBlockBeforeDisconnecting(t *testing.T) {
	// A delta of 100 is scoreInvalidBlock: every block-invalidity site in
	// SVNode's validation.cpp uses state.DoS(100, ...) (e.g. :537
	// bad-txns-oversize, :579 bad-cb-missing, :3714 blk-bad-inputs).
	const invalidBlock = 100

	tests := []struct {
		name string

		// delta and dispatchErr are what BlockDone reports for the block.
		delta       int
		dispatchErr error

		// banThreshold is the peer's own threshold. A row that wants to
		// observe a score WITHOUT the score itself ending the connection sets
		// it above the delta.
		banThreshold   int
		disableBanning bool

		wantScore int
		wantDrop  bool
	}{
		{
			// The reject the whole task is about: SVNode's own numbers, where
			// one invalid block reaches the threshold on its own.
			name:         "invalid block scores and drops",
			delta:        invalidBlock,
			dispatchErr:  errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: block was rejected"),
			banThreshold: 100,
			wantScore:    invalidBlock,
			wantDrop:     true,
		},
		{
			// The score has to land even when it is not yet fatal, because
			// that is the whole point of a counter: the next reject from the
			// same peer must find it.
			name:         "score lands below the threshold and the peer stays",
			delta:        invalidBlock,
			banThreshold: 1000,
			wantScore:    invalidBlock,
			wantDrop:     false,
		},
		{
			// legacy addBanScore (services/legacy/peer_server.go:539-543)
			// returns before touching the counter when cfg.DisableBanning is
			// set, and peer_server.go:1667-1676 is explicit that the peer is
			// still "disconnected regardless of whether it was banned". So
			// banning off suppresses the SCORE, never the disconnect.
			name:           "banning disabled suppresses the score, not the disconnect",
			delta:          invalidBlock,
			dispatchErr:    errors.New(errors.ERR_NETWORK_PEER_MALICIOUS, "svp2p: block was rejected"),
			banThreshold:   100,
			disableBanning: true,
			wantScore:      0,
			wantDrop:       true,
		},
		{
			// A local fault carries no delta at all, so nothing is recorded
			// against a peer that did its job.
			name:         "local fault scores nothing and keeps the peer",
			delta:        0,
			banThreshold: 100,
			wantScore:    0,
			wantDrop:     false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ingestor := newCompletingIngestor(IngestOutcome{})

			p, far := newIngestingTestPeer(t, time.Hour, time.Hour, ingestor)
			p.cfg.Sync = &scoringDispatcher{delta: tc.delta, err: tc.dispatchErr}
			p.cfg.BanThreshold = tc.banThreshold
			p.cfg.DisableBanning = tc.disableBanning

			errCh := make(chan error, 1)
			go func() { errCh <- p.Run(context.Background()) }()

			defer p.Disconnect("test teardown")

			completeHandshake(t, far)

			genesis := syncGenesis()
			far.writeAsync(blockFor(minedChild(genesis, testEasyBits, 40)))

			select {
			case <-ingestor.started:
			case <-time.After(5 * time.Second):
				t.Fatal("the block never reached the ingestor")
			}

			if tc.wantDrop {
				select {
				case err := <-errCh:
					require.Error(t, err, "a block the pipeline rejected must cost the peer its connection")
				case <-time.After(5 * time.Second):
					t.Fatal("the peer was never disconnected")
				}
			}

			// Read the counter only once the loop has finished with this
			// block: on a dropping row that is when Run returned, and on a
			// staying row the score is the only observable, so poll it.
			require.Eventually(t, func() bool {
				p.mu.Lock()
				defer p.mu.Unlock()

				return p.hs.MisbehaviorScore() == tc.wantScore
			}, 5*time.Second, 10*time.Millisecond,
				"the ban counter must carry exactly the delta the reject earned")

			if !tc.wantDrop {
				select {
				case err := <-errCh:
					t.Fatalf("the peer was disconnected without a disconnect verdict: %v", err)
				case <-time.After(200 * time.Millisecond):
				}
			}
		})
	}
}
