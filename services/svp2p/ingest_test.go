package svp2p

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/svp2p/bridge"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	blobmemory "github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// stubBridge stands in for the relocated ingestion pipeline, which has its own
// coverage (Tasks 9, 10 and 13). What is under test here is the composition
// around it: the admission gate, the release discipline, and how a failure
// maps onto the outcome the peer loop acts on.
type stubBridge struct {
	mu    sync.Mutex
	calls int
	err   error

	// preAdmit is what the bounded pre-admission lookups answer, and
	// preAdmitErr what they fail with. preAdmitBlock, when non-nil, holds
	// those lookups until it is closed or the context ends, standing in for a
	// wedged blockchain client.
	preAdmit      bridge.PreAdmitResult
	preAdmitErr   error
	preAdmitBlock chan struct{}
	preAdmitCalls int

	// preAdmitFor overrides preAdmit for the named block hashes.
	preAdmitFor map[chainhash.Hash]bridge.PreAdmitResult
	// errFor overrides err for the named block hashes, so one run can fail a
	// child's ingest while its parent succeeds.
	errFor map[chainhash.Hash]error
	// ingested records every IngestBlock call: header hash and the bytes read.
	ingested []ingestedBlock
}

type ingestedBlock struct {
	hash    chainhash.Hash
	txCount uint64
	payload []byte
	peer    string
}

func (s *stubBridge) PreAdmit(ctx context.Context, header *wire.BlockHeader) (bridge.PreAdmitResult, error) {
	s.mu.Lock()
	s.preAdmitCalls++
	block := s.preAdmitBlock
	result, err := s.preAdmit, s.preAdmitErr

	if override, ok := s.preAdmitFor[header.BlockHash()]; ok {
		result = override
	}
	s.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return bridge.PreAdmitResult{}, ctx.Err()
		}
	}

	return result, err
}

func (s *stubBridge) IngestBlock(_ context.Context, header *wire.BlockHeader, txCount uint64, txReader io.Reader, peer string) error {
	payload, _ := io.ReadAll(txReader)

	s.mu.Lock()
	s.calls++

	err := s.err
	if override, ok := s.errFor[header.BlockHash()]; ok {
		err = override
	}

	s.ingested = append(s.ingested, ingestedBlock{hash: header.BlockHash(), txCount: txCount, payload: payload, peer: peer})
	s.mu.Unlock()

	if closer, ok := txReader.(io.Closer); ok {
		_ = closer.Close()
	}

	return err
}

func (s *stubBridge) ingestedBlocks() []ingestedBlock {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]ingestedBlock(nil), s.ingested...)
}

func (s *stubBridge) HeaderEvents() <-chan bridge.HeaderEvent { return nil }

// IngestTx and TxRejected are Task 14's additions to bridge.Bridge. Nothing
// in this file exercises them — this suite is about the block-ingest
// admission/release composition (see the type doc comment above); the tx
// adapter has its own suite in ingest_tx_test.go.
func (s *stubBridge) IngestTx(context.Context, []byte, string) (bridge.IngestTxResult, error) {
	panic("stubBridge: IngestTx is not exercised by this suite")
}

func (s *stubBridge) TxRejected(chainhash.Hash) bool {
	panic("stubBridge: TxRejected is not exercised by this suite")
}

// FetchBlock and FetchTx are Task 9's read-side additions to bridge.Bridge.
// Nothing in this file exercises them — this suite is about the admission
// gate and IngestBlock composition (see the comment above) — so the stub
// only needs to satisfy the interface.
func (s *stubBridge) FetchBlock(context.Context, *chainhash.Hash) (io.ReadCloser, uint64, error) {
	panic("stubBridge: FetchBlock is not exercised by this suite")
}

func (s *stubBridge) FetchTx(context.Context, *chainhash.Hash) ([]byte, error) {
	panic("stubBridge: FetchTx is not exercised by this suite")
}

func (s *stubBridge) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

// countingStream is a block's transaction stream that records its releases, so
// a test can prove the transport read loop is never left parked.
type countingStream struct {
	io.Reader

	mu     sync.Mutex
	closes int
}

func (c *countingStream) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closes++

	return nil
}

func (c *countingStream) closeCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.closes
}

func newTestIngestor(t *testing.T, br bridge.Bridge) (*blockIngestor, *bridge.Admission) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)
	require.Positive(t, tSettings.Legacy.BlockPrefetchBufferBytes, "the budget half of the gate must be on for this test")

	admission := bridge.NewAdmission(ulogger.TestLogger{}, tSettings)
	t.Cleanup(admission.Stop)

	return &blockIngestor{logger: ulogger.TestLogger{}, bridge: br, admission: admission}, admission
}

func testIngestRequest(header *wire.BlockHeader, stream io.ReadCloser) protocol.BlockIngestRequest {
	return protocol.BlockIngestRequest{
		Header:    header,
		TxCount:   1,
		SizeBytes: 1024,
		TxReader:  stream,
		PeerAddr:  "127.0.0.1:8333",
	}
}

func testIngestHeader() *wire.BlockHeader {
	zero := chainhash.Hash{}

	return wire.NewBlockHeader(1, &zero, &zero, 0x207fffff, 1)
}

func TestBlockIngestorAdmitsAndReleases(t *testing.T) {
	br := &stubBridge{}
	ingestor, admission := newTestIngestor(t, br)

	header := testIngestHeader()

	// Two sequential ingests of the same block: the second can only be
	// admitted if the first released both halves of the gate.
	for i := 0; i < 2; i++ {
		stream := &countingStream{Reader: bytes.NewReader(nil)}

		outcome := ingestor.Ingest(context.Background(), testIngestRequest(header, stream))

		require.NoError(t, outcome.Err)
		require.False(t, outcome.Duplicate)
		require.False(t, outcome.Rotate)
		require.False(t, outcome.TransientLocal)
		require.Equal(t, 1, stream.closeCount(), "the block stream must be released exactly once")
	}

	require.Equal(t, 2, br.callCount())
	require.Equal(t, int64(0), admission.Waiters())
}

func TestBlockIngestorReportsDuplicate(t *testing.T) {
	br := &stubBridge{}
	ingestor, admission := newTestIngestor(t, br)

	header := testIngestHeader()
	hash := header.BlockHash()

	// A copy of this block is already admitted and mid-ingest.
	weight, err := admission.Acquire(context.Background(), nil, hash, 1024)
	require.NoError(t, err)

	defer admission.Release(hash, weight)

	stream := &countingStream{Reader: bytes.NewReader(nil)}

	outcome := ingestor.Ingest(context.Background(), testIngestRequest(header, stream))

	require.True(t, outcome.Duplicate, "a second copy must be reported as a duplicate, not a failure of this peer")
	require.False(t, outcome.Rotate)
	require.ErrorIs(t, outcome.Err, bridge.ErrDuplicateBlockInFlight)
	require.Zero(t, br.callCount(), "a duplicate must never reach the pipeline")
	require.Equal(t, 1, stream.closeCount(), "a rejected block still has to release the stream")
}

func TestBlockIngestorSkipsBlocksWeAlreadyHold(t *testing.T) {
	br := &stubBridge{preAdmit: bridge.PreAdmitResult{Exists: true}}
	ingestor, _ := newTestIngestor(t, br)

	stream := &countingStream{Reader: bytes.NewReader(nil)}

	outcome := ingestor.Ingest(context.Background(), testIngestRequest(testIngestHeader(), stream))

	require.NoError(t, outcome.Err, "a block we already hold is a completed download, not a failure")
	require.False(t, outcome.TransientLocal)
	require.Zero(t, br.callCount(), "a block we already hold must not be re-ingested")
	require.Equal(t, 1, stream.closeCount())
}

func TestBlockIngestorHoldsBackOrphans(t *testing.T) {
	br := &stubBridge{preAdmit: bridge.PreAdmitResult{ParentMissing: true}}
	ingestor, _ := newTestIngestor(t, br)

	stream := &countingStream{Reader: bytes.NewReader(nil)}

	outcome := ingestor.Ingest(context.Background(), testIngestRequest(testIngestHeader(), stream))

	require.Error(t, outcome.Err)
	require.True(t, outcome.TransientLocal, "our chain being behind is not the peer's fault")
	require.False(t, outcome.Rotate)
	require.Zero(t, br.callCount(), "an orphan must not run the pipeline")
	require.Equal(t, 1, stream.closeCount())
}

// TestBlockIngestorRotatesOnPreAdmitDeadline pins which phase the pre-admission
// deadline covers: the blockchain lookups, and only those.
func TestBlockIngestorRotatesOnPreAdmitDeadline(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.PeerProcessingTimeout = 50 * time.Millisecond

	admission := bridge.NewAdmission(ulogger.TestLogger{}, tSettings)
	t.Cleanup(admission.Stop)

	// A wedged blockchain client: the lookups never answer.
	br := &stubBridge{preAdmitBlock: make(chan struct{})}
	defer close(br.preAdmitBlock)

	ingestor := &blockIngestor{logger: ulogger.TestLogger{}, bridge: br, admission: admission}

	stream := &countingStream{Reader: bytes.NewReader(nil)}

	outcome := ingestor.Ingest(context.Background(), testIngestRequest(testIngestHeader(), stream))

	require.Error(t, outcome.Err)
	require.True(t, outcome.Rotate, "a wedged lookup must rotate the sync peer rather than park the read loop")
	require.False(t, outcome.TransientLocal, "a rotation is not also a stall-clock refresh")
	require.Zero(t, br.callCount())
	require.Equal(t, 1, stream.closeCount())
}

// TestBlockIngestorDoesNotDeadlineTheBudgetWait is the other half of the same
// rule: a caller parked on the admission budget is waiting on OUR in-flight
// blocks, so it must not inherit the pre-admission deadline and must never
// rotate the delivering peer.
func TestBlockIngestorDoesNotDeadlineTheBudgetWait(t *testing.T) {
	tSettings := test.CreateBaseTestSettings(t)
	tSettings.Legacy.PeerProcessingTimeout = 50 * time.Millisecond
	tSettings.Legacy.BlockPrefetchBufferBytes = 128 * 1024

	admission := bridge.NewAdmission(ulogger.TestLogger{}, tSettings)
	t.Cleanup(admission.Stop)

	// Fill the whole budget with another block, so this ingest parks.
	other := chainhash.Hash{0xaa}

	weight, err := admission.Acquire(context.Background(), nil, other, tSettings.Legacy.BlockPrefetchBufferBytes)
	require.NoError(t, err)

	br := &stubBridge{}
	ingestor := &blockIngestor{logger: ulogger.TestLogger{}, bridge: br, admission: admission}

	stream := &countingStream{Reader: bytes.NewReader(nil)}

	done := make(chan protocol.IngestOutcome, 1)
	go func() {
		done <- ingestor.Ingest(context.Background(), testIngestRequest(testIngestHeader(), stream))
	}()

	// Well past the pre-admission deadline: a budget wait must survive it.
	select {
	case outcome := <-done:
		t.Fatalf("the budget wait inherited the pre-admission deadline: %+v", outcome)
	case <-time.After(10 * tSettings.Legacy.PeerProcessingTimeout):
	}

	admission.Release(other, weight)

	select {
	case outcome := <-done:
		require.NoError(t, outcome.Err)
		require.False(t, outcome.Rotate, "our own backpressure must never rotate the delivering peer")
		require.Equal(t, 1, br.callCount())
	case <-time.After(5 * time.Second):
		t.Fatal("the ingest never resumed after the budget drained")
	}
}

// TestBlockIngestorFlagsPipelineRejectionAsPeerFault is the other end of the
// disconnect contract: the adapter is what decides a failure is the peer's,
// and the manager disconnects on that flag alone.
func TestBlockIngestorFlagsPipelineRejectionAsPeerFault(t *testing.T) {
	br := &stubBridge{err: errors.NewBlockInvalidError("svp2p: test block is invalid")}
	ingestor, _ := newTestIngestor(t, br)

	stream := &countingStream{Reader: bytes.NewReader(nil)}

	outcome := ingestor.Ingest(context.Background(), testIngestRequest(testIngestHeader(), stream))

	require.Error(t, outcome.Err)
	require.True(t, outcome.PeerFault, "a block the pipeline rejected is the peer's fault")
	require.False(t, outcome.TransientLocal)
	require.False(t, outcome.Rotate)
}

func TestBlockIngestorBacksOffLocalFailures(t *testing.T) {
	br := &stubBridge{err: errors.NewServiceError("utxo store is unavailable")}
	ingestor, _ := newTestIngestor(t, br)

	header := testIngestHeader()

	first := &countingStream{Reader: bytes.NewReader(nil)}
	outcome := ingestor.Ingest(context.Background(), testIngestRequest(header, first))

	require.Error(t, outcome.Err)
	require.True(t, outcome.TransientLocal, "a local store fault must not be charged to the peer")
	require.False(t, outcome.Rotate)
	require.Equal(t, 1, br.callCount())

	// The failure is now inside its backoff window, so a re-delivery is
	// skipped cheaply instead of re-running the pipeline — and it is still a
	// local fault, so the peer keeps its stall clock refreshed.
	second := &countingStream{Reader: bytes.NewReader(nil)}
	outcome = ingestor.Ingest(context.Background(), testIngestRequest(header, second))

	require.Error(t, outcome.Err)
	require.True(t, outcome.TransientLocal)
	require.Equal(t, 1, br.callCount(), "a backed-off block must not reach the pipeline again")
	require.Equal(t, 1, second.closeCount(), "a skipped block still has to release the stream")
}

// TestBlockIngestorClassifiesPipelineRejects is Task 20 part (a): the outcome
// classification is an ALLOW-LIST of peer-attributable faults, not the
// deny-list it inherited.
//
// The inherited rule was legacy's shouldDisconnectOnBlockErr
// (services/legacy/peer_server.go:1203-1209), which returns
// !errors.IsTransientLocalError(err) — so every error code outside
// {ErrServiceError, ErrStorageError, ErrServiceUnavailable,
// ErrStorageUnavailable} disconnected the delivering peer. That set omits
// ERR_PROCESSING, and the block-assembly readiness gate exhausts its retries
// with exactly a ProcessingError ("block assembly is behind",
// util/blockassemblyutil/blockassembly_wait.go). An honest peer therefore lost
// its connection because OUR block assembly lagged.
//
// The expectations below are derived from the reject SITES in the svp2p
// bridge, never from the classifier: every ErrBlockInvalid site is a property
// of the block bytes (decode failures at bridge/ingest.go:110,:143,
// bridge/handle_block.go:1871; transaction count at bridge/ingest.go:312,:317;
// header, merkle root and duplicate-transaction at bridge/ingest.go:463,:477,
// :481), and every ErrTxInvalid site is a property of a transaction IN the
// block (bridge/handle_block.go:1660,:1665,:1671). Everything else names our
// own service state.
func TestBlockIngestorClassifiesPipelineRejects(t *testing.T) {
	tests := []struct {
		name string
		err  error

		// peerFault is the disconnect verdict. TransientLocal is asserted as
		// its complement on every row, because the two flags are what the
		// scheduler picks between: charge the peer, or refresh its stall clock.
		peerFault bool
	}{
		{
			// The bug this task exists to fix. blockassemblyutil's
			// WaitForBlockAssemblyReady gives up after retry.WithRetryCount(100)
			// and returns this; the delivering peer had nothing to do with it.
			name: "block assembly readiness exhausted",
			err:  errors.NewProcessingError("block assembly is behind, block height 100, block assembly height 88"),
		},
		{
			name: "our own lookup failed",
			err:  errors.NewProcessingError("failed to check if block exists"),
		},
		{
			name: "service fault",
			err:  errors.NewServiceError("utxo store is unavailable"),
		},
		{
			name: "service unavailable",
			err:  errors.NewServiceUnavailableError("blockchain client is not ready"),
		},
		{
			name: "storage fault",
			err:  errors.NewStorageError("no aerospike nodes available"),
		},
		{
			name: "subtree machinery fault",
			err:  errors.NewSubtreeError("failed to create subtree 3"),
		},
		{
			// bridge/handle_block.go:1558,:1781-:1791 are all our own txMap and
			// subtree bookkeeping under one generic code, so ERR_TX_ERROR is
			// deliberately NOT on the allow-list.
			name: "generic tx error from our own bookkeeping",
			err:  errors.NewTxError("failed to add node to subtree"),
		},
		{
			name:      "block invalid",
			err:       errors.NewBlockInvalidError("merkle root mismatch"),
			peerFault: true,
		},
		{
			name: "block format invalid",
			// No constructor and no current bridge site uses this code — the
			// decode failures at bridge/ingest.go:110,:143 use ErrBlockInvalid
			// — but it is the dedicated code for a malformed block, so the
			// allow-list admits it rather than waiting for the first site that
			// picks the more specific one to silently stop disconnecting.
			err:       errors.New(errors.ERR_BLOCK_INVALID_FORMAT, "failed to decode transaction 3 of 9"),
			peerFault: true,
		},
		{
			name:      "transaction in the block is invalid",
			err:       errors.NewTxInvalidError("tx input 0 references missing previous transaction"),
			peerFault: true,
		},
		{
			name:      "transaction in the block double spends",
			err:       errors.NewTxInvalidDoubleSpendError("tx double spends"),
			peerFault: true,
		},
		{
			// Local precedence: a local fault ANYWHERE in the wrapped chain
			// keeps the peer, even under a peer-attributable head code. The
			// bridge wraps freely (bridge/ingest.go:463 wraps whatever the
			// header check returned), so the safe direction is a re-offer.
			name: "block invalid wrapping a storage fault keeps the peer",
			err:  errors.NewBlockInvalidError("invalid block header", errors.NewStorageError("aerospike timeout")),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			br := &stubBridge{err: tc.err}
			ingestor, _ := newTestIngestor(t, br)

			stream := &countingStream{Reader: bytes.NewReader(nil)}
			outcome := ingestor.Ingest(context.Background(), testIngestRequest(testIngestHeader(), stream))

			require.Error(t, outcome.Err)
			require.Equal(t, tc.peerFault, outcome.PeerFault)
			require.Equal(t, !tc.peerFault, outcome.TransientLocal,
				"every reject is either the peer's fault or a stall-clock refresh, never neither and never both")
			require.False(t, outcome.Rotate, "a pipeline reject is not a pre-admission timeout")
			require.Equal(t, 1, br.callCount())
		})
	}
}

// retainingIngestor is newTestIngestor with orphan-block retention over an
// in-memory temp store and the given byte budget.
func retainingIngestor(t *testing.T, br bridge.Bridge, budget int64) (*blockIngestor, *bridge.Admission) {
	t.Helper()

	ingestor, admission := newTestIngestor(t, br)
	ingestor.retained = newOrphanBlocks(ulogger.TestLogger{}, blobmemory.New(), budget)

	return ingestor, admission
}

// TestBlockIngestorRetainsAnOrphanAndReplaysItWhenTheParentLands closes the
// phase-3 ledger's carried residual 1: a block that arrives before its parent
// used to be refused and its bytes discarded, so it crossed the wire again
// once the parent landed — measured at about five times over by the parity
// harness on a three-peer download (2026-08-26). SVNode keeps the block. Now
// the bytes are spooled into the temp store, the download counts as received,
// and the block is ingested from the spool the moment its parent is in.
func TestBlockIngestorRetainsAnOrphanAndReplaysItWhenTheParentLands(t *testing.T) {
	parent := testIngestHeader()
	parentHash := parent.BlockHash()
	child := wire.NewBlockHeader(1, &parentHash, &chainhash.Hash{0x01}, 0x207fffff, 2)
	childHash := child.BlockHash()

	br := &stubBridge{preAdmitFor: map[chainhash.Hash]bridge.PreAdmitResult{childHash: {ParentMissing: true}}}
	ingestor, admission := retainingIngestor(t, br, 1<<20)

	body := []byte("child block transactions")
	stream := &countingStream{Reader: bytes.NewReader(body)}

	outcome := ingestor.Ingest(context.Background(), testIngestRequest(child, stream))

	require.NoError(t, outcome.Err, "a retained orphan is a completed download")
	require.True(t, outcome.Retained)
	require.False(t, outcome.ParentMissing, "the scheduler must not defer or re-request a retained block")
	require.False(t, outcome.TransientLocal)
	require.Zero(t, br.callCount(), "the orphan must not run the pipeline before its parent")
	require.Equal(t, 1, stream.closeCount())
	require.Equal(t, 1, ingestor.retained.Len())
	require.Equal(t, int64(len(body)), ingestor.retained.Bytes())

	// The parent lands: the child is ingested from the spool, with the bytes
	// the peer sent, attributed to the peer that sent them.
	br.mu.Lock()
	br.preAdmitFor[childHash] = bridge.PreAdmitResult{}
	br.mu.Unlock()

	parentStream := &countingStream{Reader: bytes.NewReader([]byte("parent"))}
	outcome = ingestor.Ingest(context.Background(), testIngestRequest(parent, parentStream))
	require.NoError(t, outcome.Err)

	ingestor.retained.Wait()

	got := br.ingestedBlocks()
	require.Len(t, got, 2)
	require.Equal(t, parentHash, got[0].hash)
	require.Equal(t, childHash, got[1].hash)
	require.Equal(t, body, got[1].payload, "the replay must carry the bytes the peer sent")
	require.Equal(t, uint64(1), got[1].txCount)
	require.Equal(t, "127.0.0.1:8333", got[1].peer)
	require.Zero(t, ingestor.retained.Len(), "a replayed block leaves the spool")
	require.Equal(t, int64(0), admission.Waiters())
}

// TestBlockIngestorHoldsBackAnOrphanOverTheRetentionBudget keeps today's
// behaviour when the spool is full: refuse, release, let the scheduler defer.
func TestBlockIngestorHoldsBackAnOrphanOverTheRetentionBudget(t *testing.T) {
	br := &stubBridge{preAdmit: bridge.PreAdmitResult{ParentMissing: true}}
	ingestor, _ := retainingIngestor(t, br, 8)

	stream := &countingStream{Reader: bytes.NewReader(nil)}
	req := testIngestRequest(testIngestHeader(), stream)
	req.SizeBytes = 1024

	outcome := ingestor.Ingest(context.Background(), req)

	require.Error(t, outcome.Err)
	require.True(t, outcome.ParentMissing)
	require.True(t, outcome.TransientLocal)
	require.False(t, outcome.Retained)
	require.Equal(t, 1, stream.closeCount())
	require.Zero(t, ingestor.retained.Len())
}

// TestBlockIngestorReportsTheBackoffWindow: a block skipped for backoff, or one
// that just failed on a local fault, must tell the scheduler how long to keep it
// off the wire — otherwise the same peer re-serves it every tick.
func TestBlockIngestorReportsTheBackoffWindow(t *testing.T) {
	br := &stubBridge{err: errors.NewStorageError("store down")}
	ing, adm := newTestIngestor(t, br)

	header := testIngestHeader()

	outcome := ing.Ingest(context.Background(), testIngestRequest(header, &countingStream{Reader: bytes.NewReader(nil)}))
	require.Error(t, outcome.Err)
	require.True(t, outcome.TransientLocal)
	require.Greater(t, outcome.RetryAfter, time.Duration(0), "a recorded failure must report its backoff window")

	remaining, _, skip := adm.BackoffRemaining(header.BlockHash())
	require.True(t, skip)

	skipped := ing.Ingest(context.Background(), testIngestRequest(header, &countingStream{Reader: bytes.NewReader(nil)}))
	require.True(t, skipped.TransientLocal)
	require.Greater(t, skipped.RetryAfter, time.Duration(0))
	require.LessOrEqual(t, skipped.RetryAfter, remaining)
}
