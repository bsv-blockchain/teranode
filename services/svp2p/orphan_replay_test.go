package svp2p

import (
	"bytes"
	"context"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/svp2p/bridge"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/stores/blob/memory"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestRetainedReplayFailureKeepsTheSpool is the second half of the mainnet
// defect of 2026-08-30: the replay of a retained block failed on a local fault
// and the spool entry was deleted anyway, so the bytes were lost as well as the
// download. A transient fault is by definition retryable, so the block stays
// spooled and stays armed under its parent.
func TestRetainedReplayFailureKeepsTheSpool(t *testing.T) {
	parent := testIngestHeader()
	parentHash := parent.BlockHash()
	child := wire.NewBlockHeader(1, &parentHash, &chainhash.Hash{0x02}, 0x207fffff, 2)
	childHash := child.BlockHash()

	br := &stubBridge{preAdmitFor: map[chainhash.Hash]bridge.PreAdmitResult{childHash: {ParentMissing: true}}}
	store := memory.New()

	ingestor, _ := newTestIngestor(t, br)
	ingestor.retained = newOrphanBlocks(ulogger.TestLogger{}, store, 1<<20)

	var (
		mu       sync.Mutex
		reported []protocol.IngestOutcome
	)

	ingestor.retained.report = func(hash chainhash.Hash, outcome protocol.IngestOutcome) {
		mu.Lock()
		defer mu.Unlock()

		require.Equal(t, childHash, hash)

		reported = append(reported, outcome)
	}

	body := []byte("child block transactions")
	outcome := ingestor.Ingest(context.Background(), testIngestRequest(child, &countingStream{Reader: bytes.NewReader(body)}))
	require.True(t, outcome.Retained)

	// The parent lands, but the child's own ingest faults on our store.
	br.mu.Lock()
	br.preAdmitFor[childHash] = bridge.PreAdmitResult{}
	br.errFor = map[chainhash.Hash]error{childHash: errors.NewStorageError("aerospike timeout")}
	br.mu.Unlock()

	outcome = ingestor.Ingest(context.Background(), testIngestRequest(parent, &countingStream{Reader: bytes.NewReader([]byte("parent"))}))
	require.NoError(t, outcome.Err)

	ingestor.retained.Wait()

	spooled, err := store.Exists(context.Background(), childHash[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.True(t, spooled, "a transient replay failure must not throw the spooled bytes away")

	require.Equal(t, 1, ingestor.retained.Len(), "the block stays armed for the next replay")
	require.Equal(t, int64(len(body)), ingestor.retained.Bytes(), "the retained bytes stay accounted for")

	// The scheduler counts a retained block as one we hold, so the failure has
	// to reach it or the block is never offered again.
	mu.Lock()
	defer mu.Unlock()

	require.Len(t, reported, 1, "a failed replay must reach the download scheduler")
	require.Error(t, reported[0].Err)
	require.True(t, reported[0].TransientLocal)
	require.Positive(t, reported[0].RetryAfter, "the scheduler needs the backoff window to defer the re-fetch")
}

// TestRefetchedBlockDiscardsItsRetainedCopy: a block re-armed by a failed
// replay is fetched from the network again once the scheduler offers it. The
// spooled copy is then dead weight against the retention budget, so ingesting
// the block releases it.
func TestRefetchedBlockDiscardsItsRetainedCopy(t *testing.T) {
	parent := testIngestHeader()
	parentHash := parent.BlockHash()
	child := wire.NewBlockHeader(1, &parentHash, &chainhash.Hash{0x04}, 0x207fffff, 2)
	childHash := child.BlockHash()

	br := &stubBridge{preAdmitFor: map[chainhash.Hash]bridge.PreAdmitResult{childHash: {ParentMissing: true}}}
	store := memory.New()

	ingestor, _ := newTestIngestor(t, br)
	ingestor.retained = newOrphanBlocks(ulogger.TestLogger{}, store, 1<<20)

	body := []byte("child block transactions")
	outcome := ingestor.Ingest(context.Background(), testIngestRequest(child, &countingStream{Reader: bytes.NewReader(body)}))
	require.True(t, outcome.Retained)
	require.Equal(t, 1, ingestor.retained.Len())

	// The block now arrives from the network with its parent in our chain.
	br.mu.Lock()
	br.preAdmitFor[childHash] = bridge.PreAdmitResult{}
	br.mu.Unlock()

	outcome = ingestor.Ingest(context.Background(), testIngestRequest(child, &countingStream{Reader: bytes.NewReader(body)}))
	require.NoError(t, outcome.Err)

	require.Zero(t, ingestor.retained.Len(), "the spooled copy must not hold budget after the block is ingested")
	require.Equal(t, int64(0), ingestor.retained.Bytes())

	spooled, err := store.Exists(context.Background(), childHash[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.False(t, spooled, "the spool entry goes with the retained copy")
}

// TestRetainOverwritesAStaleSpoolEntry: the spool is keyed by block hash, and a
// hash can be spooled twice — a replay that finds the parent still missing
// re-retains the block before the first entry is removed. The live symptom was
// "failed to spool orphan block ... [File][allowOverwrite]", after which the
// block was refused instead of retained.
func TestRetainOverwritesAStaleSpoolEntry(t *testing.T) {
	parent := testIngestHeader()
	parentHash := parent.BlockHash()
	child := wire.NewBlockHeader(1, &parentHash, &chainhash.Hash{0x03}, 0x207fffff, 2)
	childHash := child.BlockHash()

	store := memory.New()
	require.NoError(t, store.Set(context.Background(), childHash[:], fileformat.FileTypeBlock, []byte("stale bytes")))

	retained := newOrphanBlocks(ulogger.TestLogger{}, store, 1<<20)

	body := []byte("the bytes the peer actually sent")
	req := testIngestRequest(child, &countingStream{Reader: bytes.NewReader(body)})

	require.NoError(t, retained.Retain(context.Background(), req))

	got, err := store.Get(context.Background(), childHash[:], fileformat.FileTypeBlock)
	require.NoError(t, err)
	require.Equal(t, body, got, "the retained bytes must replace whatever the key held")
}

// TestExistingParentReplaysItsRetainedChildren closes the same permanent-stall
// class one level up (review M1). A block can reach our chain without svp2p
// ever running a successful Ingest for it: block validation's own parent
// catchup stores it, or ProcessBlock stores it and then fails, so the next
// delivery answers Exists. Replay used to hang off the success path alone, so
// a child retained under such a parent was never replayed — and the scheduler
// still counted it as held from its Retained report, so it was never re-fetched
// either.
func TestExistingParentReplaysItsRetainedChildren(t *testing.T) {
	parent := testIngestHeader()
	parentHash := parent.BlockHash()
	child := wire.NewBlockHeader(1, &parentHash, &chainhash.Hash{0x05}, 0x207fffff, 2)
	childHash := child.BlockHash()

	br := &stubBridge{preAdmitFor: map[chainhash.Hash]bridge.PreAdmitResult{
		childHash:  {ParentMissing: true},
		parentHash: {Exists: true},
	}}

	ingestor, _ := newTestIngestor(t, br)
	ingestor.retained = newOrphanBlocks(ulogger.TestLogger{}, memory.New(), 1<<20)

	body := []byte("child block transactions")
	outcome := ingestor.Ingest(context.Background(), testIngestRequest(child, &countingStream{Reader: bytes.NewReader(body)}))
	require.True(t, outcome.Retained)

	// The parent is already in our chain by the time it is delivered, and the
	// child's own parent check now passes.
	br.mu.Lock()
	br.preAdmitFor[childHash] = bridge.PreAdmitResult{}
	br.mu.Unlock()

	outcome = ingestor.Ingest(context.Background(), testIngestRequest(parent, &countingStream{Reader: bytes.NewReader([]byte("parent"))}))
	require.NoError(t, outcome.Err)

	ingestor.retained.Wait()

	got := br.ingestedBlocks()
	require.Len(t, got, 1, "the parent itself is not re-ingested, but its child must be")
	require.Equal(t, childHash, got[0].hash)
	require.Equal(t, body, got[0].payload, "the replay must carry the bytes the peer sent")
	require.Zero(t, ingestor.retained.Len(), "a replayed block leaves the spool")
}
