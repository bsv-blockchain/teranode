package svp2p

import (
	"context"
	"io"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/svp2p/protocol"
	"github.com/bsv-blockchain/teranode/stores/blob"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
)

// orphanBlocks retains blocks that arrive before their parent, so they are
// ingested from the spool when the parent lands instead of being fetched from
// the network again.
//
// SVNode accepts an out-of-order block whose header it already holds straight
// into its block store (validation.cpp AcceptBlock) and never re-requests it.
// This port's bridge cannot ingest a block before its parent (legacy
// semantics, waitForPreviousBlockMined), and until now it refused the block
// and discarded the bytes — one wasted transfer per out-of-order arrival, which
// the parity harness measured at about five transfers per block on a
// three-peer download whose peers outran the ingest (phase-3 ledger residual 1,
// 2026-08-26). The spool is the svp2p TempStore, which had no consumer.
//
// The byte budget is the admission budget (legacy_blockPrefetchBufferBytes):
// bytes waiting for a parent are the same kind of pressure as bytes waiting for
// admission, and reusing the dial avoids a new settings key. Over budget the
// ingestor falls back to refusing the block, exactly as before.
type orphanBlocks struct {
	logger ulogger.Logger
	store  blob.Store
	budget int64

	// report hands a replay's outcome to the download scheduler. A replay has
	// no delivering peer — the one that sent the bytes was released the moment
	// the block was retained — so without this the scheduler keeps counting a
	// failed replay as a block it holds, and never asks for it again. Wired to
	// PeerManager.BlockDone with no sync peer (Server.startSync); nil in a test
	// that does not exercise the seam.
	report func(hash chainhash.Hash, outcome protocol.IngestOutcome)

	mu       sync.Mutex
	bytes    int64
	byHash   map[chainhash.Hash]retainedBlock
	byParent map[chainhash.Hash][]chainhash.Hash

	replays sync.WaitGroup
}

type retainedBlock struct {
	header    *wire.BlockHeader
	txCount   uint64
	sizeBytes uint64
	peerAddr  string
	spooled   int64
}

func newOrphanBlocks(logger ulogger.Logger, store blob.Store, budget int64) *orphanBlocks {
	return &orphanBlocks{
		logger:   logger,
		store:    store,
		budget:   budget,
		byHash:   make(map[chainhash.Hash]retainedBlock),
		byParent: make(map[chainhash.Hash][]chainhash.Hash),
	}
}

// Len is the number of retained blocks.
func (o *orphanBlocks) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()

	return len(o.byHash)
}

// Bytes is the spooled payload total.
func (o *orphanBlocks) Bytes() int64 {
	o.mu.Lock()
	defer o.mu.Unlock()

	return o.bytes
}

// Wait blocks until every replay in progress has finished. Server.Stop calls it
// after the manager is joined, so no replay outlives the service; a test uses it
// to observe a replay that has already been started.
func (o *orphanBlocks) Wait() { o.replays.Wait() }

// fits reserves budget for a block of the declared size; false means the
// caller must fall back to refusing the block.
func (o *orphanBlocks) fits(hash chainhash.Hash, sizeBytes uint64) bool {
	o.mu.Lock()
	defer o.mu.Unlock()

	if _, dup := o.byHash[hash]; dup {
		return false
	}

	return o.bytes+int64(sizeBytes) <= o.budget //nolint:gosec // a block payload is bounded by MaxBlockPayload
}

// Retain spools the request's remaining stream into the store and indexes the
// block under its parent. The stream is fully consumed and closed either way.
func (o *orphanBlocks) Retain(ctx context.Context, req protocol.BlockIngestRequest) error {
	hash := req.Header.BlockHash()

	counted := &countingReader{r: req.TxReader}

	// The spool is keyed by block hash, and the same hash can reach here twice:
	// a replay that finds the parent STILL missing re-retains the block before
	// the previous entry is removed, and a Del that failed leaves one behind.
	// Without the overwrite the second attempt fails with BLOB_EXISTS and the
	// block is refused instead of retained.
	if err := o.store.SetFromReader(ctx, hash[:], fileformat.FileTypeBlock, counted,
		options.WithAllowOverwrite(true)); err != nil {
		return errors.NewStorageError("[svp2p] failed to spool orphan block %s", hash, err)
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	o.byHash[hash] = retainedBlock{
		header:    req.Header,
		txCount:   req.TxCount,
		sizeBytes: req.SizeBytes,
		peerAddr:  req.PeerAddr,
		spooled:   counted.n,
	}
	o.byParent[req.Header.PrevBlock] = append(o.byParent[req.Header.PrevBlock], hash)
	o.bytes += counted.n

	return nil
}

// take removes and returns the blocks retained under parent.
func (o *orphanBlocks) take(parent chainhash.Hash) []retainedBlock {
	o.mu.Lock()
	defer o.mu.Unlock()

	hashes := o.byParent[parent]
	delete(o.byParent, parent)

	out := make([]retainedBlock, 0, len(hashes))

	for _, h := range hashes {
		if rb, ok := o.byHash[h]; ok {
			out = append(out, rb)
			delete(o.byHash, h)
			o.bytes -= rb.spooled
		}
	}

	return out
}

// rearm puts a block back under its parent after a replay that did not consume
// it. The spool entry it names is still in the store.
//
// The re-armed entry has two release routes, and the network one is the one to
// expect. Its parent has already landed, so a second parent-landed event needs
// that parent to be delivered again (which then answers Exists and replays from
// there). The ordinary route is the block's own re-fetch: the scheduler was told
// to offer it again, and the ingest that follows calls discard, which frees both
// the budget and the spool file.
//
// Neither route is guaranteed. A block on a branch nobody offers again, or a
// shutdown before the re-fetch lands, leaves the entry holding retention budget
// for the life of the process and the spool file behind after it — there is no
// startup sweep of the spool. The budget is the block prefetch buffer, so enough
// leaked entries degrade retention to the refuse-the-block fallback rather than
// failing anything.
func (o *orphanBlocks) rearm(rb retainedBlock) {
	hash := rb.header.BlockHash()

	o.mu.Lock()
	defer o.mu.Unlock()

	if _, dup := o.byHash[hash]; dup {
		return
	}

	o.byHash[hash] = rb
	o.byParent[rb.header.PrevBlock] = append(o.byParent[rb.header.PrevBlock], hash)
	o.bytes += rb.spooled
}

// discard forgets a retained copy of hash and deletes its spool entry. It is
// what stops a re-armed block leaking its budget when the same block is
// ingested from the network instead of from the spool.
func (o *orphanBlocks) discard(ctx context.Context, hash chainhash.Hash) {
	o.mu.Lock()

	rb, held := o.byHash[hash]
	if held {
		delete(o.byHash, hash)
		o.bytes -= rb.spooled

		siblings := o.byParent[rb.header.PrevBlock]

		for i, sibling := range siblings {
			if sibling == hash {
				o.byParent[rb.header.PrevBlock] = append(siblings[:i], siblings[i+1:]...)
				break
			}
		}

		if len(o.byParent[rb.header.PrevBlock]) == 0 {
			delete(o.byParent, rb.header.PrevBlock)
		}
	}

	o.mu.Unlock()

	if !held {
		return
	}

	if err := o.store.Del(ctx, hash[:], fileformat.FileTypeBlock); err != nil {
		o.logger.Debugf("[svp2p] spool entry %s not removed: %v", hash, err)
	}
}

// Replay ingests every block retained under parent, in the background so the
// caller's own ingest returns at once. Each replay runs through the ingestor's
// full path — pre-admission, the budget, the pipeline — and may itself land
// grandchildren.
func (o *orphanBlocks) Replay(ctx context.Context, parent chainhash.Hash, ingest func(context.Context, protocol.BlockIngestRequest) protocol.IngestOutcome) {
	children := o.take(parent)
	if len(children) == 0 {
		return
	}

	o.replays.Add(1)

	go func() {
		defer o.replays.Done()

		for _, child := range children {
			hash := child.header.BlockHash()

			reader, err := o.store.GetIoReader(ctx, hash[:], fileformat.FileTypeBlock)
			if err != nil {
				o.logger.Errorf("[svp2p] retained block %s lost from the spool, it will be fetched again: %v", hash, err)
				continue
			}

			outcome := ingest(ctx, protocol.BlockIngestRequest{
				Header:    child.header,
				TxCount:   child.txCount,
				SizeBytes: child.sizeBytes,
				TxReader:  reader,
				PeerAddr:  child.peerAddr,
			})

			if outcome.Retained {
				// The replay found the parent still missing and spooled the
				// block again, so the entry and its bytes belong to that new
				// retention: neither the spool entry nor the scheduler's record
				// may be touched here.
				o.logger.Debugf("[svp2p] retained block %s re-spooled: its parent is still not in our chain", hash)
				continue
			}

			if outcome.Err == nil {
				if delErr := o.store.Del(ctx, hash[:], fileformat.FileTypeBlock); delErr != nil {
					o.logger.Debugf("[svp2p] spool entry %s not removed: %v", hash, delErr)
				}

				o.logger.Infof("[svp2p] replayed retained block %s (%d bytes) after its parent %s landed", hash, child.spooled, parent)

				o.notify(hash, outcome)

				continue
			}

			if outcome.TransientLocal {
				// Our own fault, so the block is still wanted and the bytes are
				// still good: keep the spool entry and arm it again for the
				// next parent-landed event. The scheduler is told separately,
				// because it counts this block as one we hold and would
				// otherwise never offer it again.
				o.rearm(child)

				o.logger.Warnf("[svp2p] replay of retained block %s failed on a local fault, it stays spooled: %v", hash, outcome.Err)
			} else {
				if delErr := o.store.Del(ctx, hash[:], fileformat.FileTypeBlock); delErr != nil {
					o.logger.Debugf("[svp2p] spool entry %s not removed: %v", hash, delErr)
				}

				o.logger.Warnf("[svp2p] replay of retained block %s failed: %v", hash, outcome.Err)
			}

			o.notify(hash, outcome)
		}
	}()
}

// notify hands one replay outcome to the scheduler, if the seam is wired.
func (o *orphanBlocks) notify(hash chainhash.Hash, outcome protocol.IngestOutcome) {
	if o.report == nil {
		return
	}

	o.report(hash, outcome)
}

type countingReader struct {
	r io.ReadCloser
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)

	return n, err
}

func (c *countingReader) Close() error { return c.r.Close() }
