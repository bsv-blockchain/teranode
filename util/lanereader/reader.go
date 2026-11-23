// Package lanereader keeps a process's index of the lane map current and answers the parent check
// from it beside today's answer, at utxostore_laneMigration shadow, Phase 1 of lanes.
//
// One Reader runs per process, started by the daemon at shadow and reached with Current; at off
// there is none, Current returns nil, and every method on a nil Reader does nothing, so a reader's
// hot path pays one nil check. Nothing acts on a lanes answer: each is compared with the answer
// today's mined lists give and the result is counted (teranode_lane_reader_answers_total) and, for
// the first disagreements of each kind, logged.
//
// The Reader loads the rows of the last two days of main-chain blocks at start, then follows the
// main chain: once a second it reads the best block, walks back along the chain until it meets a
// block it holds, loads the rows of every block on the way, one call per block by ID, and moves
// its index's tip. Loading by ID along the chain is the only way rows enter the index, so the
// rows of a block deleted before the index loaded it, which name an ID no chain reaches, never
// enter. A block deleted after it was loaded keeps its rows in use until the next tip move, and is
// then off the main chain (counted off_chain where a height names it) until the floors pass it.
// A reorg moves the tip
// to the new chain; one deeper than the settle depth, past rows already folded into the floors,
// rebuilds the index. The follower reads the best block one second after each step finishes, so
// while it is idle it is about a second behind; during a load of many blocks or a rebuild, readers
// keep the previous snapshot until that step finishes. Block validation therefore loads a block's
// own ancestry the same way before it judges it (Prepare).
package lanereader

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/model"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/lane"
	"golang.org/x/sync/errgroup"
)

const (
	// startBlocks is how many main-chain blocks the index loads at start: two days. The rows of
	// those 100 or more deep fold straight into the floors.
	startBlocks = 288

	// pollInterval is how often the follower reads the best block.
	pollInterval = time.Second

	// walkBatch is how many headers one step of a walk back along the chain reads, and maxWalk
	// how far back a walk goes looking for a block the index holds before it rebuilds instead.
	walkBatch = 64
	maxWalk   = 1_000

	// loadConcurrency bounds the row reads of one load.
	loadConcurrency = 8

	// retryAfter is how long the follower waits after a failed step.
	retryAfter = 5 * time.Second
)

// Source is what the Reader reads: the blockchain client.
type Source interface {
	GetBestBlockHeader(ctx context.Context) (*model.BlockHeader, *model.BlockHeaderMeta, error)
	GetBlockHeaders(ctx context.Context, blockHash *chainhash.Hash, numberOfHeaders uint64) ([]*model.BlockHeader, []*model.BlockHeaderMeta, error)
	LaneMapRows(ctx context.Context, blockID uint32) ([]lane.MapRow, error)
}

// Reader is one process's index of the lane map and the counts of its answers.
type Reader struct {
	logger      ulogger.Logger
	source      Source
	settleDepth uint32

	idx atomic.Pointer[lane.Index]
	// loading holds one token while a load runs, so the follower and block validation do not
	// read the same rows twice. It is a channel, not a mutex, so a caller waiting for it gives up
	// when its context ends.
	loading chan struct{}

	lookups  atomic.Uint64
	disagree disagreementLog
}

var current atomic.Pointer[Reader]

// Current is the process's Reader, nil at off or before the daemon starts it.
func Current() *Reader { return current.Load() }

// Start starts the process's Reader at shadow and returns it; at off it returns nil and leaves
// none. The Reader follows the chain until ctx ends. A Reader started earlier in the process is
// replaced.
func Start(ctx context.Context, logger ulogger.Logger, tSettings *settings.Settings, source Source) *Reader {
	if !tSettings.UtxoStore.LaneMigration.Stamps() {
		current.Store(nil)

		return nil
	}

	settleDepth := uint32(lane.DefaultSettleDepth)
	if tSettings.ChainCfgParams != nil && tSettings.ChainCfgParams.CoinbaseMaturity > 0 {
		settleDepth = uint32(tSettings.ChainCfgParams.CoinbaseMaturity)
	}

	r := newReader(logger, source, settleDepth)
	current.Store(r)

	go r.follow(ctx)

	return r
}

func newReader(logger ulogger.Logger, source Source, settleDepth uint32) *Reader {
	initMetrics()

	r := &Reader{logger: logger, source: source, settleDepth: settleDepth, loading: make(chan struct{}, 1)}
	r.idx.Store(lane.NewIndex(settleDepth))

	return r
}

// Snapshot is the index's newest snapshot, nil until it has loaded its first tip.
func (r *Reader) Snapshot() *lane.IndexSnapshot {
	if r == nil {
		return nil
	}

	return r.idx.Load().Snapshot()
}

// ViewOfIDs is the view of the chain whose blocks are ids in the newest snapshot, nil until the
// index has its first tip.
func (r *Reader) ViewOfIDs(ids map[uint32]bool) *lane.ChainView {
	if s := r.Snapshot(); s != nil {
		return s.ViewOfIDs(ids)
	}

	return nil
}

// MainView is the view of the main chain in the newest snapshot, nil until the index has its
// first tip.
func (r *Reader) MainView() *lane.ChainView {
	if s := r.Snapshot(); s != nil {
		return s.Main()
	}

	return nil
}

// follow builds the index, then keeps its tip on the main chain's until ctx ends.
func (r *Reader) follow(ctx context.Context) {
	var tip chainhash.Hash

	for ctx.Err() == nil {
		next, err := r.step(ctx, tip)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			prometheusLaneReaderFollowErrors.Inc()
			r.logger.Warnf("[lanereader] following the chain: %v", err)

			sleep(ctx, retryAfter)

			continue
		}

		tip = next

		sleep(ctx, pollInterval)
	}
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()

	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// step reads the best block and, if it is not tip, moves the index to it, building the index
// first if it has no tip yet. It returns the tip the index now has.
func (r *Reader) step(ctx context.Context, tip chainhash.Hash) (chainhash.Hash, error) {
	header, _, err := r.source.GetBestBlockHeader(ctx)
	if err != nil {
		return tip, errors.NewServiceError("reading the best block", err)
	}

	best := *header.Hash()
	if best == tip && r.Snapshot() != nil {
		return tip, nil
	}

	if !r.lockLoad(ctx) {
		return tip, ctx.Err()
	}
	defer r.unlockLoad()

	if r.Snapshot() == nil {
		if err = r.rebuild(ctx, best, "start"); err != nil {
			return tip, err
		}

		return best, nil
	}

	found, err := r.loadAncestry(ctx, best)
	if err != nil {
		return tip, err
	}

	if !found {
		if err = r.rebuild(ctx, best, "gap"); err != nil {
			return tip, err
		}

		return best, nil
	}

	if err = r.idx.Load().SetTip(best); err != nil {
		if !errors.Is(err, lane.ErrDeepReorg) {
			return tip, err
		}

		if err = r.rebuild(ctx, best, "deep_reorg"); err != nil {
			return tip, err
		}
	}

	r.publishMetrics()

	return best, nil
}

// lockLoad takes the load token, or reports false if ctx ends first.
func (r *Reader) lockLoad(ctx context.Context) bool {
	select {
	case r.loading <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *Reader) unlockLoad() { <-r.loading }

// rebuild replaces the index with one built from the startBlocks main-chain blocks ending at tip.
// The caller holds the load token.
func (r *Reader) rebuild(ctx context.Context, tip chainhash.Hash, reason string) error {
	start := time.Now()

	headers, metas, err := r.source.GetBlockHeaders(ctx, &tip, startBlocks)
	if err != nil {
		return errors.NewServiceError("reading the last %d block headers", startBlocks, err)
	}

	idx := lane.NewIndex(r.settleDepth)

	if err = r.load(ctx, idx, headers, metas); err != nil {
		return err
	}

	if err = idx.SetTip(tip); err != nil {
		return err
	}

	r.idx.Store(idx)
	prometheusLaneReaderRebuilds.WithLabelValues(reason).Inc()
	r.publishMetrics()

	s := idx.Snapshot()
	r.logger.Infof("[lanereader] index built (%s) from %d blocks, heights %d to %d, in %s: %d rows, %d bytes", reason, len(headers), s.StartHeight(), s.TipHeight(), time.Since(start), s.Stats().Rows, s.Stats().Bytes)

	return nil
}

// loadAncestry loads into the index every block from hash back to the first one it holds. It
// reports false when it found none within maxWalk blocks. The caller holds the load token.
func (r *Reader) loadAncestry(ctx context.Context, hash chainhash.Hash) (bool, error) {
	idx := r.idx.Load()

	var (
		headers []*model.BlockHeader
		metas   []*model.BlockHeaderMeta
		found   bool
	)

	for from := hash; len(headers) < maxWalk && !found; {
		hs, ms, err := r.source.GetBlockHeaders(ctx, &from, walkBatch)
		if err != nil {
			return false, errors.NewServiceError("reading block headers from %s", from.String(), err)
		}

		if len(hs) == 0 || len(hs) != len(ms) {
			return false, errors.NewServiceError("block headers from %s: %d headers, %d metas", from.String(), len(hs), len(ms))
		}

		for i, h := range hs {
			if idx.Has(*h.Hash()) {
				found = true

				break
			}

			headers, metas = append(headers, h), append(metas, ms[i])
		}

		last := hs[len(hs)-1]
		if len(hs) < walkBatch || last.HashPrevBlock == nil {
			// The walk reached genesis.
			found = true

			break
		}

		from = *last.HashPrevBlock
	}

	if !found {
		return false, nil
	}

	return true, r.load(ctx, idx, headers, metas)
}

// load reads the rows of each block and adds the blocks to idx.
func (r *Reader) load(ctx context.Context, idx *lane.Index, headers []*model.BlockHeader, metas []*model.BlockHeaderMeta) error {
	rows := make([][]lane.MapRow, len(headers))

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(loadConcurrency)

	for i := range headers {
		g.Go(func() error {
			got, err := r.source.LaneMapRows(gCtx, metas[i].ID)
			if err != nil {
				prometheusLaneReaderLoads.WithLabelValues("error").Inc()

				return errors.NewServiceError("reading the lane map rows of block %d", metas[i].ID, err)
			}

			prometheusLaneReaderLoads.WithLabelValues("ok").Inc()

			rows[i] = got

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	for i, h := range headers {
		b := lane.IndexBlock{ID: metas[i].ID, Height: metas[i].Height, Hash: *h.Hash()}
		if h.HashPrevBlock != nil {
			b.Parent = *h.HashPrevBlock
		}

		if err := idx.AddBlock(b, rows[i]); err != nil {
			return err
		}
	}

	return nil
}

// Prepare loads the ancestry of block hash, which must be stored, into the index and returns the
// view of the chain ending at it, so block validation judges a block against rows its parent's
// chain holds even before the follower has moved. It reports false when the index has no tip yet
// or the ancestry could not be loaded before ctx ended.
func (r *Reader) Prepare(ctx context.Context, hash chainhash.Hash) (*lane.ChainView, bool) {
	if r == nil {
		return nil, false
	}

	if s := r.Snapshot(); s != nil {
		if v, ok := s.ViewFrom(hash); ok {
			return v, true
		}
	} else {
		return nil, false
	}

	if !r.lockLoad(ctx) {
		return nil, false
	}
	defer r.unlockLoad()

	found, err := r.loadAncestry(ctx, hash)
	if err != nil || !found {
		if err != nil && ctx.Err() == nil {
			r.logger.Debugf("[lanereader] loading the ancestry of %s: %v", hash.String(), err)
		}

		return nil, false
	}

	idx := r.idx.Load()
	idx.Publish()

	return idx.Snapshot().ViewFrom(hash)
}

func (r *Reader) publishMetrics() {
	s := r.Snapshot()
	if s == nil {
		return
	}

	st := s.Stats()
	prometheusLaneReaderIndexBytes.Set(float64(st.Bytes))
	prometheusLaneReaderIndexRows.Set(float64(st.Rows))
	prometheusLaneReaderIndexBlocks.Set(float64(st.Blocks))
	prometheusLaneReaderIndexListed.Set(float64(st.Listed))
	prometheusLaneReaderTipHeight.Set(float64(s.TipHeight()))
	prometheusLaneReaderAnchorHeight.Set(float64(s.AnchorHeight()))
}
