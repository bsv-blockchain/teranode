package postgres

import (
	"context"
	"time"
)

// pending_unmined write-behind projector.
//
// The create hot path used to upsert every unmined tx into pending_unmined
// synchronously via a _pu CTE arm on the txs INSERT. Measured on the pruned
// throughput bench that side table generated MORE dead tuples than txs itself
// (upsert + lazy-cleanup churn) and its second heap insert + random-hash PK
// btree insert were a large share of per-create cost; stripping it was worth
// ~+8% sustained TPS at 10k workers.
//
// The projection is now write-behind: create paths append (hash, unmined_since)
// to an in-memory buffer and a single background writer batch-INSERTs it with
// ON CONFLICT DO NOTHING every puFlushInterval (or sooner when the buffer
// passes puFlushKickLen). Rows whose tx mines before the pruner's cutoff ever
// reaches them are pruned from pending_unmined by the existing lazy cleanup —
// the seconds of projection lag are irrelevant because the prunable cutoff
// trails the tip by far more than that.
//
// Crash safety: a crash loses only the un-flushed buffer. Startup repairs any
// gap — pendingUnminedBackfillDDL copies all non-conflicting unmined txs from
// txs into pending_unmined (idempotent ON CONFLICT DO NOTHING). That seq scan
// is gated by the store_clean_shutdown marker (see resolvePendingUnminedBackfill
// in schema.go): a graceful Store.Stop()/Close() already drains this
// projector's buffer via stopPendingUnminedProjector(), making pending_unmined
// complete on its own, so the backfill is skipped after a clean shutdown and
// only runs to recover from an unclean one (crash, or a missing/unreadable
// marker on an older database).
//
// A partial btree on txs(unmined_since) was considered and REJECTED: PG16+
// BRIN is a summarizing AM so SetMinedMulti's unmined_since→NULL UPDATE stays
// HOT under the existing BRIN (83% measured), but a btree there tanked the txs
// HOT ratio to 33.7% (see the txsIndexesDDLBase comment).
//
// The U1 (UnsetMined reorg re-insert) and U4 (conflicting transitions) writes
// remain synchronous — they are rare, off the hot path, and transactional with
// the state change they mirror.

const (
	// puFlushInterval is the projector's idle flush cadence. 1s (was 20ms):
	// paired with the mined-aware flush filter below, a longer lag means most
	// short-lived hashes are ALREADY mined by flush time and are filtered out
	// entirely — measured on the pruned bench the projector was 15% of ALL WAL
	// (582MB, ~4 WAL records per 36-byte logical row) for rows nothing read or
	// deleted in-window. The wider crash window is covered by the same
	// unclean-shutdown backfill that covered the 20ms one.
	puFlushInterval = 1 * time.Second
	// puFlushKickLen wakes the writer early when the buffer passes this length.
	puFlushKickLen = 8192
	// puBufHardCap bounds buffer growth if postgres stalls: beyond this the
	// oldest entries are dropped with a warning — the startup backfill (or the
	// next lazy-cleanup-visible pruner pass reading txs) repairs the invariant.
	puBufHardCap = 1 << 20
)

// puEntry is one buffered pending_unmined projection.
type puEntry struct {
	hash  []byte
	since int32
}

// enqueuePendingUnmined buffers one unmined create for background projection.
// Called on the create paths AFTER the txs INSERT succeeds; never blocks on
// the database. Lazily starts the writer goroutine on first use.
func (s *Store) enqueuePendingUnmined(hash []byte, since int32) {
	s.puOnce.Do(s.startPendingUnminedProjector)

	s.puMu.Lock()
	if len(s.puBuf) >= puBufHardCap {
		// Drop-oldest under pathology (DB stalled for a long time). Losing
		// projection lag never loses tx data, but a dropped row followed by a
		// CLEAN shutdown would skip the reconciliation backfill and omit a
		// genuinely-unmined tx from the block-assembly reload. Latch puDropped so
		// markCleanShutdown records this shutdown as UNCLEAN, forcing the next
		// startup to run the fail-safe backfill from txs.
		s.puBuf = s.puBuf[len(s.puBuf)/2:]
		s.puDropped.Store(true)
		s.logger.Warnf("[pendingUnminedProjector] buffer hit hard cap %d — dropped oldest half (next shutdown forced unclean, startup backfill reconciles)", puBufHardCap)
	}
	s.puBuf = append(s.puBuf, puEntry{hash: hash, since: since})
	n := len(s.puBuf)
	s.puMu.Unlock()

	if n >= puFlushKickLen {
		select {
		case s.puKick <- struct{}{}:
		default:
		}
	}
}

// startPendingUnminedProjector launches the single background writer. Invoked
// exactly once via s.puOnce on the first enqueue.
func (s *Store) startPendingUnminedProjector() {
	s.puKick = make(chan struct{}, 1)
	s.puStop = make(chan struct{})
	s.puDone = make(chan struct{})

	go func() {
		defer close(s.puDone)

		ticker := time.NewTicker(puFlushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-s.puStop:
				// Final drain so a clean Stop() loses nothing.
				if err := s.flushPendingUnmined(context.Background()); err != nil {
					s.logger.Warnf("[pendingUnminedProjector] final flush failed (startup backfill reconciles): %v", err)
				}

				return
			case <-ticker.C:
			case <-s.puKick:
			}

			if err := s.flushPendingUnmined(context.Background()); err != nil {
				s.logger.Warnf("[pendingUnminedProjector] flush failed (buffer retained, will retry): %v", err)
			}
		}
	}()
}

// stopPendingUnminedProjector stops the writer (if it ever started) after a
// final drain. Called from Store.Stop().
func (s *Store) stopPendingUnminedProjector() {
	// Latch the Once so a post-Stop enqueue cannot start a new writer.
	s.puOnce.Do(func() {})

	if s.puStop == nil {
		return // never started
	}

	select {
	case <-s.puStop: // already closed
	default:
		close(s.puStop)
	}
	<-s.puDone
}

// flushPendingUnmined drains the buffer into pending_unmined in one batched
// INSERT. ON CONFLICT DO NOTHING keeps re-projection free (no dead tuple for
// rows already present — unlike the old DO UPDATE upsert). On error the batch
// is re-queued (bounded by puBufHardCap). Exported to tests (same package) as
// the deterministic "projector has caught up" hook.
func (s *Store) flushPendingUnmined(ctx context.Context) error {
	s.puMu.Lock()
	buf := s.puBuf
	s.puBuf = nil
	s.puMu.Unlock()

	if len(buf) == 0 {
		return nil
	}

	hashes := make([][]byte, len(buf))
	sinces := make([]int32, len(buf))
	for i, e := range buf {
		hashes[i] = e.hash
		sinces[i] = e.since
	}

	// Mined-aware filter: only project rows whose tx is STILL unmined at flush
	// time (txs.unmined_since IS NOT NULL). A tx that mined inside the flush lag
	// never needed the row (SetMinedMulti clears unmined_since; the row would sit
	// unread until lazy cleanup), and a tx already pruned must not resurrect one.
	// The join costs one pkey probe per buffered row once per flush and replaces
	// a heap insert + random-hash PK btree insert + ~326B WAL for every filtered
	// row. unnest-JOIN (not = ANY) so each probe prunes to one leaf.
	if _, err := s.pool.Exec(ctx, `
		INSERT INTO pending_unmined (hash, unmined_since)
		SELECT u.h, u.s FROM UNNEST($1::bytea[], $2::int[]) AS u(h, s)
		JOIN txs t ON t.hash = u.h
		WHERE t.unmined_since IS NOT NULL
		ON CONFLICT (hash) DO NOTHING`, hashes, sinces); err != nil {
		// Re-queue in front so ordering is roughly preserved; hard cap bounds it.
		s.puMu.Lock()
		s.puBuf = append(buf, s.puBuf...)
		s.puMu.Unlock()

		return err
	}

	return nil
}
