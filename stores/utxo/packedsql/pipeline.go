package packedsql

import (
	"bytes"
	"context"
	"hash/fnv"
	"sort"
	"time"

	"github.com/bsv-blockchain/go-batcher/v2"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
)

type createItem struct {
	rows *txRows
	done chan error
}

type spendWork struct {
	g           *parentSpendGroup
	blockHeight uint32
	ig          utxo.IgnoreFlags
	done        chan error
}

// defaultBatchTimeout bounds the database work of a group-committed batch. Callers have
// already been detached from their own context by the time a batch flushes, so without
// this a wedged connection would stall the pipeline indefinitely.
const defaultBatchTimeout = 30 * time.Second

func (s *Store) batchTimeout() time.Duration {
	if t := s.settings.UtxoStore.SpendWaitTimeout; t > 0 {
		return t
	}

	return defaultBatchTimeout
}

// batchContext derives a bounded context from the store lifetime for pipeline work.
func (s *Store) batchContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(s.bgCtx, s.batchTimeout())
}

func (s *Store) startPipeline() {
	if s.settings.UtxoStore.StoreBatcherSize > 1 {
		size := s.settings.UtxoStore.StoreBatcherSize
		duration := time.Duration(s.settings.UtxoStore.StoreBatcherDurationMillis) * time.Millisecond
		s.createBatcher = batcher.NewWithPool(size, duration, s.sendCreateBatch, true)
	}

	workers := s.settings.UtxoStore.PackedSQLSpendWorkers
	if workers > 0 && s.settings.UtxoStore.SpendBatcherSize > 1 {
		s.spendChans = make([]chan *spendWork, workers)

		for i := range s.spendChans {
			s.spendChans[i] = make(chan *spendWork, s.settings.UtxoStore.SpendBatcherSize*2)

			s.spendWG.Add(1)

			go s.spendWorker(s.spendChans[i])
		}
	}
}

func (s *Store) sendCreateBatch(items []*createItem) {
	ctx, cancel := s.batchContext()
	defer cancel()

	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		batchErr := errors.NewStorageError("packedsql: failed to begin create batch transaction", err)
		for _, item := range items {
			item.done <- batchErr
		}

		return
	}

	itemErrs := make([]error, len(items))

	for i, item := range items {
		if err := s.insertTxRowsOn(ctx, dbTx, item.rows); err != nil {
			if errors.Is(err, errors.ErrTxExists) {
				itemErrs[i] = err
				continue
			}

			_ = dbTx.Rollback(ctx)

			batchErr := errors.NewStorageError("packedsql: create batch failed", err)
			for _, it := range items {
				it.done <- batchErr
			}

			return
		}
	}

	if err = dbTx.Commit(ctx); err != nil {
		batchErr := errors.NewStorageError("packedsql: failed to commit create batch", err)
		for _, item := range items {
			item.done <- batchErr
		}

		return
	}

	for i, item := range items {
		item.done <- itemErrs[i]
	}
}

func (s *Store) createViaBatcher(ctx context.Context, rows *txRows) error {
	done := make(chan error, 1)

	if err := s.putCreateItem(ctx, &createItem{rows: rows, done: done}); err != nil {
		return err
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// putCreateItem hands an item to the create batcher under the close fence so the batcher
// cannot be closed underneath an in-flight submission.
func (s *Store) putCreateItem(ctx context.Context, item *createItem) error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()

	if s.closed {
		return errors.NewServiceUnavailableError("packedsql: store is closed")
	}

	s.createBatcher.PutCtx(ctx, item)

	return nil
}

// sendSpendWork routes a group to its worker under the close fence. Holding the read lock
// across the send is what makes Close safe: Close cannot take the write lock (and therefore
// cannot close the channels) until every send has landed, and the workers keep draining in
// the meantime, so the send cannot deadlock.
func (s *Store) sendSpendWork(ctx context.Context, w *spendWork) error {
	s.closeMu.RLock()
	defer s.closeMu.RUnlock()

	if s.closed {
		return errors.NewServiceUnavailableError("packedsql: store is closed")
	}

	select {
	case s.spendChans[spendWorkerIndex(w.g.hash[:], len(s.spendChans))] <- w:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func spendWorkerIndex(hash []byte, workers int) int {
	h := fnv.New32a()
	_, _ = h.Write(hash)

	return int(h.Sum32() % uint32(workers)) //nolint:gosec
}

func (s *Store) spendViaWorkers(ctx context.Context, groups []*parentSpendGroup, blockHeight uint32, ig utxo.IgnoreFlags) error {
	dones := make([]chan error, len(groups))

	for i, g := range groups {
		dones[i] = make(chan error, 1)

		if err := s.sendSpendWork(ctx, &spendWork{
			g:           g,
			blockHeight: blockHeight,
			ig:          ig,
			done:        dones[i],
		}); err != nil {
			return err
		}
	}

	timer := time.NewTimer(s.batchTimeout())
	defer timer.Stop()

	var firstErr error

	for i := range dones {
		select {
		case err := <-dones[i]:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-timer.C:
			return errors.NewStorageError("packedsql: timed out waiting for spend worker")
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if firstErr != nil {
		return errors.NewUtxoError("packedsql: spend failed", firstErr)
	}

	return nil
}

func (s *Store) spendWorker(ch chan *spendWork) {
	defer s.spendWG.Done()

	size := s.settings.UtxoStore.SpendBatcherSize
	duration := time.Duration(s.settings.UtxoStore.SpendBatcherDurationMillis) * time.Millisecond

	if duration <= 0 {
		duration = 100 * time.Millisecond
	}

	batch := make([]*spendWork, 0, size)
	timer := time.NewTimer(duration)

	if !timer.Stop() {
		<-timer.C
	}

	flush := func() {
		if len(batch) > 0 {
			s.flushSpendBatch(batch)
			batch = make([]*spendWork, 0, size)
		}
	}

	for {
		select {
		case w, ok := <-ch:
			if !ok {
				flush()
				return
			}

			if len(batch) == 0 {
				timer.Reset(duration)
			}

			batch = append(batch, w)

			if len(batch) >= size {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}

				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}

func (s *Store) flushSpendBatch(batch []*spendWork) {
	sort.Slice(batch, func(i, j int) bool {
		c := bytes.Compare(batch[i].g.hash[:], batch[j].g.hash[:])
		if c != 0 {
			return c < 0
		}

		return batch[i].g.page < batch[j].g.page
	})

	ctx, cancel := s.batchContext()
	defer cancel()

	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		batchErr := errors.NewStorageError("packedsql: failed to begin spend batch transaction", err)
		for _, w := range batch {
			w.done <- batchErr
		}

		return
	}

	itemErrs := make([]error, len(batch))
	abortedAt := -1

	for i, w := range batch {
		itemErrs[i] = s.spendGroup(ctx, dbTx, w.g, w.blockHeight, w.ig)

		if itemErrs[i] != nil && errors.Is(itemErrs[i], errors.ErrStorageError) {
			abortedAt = i
			break
		}
	}

	if abortedAt >= 0 {
		_ = dbTx.Rollback(ctx)

		batchErr := errors.NewStorageError("packedsql: spend batch aborted", itemErrs[abortedAt])
		for _, w := range batch {
			w.done <- batchErr
		}

		return
	}

	if err = dbTx.Commit(ctx); err != nil {
		batchErr := errors.NewStorageError("packedsql: failed to commit spend batch", err)
		for _, w := range batch {
			w.done <- batchErr
		}

		return
	}

	for i, w := range batch {
		w.done <- itemErrs[i]
	}
}
