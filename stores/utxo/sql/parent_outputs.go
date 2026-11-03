package sql

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/bscript"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
	"github.com/bsv-blockchain/teranode/util/tracing"
)

// ParentOutputsForValidation implements utxo.Store. Each chunk of distinct
// outpoints is one statement that reads exactly one output row per outpoint,
// never every output of the parent, plus the lowest height recorded in
// block_ids. It reads the outputs table only: the inputs table holds children's
// copies of previous outputs, which a peer controls.
//
// A transaction row with no output row at the requested index is NoSuchIndex:
// the store keeps every non-nil output of a transaction it holds, so a missing
// row means the parent has no output there. No transaction row is TxNotFound.
// A failed chunk marks each of its outpoints with the error rather than
// failing the call, so one slow chunk never reads as a missing parent.
func (s *Store) ParentOutputsForValidation(ctx context.Context, outpoints []utxo.Outpoint, _ ...utxo.ParentOutputOption) ([]utxo.ParentOutput, error) {
	ctx, _, deferFn := tracing.Tracer("utxo").Start(ctx, "sql:ParentOutputsForValidation")
	defer deferFn()

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	answers := make([]utxo.ParentOutput, len(outpoints))
	if len(outpoints) == 0 {
		return answers, nil
	}

	// Distinct outpoints, and the answer slots each one fills.
	slotsByOutpoint := make(map[utxo.Outpoint][]int, len(outpoints))
	pairs := make([]outpointPair, 0, len(outpoints))

	for i, op := range outpoints {
		if _, seen := slotsByOutpoint[op]; !seen {
			h := op.TxID
			pairs = append(pairs, outpointPair{hash: h[:], idx: op.Vout})
		}

		slotsByOutpoint[op] = append(slotsByOutpoint[op], i)
	}

	chunkSize := maxINClauseSize
	if s.engine == string(util.Postgres) {
		chunkSize = postgresBatchDecorateChunkSize
	}

	if batchDecorateChunkSizeOverride > 0 {
		chunkSize = batchDecorateChunkSizeOverride
	}

	for start := 0; start < len(pairs); start += chunkSize {
		end := min(start+chunkSize, len(pairs))
		chunk := pairs[start:end]

		found, err := s.parentOutputsChunkWithRetry(ctx, chunk)
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}

			err = errors.NewStorageError("error reading parent outputs", err)
		}

		for _, p := range chunk {
			var h chainhash.Hash
			copy(h[:], p.hash)

			op := utxo.Outpoint{TxID: h, Vout: p.idx}

			var answer utxo.ParentOutput
			if err != nil {
				answer = utxo.ParentOutput{Err: err}
			} else if a, ok := found[op]; ok {
				answer = a
			} else {
				answer = utxo.ParentOutput{Status: utxo.ParentOutputTxNotFound}
			}

			for _, slot := range slotsByOutpoint[op] {
				answers[slot] = answer
			}
		}
	}

	return answers, nil
}

// parentOutputsReadRetries bounds how many times one chunk is tried when the
// database refuses it with a lock error, before its outpoints are marked with
// the error.
const parentOutputsReadRetries = 3

// parentOutputsChunkWithRetry retries a chunk the database refused with a lock
// error. The chunk's one statement reads transactions, outputs and block_ids
// together, so under SQLite's shared cache, which sqlitememory uses and which
// locks whole tables, it can meet a spend transaction that holds one of those
// tables and waits for another: SQLite breaks the cycle by failing one side with
// SQLITE_LOCKED. sendSpendBatch retries the spend side; this is the read side.
// A failed read holds no locks, so running it again is safe.
func (s *Store) parentOutputsChunkWithRetry(ctx context.Context, chunk []outpointPair) (map[utxo.Outpoint]utxo.ParentOutput, error) {
	var found map[utxo.Outpoint]utxo.ParentOutput

	err := retryReadOnLockError(ctx, s.logger, len(chunk), func() error {
		var err error

		found, err = s.parentOutputsChunk(ctx, chunk)

		return err
	})

	return found, err
}

// retryReadOnLockError runs read up to parentOutputsReadRetries times while it
// fails with an error isDeadlock recognises, and returns its last error.
func retryReadOnLockError(ctx context.Context, logger ulogger.Logger, outpoints int, read func() error) error {
	for attempt := 1; ; attempt++ {
		err := read()
		if err == nil || !isDeadlock(err) || attempt >= parentOutputsReadRetries {
			return err
		}

		logger.Warnf("[ParentOutputsForValidation] lock error (attempt %d/%d), retrying chunk of %d outpoints: %v", attempt, parentOutputsReadRetries, outpoints, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 10 * time.Millisecond):
		}
	}
}

// parentOutputsChunk runs one statement for chunk and returns an answer for
// every outpoint whose transaction exists. Outpoints with no transaction row are
// absent from the map.
func (s *Store) parentOutputsChunk(ctx context.Context, chunk []outpointPair) (map[utxo.Outpoint]utxo.ParentOutput, error) {
	valuesClause, args := buildCompositeValuesPairs(chunk, 1, s.engine)

	// The CTE form works on both Postgres and SQLite (see PreviousOutputsDecorate).
	// The height subquery is served by block_ids' (transaction_id, block_id) key.
	q := `WITH v(h, i) AS (` + valuesClause + `)
		SELECT t.hash, v.i, o.transaction_id IS NOT NULL, o.satoshis, o.locking_script,
		       (SELECT MIN(b.block_height) FROM block_ids b WHERE b.transaction_id = t.id)
		FROM v
		JOIN transactions t ON t.hash = v.h
		LEFT JOIN outputs o ON o.transaction_id = t.id AND o.idx = v.i`

	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	found := make(map[utxo.Outpoint]utxo.ParentOutput, len(chunk))

	for rows.Next() {
		var (
			hashBytes     []byte
			idx           uint32
			hasOutput     bool
			satoshis      *uint64
			lockingScript []byte
			height        *uint32
		)

		if err := rows.Scan(&hashBytes, &idx, &hasOutput, &satoshis, &lockingScript, &height); err != nil {
			return nil, err
		}

		var op utxo.Outpoint
		copy(op.TxID[:], hashBytes)
		op.Vout = idx

		if !hasOutput {
			found[op] = utxo.ParentOutput{Status: utxo.ParentOutputNoSuchIndex}
			continue
		}

		answer := utxo.ParentOutput{
			Status:        utxo.ParentOutputNotMined,
			LockingScript: bscript.NewFromBytes(lockingScript),
		}

		if satoshis != nil {
			answer.Satoshis = *satoshis
		}

		if height != nil {
			answer.Status = utxo.ParentOutputMined
			answer.Height = *height
		}

		found[op] = answer
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return found, nil
}
