package packedsql

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const setMinedChunkSize = 400

func (s *Store) SetMinedMulti(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo) (map[chainhash.Hash][]uint32, error) {
	result := make(map[chainhash.Hash][]uint32)

	for start := 0; start < len(hashes); start += setMinedChunkSize {
		end := min(start+setMinedChunkSize, len(hashes))

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if err := s.setMinedChunk(ctx, hashes[start:end], info, result); err != nil {
			return nil, err
		}
	}

	return result, nil
}

type minedRowState struct {
	hash           chainhash.Hash
	flags          int16
	blockRefs      []byte
	spentCount     int32
	page0Count     int32
	pagesSpent     int32
	pagesTotal     int32
	preserveUntil  *int64
	deleteAtHeight *int64
	unminedSince   *int64
}

func (s *Store) setMinedChunk(ctx context.Context, hashes []*chainhash.Hash, info utxo.MinedBlockInfo, result map[chainhash.Hash][]uint32) error {
	hashBytes := make([][]byte, len(hashes))
	for i, h := range hashes {
		hashBytes[i] = h[:]
	}

	dbTx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return errors.NewStorageError("packedsql: failed to begin SetMinedMulti transaction", err)
	}

	defer func() { _ = dbTx.Rollback(ctx) }()

	// ORDER BY hash gives every concurrent SetMinedMulti the same row lock order, matching
	// the hash ordering the spend paths use. Without it two overlapping calls can deadlock.
	rows, err := dbTx.Query(ctx,
		`SELECT hash, flags, block_refs, spent_count, page0_count, pages_spent, pages_total,
		        preserve_until, delete_at_height, unmined_since
		 FROM packed_txs WHERE hash = ANY($1::bytea[]) ORDER BY hash FOR UPDATE`, hashBytes)
	if err != nil {
		return errors.NewStorageError("packedsql: SetMinedMulti select failed", err)
	}

	states := make(map[chainhash.Hash]*minedRowState, len(hashes))
	ordered := make([]*minedRowState, 0, len(hashes))

	for rows.Next() {
		st := &minedRowState{}

		var hb []byte
		if err = rows.Scan(&hb, &st.flags, &st.blockRefs, &st.spentCount, &st.page0Count,
			&st.pagesSpent, &st.pagesTotal, &st.preserveUntil, &st.deleteAtHeight, &st.unminedSince); err != nil {
			rows.Close()
			return errors.NewStorageError("packedsql: SetMinedMulti scan failed", err)
		}

		st.hash = chainhash.Hash(hb)
		states[st.hash] = st
		ordered = append(ordered, st)
	}

	rows.Close()

	if err = rows.Err(); err != nil {
		return errors.NewStorageError("packedsql: SetMinedMulti rows failed", err)
	}

	if !info.UnsetMined && len(states) != len(hashes) {
		for _, h := range hashes {
			if _, ok := states[*h]; !ok {
				return errors.NewTxNotFoundError("packedsql: transaction not found: %s", h)
			}
		}
	}

	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	newDAH := int64(info.BlockHeight) + int64(retention)

	// Iterate in hash order (as returned by the locking SELECT), not map order, so the
	// UPDATE sequence is deterministic too.
	for _, st := range ordered {
		newFlags := st.flags &^ flagLocked
		newRefs := st.blockRefs
		newUnmined := st.unminedSince
		newDAHValue := st.deleteAtHeight

		if info.UnsetMined {
			newRefs = removeBlockRef(newRefs, info.BlockID)
			newDAHValue = nil

			if len(newRefs) == 0 {
				h := int64(s.GetBlockHeight()) + 1
				newUnmined = &h
			}
		} else {
			newRefs = appendBlockRef(newRefs, info)

			if info.OnLongestChain {
				newUnmined = nil

				if retention > 0 {
					fullySpent := st.spentCount >= st.page0Count && st.pagesSpent >= st.pagesTotal

					switch {
					case st.preserveUntil != nil:
					case st.deleteAtHeight != nil && *st.deleteAtHeight < newDAH:
						newDAHValue = &newDAH
					case st.deleteAtHeight == nil && fullySpent:
						newDAHValue = &newDAH
					}
				}
			} else {
				newDAHValue = nil
			}
		}

		if _, err = dbTx.Exec(ctx,
			`UPDATE packed_txs SET flags = $2, block_refs = $3, unmined_since = $4, delete_at_height = $5 WHERE hash = $1`,
			st.hash[:], newFlags, newRefs, newUnmined, newDAHValue); err != nil {
			return errors.NewStorageError("packedsql: SetMinedMulti update failed for %s", st.hash, err)
		}

		ids, _, _ := unpackBlockRefs(newRefs)
		result[st.hash] = ids
	}

	if err = dbTx.Commit(ctx); err != nil {
		return errors.NewStorageError("packedsql: SetMinedMulti commit failed", err)
	}

	return nil
}

func (s *Store) MarkTransactionsOnLongestChain(ctx context.Context, txHashes []chainhash.Hash, onLongestChain bool) error {
	for start := 0; start < len(txHashes); start += setMinedChunkSize {
		end := min(start+setMinedChunkSize, len(txHashes))
		chunk := txHashes[start:end]

		hashBytes := make([][]byte, len(chunk))
		for i := range chunk {
			hashBytes[i] = chunk[i][:]
		}

		var (
			ct  pgconn.CommandTag
			err error
		)

		if onLongestChain {
			ct, err = s.pool.Exec(ctx,
				`UPDATE packed_txs SET unmined_since = NULL WHERE hash = ANY($1::bytea[])`, hashBytes)
		} else {
			ct, err = s.pool.Exec(ctx,
				`UPDATE packed_txs SET unmined_since = $2 WHERE hash = ANY($1::bytea[])`,
				hashBytes, int64(s.GetBlockHeight()))
		}

		if err != nil {
			return errors.NewStorageError("packedsql: MarkTransactionsOnLongestChain update failed", err)
		}

		if int(ct.RowsAffected()) != len(chunk) {
			return errors.NewProcessingError("packedsql: MarkTransactionsOnLongestChain matched %d of %d transactions",
				ct.RowsAffected(), len(chunk))
		}
	}

	return nil
}
