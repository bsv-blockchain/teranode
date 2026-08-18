package packedsql

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type pgxQuerier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type spendSlot struct {
	slot  uint32
	spend *utxo.Spend
}

type parentSpendGroup struct {
	hash  chainhash.Hash
	page  uint32
	slots []spendSlot
}

func groupSpendsByParentPage(spends []*utxo.Spend, pageSize uint32) []*parentSpendGroup {
	type key struct {
		hash chainhash.Hash
		page uint32
	}

	byKey := make(map[key]*parentSpendGroup)
	order := make([]key, 0, len(spends))

	for _, sp := range spends {
		k := key{hash: *sp.TxID, page: pageOfVout(sp.Vout, pageSize)}

		g, ok := byKey[k]
		if !ok {
			g = &parentSpendGroup{hash: k.hash, page: k.page}
			byKey[k] = g

			order = append(order, k)
		}

		g.slots = append(g.slots, spendSlot{slot: slotOfVout(sp.Vout, pageSize), spend: sp})
	}

	groups := make([]*parentSpendGroup, 0, len(order))

	sort.Slice(order, func(i, j int) bool {
		c := bytes.Compare(order[i].hash[:], order[j].hash[:])
		if c != 0 {
			return c < 0
		}

		return order[i].page < order[j].page
	})

	for _, k := range order {
		groups = append(groups, byKey[k])
	}

	return groups
}

func (s *Store) dahParam(blockHeight uint32) *int64 {
	retention := s.settings.GetUtxoStoreBlockHeightRetention()
	if retention == 0 {
		return nil
	}

	dah := int64(blockHeight) + int64(retention)

	return &dah
}

func guardMask(ig utxo.IgnoreFlags) int16 {
	mask := guardFlagsMask

	if ig.IgnoreConflicting {
		mask &^= flagConflicting
	}

	if ig.IgnoreLocked {
		mask &^= flagLocked
	}

	return mask
}

func (s *Store) Spend(ctx context.Context, tx *bt.Tx, blockHeight uint32, ignoreFlags ...utxo.IgnoreFlags) ([]*utxo.Spend, error) {
	if blockHeight == 0 {
		return nil, errors.NewProcessingError("packedsql: blockHeight must be greater than zero")
	}

	var ig utxo.IgnoreFlags
	if len(ignoreFlags) > 0 {
		ig = ignoreFlags[0]
	}

	var (
		spends []*utxo.Spend
		err    error
	)

	if ig.SkipUTXOHashCheck {
		spends, err = utxo.GetSpendsOutpointOnly(tx)
	} else {
		spends, err = utxo.GetSpends(tx)
	}

	if err != nil {
		return nil, err
	}

	if len(spends) == 0 {
		return nil, errors.NewProcessingError("packedsql: no spends provided")
	}

	if len(s.spendChans) > 0 {
		err = s.spendViaWorkers(ctx, groupSpendsByParentPage(spends, s.pageSize), blockHeight, ig)
	} else {
		err = s.spendOnQuerier(ctx, s.pool, spends, blockHeight, ig)
	}

	if err != nil && needsRollback(spends) {
		succeeded := make([]*utxo.Spend, 0, len(spends))

		for _, sp := range spends {
			if sp.Err == nil {
				succeeded = append(succeeded, sp)
			}
		}

		if len(succeeded) > 0 {
			if rbErr := s.Unspend(ctx, succeeded); rbErr != nil {
				s.logger.Errorf("packedsql: failed to roll back %d spends after spend failure: %v", len(succeeded), rbErr)
			}
		}
	}

	return spends, err
}

func (s *Store) spendOnQuerier(ctx context.Context, q pgxQuerier, spends []*utxo.Spend, blockHeight uint32, ig utxo.IgnoreFlags) error {
	groups := groupSpendsByParentPage(spends, s.pageSize)

	var firstErr error

	for _, g := range groups {
		if err := s.spendGroup(ctx, q, g, blockHeight, ig); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if firstErr != nil {
		return errors.NewUtxoError("packedsql: spend failed", firstErr)
	}

	return nil
}

func (s *Store) spendGroup(ctx context.Context, q pgxQuerier, g *parentSpendGroup, blockHeight uint32, ig utxo.IgnoreFlags) error {
	ok, err := s.trySpendSlots(ctx, q, g, g.slots, blockHeight, ig, false)
	if err != nil {
		return err
	}

	if ok {
		return nil
	}

	var firstErr error

	for _, sl := range g.slots {
		ok, err = s.trySpendSlots(ctx, q, g, []spendSlot{sl}, blockHeight, ig, false)
		if err != nil {
			return err
		}

		if !ok {
			if cErr := s.classifySlot(ctx, q, g, sl, blockHeight, ig); cErr != nil {
				sl.spend.Err = cErr

				if firstErr == nil {
					firstErr = cErr
				}
			}
		}
	}

	return firstErr
}

// buildSpendSQL builds the conditional spend UPDATE for a group of slots on a single row.
//
// The utxo_hashes slot is always consulted, even when skipHashCheck is set. A slot that
// belongs to an output which was never stored as a UTXO (OP_RETURN, dust, post-genesis
// unspendable) has a zeroed hash slot AND a zeroed spend slot, so the "unspent" predicate
// alone would let an outpoint-only spend succeed against a UTXO that does not exist. The
// non-zero hash guard keeps those slots unspendable on both paths.
func buildSpendSQL(page uint32, slots []spendSlot, skipHashCheck, skipOverridesBit bool) (string, int) {
	var sb strings.Builder

	overlay := "spends"

	argn := 0
	next := func() int { argn++; return argn }

	hashParam := next()

	var pageParam int
	if page > 0 {
		pageParam = next()
	}

	maskParam := next()
	heightParam := next()

	var dahParam int
	if page == 0 {
		dahParam = next()
	}

	zerosParam := next()

	var zeros32Param int
	if skipHashCheck {
		zeros32Param = next()
	}

	type slotParams struct {
		sd, off, hoff, uh int
	}

	sp := make([]slotParams, len(slots))

	for i := range slots {
		sp[i].sd = next()
		sp[i].off = next()
		sp[i].hoff = next()

		if !skipHashCheck {
			sp[i].uh = next()
		}
	}

	for i := range slots {
		overlay = fmt.Sprintf("overlay(%s PLACING $%d::bytea FROM $%d)", overlay, sp[i].sd, sp[i].off)
	}

	if page == 0 {
		fmt.Fprintf(&sb, `UPDATE packed_txs SET
  spends = %s,
  spent_count = spent_count + %d,
  delete_at_height = CASE
    WHEN $%d::bigint IS NOT NULL AND spent_count + %d >= page0_count AND pages_spent >= pages_total
         AND octet_length(coalesce(block_refs, ''::bytea)) > 0
         AND unmined_since IS NULL AND preserve_until IS NULL AND (flags & 4) = 0
    THEN $%d ELSE delete_at_height END
WHERE hash = $%d
  AND (flags & $%d) = 0
  AND ((flags & 1) = 0 OR coinbase_spending_height <= $%d)`,
			overlay, len(slots), dahParam, len(slots), dahParam, hashParam, maskParam, heightParam)
	} else {
		fmt.Fprintf(&sb, `UPDATE packed_tx_pages p SET
  spends = %s,
  spent_count = p.spent_count + %d
FROM packed_txs m
WHERE p.hash = $%d AND p.page = $%d AND m.hash = p.hash
  AND (m.flags & $%d) = 0
  AND ((m.flags & 1) = 0 OR m.coinbase_spending_height <= $%d)`,
			strings.ReplaceAll(overlay, "spends", "p.spends"), len(slots), hashParam, pageParam, maskParam, heightParam)
	}

	prefix := ""
	if page > 0 {
		prefix = "p."
	}

	for i := range slots {
		if skipHashCheck {
			fmt.Fprintf(&sb, "\n  AND substring(%sutxo_hashes FROM $%d FOR 32) <> $%d::bytea", prefix, sp[i].hoff, zeros32Param)
		} else {
			fmt.Fprintf(&sb, "\n  AND substring(%sutxo_hashes FROM $%d FOR 32) = $%d::bytea", prefix, sp[i].hoff, sp[i].uh)
		}

		fmt.Fprintf(&sb, "\n  AND substring(%sspends FROM $%d FOR 36) = $%d::bytea", prefix, sp[i].off, zerosParam)
	}

	sql := sb.String()

	if skipOverridesBit {
		sql = strings.Replace(sql, "(flags & $", "(flags & ~16 & $", 1)
		sql = strings.Replace(sql, "(m.flags & $", "(m.flags & ~16 & $", 1)
	}

	return sql, argn
}

func (s *Store) trySpendSlots(ctx context.Context, q pgxQuerier, g *parentSpendGroup, slots []spendSlot, blockHeight uint32, ig utxo.IgnoreFlags, skipOverridesBit bool) (bool, error) {
	sql, _ := buildSpendSQL(g.page, slots, ig.SkipUTXOHashCheck, skipOverridesBit)

	args := make([]any, 0, 6+len(slots)*4)
	args = append(args, g.hash[:])

	if g.page > 0 {
		args = append(args, g.page)
	}

	args = append(args, guardMask(ig), int64(blockHeight))

	if g.page == 0 {
		args = append(args, s.dahParam(blockHeight))
	}

	args = append(args, make([]byte, slotSpendSize))

	if ig.SkipUTXOHashCheck {
		args = append(args, make([]byte, slotHashSize))
	}

	for _, sl := range slots {
		args = append(args, packSpendingData(sl.spend.SpendingData), int(sl.slot)*slotSpendSize+1, int(sl.slot)*slotHashSize+1)

		if !ig.SkipUTXOHashCheck {
			var uh []byte
			if sl.spend.UTXOHash != nil {
				uh = sl.spend.UTXOHash[:]
			}

			args = append(args, uh)
		}
	}

	ct, err := q.Exec(ctx, sql, args...)
	if err != nil {
		return false, errors.NewStorageError("packedsql: spend update failed for %s page %d", g.hash, g.page, err)
	}

	if ct.RowsAffected() == 0 {
		return false, nil
	}

	if g.page > 0 {
		if err = s.completePageIfSpent(ctx, q, g, blockHeight); err != nil {
			return true, err
		}
	}

	return true, nil
}

func (s *Store) completePageIfSpent(ctx context.Context, q pgxQuerier, g *parentSpendGroup, blockHeight uint32) error {
	_, err := q.Exec(ctx, `UPDATE packed_txs m SET
  pages_spent = m.pages_spent + 1,
  delete_at_height = CASE
    WHEN $3::bigint IS NOT NULL AND m.spent_count >= m.page0_count AND m.pages_spent + 1 >= m.pages_total
         AND octet_length(coalesce(m.block_refs, ''::bytea)) > 0
         AND m.unmined_since IS NULL AND m.preserve_until IS NULL AND (m.flags & 4) = 0
    THEN $3 ELSE m.delete_at_height END
FROM packed_tx_pages p
WHERE m.hash = $1 AND p.hash = $1 AND p.page = $2 AND p.spent_count = p.spendable_count`,
		g.hash[:], g.page, s.dahParam(blockHeight))
	if err != nil {
		return errors.NewStorageError("packedsql: page completion update failed for %s page %d", g.hash, g.page, err)
	}

	return nil
}

func (s *Store) classifySlot(ctx context.Context, q pgxQuerier, g *parentSpendGroup, sl spendSlot, blockHeight uint32, ig utxo.IgnoreFlags) error {
	var (
		flags                  int16
		coinbaseSpendingHeight int64
		storedHash             []byte
		storedSpend            []byte
		err                    error
	)

	hashFrom := int(sl.slot)*slotHashSize + 1
	spendFrom := int(sl.slot)*slotSpendSize + 1

	if g.page == 0 {
		err = q.QueryRow(ctx,
			`SELECT flags, coinbase_spending_height, substring(utxo_hashes FROM $2 FOR 32), substring(spends FROM $3 FOR 36)
			 FROM packed_txs WHERE hash = $1`,
			g.hash[:], hashFrom, spendFrom).Scan(&flags, &coinbaseSpendingHeight, &storedHash, &storedSpend)
	} else {
		err = q.QueryRow(ctx,
			`SELECT m.flags, m.coinbase_spending_height, substring(p.utxo_hashes FROM $3 FOR 32), substring(p.spends FROM $4 FOR 36)
			 FROM packed_tx_pages p JOIN packed_txs m ON m.hash = p.hash
			 WHERE p.hash = $1 AND p.page = $2`,
			g.hash[:], g.page, hashFrom, spendFrom).Scan(&flags, &coinbaseSpendingHeight, &storedHash, &storedSpend)
	}

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errors.NewTxNotFoundError("packedsql: transaction %s not found", g.hash)
		}

		return errors.NewStorageError("packedsql: spend classification failed for %s", g.hash, err)
	}

	if flags&flagConflicting != 0 && !ig.IgnoreConflicting {
		return errors.NewTxConflictingError("packedsql: transaction %s is conflicting", g.hash)
	}

	if flags&flagLocked != 0 && !ig.IgnoreLocked {
		return errors.NewTxLockedError("packedsql: transaction %s is locked", g.hash)
	}

	if flags&flagFrozen != 0 {
		return errors.NewUtxoFrozenError("packedsql: utxo %s:%d is frozen", g.hash, sl.spend.Vout)
	}

	if flags&flagHasOverrides != 0 {
		return s.spendWithOverrides(ctx, q, g, sl, blockHeight, ig)
	}

	if flags&flagCoinbase != 0 && coinbaseSpendingHeight > int64(blockHeight) {
		return errors.NewTxCoinbaseImmatureError("packedsql: coinbase %s not spendable until height %d (spending at %d)",
			g.hash, coinbaseSpendingHeight, blockHeight)
	}

	expected := packSpendingData(sl.spend.SpendingData)

	if bytes.Equal(storedSpend, expected) {
		return nil
	}

	if existing := unpackSpendingData(storedSpend); existing != nil {
		sl.spend.ConflictingTxID = existing.TxID

		var uh chainhash.Hash
		if sl.spend.UTXOHash != nil {
			uh = *sl.spend.UTXOHash
		}

		return errors.NewUtxoSpentError(*sl.spend.TxID, sl.spend.Vout, uh, existing)
	}

	// A zeroed hash slot means the output was never stored as a UTXO, so there is nothing
	// to spend. On the hash-checking path this surfaces as a mismatch below; on the
	// outpoint-only path there is no hash to compare against, so report it explicitly.
	if ig.SkipUTXOHashCheck && isZeroBytes(storedHash) {
		return errors.NewTxNotFoundError("packedsql: utxo %s:%d does not exist, output is not spendable", g.hash, sl.spend.Vout)
	}

	if !ig.SkipUTXOHashCheck && sl.spend.UTXOHash != nil && !bytes.Equal(storedHash, sl.spend.UTXOHash[:]) {
		return errors.NewUtxoHashMismatchError("packedsql: utxo hash mismatch for %s:%d", g.hash, sl.spend.Vout)
	}

	return errors.NewUtxoError("packedsql: spend of %s:%d failed for an unknown reason", g.hash, sl.spend.Vout)
}

func (s *Store) spendWithOverrides(ctx context.Context, q pgxQuerier, g *parentSpendGroup, sl spendSlot, blockHeight uint32, ig utxo.IgnoreFlags) error {
	vout := int(g.page)*int(s.pageSize) + int(sl.slot)

	var (
		frozen         bool
		spendableIn    *int64
		reassignedHash []byte
	)

	err := q.QueryRow(ctx,
		`SELECT frozen, spendable_in, reassigned_hash FROM utxo_overrides WHERE hash = $1 AND vout = $2`,
		g.hash[:], vout).Scan(&frozen, &spendableIn, &reassignedHash)

	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return errors.NewStorageError("packedsql: override lookup failed for %s:%d", g.hash, vout, err)
	}

	if err == nil {
		if frozen {
			return errors.NewUtxoFrozenError("packedsql: utxo %s:%d is frozen", g.hash, vout)
		}

		if spendableIn != nil && *spendableIn > int64(blockHeight) {
			return errors.NewUtxoFrozenError("packedsql: utxo %s:%d is not spendable until height %d", g.hash, vout, *spendableIn)
		}

		if reassignedHash != nil && sl.spend.UTXOHash != nil && !ig.SkipUTXOHashCheck {
			if !bytes.Equal(reassignedHash, sl.spend.UTXOHash[:]) {
				return errors.NewUtxoHashMismatchError("packedsql: reassigned utxo hash mismatch for %s:%d", g.hash, vout)
			}

			ig.SkipUTXOHashCheck = true
		}
	}

	ok, err := s.trySpendSlots(ctx, q, g, []spendSlot{sl}, blockHeight, ig, true)
	if err != nil {
		return err
	}

	if ok {
		return nil
	}

	expected := packSpendingData(sl.spend.SpendingData)

	var storedSpend []byte

	spendFrom := int(sl.slot)*slotSpendSize + 1

	if g.page == 0 {
		err = q.QueryRow(ctx, `SELECT substring(spends FROM $2 FOR 36) FROM packed_txs WHERE hash = $1`,
			g.hash[:], spendFrom).Scan(&storedSpend)
	} else {
		err = q.QueryRow(ctx, `SELECT substring(spends FROM $3 FOR 36) FROM packed_tx_pages WHERE hash = $1 AND page = $2`,
			g.hash[:], g.page, spendFrom).Scan(&storedSpend)
	}

	if err != nil {
		return errors.NewStorageError("packedsql: override spend re-read failed for %s:%d", g.hash, vout, err)
	}

	if bytes.Equal(storedSpend, expected) {
		return nil
	}

	if existing := unpackSpendingData(storedSpend); existing != nil {
		sl.spend.ConflictingTxID = existing.TxID

		var uh chainhash.Hash
		if sl.spend.UTXOHash != nil {
			uh = *sl.spend.UTXOHash
		}

		return errors.NewUtxoSpentError(*sl.spend.TxID, sl.spend.Vout, uh, existing)
	}

	return errors.NewUtxoHashMismatchError("packedsql: utxo hash mismatch for %s:%d", g.hash, vout)
}

func isZeroBytes(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}

	return true
}

func needsRollback(spends []*utxo.Spend) bool {
	for _, sp := range spends {
		if sp.Err == nil {
			continue
		}

		if errors.Is(sp.Err, errors.ErrSpent) ||
			errors.Is(sp.Err, errors.ErrTxConflicting) ||
			errors.Is(sp.Err, errors.ErrFrozen) ||
			errors.Is(sp.Err, errors.ErrUtxoHashMismatch) {
			return true
		}
	}

	return false
}
