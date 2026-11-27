package utxoset

import (
	"context"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/util/lane"
	"golang.org/x/sync/errgroup"
)

// laneWindowPage is how many rows one statement of the window read returns. A variable so a test
// can make the read take several pages.
var laneWindowPage = 5_000

// The window's rows come from tx_body, the only table here partitioned by when a transaction was
// written: created_height. The read pages through the primary key (created_height, txid) in short
// statements, so it holds no snapshot open across the read, which would hold back the pruner's
// concurrent partition detach. It names txid only, never raw_tx.
const (
	laneWindowFirstSQL = `
SELECT created_height, txid FROM tx_body
 WHERE created_height >= $1::int
 ORDER BY created_height, txid
 LIMIT $2`

	laneWindowNextSQL = `
SELECT created_height, txid FROM tx_body
 WHERE created_height >= $1::int
   AND (created_height, txid) > ($2::int, $3::bytea)
 ORDER BY created_height, txid
 LIMIT $4`

	// laneWindowCarrySQL pages through tx_body_carry, the bodies of transactions that waited
	// longer than the body horizon, which the design reads with the window.
	laneWindowCarrySQL = `
SELECT txid FROM tx_body_carry
 WHERE txid > $1::bytea
 ORDER BY txid
 LIMIT $2`

	// laneWindowIdentSQL reads the identity rows of one leaf's transactions: what a reload from the
	// window reads for each one, its position, fee, size and parents. The leaf is a scalar and the
	// txids an array, the shape identByTxidSQL measured.
	laneWindowIdentSQL = `
SELECT i.txid, i.lane_position, i.created_at, i.flags, i.fee, i.size_in_bytes, i.tx_inpoints
  FROM tx_ident i
 WHERE i.leaf = $1::smallint
   AND i.txid = ANY($2::bytea[])`
)

// laneWindowRow is one tx_body or carry row of the window.
type laneWindowRow struct {
	txid     chainhash.Hash
	inMargin bool
}

var _ utxo.LaneWindowReader = (*Store)(nil)

// ReadLaneWindow reads block assembly's restart window for its dry run at utxostore_laneMigration
// shadow: every tx_body row created at or above w.FromHeight - w.MarginBlocks, then every
// tx_body_carry row, and one tx_ident read per transaction. Rows below w.FromHeight are InMargin.
// A row with no identity row is Missing. It needs lane_position on tx_ident.
func (s *Store) ReadLaneWindow(ctx context.Context, w utxo.LaneWindow, fn func([]utxo.LaneWindowRecord) error) (utxo.LaneWindowStats, error) {
	start := time.Now()

	var st utxo.LaneWindowStats

	st.MarginKnown = true

	if !s.laneColumn {
		return st, errors.NewProcessingError("[utxoset] the restart window needs lane_position on tx_ident, which this database does not have")
	}

	from := int64(w.FromHeight) - int64(w.MarginBlocks)
	if from < 0 {
		from = 0
	}

	var (
		lastHeight int32
		lastTxid   []byte
		first      = true
		page       = make([]laneWindowRow, 0, laneWindowPage)
	)

	for {
		page = page[:0]

		var (
			height int32
			err    error
		)

		if first {
			height, err = s.laneWindowPage(ctx, &page, &st, w.FromHeight, laneWindowFirstSQL, from, laneWindowPage)
		} else {
			height, err = s.laneWindowPage(ctx, &page, &st, w.FromHeight, laneWindowNextSQL, from, lastHeight, lastTxid, laneWindowPage)
		}

		if err != nil {
			return st, err
		}

		first = false

		if len(page) == 0 {
			break
		}

		lastHeight, lastTxid = height, page[len(page)-1].txid[:]

		if err = s.laneWindowJudge(ctx, page, &st, fn); err != nil {
			return st, err
		}

		if len(page) < laneWindowPage {
			break
		}
	}

	lastCarry := []byte{}

	for {
		page = page[:0]

		rows, err := s.pool.Query(ctx, laneWindowCarrySQL, lastCarry, laneWindowPage)
		if err != nil {
			return st, errors.NewStorageError("[utxoset] read tx_body_carry for the restart window", err)
		}

		for rows.Next() {
			var txid []byte
			if err = rows.Scan(&txid); err != nil {
				rows.Close()

				return st, errors.NewStorageError("[utxoset] scan tx_body_carry", err)
			}

			var row laneWindowRow
			copy(row.txid[:], txid)
			page = append(page, row)
			st.Bytes += uint64(len(txid))
		}

		rows.Close()

		if err = rows.Err(); err != nil {
			return st, errors.NewStorageError("[utxoset] read tx_body_carry for the restart window", err)
		}

		if len(page) == 0 {
			break
		}

		lastCarry = page[len(page)-1].txid[:]

		if err = s.laneWindowJudge(ctx, page, &st, fn); err != nil {
			return st, err
		}

		if len(page) < laneWindowPage {
			break
		}
	}

	st.Duration = time.Since(start)

	return st, nil
}

// laneWindowPage runs one tx_body page statement into page and returns the height of its last row.
func (s *Store) laneWindowPage(ctx context.Context, page *[]laneWindowRow, st *utxo.LaneWindowStats, fromHeight uint32, query string, args ...any) (int32, error) {
	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return 0, errors.NewStorageError("[utxoset] read tx_body for the restart window", err)
	}

	defer rows.Close()

	var last int32

	for rows.Next() {
		var (
			height int32
			txid   []byte
		)

		if err = rows.Scan(&height, &txid); err != nil {
			return 0, errors.NewStorageError("[utxoset] scan tx_body", err)
		}

		var row laneWindowRow
		copy(row.txid[:], txid)
		row.inMargin = height >= 0 && uint32(height) < fromHeight

		*page = append(*page, row)
		last = height
		st.Bytes += 4 + uint64(len(txid))
	}

	if err = rows.Err(); err != nil {
		return 0, errors.NewStorageError("[utxoset] read tx_body for the restart window", err)
	}

	return last, nil
}

// laneWindowProbes bounds the identity reads of one page that run at once, one per leaf.
const laneWindowProbes = 4

// laneWindowJudge reads the identity rows of one page, one statement per leaf, and hands the page
// to fn as records.
func (s *Store) laneWindowJudge(ctx context.Context, page []laneWindowRow, st *utxo.LaneWindowStats, fn func([]utxo.LaneWindowRecord) error) error {
	var byLeaf [8][][]byte

	for i := range page {
		leaf := LeafFor(page[i].txid[:])
		byLeaf[leaf] = append(byLeaf[leaf], page[i].txid[:])
	}

	var (
		found [8]map[chainhash.Hash]utxo.LaneWindowRecord
		bytes [8]uint64
	)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(laneWindowProbes)

	for leaf, txids := range byLeaf {
		if len(txids) == 0 {
			continue
		}

		g.Go(func() error {
			var err error

			found[leaf], bytes[leaf], err = s.laneWindowIdent(gCtx, int16(leaf), txids) //nolint:gosec // a leaf is below 8

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	for _, n := range bytes {
		st.Bytes += n
	}

	batch := make([]utxo.LaneWindowRecord, 0, len(page))

	for i := range page {
		rec, ok := found[LeafFor(page[i].txid[:])][page[i].txid]
		if !ok {
			rec = utxo.LaneWindowRecord{Hash: page[i].txid, Missing: true}
		}

		rec.InMargin = page[i].inMargin

		if rec.InMargin {
			st.MarginRecords++
		}

		batch = append(batch, rec)
	}

	st.Records += uint64(len(batch))

	return fn(batch)
}

// laneWindowIdent reads the identity rows of one leaf's transactions, with the bytes read.
func (s *Store) laneWindowIdent(ctx context.Context, leaf int16, txids [][]byte) (map[chainhash.Hash]utxo.LaneWindowRecord, uint64, error) {
	found := make(map[chainhash.Hash]utxo.LaneWindowRecord, len(txids))

	var n uint64

	rows, err := s.pool.Query(ctx, laneWindowIdentSQL, leaf, txids)
	if err != nil {
		return nil, 0, errors.NewStorageError("[utxoset] read tx_ident for the restart window", err)
	}

	defer rows.Close()

	for rows.Next() {
		var (
			txid         []byte
			lanePosition *int64
			createdAt    *int64
			flags        int16
			fee          *int64
			size         *int32
			inpoints     []byte
		)

		if err = rows.Scan(&txid, &lanePosition, &createdAt, &flags, &fee, &size, &inpoints); err != nil {
			return nil, 0, errors.NewStorageError("[utxoset] scan tx_ident", err)
		}

		var rec utxo.LaneWindowRecord

		copy(rec.Hash[:], txid)

		if lanePosition != nil {
			rec.LanePosition = lane.FromSigned(*lanePosition)
		}

		if createdAt != nil {
			rec.CreatedAt = *createdAt
		}

		rec.Conflicting = flags&FlagConflicting != 0
		rec.Coinbase = flags&FlagCoinbase != 0

		found[rec.Hash] = rec
		n += uint64(len(txid)) + 8 + 8 + 2 + 8 + 4 + uint64(len(inpoints))
	}

	if err = rows.Err(); err != nil {
		return nil, 0, errors.NewStorageError("[utxoset] read tx_ident for the restart window", err)
	}

	return found, n, nil
}
