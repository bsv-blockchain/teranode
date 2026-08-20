package packedsql

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"golang.org/x/sync/errgroup"
)

const batchDecorateChunkSize = 1000

func (s *Store) BatchDecorate(ctx context.Context, items []*utxo.UnresolvedMetaData, f ...fields.FieldName) error {
	defaultBins := utxo.MetaFieldsWithTx
	if len(f) > 0 {
		defaultBins = f
	}

	for start := 0; start < len(items); start += batchDecorateChunkSize {
		end := min(start+batchDecorateChunkSize, len(items))
		if err := s.batchDecorateChunk(ctx, items[start:end], defaultBins); err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) batchDecorateChunk(ctx context.Context, items []*utxo.UnresolvedMetaData, defaultBins []fields.FieldName) error {
	hashes := make([][]byte, len(items))
	for i, item := range items {
		h := item.Hash
		hashes[i] = h[:]
	}

	batchSQL := strings.Replace(selectRowSQL, "WHERE hash = $1", "WHERE hash = ANY($1::bytea[])", 1)

	rows, err := s.pool.Query(ctx, batchSQL, hashes)
	if err != nil {
		return errors.NewStorageError("packedsql: batch decorate query failed", err)
	}

	defer rows.Close()

	found := make(map[chainhash.Hash]*packedRow, len(items))

	for rows.Next() {
		r, err := scanPackedRow(rows)
		if err != nil {
			return errors.NewStorageError("packedsql: batch decorate scan failed", err)
		}

		found[chainhash.Hash(r.hash)] = r
	}

	if err = rows.Err(); err != nil {
		return errors.NewStorageError("packedsql: batch decorate rows failed", err)
	}

	for _, item := range items {
		r, ok := found[item.Hash]
		if !ok {
			item.Err = errors.NewTxNotFoundError("packedsql: transaction %s not found", item.Hash)
			continue
		}

		bins := defaultBins
		if len(item.Fields) > 0 {
			bins = item.Fields
		}

		item.Data, item.Err = s.rowToMeta(ctx, r, bins)
	}

	return nil
}

type outpointRef struct {
	txIdx int
	vin   int
	hash  []byte
	vout  uint32
}

func (s *Store) PreviousOutputsDecorate(ctx context.Context, tx *bt.Tx) error {
	return s.BatchPreviousOutputsDecorate(ctx, []*bt.Tx{tx})
}

func (s *Store) BatchPreviousOutputsDecorate(ctx context.Context, txs []*bt.Tx) error {
	var refs []outpointRef

	for txIdx, tx := range txs {
		for vin, input := range tx.Inputs {
			if input.PreviousTxScript != nil {
				continue
			}

			refs = append(refs, outpointRef{
				txIdx: txIdx,
				vin:   vin,
				hash:  input.PreviousTxIDChainHash()[:],
				vout:  input.PreviousTxOutIndex,
			})
		}
	}

	if len(refs) == 0 {
		return nil
	}

	concurrency := s.settings.UtxoStore.BatchPreviousOutputsDecorateConcurrency
	if concurrency < 1 {
		concurrency = 1
	}

	g, gctx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	const chunkSize = 2000

	for start := 0; start < len(refs); start += chunkSize {
		end := min(start+chunkSize, len(refs))
		chunk := refs[start:end]

		g.Go(func() error {
			return s.decorateChunk(gctx, txs, chunk)
		})
	}

	return g.Wait()
}

func (s *Store) decorateChunk(ctx context.Context, txs []*bt.Tx, refs []outpointRef) error {
	// The two 4-byte offsets that delimit output v live at bytes 4+4v and 8+4v of the
	// offset blob header (1-based for substring: v*4+5). Reading them for a vout at or past
	// the output count would read *payload* and reinterpret it as offsets, so the count is
	// checked in SQL and the resulting span is validated against the real column length
	// below. vout is attacker-controlled (it comes off the wire in a transaction input).
	offsetSQL, offsetArgs := buildOutpointQuery(refs, `CASE WHEN v.vout < `+outputCountExpr+`
	     THEN substring(t.outputs FROM v.vout * 4 + 5 FOR 8) END, octet_length(t.outputs)`)

	rows, err := s.pool.Query(ctx, offsetSQL, offsetArgs...)
	if err != nil {
		return errors.NewStorageError("packedsql: outpoint offset query failed", err)
	}

	type span struct {
		start  int64
		length int64
	}

	spans := make([]span, len(refs))
	seen := make([]bool, len(refs))

	for rows.Next() {
		var (
			idx         int
			offsetBytes []byte
			totalLen    int64
		)

		if err = rows.Scan(&idx, &offsetBytes, &totalLen); err != nil {
			rows.Close()
			return errors.NewStorageError("packedsql: outpoint offset scan failed", err)
		}

		if len(offsetBytes) != 8 {
			continue
		}

		start := int64(binary.LittleEndian.Uint32(offsetBytes[:4]))
		end := int64(binary.LittleEndian.Uint32(offsetBytes[4:]))

		if end < start || start > totalLen || end > totalLen {
			continue
		}

		spans[idx] = span{start: start + 1, length: end - start}
		seen[idx] = true
	}

	rows.Close()

	if err = rows.Err(); err != nil {
		return errors.NewStorageError("packedsql: outpoint offset rows failed", err)
	}

	for i, ok := range seen {
		if !ok {
			return errors.NewTxNotFoundError("packedsql: previous output %s:%d not found",
				chainhash.Hash(refs[i].hash), refs[i].vout)
		}
	}

	var sb strings.Builder

	sb.WriteString(`SELECT v.idx, substring(t.outputs FROM v.strt FOR v.len) FROM (VALUES `)

	args := make([]any, 0, len(refs)*4)

	for i, r := range refs {
		if i > 0 {
			sb.WriteString(",")
		}

		if i == 0 {
			fmt.Fprintf(&sb, "($%d::bytea,$%d::int,$%d::int,$%d::int)", i*4+1, i*4+2, i*4+3, i*4+4)
		} else {
			fmt.Fprintf(&sb, "($%d,$%d,$%d,$%d)", i*4+1, i*4+2, i*4+3, i*4+4)
		}

		// Bounded by octet_length(outputs) above, so the int32 narrowing cannot overflow.
		args = append(args, r.hash, i, int32(spans[i].start), int32(spans[i].length)) //nolint:gosec
	}

	sb.WriteString(`) AS v(hash, idx, strt, len) JOIN packed_txs t ON t.hash = v.hash`)

	dataRows, err := s.pool.Query(ctx, sb.String(), args...)
	if err != nil {
		return errors.NewStorageError("packedsql: outpoint data query failed", err)
	}

	defer dataRows.Close()

	for dataRows.Next() {
		var (
			idx         int
			outputBytes []byte
		)

		if err = dataRows.Scan(&idx, &outputBytes); err != nil {
			return errors.NewStorageError("packedsql: outpoint data scan failed", err)
		}

		output := &bt.Output{}
		if _, err = output.ReadFrom(bytes.NewReader(outputBytes)); err != nil {
			return errors.NewTxInvalidError("packedsql: could not read previous output", err)
		}

		ref := refs[idx]
		input := txs[ref.txIdx].Inputs[ref.vin]
		input.PreviousTxScript = output.LockingScript
		input.PreviousTxSatoshis = output.Satoshis
	}

	return dataRows.Err()
}

// outputCountExpr decodes the little-endian item count from the first four bytes of an
// offset blob header. get_byte is 0-based.
const outputCountExpr = `(get_byte(t.outputs, 0)
	     + get_byte(t.outputs, 1) * 256
	     + get_byte(t.outputs, 2) * 65536
	     + get_byte(t.outputs, 3) * 16777216)`

func buildOutpointQuery(refs []outpointRef, expr string) (string, []any) {
	var sb strings.Builder

	sb.WriteString(`SELECT v.idx, `)
	sb.WriteString(expr)
	sb.WriteString(` FROM (VALUES `)

	args := make([]any, 0, len(refs)*3)

	for i, r := range refs {
		if i > 0 {
			sb.WriteString(",")
		}

		if i == 0 {
			fmt.Fprintf(&sb, "($%d::bytea,$%d::int,$%d::int)", i*3+1, i*3+2, i*3+3)
		} else {
			fmt.Fprintf(&sb, "($%d,$%d,$%d)", i*3+1, i*3+2, i*3+3)
		}

		args = append(args, r.hash, int(r.vout), i)
	}

	sb.WriteString(`) AS v(hash, vout, idx) JOIN packed_txs t ON t.hash = v.hash`)

	return sb.String(), args
}
