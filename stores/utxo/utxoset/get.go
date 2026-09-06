package utxoset

import (
	"context"
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/utxo/fields"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// unpackMembership turns the packed 12-byte triples back into the three parallel slices the
// meta.Data carries, in the order they were written.
//
// Insertion order is load-bearing: the shared conformance suite requires subtree indexes to
// come back in the order they were written rather than sorted, so this must never reorder or
// deduplicate.
func unpackMembership(m []byte) (blockIDs, heights []uint32, subtreeIdxs []int) {
	n := len(m) / 12
	if n == 0 {
		return nil, nil, nil
	}

	blockIDs = make([]uint32, 0, n)
	heights = make([]uint32, 0, n)
	subtreeIdxs = make([]int, 0, n)

	for i := 0; i < n; i++ {
		e := m[i*12 : i*12+12]
		blockIDs = append(blockIDs, binary.BigEndian.Uint32(e[0:4]))
		heights = append(heights, binary.BigEndian.Uint32(e[4:8]))
		subtreeIdxs = append(subtreeIdxs, int(int32(binary.BigEndian.Uint32(e[8:12])))) //nolint:gosec // round-trips what create packed
	}

	return blockIDs, heights, subtreeIdxs
}

// Get returns everything the store holds about one transaction.
//
// The field list is ignored with TWO exceptions. Whichever of the four steps answers (see
// lookup.go) returns everything that step holds on one row, so narrowing the projection would
// save a few bytes on the wire and nothing else, while handing a caller a partly populated
// record it might dereference. Answering the whole question is cheaper than answering half of
// it carefully.
//
// The exceptions are the two fields that do NOT arrive on that row. The per-output spend state
// costs a second query across two tables, because this store destroys the coin row on spend
// and keeps the spender in its journal (see decorateSpendingData). The contest -- which losing
// transactions want this one's coins -- costs a second statement against conflict_children,
// because a contested parent is usually mined and a mined transaction's row is in a different
// table (see attachConflictingChildren). Both are answered only when asked for.
//
// A NIL Tx IS A LEGAL ANSWER, and fields.Tx does not change that. It happens for a transaction
// whose body window has aged out, and, when utxostore_skipTxBodyBelowCheckpoint is on, for
// every transaction mined at or below the hardcoded checkpoint, whose bytes were never written
// (see bodyFloor on Store). Both cases return the metadata with Tx nil and NO error: the
// transaction exists and its coins are live, so ErrTxNotFound would be a lie, and a caller
// reading it as a missing parent would reject that transaction's children. A caller that
// genuinely needs the bytes has to check for a nil Tx and go to the subtree data file.
func (s *Store) Get(ctx context.Context, hash *chainhash.Hash, fieldNames ...fields.FieldName) (*meta.Data, error) {
	if hash == nil {
		return nil, errors.NewProcessingError("[utxoset][Get] nil hash")
	}

	var (
		data *meta.Data
		err  error
	)

	// Batched when configured. The validator resolves parents one at a time from many
	// goroutines, and each of those is otherwise its own round trip.
	wantChildren := wantsConflictingChildren(fieldNames)

	if s.getBatcher != nil {
		data, err = s.getBatched(ctx, hash, wantChildren)
	} else {
		data, err = s.getDirect(ctx, hash, wantChildren)
	}

	if err != nil {
		return nil, err
	}

	if wantsSpendingData(fieldNames) {
		if err = s.decorateSpendingData(ctx, hash, data); err != nil {
			return nil, err
		}
	}

	return data, nil
}

// getDirect is the unbatched read: the shared read order, asked about one transaction.
//
// It is a lookupMany of one rather than a statement of its own. The order identity ->
// membership -> preserved parent -> coin is a correctness rule (see lookup.go), and a second
// copy of it here is a defect waiting for one copy to be fixed and the other forgotten.
func (s *Store) getDirect(ctx context.Context, hash *chainhash.Hash,
	wantChildren bool) (*meta.Data, error) {
	res, err := s.lookupMany(ctx, []chainhash.Hash{*hash}, wantChildren)
	if err != nil {
		return nil, err
	}

	// A row the store holds but cannot decode is this transaction's own storage fault, and
	// distinct from not holding it at all: the caller must not read a decode failure as a
	// missing parent and reject a child over it.
	if derr, bad := res.failed[*hash]; bad {
		return nil, derr
	}

	data, ok := res.found[*hash]
	if !ok {
		return nil, errors.NewTxNotFoundError("[utxoset][Get] %s", hash.String())
	}

	return data, nil
}

// GetMeta fills in an existing meta.Data from the store, which is what callers holding a
// record they want refreshed use.
func (s *Store) GetMeta(ctx context.Context, hash *chainhash.Hash, data *meta.Data) error {
	if data == nil {
		return errors.NewProcessingError("[utxoset][GetMeta] nil data")
	}

	// Every field, because the caller is refreshing a record it already holds and has no way
	// to say which parts of it it cares about. That includes the contest, which Get answers
	// only when named.
	got, err := s.Get(ctx, hash, fields.ConflictingChildren)
	if err != nil {
		return err
	}

	*data = *got

	return nil
}

// metaRow is one row as the identity table and its body return it, before it becomes the
// record a caller sees.
//
// It exists so the single read and the batched read share one conversion. Two copies of
// "what a stored transaction means" is how the two drift apart on a field nobody notices
// until something downstream reads a zero.
type metaRow struct {
	createdHeight int32
	offChainSince *int32
	membership    []byte
	fee           *int64
	sizeInBytes   *int32
	txInpoints    []byte
	locktime      *int32
	createdAt     *int64
	flags         int16
	rawTx         []byte
}

// toMeta turns a stored row into the record callers read.
func (r *metaRow) toMeta(hash *chainhash.Hash) (*meta.Data, error) {
	data := &meta.Data{
		IsCoinbase:  r.flags&FlagCoinbase != 0,
		Conflicting: r.flags&FlagConflicting != 0,
		Locked:      r.flags&FlagLocked != 0,
	}

	data.BlockIDs, data.BlockHeights, data.SubtreeIdxs = unpackMembership(r.membership)

	// ConflictingChildren is deliberately NOT filled in here. It lives in conflict_children,
	// keyed on the txid alone rather than on the identity row, and is attached by
	// attachConflictingChildren when the caller asks for it.

	if r.offChainSince != nil {
		data.UnminedSince = uint32(*r.offChainSince) //nolint:gosec // a stored height is never negative
	}

	if r.fee != nil {
		data.Fee = uint64(*r.fee) //nolint:gosec // a fee is never negative
	}

	if r.sizeInBytes != nil {
		data.SizeInBytes = uint64(*r.sizeInBytes) //nolint:gosec // a size is never negative
	}

	if r.locktime != nil {
		data.LockTime = uint32(*r.locktime) //nolint:gosec // a locktime is never negative
	}

	if r.createdAt != nil {
		data.CreatedAt = *r.createdAt
	}

	if len(r.txInpoints) > 0 {
		ip, ierr := subtree.NewTxInpointsFromBytes(r.txInpoints)
		if ierr != nil {
			return nil, errors.NewStorageError("[utxoset] inpoints %s", hash.String(), ierr)
		}

		data.TxInpoints = ip
	}

	// A body-less row is expected once its window has aged out, so this is a nil transaction
	// rather than an error. Callers that genuinely need the bytes have to check.
	if len(r.rawTx) > 0 {
		tx, terr := bt.NewTxFromBytes(r.rawTx)
		if terr != nil {
			return nil, errors.NewStorageError("[utxoset] decode body %s", hash.String(), terr)
		}

		data.Tx = tx
	}

	return data, nil
}
