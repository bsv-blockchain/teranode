package replayrecovery

import (
	"bytes"
	"context"
	"encoding/hex"

	"github.com/bsv-blockchain/go-bt/v2"
)

// TransactionReader supplies retained raw bytes parsed as a transaction. The
// adapter rehashes every result; archive availability never proves confirmation.
type TransactionReader func(context.Context, string) (*bt.Tx, error)

// EvidenceBackend adds read-only raw transaction access for external records.
// Mutation primitives and their schema/generation checks remain on Backend.
type EvidenceBackend struct {
	Backend
	source   Source
	tip      Tip
	retained TransactionReader
}

// NewEvidenceBackend pins fallback evidence to the current operation's agreed
// tip. Construct a new adapter when verify/resume deliberately uses a later tip.
func NewEvidenceBackend(backend Backend, source Source, tip Tip, retained TransactionReader) *EvidenceBackend {
	return &EvidenceBackend{Backend: backend, source: source, tip: tip, retained: retained}
}

func (b *EvidenceBackend) Transaction(ctx context.Context, id string) (*bt.Tx, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if _, err := strictHash(id); err != nil {
		return nil, err
	}
	if native, ok := b.Backend.(interface {
		Transaction(context.Context, string) (*bt.Tx, error)
	}); ok {
		tx, err := native.Transaction(ctx, id)
		if err == nil && tx != nil {
			return checkedTransaction(id, hex.EncodeToString(tx.Bytes()))
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	var evidence Evidence
	var err error
	if b.source != nil {
		evidence, err = b.source.Check(ctx, id, b.tip)
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err == nil && evidence.RawTx != "" && validHash(evidence.BlockHash) {
		if evidence.TxID != id || evidence.Tip != b.tip || !validHash(evidence.BlockHash) || evidence.BlockHeight > b.tip.Height {
			return nil, failure("external transaction confirmation identity mismatch")
		}
		return checkedTransaction(id, evidence.RawTx)
	}
	if b.retained == nil {
		return nil, failure("external transaction raw bytes unavailable: %w", ErrIncomplete)
	}
	tx, err := b.retained(ctx, id)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, failure("retained transaction absent")
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return checkedTransaction(id, hex.EncodeToString(tx.Bytes()))
}

func (b *EvidenceBackend) Inputs(ctx context.Context, id string) ([]string, error) {
	tx, err := b.Transaction(ctx, id)
	if err != nil {
		return nil, err
	}
	inputs := make([]string, 0, len(tx.Inputs))
	for _, input := range tx.Inputs {
		if input == nil || input.PreviousTxIDChainHash() == nil {
			return nil, failure("transaction input identity unavailable")
		}
		inputs = append(inputs, input.PreviousTxIDChainHash().String())
	}
	return inputs, nil
}

// VerifyParent preserves the native post-restart spend-owner verification hook.
func (b *EvidenceBackend) VerifyParent(ctx context.Context, parent Parent) error {
	native, ok := b.Backend.(interface {
		VerifyParent(context.Context, Parent) error
	})
	if !ok {
		return failure("backend cannot verify protected parent spends")
	}
	return native.VerifyParent(ctx, parent)
}

func (b *EvidenceBackend) Inventory(ctx context.Context, visit func(InventoryRecord) error) error {
	census, ok := b.Backend.(CensusBackend)
	if !ok {
		return failure("backend lacks full census")
	}
	return census.Inventory(ctx, visit)
}
func (b *EvidenceBackend) SpendReferences(ctx context.Context, visit func(SpendReference) error) error {
	census, ok := b.Backend.(CensusBackend)
	if !ok {
		return failure("backend lacks spend census")
	}
	return census.SpendReferences(ctx, visit)
}
func (b *EvidenceBackend) SnapshotAbsent(ctx context.Context, tx *bt.Tx) (Snapshot, error) {
	native, ok := b.Backend.(AbsentBackend)
	if !ok {
		return Snapshot{}, failure("backend cannot snapshot absent child")
	}
	return native.SnapshotAbsent(ctx, tx)
}

func checkedTransaction(id, raw string) (*bt.Tx, error) {
	b, err := hex.DecodeString(raw)
	if err != nil {
		return nil, failure("invalid raw transaction hex")
	}
	tx, err := ReadBoundedTransaction(bytes.NewReader(b))
	if err != nil || tx == nil || tx.TxID() != id || !bytes.Equal(tx.Bytes(), b) || len(tx.Outputs) == 0 {
		return nil, failure("raw transaction identity or encoding mismatch")
	}
	return tx, nil
}
