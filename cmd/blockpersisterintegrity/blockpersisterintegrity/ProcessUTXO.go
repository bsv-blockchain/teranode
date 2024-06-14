package blockpersisterintegrity

import (
	"bytes"
	"context"
	"fmt"

	"github.com/bitcoin-sv/ubsv/errors"
	"github.com/bitcoin-sv/ubsv/model"
	p_model "github.com/bitcoin-sv/ubsv/services/blockpersister/utxoset/model"
	"github.com/bitcoin-sv/ubsv/stores/blob"
	"github.com/bitcoin-sv/ubsv/stores/blob/options"
	"github.com/bitcoin-sv/ubsv/ulogger"
	"github.com/libsv/go-bt/v2/chainhash"
)

type UTXOProcessor struct {
	logger ulogger.Logger
	store  blob.Store
}

func NewUTXOProcessor(logger ulogger.Logger, store blob.Store) *UTXOProcessor {
	return &UTXOProcessor{
		logger: logger,
		store:  store,
	}
}

func (p *UTXOProcessor) DiffExists(blockHash chainhash.Hash) (bool, error) {
	return p.store.Exists(context.Background(), blockHash[:], options.WithFileExtension("utxodiff"))
}

func (p *UTXOProcessor) VerifyDiff(blockHeader *model.BlockHeader, diff *p_model.UTXODiff) error {
	var utxoDiff *p_model.UTXODiff
	r, err := p.store.GetIoReader(context.Background(), blockHeader.Hash()[:], options.WithFileExtension("utxodiff"))
	if err != nil {
		return errors.New(errors.ERR_PROCESSING, "error getting reader from store", err)
	}

	utxoDiff, err = p_model.NewUTXODiffFromReader(p.logger, r)
	if err != nil {
		p.logger.Errorf("failed to parse utxodiff for block %s: %s", blockHeader.Hash(), err)
	}

	if utxoDiff == nil {
		return fmt.Errorf("utxodiff for block %s is nil", blockHeader.Hash())
	}

	if !utxoDiff.BlockHash.IsEqual(blockHeader.Hash()) {
		return fmt.Errorf("utxodiff block hash %s does not match block header hash %s", utxoDiff.BlockHash, blockHeader.Hash())
	}

	if ok, difference := utxoDiff.Added.IsEqual(diff.Added, ValueCompareFn); !ok {
		return fmt.Errorf("utxodiff added set does not match block diff added set: %s", difference)
	}

	if ok, difference := utxoDiff.Removed.IsEqual(diff.Removed, ValueCompareFn); !ok {
		return fmt.Errorf("utxodiff removed set does not match block diff removed set: %s", difference)
	}

	return nil
}

func (p *UTXOProcessor) SetExists(blockHash chainhash.Hash) (bool, error) {
	return p.store.Exists(context.Background(), blockHash[:], options.WithFileExtension("utxoset"))
}

func (p *UTXOProcessor) VerifySet(blockHeader *model.BlockHeader, diff *p_model.UTXODiff) error {
	utxoSet1, err := loadSet(p, *blockHeader.HashPrevBlock)
	if err != nil {
		return err
	}

	applyDiffToSet(utxoSet1, diff)

	utxoSet2, err := loadSet(p, *blockHeader.Hash())
	if err != nil {
		return err
	}

	if ok, difference := utxoSet1.Current.IsEqual(utxoSet2.Current, ValueCompareFn); !ok {
		return fmt.Errorf("utxoset added set does not match block diff added set: %s", difference)
	}

	return nil
}

func loadSet(p *UTXOProcessor, blockHash chainhash.Hash) (*p_model.UTXOSet, error) {
	var utxoSet *p_model.UTXOSet
	r, err := p.store.GetIoReader(context.Background(), blockHash[:], options.WithFileExtension("utxoset"))
	if err != nil {
		return nil, errors.New(errors.ERR_PROCESSING, "error getting reader from store", err)
	}

	utxoSet, err = p_model.NewUTXOSetFromReader(p.logger, r)
	if err != nil {
		p.logger.Errorf("failed to parse utxoset for block %s: %s", blockHash, err)
	}

	if utxoSet == nil {
		return nil, fmt.Errorf("utxoset for block %s is nil", blockHash)
	}

	if !utxoSet.BlockHash.IsEqual(&blockHash) {
		return nil, fmt.Errorf("utxoset block hash %s does not match block header hash %s", utxoSet.BlockHash, blockHash)
	}
	return utxoSet, nil
}

func applyDiffToSet(utxoSet *p_model.UTXOSet, diff *p_model.UTXODiff) {
	diff.Removed.Iter(func(uk p_model.UTXOKey, uv *p_model.UTXOValue) (stop bool) {
		utxoSet.Delete(uk)
		return
	})

	diff.Added.Iter(func(uk p_model.UTXOKey, uv *p_model.UTXOValue) (stop bool) {
		utxoSet.Add(uk, uv)
		return
	})
}

func ValueCompareFn(a *p_model.UTXOValue, b *p_model.UTXOValue) bool {
	if a == nil && b == nil {
		return true
	}

	if a == nil || b == nil {
		return false
	}

	if a.Value != b.Value {
		return false
	}

	if !bytes.Equal(a.Script, b.Script) {
		return false
	}

	if a.Locktime != b.Locktime {
		return false
	}

	return true
}
