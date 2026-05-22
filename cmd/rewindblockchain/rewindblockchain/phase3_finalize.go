package rewindblockchain

import (
	"context"
	"database/sql"
	"encoding/binary"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
)

const (
	blockAssemblerStateKey    = "BlockAssembler"
	blockPersisterHeightKey   = "BlockPersisterHeight"
	utxoPersisterLastHeightFn = "lastProcessed.dat"
)

// phase3Finalize rewrites persisted state keys and triggers cache /
// on_main_chain rebuild so the next node startup sees a consistent view.
func (e *env) phase3Finalize(ctx context.Context, pf *preflightResult) error {
	if err := e.resetBlockAssemblerState(ctx, pf); err != nil {
		return err
	}

	if err := e.resetBlockPersisterHeight(ctx, pf); err != nil {
		return err
	}

	if err := e.deleteUTXOPersisterLastProcessed(ctx); err != nil {
		return err
	}

	return nil
}

// resetBlockAssemblerState writes state["BlockAssembler"] = {target, targetHeader}
// so BA bootstraps from the correct post-rewind tip.
//
// Format matches services/blockassembly/BlockAssembler.go:904-918:
//
//	[0:4]   LE uint32 height
//	[4:]    block header bytes
func (e *env) resetBlockAssemblerState(ctx context.Context, pf *preflightResult) error {
	header, _, err := e.blockchainStore.GetBlockHeader(ctx, pf.targetHash)
	if err != nil {
		return errors.NewStorageError("failed to read target header: %w", err)
	}

	headerBytes := header.Bytes()
	payload := make([]byte, 4, 4+len(headerBytes))
	binary.LittleEndian.PutUint32(payload, pf.target)
	payload = append(payload, headerBytes...)

	if err = e.blockchainStore.SetState(ctx, blockAssemblerStateKey, payload); err != nil {
		return errors.NewStorageError("failed to write state[BlockAssembler]: %w", err)
	}

	e.logger.Infof("state[BlockAssembler] rewritten to height %d (%s)", pf.target, pf.targetHash)
	return nil
}

// resetBlockPersisterHeight writes the persisted height down to the target.
// The underlying value is a LE uint32 — see services/blockpersister/Server.go.
func (e *env) resetBlockPersisterHeight(ctx context.Context, pf *preflightResult) error {
	existing, err := e.blockchainStore.GetState(ctx, blockPersisterHeightKey)
	if err != nil {
		// SQL blockchain store returns sql.ErrNoRows directly for missing keys;
		// future implementations may wrap into errors.ErrNotFound.
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, errors.ErrNotFound) {
			e.logger.Debugf("state[%s] not present: %v", blockPersisterHeightKey, err)
			return nil
		}
		return errors.NewStorageError("failed to read state[%s]: %w", blockPersisterHeightKey, err)
	}
	if len(existing) == 0 {
		return nil
	}

	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, pf.target)

	if err = e.blockchainStore.SetState(ctx, blockPersisterHeightKey, payload); err != nil {
		return errors.NewStorageError("failed to rewrite state[%s]: %w", blockPersisterHeightKey, err)
	}

	e.logger.Infof("state[%s] rewritten to %d", blockPersisterHeightKey, pf.target)
	return nil
}

// deleteUTXOPersisterLastProcessed removes the file the utxo persister uses
// to track its position. On next startup it will recompute from block
// data — which is the correct behaviour post-rewind.
func (e *env) deleteUTXOPersisterLastProcessed(ctx context.Context) error {
	key := []byte(utxoPersisterLastHeightFn)
	if err := e.subtreeStore.Del(ctx, key, fileformat.FileTypeDat, options.WithFilename(utxoPersisterLastHeightFn)); err != nil {
		// Best-effort. Log and continue.
		e.logger.Debugf("delete utxo-persister lastProcessed.dat: %v", err)
	}
	return nil
}
