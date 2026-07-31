package rewindblockchain

import (
	"context"
	"database/sql"
	"encoding/binary"

	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
)

const (
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
// so BA bootstraps from the correct post-rewind tip. It uses the shared
// blockassembly.EncodeState / StateKey helpers so the on-disk format cannot
// drift from what the BlockAssembler reads back on startup.
func (e *env) resetBlockAssemblerState(ctx context.Context, pf *preflightResult) error {
	header, _, err := e.blockchainStore.GetBlockHeader(ctx, pf.targetHash)
	if err != nil {
		return errors.NewStorageError("failed to read target header", err)
	}

	if err = e.blockchainStore.SetState(ctx, blockassembly.StateKey, blockassembly.EncodeState(header, pf.target)); err != nil {
		return errors.NewStorageError("failed to write state[BlockAssembler]", err)
	}

	e.logger.Infof("state[BlockAssembler] rewritten to height %d (%s)", pf.target, pf.targetHash)
	return nil
}

// readBlockPersisterHeight reads and decodes state["BlockPersisterHeight"].
// known is false when the key is absent, empty, or too short to decode — in
// those cases the caller cannot assume anything about the persister's real
// position. Genuine storage failures are returned as errors.
func (e *env) readBlockPersisterHeight(ctx context.Context) (uint32, bool, error) {
	existing, err := e.blockchainStore.GetState(ctx, blockPersisterHeightKey)
	if err != nil {
		// SQL blockchain store returns sql.ErrNoRows directly for missing keys;
		// future implementations may wrap into errors.ErrNotFound.
		if errors.Is(err, sql.ErrNoRows) || errors.Is(err, errors.ErrNotFound) {
			e.logger.Debugf("state[%s] not present: %v", blockPersisterHeightKey, err)
			return 0, false, nil
		}
		return 0, false, errors.NewStorageError("failed to read state[%s]", blockPersisterHeightKey, err)
	}

	// A present-but-empty row (len 0) is as undecodable as a truncated one, and
	// is the more anomalous of the two — both take the same warning.
	if len(existing) < 4 {
		e.logger.Warnf("state[%s] is %d bytes, too short to decode as LE uint32; leaving it untouched", blockPersisterHeightKey, len(existing))
		return 0, false, nil
	}

	return binary.LittleEndian.Uint32(existing), true, nil
}

// blockPersisterGroundTruth derives the block persister's real position from the
// blocks table — the lowest block with persisted_at IS NULL, minus one — which
// is the same derivation block persister performs on startup
// (services/blockpersister/Server.go:322-334). The state key is only a cache of
// this; the table is the truth.
//
// known is false when there is no pending block above genesis to derive from,
// or when the query fails. Diagnostics only, so a failure is logged and never
// fatal. The query is backed by idx_not_persisted_height
// (stores/blockchain/sql/sql.go:786), so it stays cheap on a full chain.
//
// Genesis is skipped: it carries persisted_at IS NULL for the life of the
// database, so it would otherwise mask every real pending block. Block
// persister skips it the same way, via its blocks[0].Height > 0 guard — hence
// asking for two rows, since an unpersisted genesis always sorts first.
func (e *env) blockPersisterGroundTruth(ctx context.Context) (uint32, bool) {
	blocks, err := e.blockchainStore.GetBlocksNotPersisted(ctx, 2)
	if err != nil {
		e.logger.Debugf("could not derive the real block persister position: %v", err)
		return 0, false
	}

	for _, b := range blocks {
		if b.Height == 0 {
			continue
		}
		return b.Height - 1, true
	}

	return 0, false
}

// resetBlockPersisterHeight moves the persisted height down to the target. It
// never moves it forward: when the block persister is behind the target —
// normal on a recently-resynced node — the value is left alone.
//
// Raising it is not a cosmetic misreport. The pruner reads this key once in
// Init() (services/pruner/server.go:250) and afterwards only updates its cached
// copy from BlockPersisted notifications (:163-176). Block persister's
// corrective republish on startup is a bare SetState with no notification
// (services/blockpersister/Server.go:329), so an inflated value can be latched
// by the pruner and drive irreversible blob deletion for blocks that were never
// persisted. The underlying value is a LE uint32 — see
// services/blockpersister/Server.go.
func (e *env) resetBlockPersisterHeight(ctx context.Context, pf *preflightResult) error {
	existingHeight, known, err := e.readBlockPersisterHeight(ctx)
	if err != nil {
		return err
	}

	// Diagnostics, not a write guard: unknown decodes to 0, which the <= check
	// below would also skip. Returning here keeps the log from claiming the
	// height is 0 when it is actually unreadable.
	if !known {
		return nil
	}

	if existingHeight <= pf.target {
		e.logger.Infof("state[%s]=%d already at/below target %d; leaving it untouched", blockPersisterHeightKey, existingHeight, pf.target)
		return nil
	}

	payload := make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, pf.target)

	if err = e.blockchainStore.SetState(ctx, blockPersisterHeightKey, payload); err != nil {
		return errors.NewStorageError("failed to rewrite state[%s]", blockPersisterHeightKey, err)
	}

	e.logger.Infof("state[%s] rewritten from %d down to %d", blockPersisterHeightKey, existingHeight, pf.target)
	return nil
}

// deleteUTXOPersisterLastProcessed removes the file the utxo persister uses
// to track its position. On next startup it will recompute from block
// data — which is the correct behaviour post-rewind.
//
// NOTE: this Del does not currently match what the persister writes, and never
// has. services/utxopersister/Server.go:772,826 uses the BLOCK store with
// options.WithFilename("lastProcessed") and options.WithNoHashPrefix(), which
// resolves to <blockstore>/lastProcessed.dat. This call uses the SUBTREE store
// with filename "lastProcessed.dat", and ConstructFilename appends "." +
// fileType, so it targets <subtreestore>/lastProcessed.dat.dat — a different
// store and a different name. The Del is best-effort, so the miss is swallowed
// by the Debugf below. Fixing it needs the block store, which resolveStores
// does not open; tracked in #1353 rather than widened into the hashPrefix fix.
//
// WithNoHashPrefix does more than keep this path byte-identical now that the
// store carries the node's hashPrefix (see newSubtreeStore). Without it
// CalculatePrefix would derive "la" from the filename "lastProcessed.dat", and
// ConstructFilename MkdirAlls the prefix folder on every call including Del
// (stores/blob/options/Options.go:397-401) — so each rewind would leave a stray
// empty <subtreestore>/la/ behind while still deleting nothing.
func (e *env) deleteUTXOPersisterLastProcessed(ctx context.Context) error {
	key := []byte(utxoPersisterLastHeightFn)
	if err := e.subtreeStore.Del(ctx, key, fileformat.FileTypeDat,
		options.WithFilename(utxoPersisterLastHeightFn), options.WithNoHashPrefix()); err != nil {
		// Best-effort. Log and continue.
		e.logger.Debugf("delete utxo-persister lastProcessed.dat: %v", err)
	}
	return nil
}
