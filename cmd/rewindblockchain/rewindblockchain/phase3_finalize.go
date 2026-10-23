package rewindblockchain

import (
	"context"
	"database/sql"
	"encoding/binary"
	"strconv"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly"
	"github.com/bsv-blockchain/teranode/services/utxopersister"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
)

const (
	blockPersisterHeightKey = "BlockPersisterHeight"
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

	if err := e.deleteUTXOPersisterLastProcessed(ctx, pf); err != nil {
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

// utxoMarkerFileOptions addresses the UTXO persister's lastProcessed marker
// exactly as the persister does (services/utxopersister/Server.go): the BLOCK
// store, a nil key, filename utxopersister.LastProcessedFilename and no hash
// prefix, which resolves to <blockstore>/lastProcessed.dat.
func utxoMarkerFileOptions() []options.FileOption {
	return []options.FileOption{options.WithFilename(utxopersister.LastProcessedFilename), options.WithNoHashPrefix()}
}

// readUTXOPersisterMarker reads the persister's lastProcessed marker from the
// block store. known is true when the marker is present and decodes as the
// decimal height the persister writes; parseErr is true when it is present but
// undecodable (which bricks the persister's next start until removed). A missing
// marker, an absent block store, or a read failure all return (0,false,false):
// nothing actionable. Read in preflight so the operator learns the plan before
// the destructive phases run, not after.
func (e *env) readUTXOPersisterMarker(ctx context.Context) (height uint64, known, parseErr bool) {
	if e.blockStore == nil {
		return 0, false, false
	}

	b, err := e.blockStore.Get(ctx, nil, fileformat.FileTypeDat, utxoMarkerFileOptions()...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			return 0, false, false
		}
		e.logger.Warnf("could not read the utxo-persister lastProcessed marker: %v", err)
		return 0, false, false
	}

	h, perr := strconv.ParseUint(strings.TrimSpace(string(b)), 10, 32)
	if perr != nil {
		return 0, false, true
	}
	return h, true, false
}

// deleteUTXOPersisterLastProcessed removes the UTXO persister's lastProcessed
// marker when it sits above the rewind target (or is unparseable) AND the
// operator opted in with --reset-utxo-persister. The persister trails the tip
// and refuses to move its marker backwards (services/utxopersister/Server.go),
// so a marker above the rewound tip stalls it on next start; deleting it makes
// the persister recompute from block data — a rebuild from genesis.
//
// That rebuild is safe ONLY when the node has full per-block delta history back
// to genesis. A UTXO-seeded node has no deltas below its seed height, so the
// rebuild stalls at ErrNotFound (processNextBlock treats it as "wait") and the
// persister never advances. The tool cannot distinguish the two cases — no seed
// height is recorded — so deleting is gated behind Options.ResetUTXOPersister
// (the operator asserting full history) rather than done by default. A marker at
// or below the target still matches the blocks the rewind kept and is always
// left alone. preflight has already read and reported the marker; this acts on
// pf for the resolved old-marker-block hash, re-reading the marker itself so the
// decision is made against the live file. Best-effort throughout: a rewind whose
// destructive phases already ran must not fail on a marker-cleanup hiccup.
func (e *env) deleteUTXOPersisterLastProcessed(ctx context.Context, pf *preflightResult) error {
	if e.blockStore == nil {
		e.logger.Debugf("no block store supplied; not touching the utxo-persister lastProcessed marker")
		return nil
	}

	marker := utxoMarkerFileOptions()

	b, err := e.blockStore.Get(ctx, nil, fileformat.FileTypeDat, marker...)
	if err != nil {
		if errors.Is(err, errors.ErrNotFound) {
			// A missing marker is the normal case, but an unmounted or
			// mis-pathed block store looks identical (the file backend's
			// MkdirAll just makes an empty dir), so say so at Info rather than
			// hiding it at Debug: if the operator expected a marker here, the
			// block store path is the thing to check.
			e.logger.Infof("no utxo-persister lastProcessed marker at <blockstore>/%s.dat; nothing to reset (if you expected one, verify the block store path is mounted and correct)", utxopersister.LastProcessedFilename)
			return nil
		}

		e.logger.Warnf("could not read the utxo-persister lastProcessed marker; if it exists and is above target %d, re-run with --reset-utxo-persister or delete <blockstore>/lastProcessed.dat by hand so the persister recomputes: %v", pf.target, err)
		return nil
	}

	raw := strings.TrimSpace(string(b))
	existing, parseErr := strconv.ParseUint(raw, 10, 32)

	// A present-but-valid marker at/below the target is still correct for the
	// kept chain; never touch it.
	if parseErr == nil && existing <= uint64(pf.target) {
		e.logger.Infof("utxo-persister lastProcessed marker is %d, at/below target %d; leaving it untouched", existing, pf.target)
		return nil
	}

	// From here the marker is above the target or unparseable — the stale case
	// this tool cleans up. An unparseable marker also bricks the persister's
	// next start (readLastHeight returns the ParseUint error), so resetting it
	// has the same safe outcome as the above-target case.
	if !e.opts.ResetUTXOPersister {
		if parseErr != nil {
			e.logger.Warnf("utxo-persister lastProcessed marker %q is unparseable and will block the persister's next start; re-run with --reset-utxo-persister to delete it (safe only with full delta history to genesis), or fix <blockstore>/lastProcessed.dat by hand", raw)
		} else {
			e.logger.Warnf("utxo-persister lastProcessed marker is %d, above target %d; LEAVING it in place. The persister will stall on a marker above the rewound chain. Deleting it forces a rebuild from genesis, safe ONLY if this node has full per-block delta history to genesis (a seeded node has none below its seed height and will stall). If you have full history, re-run with --reset-utxo-persister; otherwise reset the persister by hand.", existing, pf.target)
		}
		return nil
	}

	if err = e.blockStore.Del(ctx, nil, fileformat.FileTypeDat, marker...); err != nil {
		e.logger.Warnf("could not delete the utxo-persister lastProcessed marker; delete <blockstore>/lastProcessed.dat by hand so the persister rebuilds from genesis: %v", err)
		return nil
	}

	if parseErr != nil {
		e.logger.Infof("deleted the unparseable utxo-persister lastProcessed marker %q; the persister will rebuild from genesis on its next start", raw)
	} else {
		e.logger.Infof("deleted the utxo-persister lastProcessed marker (was %d, above target %d); the persister will rebuild from genesis on its next start", existing, pf.target)
	}

	// Clean up the stale utxo-set files for the old marker block. The persister
	// only deletes the PREVIOUS block's set as it advances and never genesis's
	// (Server.go), so after a reset the set/headers/set-hash for the rewound-away
	// marker block leak forever — a full UTXO set is many GB on mainnet. Its hash
	// was resolved in preflight while the block was still on the main chain.
	if pf.utxoMarkerHash != nil {
		e.cleanupStaleUTXOSet(ctx, pf.utxoMarkerHash)
	} else if parseErr == nil {
		e.logger.Warnf("could not resolve the hash of the old marker block (height %d) to clean up its utxo-set files; if <blockstore>/<hash>.utxo-set exists for that block, remove it by hand", existing)
	}

	return nil
}

// cleanupStaleUTXOSet best-effort deletes the utxo-set, utxo-set-hash and
// utxo-headers files keyed off the given block hash. A missing file is fine
// (the set may legitimately not exist); any other failure is logged, not fatal.
func (e *env) cleanupStaleUTXOSet(ctx context.Context, hash *chainhash.Hash) {
	for _, ft := range []fileformat.FileType{fileformat.FileTypeUtxoSet, fileformat.FileTypeUtxoSetHash, fileformat.FileTypeUtxoHeaders} {
		if err := e.blockStore.Del(ctx, hash[:], ft); err != nil {
			if !errors.Is(err, errors.ErrNotFound) {
				e.logger.Warnf("could not delete stale %s for old marker block %s: %v", ft, hash.String(), err)
			}
			continue
		}
		e.logger.Infof("deleted stale %s for old marker block %s", ft, hash.String())
	}
}
