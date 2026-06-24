package blockvalidation

import (
	"context"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob"
)

// findLocalSubtreeFile reports whether the subtree store already has a copy of
// the given subtree, and under which file type. It checks FileTypeSubtreeToCheck
// first (the "downloaded from peer, pending validation" marker used on the
// normal p2p path) and then falls back to FileTypeSubtree (the "already
// validated" marker used by block assembly, block persister, and quick
// validation's writer). Either file carries the same tx-hash list, so callers
// can proceed either way.
//
// Mirrors services/subtreevalidation.findLocalSubtreeFile — kept package-local
// here to avoid a cross-service import. See PR #863 for motivation: without the
// fallback, callers needlessly re-fetch subtrees they already have, and on the
// legacy catch-up path the synthetic baseURL="legacy" has no scheme so the
// HTTP fetch fails outright.
func findLocalSubtreeFile(ctx context.Context, store blob.Store, subtreeHash chainhash.Hash) (fileformat.FileType, bool, error) {
	exists, err := store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtreeToCheck)
	if err != nil {
		return fileformat.FileTypeUnknown, false, err
	}
	if exists {
		return fileformat.FileTypeSubtreeToCheck, true, nil
	}
	exists, err = store.Exists(ctx, subtreeHash[:], fileformat.FileTypeSubtree)
	if err != nil {
		return fileformat.FileTypeUnknown, false, err
	}
	if exists {
		return fileformat.FileTypeSubtree, true, nil
	}
	return fileformat.FileTypeUnknown, false, nil
}
