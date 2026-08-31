package model

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/stores/blob/options"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util"
)

// SubtreeMetaRegeneratorI defines the interface for regenerating missing subtree meta files
type SubtreeMetaRegeneratorI interface {
	// RegenerateMeta attempts to rebuild meta from subtreedata (local or from peers).
	// isFirstSubtree says whether this is the block's first subtree, which is the
	// only one whose node 0 may hold the coinbase placeholder; the rebuild has to
	// apply the same rule validateSubtree does, or it produces a meta with a hole
	// the caller then condemns the block for.
	RegenerateMeta(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, isFirstSubtree bool) (*subtreepkg.Meta, error)
}

// SubtreeStoreReader is a subset of blob.Store for reading subtree data
type SubtreeStoreReader interface {
	GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error)
}

// SubtreeStoreWriter extends SubtreeStoreReader with write capability for storing regenerated meta
type SubtreeStoreWriter interface {
	SubtreeStoreReader
	Set(ctx context.Context, key []byte, fileType fileformat.FileType, value []byte, opts ...options.FileOption) error
}

// SubtreeMetaRegenerator handles regenerating missing subtree meta files
type SubtreeMetaRegenerator struct {
	logger               ulogger.Logger
	subtreeStore         SubtreeStoreWriter
	peerURLs             []string
	getBlockHeight       func() uint32
	blockHeightRetention uint32
	peerFetchTimeout     time.Duration
}

// NewSubtreeMetaRegenerator creates a new SubtreeMetaRegenerator instance.
// peerURLs are the announcing peers' DataHub base URLs, which already include
// the peer's API prefix (e.g. http://peer:9090/api/v1) — the same base every
// other subtree_data fetcher appends only the resource path to.
//
// peerFetchTimeout bounds one peer's fetch; a non-positive value falls back to
// DefaultPeerFetchTimeout so the fetch is never left unbounded.
func NewSubtreeMetaRegenerator(logger ulogger.Logger, subtreeStore SubtreeStoreWriter, peerURLs []string,
	getBlockHeight func() uint32, blockHeightRetention uint32, peerFetchTimeout time.Duration) *SubtreeMetaRegenerator {
	if peerFetchTimeout <= 0 {
		peerFetchTimeout = DefaultPeerFetchTimeout
	}

	return &SubtreeMetaRegenerator{
		logger:               logger.New("meta_regenerator"),
		subtreeStore:         subtreeStore,
		peerURLs:             peerURLs,
		getBlockHeight:       getBlockHeight,
		blockHeightRetention: blockHeightRetention,
		peerFetchTimeout:     peerFetchTimeout,
	}
}

// RegenerateMeta attempts to rebuild meta from subtreedata (local store or peers)
// Returns the regenerated meta or an error if regeneration fails
func (r *SubtreeMetaRegenerator) RegenerateMeta(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, isFirstSubtree bool) (*subtreepkg.Meta, error) {
	r.logger.Warnf("[RegenerateMeta][%s] attempting to regenerate subtree meta", subtreeHash.String())

	// A successful read is not a successful rebuild: go-subtree's data
	// deserializer stops at io.EOF without reporting that it filled fewer nodes
	// than the subtree has, so a truncated .subtreeData comes back with a nil
	// error and only fails buildAndStoreMeta's completeness check. Treating that
	// like a read failure — rather than returning it — is what keeps the peer
	// fallback reachable; otherwise a truncated local file strands a block that
	// any peer could repair.
	var lastErr error

	tryBuild := func(source string, data *subtreepkg.Data, readErr error) (*subtreepkg.Meta, bool) {
		if readErr != nil {
			lastErr = readErr

			r.logger.Warnf("[RegenerateMeta][%s] %s subtreedata not available: %v", subtreeHash.String(), source, readErr)

			return nil, false
		}

		meta, buildErr := r.buildAndStoreMeta(ctx, subtreeHash, subtree, data, isFirstSubtree)
		if buildErr != nil {
			lastErr = buildErr

			r.logger.Warnf("[RegenerateMeta][%s] %s subtreedata unusable: %v", subtreeHash.String(), source, buildErr)

			return nil, false
		}

		return meta, true
	}

	data, err := r.getLocalSubtreeData(ctx, subtreeHash, subtree)
	if meta, ok := tryBuild("local", data, err); ok {
		return meta, nil
	}

	for _, peerURL := range r.peerURLs {
		data, err = r.getSubtreeDataFromPeer(ctx, subtreeHash, subtree, peerURL)
		if meta, ok := tryBuild(peerURL, data, err); ok {
			return meta, nil
		}
	}

	return nil, errors.NewProcessingError("[RegenerateMeta][%s] subtreedata not available locally or from peers", subtreeHash.String(), lastErr)
}

// getLocalSubtreeData reads subtree data from local store
func (r *SubtreeMetaRegenerator) getLocalSubtreeData(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree) (*subtreepkg.Data, error) {
	if r.subtreeStore == nil {
		return nil, errors.NewNotFoundError("subtree store not available")
	}

	reader, err := r.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = reader.Close()
	}()

	return subtreepkg.NewSubtreeDataFromReader(subtree, reader)
}

// DefaultPeerFetchTimeout is the fallback bound on one peer's fetch (all 503
// retries plus the body stream) when the caller supplies no timeout. This fetch
// runs inline in Block.Valid on a context with no deadline, where the shared
// client would otherwise allow a hung peer the full http_streaming_timeout
// (10 minutes as shipped in settings.conf) per attempt. Note this is a
// whole-peer budget, not a per-attempt one: the previous bare http.Client gave
// 30s to a single attempt with no retries, so under sustained 503 backoff the
// last attempt here gets considerably less than 30s.
//
// Operators configure this via blockvalidation_subtree_meta_peer_fetch_timeout;
// settings.DefaultSubtreeMetaPeerFetchTimeout carries the same value. The two
// are separate constants only because model must not import settings.
const DefaultPeerFetchTimeout = 30 * time.Second

// getSubtreeDataFromPeer fetches subtree data from a peer via HTTP. The peer's
// base URL already carries its API prefix, so only the resource path is
// appended. Retries on 503 — the peer's asset service may reject under
// admission control while it generates the file on-demand.
func (r *SubtreeMetaRegenerator) getSubtreeDataFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, peerURL string) (*subtreepkg.Data, error) {
	ctx, cancel := context.WithTimeout(ctx, r.peerFetchTimeout)
	defer cancel()

	url := fmt.Sprintf("%s/subtree_data/%s", peerURL, subtreeHash.String())

	body, err := util.DoHTTPRequestBodyReaderWithRetry(ctx, url)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = body.Close()
	}()

	return subtreepkg.NewSubtreeDataFromReader(subtree, body)
}

// buildAndStoreMeta creates meta from subtree data and stores it for future use
func (r *SubtreeMetaRegenerator) buildAndStoreMeta(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, data *subtreepkg.Data, isFirstSubtree bool) (*subtreepkg.Meta, error) {
	meta, err := r.buildMetaFromSubtreeData(subtree, data, isFirstSubtree)
	if err != nil {
		return nil, err
	}

	cached, err := r.storeRegeneratedMeta(ctx, subtreeHash, meta)
	if err != nil {
		return nil, err
	}

	// Two outcomes, two lines. A rebuild that did not reach the store is usable for
	// this validation and is not a repair: whatever was on disk is still there, so
	// the next read of this subtree rebuilds again. Logging both as success is what
	// let an operator grep "successfully regenerated meta" and believe a rebuild
	// loop had been fixed.
	if !cached {
		r.logger.Errorf("[RegenerateMeta][%s] regenerated meta for this validation but did not cache it, the next read rebuilds it again", subtreeHash.String())

		return meta, nil
	}

	r.logger.Warnf("[RegenerateMeta][%s] successfully regenerated meta", subtreeHash.String())

	return meta, nil
}

// buildMetaFromSubtreeData creates meta from subtree data containing all transactions
func (r *SubtreeMetaRegenerator) buildMetaFromSubtreeData(subtree *subtreepkg.Subtree, data *subtreepkg.Data, isFirstSubtree bool) (*subtreepkg.Meta, error) {
	meta := subtreepkg.NewSubtreeMeta(subtree)

	// Same predicate validateSubtree uses to skip node 0 (sIdx == 0 && snIdx == 0 &&
	// node is the placeholder). Exempting node 0 of any subtree that happens to
	// carry a placeholder would leave TxInpoints[0] unset on a later subtree that
	// validateSubtree does not skip, and GetParentTxHashes returning nil there is
	// a BlockInvalidError on a valid block.
	hasCoinbasePlaceholder := isFirstSubtree && subtree.Length() > 0 && subtree.Nodes[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue)

	for i, tx := range data.Txs {
		if tx == nil {
			continue // Skip nil entries (e.g., coinbase placeholder)
		}

		// Skip coinbase placeholder at index 0
		if i == 0 && hasCoinbasePlaceholder {
			continue
		}

		if err := meta.SetTxInpointsFromTx(tx); err != nil {
			return nil, errors.NewProcessingError("[buildMetaFromSubtreeData] failed to set inpoints for tx %s: %v", tx.TxID(), err)
		}
	}

	// The subtree data deserializer stops at EOF without checking it filled every node, so a
	// short or empty body yields trailing nil transactions and a meta with no recorded parents
	// for the tail. That meta is worse than no meta: GetParentTxHashes returns nil with no
	// error, validOrderAndBlessed reads that as "transaction could not be found in tx meta
	// data" and raises ErrBlockInvalid, and ValidateBlock then calls storeInvalidBlock — a
	// valid block permanently invalidated, which is the outcome this PR exists to prevent.
	// Fail regeneration instead so the error stays transient, and so RegenerateMeta moves on
	// to the peers rather than accepting a rebuild it cannot use.
	//
	// Two distinct failures, both of which must reject the rebuild.
	//
	// data.Txs[i] == nil means the deserializer never filled that node — a short
	// .subtreeData. It is the signal the loop above already skips on.
	//
	// A nil ParentTxHashes means go-subtree cannot serialize the meta at all:
	// Meta.Serialize rejects it for every index except 0. newSizedFromInputs
	// leaves it nil for a transaction with no inputs, so such a node yields a meta
	// that builds fine, fails to serialize, and is therefore never written —
	// leaving every later read to rebuild from .subtreeData and, on a local miss,
	// pay the peer fetch again. Returning it as success is the rebuild-forever
	// loop WithAllowOverwrite exists to break, so it is rejected here instead.
	//
	// Node 0 is exempt only where it holds the coinbase placeholder — the same
	// condition validateSubtree uses to skip it, so the two agree rather than one
	// leaving a hole the other then condemns the block for.
	for i := 0; i < subtree.Length(); i++ {
		if i == 0 && hasCoinbasePlaceholder {
			continue
		}

		if data.Txs[i] == nil {
			return nil, errors.NewProcessingError("[buildMetaFromSubtreeData] incomplete subtree data: no transaction for node %d of %d", i, subtree.Length())
		}

		if meta.TxInpoints[i].ParentTxHashes == nil {
			return nil, errors.NewProcessingError("[buildMetaFromSubtreeData] unserializable subtree meta: no parent inpoints for node %d of %d", i, subtree.Length())
		}
	}

	return meta, nil
}

// storeRegeneratedMeta caches the regenerated meta and reports whether it
// reached the store.
//
// A serialization failure is returned rather than logged: a meta that cannot be
// written is one every later read has to rebuild, so reporting success for it
// hides an unbounded repeat of the .subtreeData read and the peer fetch behind
// it. The completeness check in buildMetaFromSubtreeData should already have
// rejected the only input that causes this, so reaching it means that check and
// Meta.Serialize have drifted apart.
//
// A store write failure is not returned, because returning it would send
// RegenerateMeta on to the peers and no peer can fix a local write. It is not
// success either: regeneration now runs for a file that is present but rejected
// as well as for one that is missing, so a failed write leaves the rejected bytes
// exactly where they were and every later read of that subtree rebuilds from
// .subtreeData, paying the peer fetch again on a local miss. The caller gets
// cached false and says so rather than logging a repair that did not happen.
//
// A nil store is the same shape: nothing was cached, so cached is false. It is
// not an error, because there is nowhere to write and the meta is still usable.
func (r *SubtreeMetaRegenerator) storeRegeneratedMeta(ctx context.Context, subtreeHash *chainhash.Hash, meta *subtreepkg.Meta) (bool, error) {
	if r.subtreeStore == nil {
		return false, nil
	}

	metaBytes, err := meta.Serialize()
	if err != nil {
		return false, errors.NewProcessingError("[storeRegeneratedMeta][%s] regenerated meta cannot be serialized, so it could never be cached", subtreeHash.String(), err)
	}

	// Regeneration runs for a corrupt meta file as well as a missing one, so the write has to
	// replace whatever is already there. Without this the torn file stays on disk and the meta
	// is rebuilt from subtree data on every read of that subtree.
	dah := r.getBlockHeight() + r.blockHeightRetention
	if err := r.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeMeta, metaBytes,
		options.WithDeleteAt(dah), options.WithAllowOverwrite(true)); err != nil {
		r.logger.Errorf("[storeRegeneratedMeta][%s] failed to store meta, the rejected file stays on disk and will be rebuilt on every read: %v", subtreeHash.String(), err)

		return false, nil
	}

	return true, nil
}

// SubtreeStoreAdapter adapts a SubtreeStore (read-only) to SubtreeStoreWriter
// Use this when you don't need to store regenerated meta.
//
// Note the interaction with storeRegeneratedMeta's cached return: Set here
// discards the write and reports success, so a regenerator wired to this adapter
// logs "successfully regenerated meta" for a meta that was never written.
// Nothing in production uses it (blockvalidation passes a real writer), and the
// deliberate case for it is a caller that has no meta file to poison. Anyone
// wiring it to a store that does hold meta files wants a real writer instead.
type SubtreeStoreAdapter struct {
	SubtreeStore
}

// Set is a no-op for read-only stores
func (a *SubtreeStoreAdapter) Set(_ context.Context, _ []byte, _ fileformat.FileType, _ []byte, _ ...options.FileOption) error {
	return nil
}

// GetIoReader delegates to the underlying SubtreeStore
func (a *SubtreeStoreAdapter) GetIoReader(ctx context.Context, key []byte, fileType fileformat.FileType, opts ...options.FileOption) (io.ReadCloser, error) {
	return a.SubtreeStore.GetIoReader(ctx, key, fileType, opts...)
}
