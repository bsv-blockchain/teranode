package model

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
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

// cacheBustCounter produces the token appended to a peer request URL when a
// first attempt came back with an unusable body, so the retry cannot be served
// from that peer's cache.
//
// Process-wide and clock-seeded, deliberately, because it has to be unique
// across regenerators and not merely within one. blockvalidation builds a fresh
// SubtreeMetaRegenerator for every validation attempt (createMetaRegenerator is
// called per ValidateBlock and per ReValidateBlock), so a per-instance counter
// would restart at zero each time and every retry would request the identical
// "?cachebust=1" URL. nginx caches that URL under its own key like any other, so
// a busted request whose generation also aborted would leave the block wedged
// for the whole upstream TTL, exactly the failure this retry exists to break.
// blockvalidation's sibling counter gets process-lifetime uniqueness by living
// on the long-lived Server; this is the model-package equivalent, with a clock
// seed so a node restart mid-poison does not replay a token either.
var cacheBustCounter = newCacheBustCounter()

func newCacheBustCounter() *atomic.Uint64 {
	c := &atomic.Uint64{}
	c.Store(uint64(time.Now().UnixNano()))

	return c
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
// Returns the regenerated meta or an error if regeneration fails.
//
// Two things happen here beyond walking the sources in order. A peer that
// answered with a body too short for the subtree is asked once more on a
// cache-busting URL, because the likeliest cause is a proxy cache replaying an
// aborted generation. And a local subtree_data file that exists but cannot
// satisfy the subtree is overwritten with the first complete peer body, because
// this node serves that file outward whether or not it can use it itself.
func (r *SubtreeMetaRegenerator) RegenerateMeta(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, isFirstSubtree bool) (*subtreepkg.Meta, error) {
	r.logger.Warnf("[RegenerateMeta][%s] attempting to regenerate subtree meta", subtreeHash.String())

	// A successful read is not a successful rebuild: go-subtree's data
	// deserializer stops at io.EOF without reporting that it filled fewer nodes
	// than the subtree has, so a truncated .subtreeData comes back with a nil
	// error and is rejected only by a completeness check — getLocalSubtreeData's
	// for the local file, buildAndStoreMeta's for a peer body. Treating that like
	// a read failure — rather than returning it — is what keeps the peer fallback
	// reachable; otherwise a truncated local file strands a block that any peer
	// could repair.
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

	// localPoisoned says a subtree_data file is present locally and cannot satisfy
	// this subtree. That is worse than a source we simply cannot use: the asset
	// service serves that same file verbatim to any peer that asks for
	// GET /api/v1/subtree_data/<hash>, checking only that it Exists. Falling
	// through to a peer would leave this node validating happily while still
	// handing the bad body outward, so a peer body that turns out complete is
	// written back over it below.
	data, localPoisoned, err := r.getLocalSubtreeData(ctx, subtreeHash, subtree, isFirstSubtree)
	if meta, ok := tryBuild("local", data, err); ok {
		return meta, nil
	}

	if localPoisoned {
		r.logger.Warnf("[RegenerateMeta][%s] the local subtree_data cannot satisfy this subtree, so this node is serving a bad body to every peer that asks for it; a complete peer body will be written back over it", subtreeHash.String())
	}

	for _, peerURL := range r.peerURLs {
		peerData, answered, peerErr := r.getSubtreeDataFromPeer(ctx, subtreeHash, subtree, peerURL, false)

		meta, ok := tryBuild(peerURL, peerData, peerErr)

		// A peer that answered with a body we could not use gets one more fetch,
		// past its cache and on a budget of its own. Peers front the asset service
		// with an nginx proxy_cache that stores any 200 for its TTL, so an aborted
		// on-demand generation that reached the client as "200 + empty body" is
		// replayed to every byte-identical request (issue 1368). The cache key
		// includes the query string while nginx location matching ignores it, so the
		// busted URL reaches the same handler and misses the cache, which is the only
		// lever available against a fleet we cannot update.
		//
		// Only when the peer answered. A refused connection or an expired budget left
		// no body for the cache to be blamed for, and a second request would just
		// spend the budget again before moving on.
		if !ok && answered {
			peerData, _, peerErr = r.getSubtreeDataFromPeer(ctx, subtreeHash, subtree, peerURL, true)
			meta, ok = tryBuild(peerURL+" (cache-busted)", peerData, peerErr)
		}

		if ok {
			// The stored meta spares our own future validations the regenerator
			// entirely, but the file the asset service serves is subtree_data, not the
			// meta, and nothing on this path rewrites it before its DAH expires.
			//
			// Three other writers exist and none of them is a reason to skip this.
			// blockvalidation's catchup fetchAndStoreSubtreeData would have to fetch
			// this subtree again to get there. blockpersister's
			// streaming_process_subtree deletes an unreadable file and writes a fresh
			// one, but only when the block reaches persistence, and it judges the file
			// by whether it deserializes rather than by whether it filled every node,
			// so a short-but-parseable body survives it. The asset service regenerates
			// a MISSING file on demand and does nothing about a present bad one, which
			// is the whole reason this repair exists.
			if localPoisoned {
				r.repairLocalSubtreeData(ctx, subtreeHash, peerData)
			}

			return meta, nil
		}
	}

	return nil, errors.NewProcessingError("[RegenerateMeta][%s] subtreedata not available locally or from peers", subtreeHash.String(), lastErr)
}

// repairLocalSubtreeData overwrites a poisoned local subtree_data file with a
// peer body already verified to satisfy the subtree.
//
// Best effort throughout, deliberately: the regeneration this is called from has
// already succeeded, and failing it because the repair failed would turn a
// recovered block back into a stalled one. Every failure is a Warn, because a
// node that stays a poisoned source is worth reporting even when its own
// validation is fine.
//
// The DAH matches storeRegeneratedMeta's, so the repaired body expires with the
// meta built from it rather than outliving it.
func (r *SubtreeMetaRegenerator) repairLocalSubtreeData(ctx context.Context, subtreeHash *chainhash.Hash, data *subtreepkg.Data) {
	if r.subtreeStore == nil {
		return
	}

	// Data.Serialize indexes Subtree.Nodes[0] before it validates anything, so a
	// data carrying no subtree, or one with no nodes, panics rather than
	// returning an error. This runs in a validOrderAndBlessed errgroup goroutine
	// that no recover() covers, and neither shape is repairable anyway.
	if data == nil || data.Subtree == nil || len(data.Subtree.Nodes) == 0 {
		return
	}

	serialized, err := data.Serialize()
	if err != nil {
		r.logger.Warnf("[repairLocalSubtreeData][%s] peer body will not serialize, the poisoned local subtree_data stays in place and is still served to peers: %v", subtreeHash.String(), err)

		return
	}

	dah := r.getBlockHeight() + r.blockHeightRetention
	if err := r.subtreeStore.Set(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData, serialized,
		options.WithAllowOverwrite(true), options.WithDeleteAt(dah)); err != nil {
		r.logger.Warnf("[repairLocalSubtreeData][%s] failed to overwrite the poisoned local subtree_data, it is still served to peers: %v", subtreeHash.String(), err)

		return
	}

	r.logger.Warnf("[repairLocalSubtreeData][%s] replaced the poisoned local subtree_data with the complete peer body", subtreeHash.String())
}

// getLocalSubtreeData reads subtree data from local store.
//
// The second return value reports that a subtree_data file exists in the store
// and cannot satisfy the subtree, which is a strictly worse condition than the
// file being absent and is why it is reported separately from the error. The
// asset service's GetSubtreeDataReader checks only Exists before streaming the
// file back on GET /api/v1/subtree_data/<hash>, with no validation of the body,
// so while such a file sits on disk this node is a poisoned source for every
// peer that asks. A missing file has no such consequence: the asset service
// regenerates it on demand from the subtree instead.
//
// Both unusable shapes count. A body that stops at a clean io.EOF short of the
// subtree's length deserializes "successfully" and is caught by
// MissingSubtreeDataTxs; a body truncated mid-transaction fails inside
// NewSubtreeDataFromReader instead. The file is on disk either way, so it is
// served outward either way.
func (r *SubtreeMetaRegenerator) getLocalSubtreeData(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, isFirstSubtree bool) (*subtreepkg.Data, bool, error) {
	if r.subtreeStore == nil {
		return nil, false, errors.NewNotFoundError("subtree store not available")
	}

	reader, err := r.subtreeStore.GetIoReader(ctx, subtreeHash[:], fileformat.FileTypeSubtreeData)
	if err != nil {
		// No file, or none we can open. Nothing is being served outward.
		return nil, false, err
	}
	defer func() {
		_ = reader.Close()
	}()

	data, err := subtreepkg.NewSubtreeDataFromReader(subtree, reader)
	if err != nil {
		return nil, true, err
	}

	if missing := MissingSubtreeDataTxs(subtree, data, isFirstSubtree); missing > 0 {
		return nil, true, errors.NewProcessingError("[RegenerateMeta][%s] local subtree_data is incomplete (%d of %d txs missing)", subtreeHash.String(), missing, subtree.Length())
	}

	return data, false, nil
}

// DefaultPeerFetchTimeout is the fallback bound on one fetch (all 503 retries
// plus the body stream) when the caller supplies no timeout. This fetch runs
// inline in Block.Valid on a context with no deadline, where the shared client
// would otherwise allow a hung peer the full http_streaming_timeout per attempt,
// retries multiplied by that window.
//
// It bounds one fetch, covering all of that fetch's 503 retries and its body
// stream rather than any single attempt: under sustained 503 backoff the later
// attempts get progressively less of it. It is NOT a whole-peer bound, because
// RegenerateMeta can make a second, cache-busting fetch to the same peer, and
// each fetch starts this budget afresh. Worst case per peer is therefore twice
// this value, and worst case for one RegenerateMeta call is that again per
// configured peer, which matters because the call runs in a validOrderAndBlessed
// errgroup goroutine that holds a pooled parent-spends map for its whole
// duration.
//
// Sized to match settings.DefaultSubtreeDataFetchTimeout, which bounds the same
// subtree_data payload fetched from the same peer endpoint by
// check_block_subtrees.go. The budget has to cover streaming the body, so it is
// set by how long the payload takes rather than by how long a validation ought
// to take: a mainnet-size subtree_data cannot be streamed in seconds, and a
// budget too small to finish makes the fetch fail every time on exactly the
// blocks that need it most.
//
// Operators configure this via blockvalidation_subtree_meta_peer_fetch_timeout;
// settings.DefaultSubtreeMetaPeerFetchTimeout carries the same value, held to it
// by TestSubtreeMetaPeerFetchTimeout_ConstantsDoNotDrift. The two are separate
// constants only because model must not import settings.
const DefaultPeerFetchTimeout = 10 * time.Minute

// getSubtreeDataFromPeer fetches subtree data from a peer via HTTP. The peer's
// base URL already carries its API prefix, so only the resource path is
// appended. Retries on 503 — the peer's asset service may reject under
// admission control while it generates the file on-demand.
//
// With bypassCache set the URL carries a cache-busting query parameter, so a
// proxy cache in front of the peer cannot replay the answer the first attempt
// already rejected.
//
// The second return value says the peer answered with a body, whether or not
// that body deserialized. It is what tells the caller a cache-busting retry is
// worth making: a body we could not use may be a poisoned cache entry, while a
// refused connection or an expired budget produced no body for a cache to be
// blamed for.
func (r *SubtreeMetaRegenerator) getSubtreeDataFromPeer(ctx context.Context, subtreeHash *chainhash.Hash, subtree *subtreepkg.Subtree, peerURL string, bypassCache bool) (*subtreepkg.Data, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, r.peerFetchTimeout)
	defer cancel()

	url := fmt.Sprintf("%s/subtree_data/%s", peerURL, subtreeHash.String())
	if bypassCache {
		url = fmt.Sprintf("%s?cachebust=%d", url, cacheBustCounter.Add(1))
	}

	body, err := util.DoHTTPRequestBodyReaderWithRetry(ctx, url)
	if err != nil {
		return nil, false, err
	}
	defer func() {
		_ = body.Close()
	}()

	data, err := subtreepkg.NewSubtreeDataFromReader(subtree, body)
	if err != nil {
		return nil, true, err
	}

	return data, true, nil
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
	//
	// One read of the slice header, with both the guard and the index derived from
	// it, for the reason MissingSubtreeDataTxs spells out: Length() takes the
	// subtree's RWMutex and the direct index does not, while ReleaseNodes sets
	// Nodes to nil without taking that mutex, so a release landing between the two
	// reads turns the index into an out-of-range panic in an errgroup goroutine
	// nothing recovers. Snapshotting removes the panic; it does not make the read
	// synchronised, and cannot from here.
	nodesSlice := subtree.Nodes
	nodes := len(nodesSlice)
	hasCoinbasePlaceholder := isFirstSubtree && nodes > 0 && nodesSlice[0].Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue)

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
	for i := 0; i < nodes; i++ {
		if i == 0 && hasCoinbasePlaceholder {
			continue
		}

		if i >= len(data.Txs) || data.Txs[i] == nil {
			return nil, errors.NewProcessingError("[buildMetaFromSubtreeData] incomplete subtree data: no transaction for node %d of %d", i, nodes)
		}

		if meta.TxInpoints[i].ParentTxHashes == nil {
			return nil, errors.NewProcessingError("[buildMetaFromSubtreeData] unserializable subtree meta: no parent inpoints for node %d of %d", i, nodes)
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
