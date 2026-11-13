package txmetacache

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// cacheBackend is the storage primitive used by TxMetaCache. Two
// implementations satisfy it:
//
//   - ImprovedCache: off-heap byte ring buffer. Set/SetFromBytes are native;
//     Get allocates a fresh *meta.Data and deserialises into it.
//   - PointerCache: sharded map of *meta.Data. Set stores the pointer
//     directly; Get returns the stored pointer.
//
// Pointer aliasing: the *meta.Data returned by Get is shared in pointer mode
// (any concurrent reader sees the same allocation) and freshly allocated in
// byte mode. Callers MUST treat it as read-only — the byte mode does not
// promise allocation per call forever; the cleanest contract is uniform
// read-only sharing.
type cacheBackend interface {
	// SetFromBytes inserts an already-serialised value. ImprovedCache stores
	// the bytes directly; PointerCache deserialises once.
	SetFromBytes(key, value []byte) error

	// SetMultiFromBytes is the batch form. ImprovedCache fans out across
	// shards in parallel; PointerCache loops serially.
	SetMultiFromBytes(keys, values [][]byte) error

	// SetMultiSequential is the partition-aware variant: like
	// SetMultiFromBytes but without goroutine fan-out. The caller provides
	// parallelism (typically one goroutine per Kafka partition, aligned with
	// disjoint bucket ranges). PointerCache shares its implementation with
	// SetMultiFromBytes — it has no per-bucket scratch fan-out to skip.
	SetMultiSequential(keys, values [][]byte) error

	// SetMultiSequentialWithHashes is SetMultiSequential with caller-supplied
	// xxhash values for each key, letting the v2 txmeta receiver avoid
	// recomputing the hash when the wire format already carries it.
	// hashes[i] MUST equal xxhash.Sum64(keys[i]).
	SetMultiSequentialWithHashes(keys, values [][]byte, hashes []uint64) error

	// Set inserts a *meta.Data. ImprovedCache serialises via MetaBytes;
	// PointerCache stores the pointer directly.
	Set(hash *chainhash.Hash, data *meta.Data) error

	// Get returns the cached entry as a pointer. Returns (nil, false) on miss.
	// In pointer mode the pointer is shared with every other reader and the
	// cache itself; in byte mode it is freshly allocated. Either way it must
	// be treated as read-only.
	Get(hash chainhash.Hash) (*meta.Data, bool)

	// GetMulti is the batch form of Get: out[i] is the entry for hashes[i], or
	// nil on a miss. keyHashes, if non-nil, holds KeyHash(&hashes[i]). Same
	// read-only contract as Get.
	GetMulti(hashes []chainhash.Hash, keyHashes []uint64, out []*meta.Data)

	// GetRawMulti appends the serialised entry of every found hash to *dst:
	// entry i is (*dst)[spans[i][0]:spans[i][1]], or spans[i] = {-1, -1} on a
	// miss. Nothing is decoded or allocated per entry; GetMetaCachedInto
	// decodes the result into caller-owned storage.
	// keyHashes, if non-nil, holds KeyHash(&hashes[i]) so a backend that
	// places entries by it does not hash them again.
	GetRawMulti(hashes []chainhash.Hash, keyHashes []uint64, dst *[]byte, spans [][2]int)

	// Del removes the entry under key (raw bytes, typically hash[:]).
	Del(key []byte)

	// UpdateStats populates s with backend statistics.
	UpdateStats(s *Stats)
}
