package txmetacache

import (
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/stores/utxo/meta"
)

// lockFreeBackend adapts *LaneCache to cacheBackend.
type lockFreeBackend struct {
	cache *LaneCache
}

var _ cacheBackend = (*lockFreeBackend)(nil)

func (b *lockFreeBackend) SetFromBytes(key, value []byte) error {
	return b.cache.Set(key, value)
}

// setMulti writes every entry, carrying on past a rejected one (as the other
// backends do) and returning the first error.
func (b *lockFreeBackend) setMulti(keys, values [][]byte) error {
	return b.cache.SetMulti(keys, values)
}

// SetMultiFromBytes writes on the calling goroutine: there are no locks to
// spread across goroutines, so a fan-out would only add scheduling cost.
func (b *lockFreeBackend) SetMultiFromBytes(keys, values [][]byte) error {
	return b.setMulti(keys, values)
}

func (b *lockFreeBackend) SetMultiSequential(keys, values [][]byte) error {
	return b.setMulti(keys, values)
}

// SetMultiSequentialWithHashes ignores the xxhash values: the lock-free cache
// places entries by the transaction hash's own bytes.
func (b *lockFreeBackend) SetMultiSequentialWithHashes(keys, values [][]byte, _ []uint64) error {
	return b.setMulti(keys, values)
}

func (b *lockFreeBackend) Set(hash *chainhash.Hash, data *meta.Data) error {
	bufPtr := metaBytesBufPool.Get().(*[]byte)
	defer putMetaBytesBuf(bufPtr)

	bts, err := data.MetaBytesInto((*bufPtr)[:0])
	if err != nil {
		return err
	}

	*bufPtr = bts

	return b.cache.Set(hash[:], bts)
}

// Get allocates a *meta.Data per hit, as the byte backend does; the batch
// read path (GetRawMulti + GetMetaCachedInto) does not.
func (b *lockFreeBackend) Get(hash chainhash.Hash) (*meta.Data, bool) {
	scratchPtr := improvedCacheGetScratchPool.Get().(*[]byte)
	scratch := (*scratchPtr)[:0]

	defer func() {
		*scratchPtr = scratch[:0]
		improvedCacheGetScratchPool.Put(scratchPtr)
	}()

	if !b.cache.Get(&scratch, hash[:]) {
		return nil, false
	}

	data := &meta.Data{}
	if err := meta.NewMetaDataFromBytes(scratch, data); err != nil {
		b.cache.Del(hash[:])
		return nil, false
	}

	return data, true
}

func (b *lockFreeBackend) GetMulti(hashes []chainhash.Hash, _ []uint64, out []*meta.Data) {
	for i := range hashes {
		out[i], _ = b.Get(hashes[i])
	}
}

func (b *lockFreeBackend) GetRawMulti(hashes []chainhash.Hash, _ []uint64, dst *[]byte, spans [][2]int) {
	b.cache.GetMulti(dst, hashes, spans)
}

func (b *lockFreeBackend) Del(key []byte) {
	b.cache.Del(key)
}

func (b *lockFreeBackend) UpdateStats(s *Stats) {
	b.cache.UpdateStats(s)
}
