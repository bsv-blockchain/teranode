package diskblocks

import (
	"math"
	"path/filepath"
	"sort"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/btcsuite/goleveldb/leveldb"
	"github.com/btcsuite/goleveldb/leveldb/opt"
	"github.com/btcsuite/goleveldb/leveldb/util"
)

// IndexDB is a read-only handle to an SV Node blocks/index LevelDB.
type IndexDB struct {
	db *leveldb.DB
}

// OpenIndex opens <datadir>/blocks/index read-only. A read-only open does not
// replay the write-ahead log, so a dirty/crashed datadir is tolerated: only
// sealed records are visible and the sync stops at that frontier.
func OpenIndex(datadir string) (*IndexDB, error) {
	path := filepath.Join(datadir, "blocks", "index")
	db, err := leveldb.OpenFile(path, &opt.Options{
		Compression: opt.NoCompression,
		ReadOnly:    true,
	})
	if err != nil {
		return nil, errors.NewProcessingError("failed to open block index at %s", path, err)
	}
	return &IndexDB{db: db}, nil
}

// Close releases the LevelDB handle.
func (in *IndexDB) Close() error {
	if in.db == nil {
		return nil
	}
	return in.db.Close()
}

// ReadChain returns the active chain, ordered genesis -> tip, derived purely
// from the block index (no chainstate dependency). If stopHeight > 0 the result
// is truncated to that height inclusive.
func (in *IndexDB) ReadChain(stopHeight uint32) ([]*BlockRef, error) {
	refs := make(map[chainhash.Hash]*BlockRef)

	iter := in.db.NewIterator(util.BytesPrefix([]byte("b")), nil)
	defer iter.Release()

	for iter.Next() {
		key := iter.Key()
		if len(key) != 33 { // 'b' + 32-byte hash
			continue
		}
		ref, err := parseBlockIndexRecord(iter.Value())
		if err != nil {
			continue // skip records without data / invalid
		}
		refs[ref.Hash] = ref
	}
	if err := iter.Error(); err != nil {
		return nil, errors.NewProcessingError("error iterating block index", err)
	}

	return selectChain(refs, stopHeight)
}

// selectChain picks the highest-height ref whose entire ancestry back to a
// genesis (height 0) ref is present, walks it back to genesis, and returns it
// ordered genesis -> tip. A frontier gap or stale fork simply lowers the chosen
// tip rather than erroring.
// Complexity: each candidate tip is tried highest-height first, so a healthy
// chain resolves in O(n) (the top tip completes immediately). A pathological
// datadir with many incomplete high-height orphans degrades toward O(n*m);
// acceptable for an offline benchmark tool.
func selectChain(refs map[chainhash.Hash]*BlockRef, stopHeight uint32) ([]*BlockRef, error) {
	tips := make([]*BlockRef, 0, len(refs))
	for _, r := range refs {
		tips = append(tips, r)
	}
	sort.Slice(tips, func(i, j int) bool { return tips[i].Height > tips[j].Height })

	for _, tip := range tips {
		chain := make([]*BlockRef, 0, int(tip.Height)+1)
		cur := tip
		complete := true
		for {
			chain = append(chain, cur)
			if cur.Height == 0 {
				break
			}
			parent, ok := refs[cur.PrevHash]
			if !ok {
				complete = false
				break
			}
			cur = parent
		}
		if !complete {
			continue
		}
		// reverse into genesis -> tip order
		for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
			chain[i], chain[j] = chain[j], chain[i]
		}
		if stopHeight > 0 && stopHeight < math.MaxUint32 && uint32(len(chain)) > stopHeight+1 {
			chain = chain[:stopHeight+1]
		}
		return chain, nil
	}

	return nil, errors.NewProcessingError("no complete chain found in block index")
}
