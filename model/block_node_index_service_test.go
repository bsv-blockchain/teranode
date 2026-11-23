package model

import (
	"context"
	"encoding/hex"
	"math"
	"math/rand"
	"net/url"
	"regexp"
	"strings"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2"
	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/settings"
	"github.com/bsv-blockchain/teranode/stores/utxo"
	"github.com/bsv-blockchain/teranode/stores/utxo/sql"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// hexRe matches the random hashes of a generated fixture.
var hexRe = regexp.MustCompile("[0-9a-f]{64}")

// shapeBlock builds a block from subtrees of the given node counts (all with the capacity of
// the first) whose node hashes come from hashFor(global index).

func shapeBlock(t *testing.T, size int, lens []int, hashFor func(g int) chainhash.Hash) *Block {
	t.Helper()

	blockHeaderBytes, _ := hex.DecodeString(block1Header)
	blockHeader, err := NewBlockHeaderFromBytes(blockHeaderBytes)
	require.NoError(t, err)

	coinbase, err := bt.NewTxFromString(CoinbaseHex)
	require.NoError(t, err)

	subs := make([]*subtreepkg.Subtree, len(lens))
	roots := make([]*chainhash.Hash, len(lens))
	total := uint64(0)

	for s, l := range lens {
		capacity := size
		for capacity < l {
			capacity *= 2
		}

		st, err := subtreepkg.NewTreeByLeafCount(capacity)
		require.NoError(t, err)

		for i := 0; i < l; i++ {
			if s == 0 && i == 0 {
				require.NoError(t, st.AddCoinbaseNode())
			} else {
				require.NoError(t, st.AddNode(hashFor(s*size+i), 1, 0))
			}
		}

		subs[s] = st
		roots[s] = st.RootHash()
		total += uint64(l)
	}

	block, err := NewBlock(blockHeader, coinbase, roots, total, 123, 0, 0)
	require.NoError(t, err)

	block.SubtreeSlices = subs

	return block
}

// legacyDedup is what checkDuplicateTransactions did before the node index: the same
// subtree walk into a 64-bit swiss map.
func legacyDedup(b *Block) error {
	b.txMap = newBlockTxMap(b.txMapEntryCount())
	size := b.SubtreeSlices[0].Size()

	for s, st := range b.SubtreeSlices {
		if err := b.putSubtreeInTxMap("blk", st, s, size); err != nil {
			return err
		}
	}

	b.txMap.Freeze()

	return nil
}

func TestCheckDuplicateTransactions_BuildsNodeIndex(t *testing.T) {
	block := shapeBlock(t, 4, []int{4, 4, 3}, seqHashAt)

	require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 4, nil))

	m, ok := block.txMap.(*blockNodeIndex)
	require.True(t, ok, "got %T", block.txMap)
	require.Equal(t, 10, m.Length(), "every node but the coinbase placeholder")

	for g := 0; g < 11; g++ {
		if g == 0 {
			continue
		}

		idx, ok := m.Get(seqHashAt(g))
		if g == 11 {
			continue
		}

		require.True(t, ok, "g=%d", g)
		require.Equal(t, uint64(g), idx)
	}

	block.releaseTxMap()
	require.Nil(t, block.txMap)
}

func seqHashAt(g int) chainhash.Hash { return seqHash(uint64(g) + 1<<20) }

// The dedup walk gives the same indices and the same verdicts as the swiss map it replaced, on
// shapes the layout arithmetic could get wrong, with and without a repeated hash.
func TestCheckDuplicateTransactions_MatchesLegacy(t *testing.T) {
	shapes := map[string]struct {
		size int
		lens []int
	}{
		"empty":            {2, []int{1}},
		"coinbase and one": {2, []int{2}},
		"one subtree":      {8, []int{8}},
		"short last":       {4, []int{4, 4, 1}},
		"short middle":     {4, []int{4, 2, 4}},
		"oversized last":   {4, []int{4, 4, 8}},
		"many":             {16, []int{16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 5}},
	}

	for name, sh := range shapes {
		total := 0
		for _, l := range sh.lens {
			total += l
		}

		// no duplicate, then one hash repeated for every pair of non-coinbase positions
		type pair struct{ a, b int }

		dups := []*pair{nil}

		var positions []int

		for s, l := range sh.lens {
			for i := 0; i < l; i++ {
				if s == 0 && i == 0 {
					continue
				}

				positions = append(positions, s*sh.size+i)
			}
		}

		for i := 0; i < len(positions); i += 3 {
			for j := i + 1; j < len(positions); j += 5 {
				dups = append(dups, &pair{positions[i], positions[j]})
			}
		}

		for _, d := range dups {
			label := name

			hashFor := seqHashAt
			if d != nil {
				label += " dup"
				a, b := d.a, d.b
				hashFor = func(g int) chainhash.Hash {
					if g == b {
						return seqHashAt(a)
					}

					return seqHashAt(g)
				}
			}

			t.Run(label, func(t *testing.T) {
				newBlock := shapeBlock(t, sh.size, sh.lens, hashFor)
				oldBlock := shapeBlock(t, sh.size, sh.lens, hashFor)

				newErr := newBlock.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 3, nil)
				oldErr := legacyDedup(oldBlock)

				if oldErr == nil {
					require.NoError(t, newErr)
				} else {
					require.Error(t, newErr)
					require.True(t, errors.IsBlockCorrupt(newErr), "got %v", newErr)
					require.Equal(t, errors.IsBlockCorrupt(oldErr), errors.IsBlockCorrupt(newErr))
					require.Equal(t, strings.ReplaceAll(oldErr.Error(), "[BLOCK][blk]", "[BLOCK]["+newBlock.String()+"]"), newErr.Error())

					return
				}

				newBlock.txMap.Freeze()

				for g := 0; g < total+3; g++ {
					h := hashFor(g)
					want, wantOK := oldBlock.txMap.Get(h)
					got, gotOK := newBlock.txMap.Get(h)
					require.Equal(t, wantOK, gotOK, "g=%d", g)
					require.Equal(t, want, got, "g=%d", g)
				}
			})
		}
	}
}

// Any number of dedup workers gives the same verdict.
func TestCheckDuplicateTransactions_VerdictIndependentOfWorkers(t *testing.T) {
	lens := []int{8, 8, 8, 8, 8, 8, 8, 5}
	hashFor := func(g int) chainhash.Hash {
		if g == 50 {
			return seqHashAt(9)
		}

		return seqHashAt(g)
	}

	for _, workers := range []int{1, 2, 3, 8, 64} {
		for i := 0; i < 20; i++ {
			b := shapeBlock(t, 8, lens, hashFor)
			err := b.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, workers, nil)
			require.Error(t, err)
			require.True(t, errors.IsBlockCorrupt(err))
			require.Contains(t, err.Error(), seqHashAt(9).String(), "workers=%d", workers)
		}
	}
}

// shapeBlockUnique is shapeBlock with hashes that stay distinct when a subtree overlaps the next
// one's index range.
func shapeBlockUnique(t *testing.T, size int, lens []int) *Block {
	t.Helper()

	b := shapeBlock(t, size, lens, func(g int) chainhash.Hash { return seqHashAt(g) })
	for s, st := range b.SubtreeSlices {
		for i := range st.Nodes {
			if s != 0 || i != 0 {
				st.Nodes[i].Hash = seqHashAt(s*1000 + i)
			}
		}
	}

	return b
}

// A subtree other than the last that is longer than the first overlaps the next one's indices. The
// old map took it (and the load reports it afterwards); the index keeps those txs apart and
// accepts and deduplicates exactly the same.
func TestCheckDuplicateTransactions_OversizedMiddleSubtree(t *testing.T) {
	newBlock := shapeBlockUnique(t, 4, []int{4, 8, 4})
	oldBlock := shapeBlockUnique(t, 4, []int{4, 8, 4})

	require.NoError(t, newBlock.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 2, nil))
	require.NoError(t, legacyDedup(oldBlock))
	newBlock.txMap.Freeze()

	require.Equal(t, oldBlock.txMap.Length(), newBlock.txMap.Length())

	for s, st := range oldBlock.SubtreeSlices {
		for _, n := range st.Nodes {
			want, wantOK := oldBlock.txMap.Get(n.Hash)
			got, gotOK := newBlock.txMap.Get(n.Hash)
			require.Equal(t, wantOK, gotOK, "subtree %d", s)
			require.Equal(t, want, got, "subtree %d", s)
		}
	}

	// a hash repeated between the oversized subtree and a regular one is still a duplicate
	for _, pos := range [][2]int{{1, 2}, {2, 1}} {
		b := shapeBlockUnique(t, 4, []int{4, 8, 4})
		b.SubtreeSlices[pos[1]].Nodes[2].Hash = b.SubtreeSlices[pos[0]].Nodes[6%len(b.SubtreeSlices[pos[0]].Nodes)].Hash

		err := b.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 3, nil)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err))
		require.Contains(t, err.Error(), "block contains duplicate transaction")
	}
}

// With an oversized middle subtree and a loader that fails, the load reports exactly what it did
// before the index: the loader's error, whatever the number of workers, and without the loader
// failing, the length check after the load with its original text.
func TestGetAndValidateSubtreesWithDedup_OversizedMiddleSubtreeIsReportedAsBefore(t *testing.T) {
	const n = 6

	build := func(t *testing.T, dropLoader bool) (*Block, *countingSubtreeStore) {
		t.Helper()

		b, subtrees, store := dedupLoadFixtureN(t, nil, n, nil)

		// a middle subtree of 8 leaves in a body of 4-leaf ones, bound to the header by hash
		big, err := subtreepkg.NewTreeByLeafCount(8)
		require.NoError(t, err)

		for i := 0; i < 8; i++ {
			require.NoError(t, big.AddNode(randCorruptTestHash(t), 1, 0))
		}

		raw, ok := store.inner.(*rawSubtreeStore)
		require.True(t, ok)
		raw.put(t, big)

		// rebind the header to the body with the oversized subtree in it
		hashes := append([]*chainhash.Hash(nil), b.Subtrees...)
		hashes[2] = big.RootHash()

		root := testBodyMerkleRoot(t, b.CoinbaseTx, hashes, subtrees[0])
		header := minedHeaderOnParent(t, 1, root, regtestGenesisHeader(t).Hash())

		b, err = NewBlock(header, b.CoinbaseTx, hashes, uint64(4*n), 123, 0, 0) // nolint: gosec
		require.NoError(t, err)

		if dropLoader {
			delete(raw.data, *b.Subtrees[4])
		}

		return b, store
	}

	// reference: a single-worker load of the same block (oversized subtree 2, missing subtree 4),
	// which must report the missing subtree as a storage error, as origin/main does
	b, store := build(t, true)
	refErr := func() error {
		defer b.releaseTxMap()
		_, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, 1)

		return err
	}()
	require.Error(t, refErr)
	require.False(t, errors.IsBlockCorrupt(refErr), "a missing subtree is not corruption, got %v", refErr)
	require.ErrorIs(t, refErr, errors.ErrStorageError)

	for _, workers := range []int{1, 2, 8} {
		for run := 0; run < 3; run++ {
			b, store := build(t, true)
			_, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, workers)
			require.Error(t, err)
			require.Equal(t, hexRe.ReplaceAllString(refErr.Error(), "H"), hexRe.ReplaceAllString(err.Error(), "H"), "workers=%d run=%d", workers, run)
			require.Equal(t, errors.IsBlockCorrupt(refErr), errors.IsBlockCorrupt(err))
			b.releaseTxMap()
		}
	}

	// all loaders succeed: the post-load length check reports it, with the block id in its text
	for _, workers := range []int{1, 2, 8} {
		b, store := build(t, false)
		_, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, workers)
		require.Error(t, err)
		require.True(t, errors.IsBlockCorrupt(err), "got %v", err)
		require.Contains(t, err.Error(), "][ID 0] subtree 2 has length 8, expected 4", "workers=%d", workers)
		b.releaseTxMap()
	}
}

// A block with a nil (released) subtree fails transiently, never with a corrupt verdict or a panic.
func TestCheckDuplicateTransactions_ReleasedSubtree(t *testing.T) {
	b := shapeBlock(t, 4, []int{4, 4, 4}, seqHashAt)
	b.SubtreeSlices[1] = nil

	err := b.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 2, nil)
	require.Error(t, err)
	require.False(t, errors.IsBlockCorrupt(err), "got %v", err)
}

func TestPutSubtreeInTxMap_NodeIndexUnregisteredIndex(t *testing.T) {
	b := shapeBlock(t, 4, []int{4, 4}, seqHashAt)
	b.txMap = newBlockNodeIndex(8, 2, 4)

	// subtree 5 is outside the block's two subtrees
	err := b.putSubtreeInTxMap("blk", b.SubtreeSlices[1], 5, 4)
	require.Error(t, err)
}

// The map reads hashes from mmap-backed node arrays. They must stay readable for as long as the
// map is used, which is until Block.Valid returns; closing them only afterwards is safe.
func TestBlockNodeIndex_MmapBackedSubtrees(t *testing.T) {
	const (
		size    = 1024
		subtree = 8
	)

	dir := t.TempDir()
	subs := make([]*subtreepkg.Subtree, subtree)
	hashes := make(map[uint64]chainhash.Hash)

	for s := range subs {
		st, err := subtreepkg.NewTreeByLeafCountMmap(size, dir)
		require.NoError(t, err)
		require.True(t, st.IsMmapBacked())

		for i := 0; i < size; i++ {
			if s == 0 && i == 0 {
				require.NoError(t, st.AddCoinbaseNode())
				continue
			}

			h := seqHashAt(s*size + i)
			require.NoError(t, st.AddNode(h, 1, 0))

			hashes[uint64(s*size+i)] = h
		}

		subs[s] = st
	}

	m := newInMemoryBlockTxMap(subtree*size, subtree, size, maxTxIndex(subs, size))
	nodeIdx, ok := m.(*blockNodeIndex)
	require.True(t, ok)

	var wg sync.WaitGroup

	for s := range subs {
		wg.Add(1)

		go func() {
			defer wg.Done()

			require.NoError(t, nodeIdx.register(s, subs[s].Nodes))

			for i := range subs[s].Nodes {
				if s == 0 && i == 0 {
					continue
				}

				require.NoError(t, m.Put(subs[s].Nodes[i].Hash, uint64(s*size+i)))
			}
		}()
	}

	wg.Wait()
	m.Freeze()

	for idx, h := range hashes {
		got, ok := m.Get(h)
		require.True(t, ok)
		require.Equal(t, idx, got)
	}

	// the node arrays are the mapped regions, not copies: the map reads the file
	for _, st := range subs {
		require.NoError(t, st.Close())
	}
}

// ---- validOrderAndBlessed, the lookups of a tx and of its parents ----

type parentSpec struct {
	parents []int // global tx indexes; -1 is the external anchor
	vouts   []uint32
}

// buildOrderFixture builds a block of subtrees whose txs spend the given parents, with an anchor
// parent mined in a block on our chain, and returns the block and its validation dependencies.
func buildOrderFixture(t *testing.T, size int, lens []int, specs map[int]parentSpec) (*Block, *validationDependencies, []chainhash.Hash) {
	t.Helper()

	tSettings := test.CreateBaseTestSettings(t)

	total := 0
	for _, l := range lens {
		total += l
	}

	hashes := make([]chainhash.Hash, total+size*len(lens)+256) // room for parents outside the block
	for g := range hashes {
		hashes[g] = seqHashAt(g)
	}

	block := shapeBlock(t, size, lens, func(g int) chainhash.Hash { return hashes[g] })

	const anchorBlockID uint32 = 1

	anchorTx := newTx(99)
	anchorHash := *anchorTx.TxIDChainHash()

	storeData := make(map[string][]byte)

	for s, st := range block.SubtreeSlices {
		meta := subtreepkg.NewSubtreeMeta(st)

		for i := 0; i < st.Length(); i++ {
			g := s*size + i

			var (
				parents []chainhash.Hash
				vouts   []uint32
			)

			switch spec, ok := specs[g]; {
			case g == 0:
			case ok:
				for k, p := range spec.parents {
					if p < 0 {
						parents = append(parents, anchorHash)
					} else {
						parents = append(parents, hashes[p])
					}

					vouts = append(vouts, 1, spec.vouts[k])
				}
			default:
				// spend the anchor, one distinct output per tx
				parents = append(parents, anchorHash)
				vouts = append(vouts, 1, uint32(1000+g)) // nolint: gosec
			}

			require.NoError(t, meta.SetTxInpoints(i, subtreepkg.NewTxInpointsFromPacked(parents, vouts)))
		}

		metaBytes, err := meta.Serialize()
		require.NoError(t, err)

		storeData[string(st.RootHash()[:])] = metaBytes
	}

	utxoStoreURL, err := url.Parse("sqlitememory:///order")
	require.NoError(t, err)
	utxoStore, err := sql.New(context.Background(), ulogger.TestLogger{}, settings.NewSettings(), utxoStoreURL)
	require.NoError(t, err)
	_, err = utxoStore.Create(context.Background(), anchorTx, anchorBlockID,
		utxo.WithMinedBlockInfo(utxo.MinedBlockInfo{BlockID: anchorBlockID, BlockHeight: 1}))
	require.NoError(t, err)

	deps := &validationDependencies{
		txMetaStore:           utxoStore,
		subtreeStore:          &mockSubtreeStore{data: storeData},
		currentChain:          []*BlockHeader{},
		currentBlockHeaderIDs: []uint32{anchorBlockID},
		oldBlockIDsMap:        txmap.NewSyncedMap[chainhash.Hash, []uint32](),
	}

	_ = tSettings

	return block, deps, hashes
}

// The block verdicts of validOrderAndBlessed are the same with the node index as with the swiss
// map: valid, a parent after its child, a parent that is nowhere, a repeated input.
func TestValidOrderAndBlessed_NodeIndexMatchesLegacy(t *testing.T) {
	const size = 4

	lens := []int{4, 4, 3}

	scenarios := map[string]struct {
		specs   map[int]parentSpec
		wantErr string
	}{
		"valid, parents across subtrees": {
			specs: map[int]parentSpec{
				2: {[]int{1}, []uint32{0}},
				5: {[]int{1, 2}, []uint32{1, 0}},
				9: {[]int{5, 8}, []uint32{0, 0}},
			},
		},
		"parent is the previous tx": {
			specs: map[int]parentSpec{2: {[]int{1}, []uint32{0}}},
		},
		"parent after child, other subtree": {
			specs:   map[int]parentSpec{2: {[]int{9}, []uint32{0}}},
			wantErr: "comes before parent transaction",
		},
		"parent after child, same subtree": {
			specs:   map[int]parentSpec{2: {[]int{3}, []uint32{0}}},
			wantErr: "comes before parent transaction",
		},
		"parent is itself": {
			specs: map[int]parentSpec{2: {[]int{2}, []uint32{0}}},
		},
		"parent not in block and not on chain": {
			specs:   map[int]parentSpec{2: {[]int{200}, []uint32{0}}},
			wantErr: "not found",
		},
		"repeated input": {
			specs: map[int]parentSpec{
				2: {[]int{1}, []uint32{7}},
				6: {[]int{1}, []uint32{7}},
			},
			wantErr: "duplicate inputs",
		},
	}

	for name, sc := range scenarios {
		t.Run(name, func(t *testing.T) {
			run := func(legacy bool) error {
				block, deps, _ := buildOrderFixture(t, size, lens, sc.specs)

				if legacy {
					require.NoError(t, legacyDedup(block))
				} else {
					require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 2, nil))
					block.txMap.Freeze()

					_, ok := block.txMap.(*blockNodeIndex)
					require.True(t, ok)
				}

				return block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, 1, nil, 2)
			}

			oldErr := run(true)
			newErr := run(false)

			if sc.wantErr == "" {
				require.NoError(t, oldErr)
				require.NoError(t, newErr)

				return
			}

			require.Error(t, oldErr)
			require.Error(t, newErr)
			require.Contains(t, newErr.Error(), sc.wantErr)

			// same error class and the same text up to the block label and transient ids
			require.Equal(t, errors.Is(oldErr, errors.ErrBlockInvalid), errors.Is(newErr, errors.ErrBlockInvalid))
			require.Equal(t, errors.Is(oldErr, errors.ErrBlockIncomplete), errors.Is(newErr, errors.ErrBlockIncomplete))
			require.Equal(t, oldErr.Error(), newErr.Error())
		})
	}
}

// Randomised blocks: every tx's parents are drawn from earlier and later positions, and the two maps
// must agree on the verdict.
func TestValidOrderAndBlessed_NodeIndexMatchesLegacyRandom(t *testing.T) {
	const size = 8

	lens := []int{8, 8, 8, 8, 5}
	rng := rand.New(rand.NewSource(7))

	verdicts := make(map[string]int)

	for round := 0; round < 24; round++ {
		specs := make(map[int]parentSpec)

		for g := 1; g < 37; g++ {
			if g%8 == 0 && g/8 == 4 && g > 35 {
				continue
			}

			p := rng.Intn(45) - 1
			if rng.Intn(4) != 0 {
				p = rng.Intn(max(g, 1))
				if p == 0 {
					p = -1
				}
			}

			specs[g] = parentSpec{[]int{p}, []uint32{uint32(g)}} // nolint: gosec
		}

		var errs [2]error

		for i, legacy := range []bool{true, false} {
			block, deps, _ := buildOrderFixture(t, size, lens, specs)

			if legacy {
				require.NoError(t, legacyDedup(block))
			} else {
				require.NoError(t, block.checkDuplicateTransactions(context.Background(), ulogger.TestLogger{}, 3, nil))
				block.txMap.Freeze()
			}

			errs[i] = block.validOrderAndBlessed(context.Background(), ulogger.TestLogger{}, deps, 1, nil, 2)
		}

		require.Equal(t, errs[0] == nil, errs[1] == nil, "round %d: %v vs %v", round, errs[0], errs[1])

		switch {
		case errs[0] == nil:
			verdicts["valid"]++
		case errors.Is(errs[0], errors.ErrBlockInvalid):
			verdicts["invalid"]++
		default:
			verdicts["other"]++
		}

		if errs[0] != nil {
			require.Equal(t, errors.Is(errs[0], errors.ErrBlockInvalid), errors.Is(errs[1], errors.ErrBlockInvalid), "round %d", round)
			require.Equal(t, errors.Is(errs[0], errors.ErrBlockIncomplete), errors.Is(errs[1], errors.ErrBlockIncomplete), "round %d", round)

			// Which of several missing parents is reported first depends on the order the
			// store lookups finish in, for either map. The other verdicts are decided serially.
			if !errors.Is(errs[0], errors.ErrBlockIncomplete) {
				require.Equal(t, errs[0].Error(), errs[1].Error(), "round %d", round)
			}
		}
	}

	require.GreaterOrEqual(t, len(verdicts), 2, "the generator must reach more than one verdict: %v", verdicts)
}

// Valid wires the node index through the real load path (dedup during the subtree load) and a failed
// dedup there leaves nothing behind.
func TestGetAndValidateSubtreesWithDedup_BuildsNodeIndex(t *testing.T) {
	b, st0, st1, store := dedupLoadFixture(t, nil)

	deduped, err := b.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, 4)
	require.NoError(t, err)
	require.True(t, deduped)

	m, ok := b.txMap.(*blockNodeIndex)
	require.True(t, ok, "got %T", b.txMap)
	require.Equal(t, st0.Length()-1+st1.Length(), m.Length())

	for i, n := range b.SubtreeSlices[1].Nodes {
		got, ok := m.Get(n.Hash)
		require.True(t, ok)
		require.Equal(t, uint64(b.SubtreeSlices[0].Size()+i), got)
	}

	b.releaseTxMap()
}

func TestNodeIndex_NoWrapAbove32Bit(t *testing.T) {
	m := newBlockNodeIndex(0, 1, 4)
	b := shapeBlock(t, 4, []int{4}, seqHashAt)
	require.NoError(t, m.register(0, b.SubtreeSlices[0].Nodes))

	err := m.Put(seqHashAt(1), math.MaxUint32+1)
	require.Error(t, err)
	require.Contains(t, err.Error(), "32-bit")
}
