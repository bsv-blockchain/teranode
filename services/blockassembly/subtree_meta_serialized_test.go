package blockassembly

import (
	"math/rand"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/pkg/fileformat"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/stretchr/testify/require"
)

// legacyMetaBytes is what storeSubtreeData built before the serialized path
// existed: decode every node's inpoints, put them in a subtreepkg.Meta, serialize it.
func legacyMetaBytes(subtree *subtreepkg.Subtree, m subtreeprocessor.TxInpointsMap, deleted *txmap.SyncedMap[chainhash.Hash, subtreepkg.TxInpoints]) ([]byte, bool) {
	meta := subtreepkg.NewSubtreeMeta(subtree)

	for idx, node := range subtree.Nodes {
		if node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
			continue
		}

		inpoints, found := m.Get(node.Hash)

		if !found && deleted != nil {
			var d subtreepkg.TxInpoints

			if d, found = deleted.Get(node.Hash); found {
				inpoints = &d
			}
		}

		if !found {
			return nil, false
		}

		if err := meta.SetTxInpoints(idx, *inpoints); err != nil {
			return nil, false
		}
	}

	b, err := meta.Serialize()
	if err != nil {
		return nil, false
	}

	return b, true
}

func metaTestHash(i int) chainhash.Hash {
	return chainhash.HashH([]byte{byte(i), byte(i >> 8), 'm'})
}

func packedInpoints(r *rand.Rand, seed int) *subtreepkg.TxInpoints {
	parents := make([]chainhash.Hash, 1+r.Intn(4))
	packed := make([]uint32, 0, 2*len(parents))

	for k := range parents {
		parents[k] = chainhash.HashH([]byte{byte(seed), byte(seed >> 8), byte(k), 'p'})

		vouts := r.Intn(4) // a parent may have 0 vouts
		packed = append(packed, uint32(vouts))

		for v := 0; v < vouts; v++ {
			packed = append(packed, r.Uint32())
		}
	}

	p := subtreepkg.NewTxInpointsFromPacked(parents, packed)
	p.SubtreeIndex = int16(r.Intn(1 << 16)) //nolint:gosec // any int16

	return &p
}

// TestBuildSubtreeMetaBytes_MatchesSubtreeMeta pins that the meta built from the
// serialized records is byte-identical to the Meta.Serialize output, and fails
// exactly when that fails, over every case the old loop distinguished.
func TestBuildSubtreeMetaBytes_MatchesSubtreeMeta(t *testing.T) {
	type tc struct {
		name         string
		placeholder  bool // first node is the coinbase placeholder
		nodes        int
		size         int
		mutate       func(i int, p *subtreepkg.TxInpoints) *subtreepkg.TxInpoints // nil result: leave the node out of the map
		deletedExtra func(i int) bool                                             // nodes served from DeletedTxs only
	}

	keep := func(_ int, p *subtreepkg.TxInpoints) *subtreepkg.TxInpoints { return p }

	for _, c := range []tc{
		{name: "full subtree with placeholder", placeholder: true, nodes: 15, size: 16, mutate: keep},
		{name: "full subtree without placeholder", nodes: 16, size: 16, mutate: keep},
		{name: "partly filled subtree", placeholder: true, nodes: 5, size: 16, mutate: keep},
		{name: "single node", nodes: 1, size: 4, mutate: keep},
		{name: "only the placeholder", placeholder: true, nodes: 0, size: 4, mutate: keep},
		{name: "empty non-nil parents are fine", placeholder: true, nodes: 8, size: 16, mutate: func(i int, p *subtreepkg.TxInpoints) *subtreepkg.TxInpoints {
			if i == 3 {
				e := subtreepkg.NewTxInpoints()
				return &e
			}

			return p
		}},
		{name: "nil parents on the first node are fine", nodes: 8, size: 16, mutate: func(i int, p *subtreepkg.TxInpoints) *subtreepkg.TxInpoints {
			if i == 0 {
				return &subtreepkg.TxInpoints{}
			}

			return p
		}},
		{name: "nil parents on a later node fail", placeholder: true, nodes: 8, size: 16, mutate: func(i int, p *subtreepkg.TxInpoints) *subtreepkg.TxInpoints {
			if i == 3 {
				return &subtreepkg.TxInpoints{}
			}

			return p
		}},
		{name: "inpoints that cannot serialize fail", placeholder: true, nodes: 8, size: 16, mutate: func(i int, p *subtreepkg.TxInpoints) *subtreepkg.TxInpoints {
			if i == 4 {
				return &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{{1}}}
			}

			return p
		}},
		{name: "a node missing from the map fails", placeholder: true, nodes: 8, size: 16, mutate: func(i int, p *subtreepkg.TxInpoints) *subtreepkg.TxInpoints {
			if i == 5 {
				return nil
			}

			return p
		}},
		{name: "a node missing from the map is served by DeletedTxs", placeholder: true, nodes: 8, size: 16, mutate: func(i int, p *subtreepkg.TxInpoints) *subtreepkg.TxInpoints {
			if i == 5 || i == 6 {
				return nil
			}

			return p
		}, deletedExtra: func(i int) bool { return i == 5 || i == 6 }},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(1))

			subtree, err := subtreepkg.NewTreeByLeafCount(c.size)
			require.NoError(t, err)

			if c.placeholder {
				require.NoError(t, subtree.AddCoinbaseNode())
			}

			m := subtreeprocessor.NewSplitTxInpointsMap(16)
			deleted := txmap.NewSyncedMap[chainhash.Hash, subtreepkg.TxInpoints]()

			for i := 0; i < c.nodes; i++ {
				h := metaTestHash(i)
				require.NoError(t, subtree.AddNode(h, uint64(i), uint64(i)))

				p := c.mutate(i, packedInpoints(r, i))
				if p == nil {
					continue
				}

				m.Set(h, p)
			}

			// the deleted-tx backup holds what the processor saved when it removed the tx
			for i := 0; i < c.nodes; i++ {
				if c.deletedExtra != nil && c.deletedExtra(i) {
					deleted.Set(metaTestHash(i), *packedInpoints(rand.New(rand.NewSource(int64(i))), i))
				}
			}

			want, wantOK := legacyMetaBytes(subtree, m, deleted)
			got, err := buildSubtreeMetaBytes(subtree, m, deleted)

			if !wantOK {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, want, got)

			// and the result reads back as a Meta
			meta, err := subtreepkg.NewSubtreeMetaFromBytes(subtree, got)
			require.NoError(t, err)
			require.NotNil(t, meta)
		})
	}
}

// TestStoreSubtreeData_SerializedMetaMatchesDecodedPath stores the same subtree
// through the in-memory map (serialized path) and through a wrapper that hides
// that path (the decode path every other map takes) and requires the same
// bytes in the store.
func TestStoreSubtreeData_SerializedMetaMatchesDecodedPath(t *testing.T) {
	store := func(t *testing.T, wrap bool) []byte {
		server, subtreeStore, subtree, txMap := setup(t)

		var parent subtreeprocessor.TxInpointsMap = txMap
		if wrap {
			parent = struct{ subtreeprocessor.TxInpointsMap }{txMap}
		}

		done, allDone, err := server.storeSubtreeData(t.Context(), subtreeprocessor.NewSubtreeRequest{Subtree: subtree, ParentTxMap: parent}, make(chan *subtreeRetrySend, 8))
		require.NoError(t, err)
		require.True(t, <-done)
		<-allDone

		b, err := subtreeStore.Get(t.Context(), subtree.RootHash()[:], fileformat.FileTypeSubtreeMeta)
		require.NoError(t, err)

		return b
	}

	require.Equal(t, store(t, true), store(t, false))
}

// BenchmarkBuildSubtreeMeta compares building the meta of a full subtree from
// the serialized records with decoding every tx's inpoints and serializing a Meta.
func BenchmarkBuildSubtreeMeta(b *testing.B) {
	const leaves = 1 << 16

	subtree, err := subtreepkg.NewTreeByLeafCount(leaves)
	require.NoError(b, err)

	r := rand.New(rand.NewSource(1))
	m := subtreeprocessor.NewSplitTxInpointsMap(16384)

	for i := 0; i < leaves; i++ {
		h := metaTestHash(i)
		require.NoError(b, subtree.AddNode(h, uint64(i), uint64(i)))

		p := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{metaTestHash(i + leaves)}, []uint32{1, r.Uint32() % 4})
		m.Set(h, &p)
	}

	b.Run("serialized", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, err := buildSubtreeMetaBytes(subtree, m, nil); err != nil {
				b.Fatal(err)
			}
		}

		b.ReportMetric(float64(b.N*leaves)/b.Elapsed().Seconds()/1e6, "Mtx/s")
	})

	b.Run("decoded", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			if _, ok := legacyMetaBytes(subtree, m, nil); !ok {
				b.Fatal("failed")
			}
		}

		b.ReportMetric(float64(b.N*leaves)/b.Elapsed().Seconds()/1e6, "Mtx/s")
	})
}
