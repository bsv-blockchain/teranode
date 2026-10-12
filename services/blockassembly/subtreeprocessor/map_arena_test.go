package subtreeprocessor

import (
	"context"
	"encoding/binary"
	"math"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/stretchr/testify/require"
)

// arenaHash is a deterministic hash for key i whose first two bytes (the bucket
// selector) are spread out.
func arenaHash(i uint64) chainhash.Hash {
	return chainhash.HashH(binary.LittleEndian.AppendUint64(nil, i))
}

// inpointsShape builds the inpoints used for key i. The shape cycles through
// everything the map has to round-trip exactly.
type inpointsShape int

const (
	shapeNilPointer  inpointsShape = iota
	shapeZeroValue                 // &TxInpoints{}: nil parents
	shapeEmptyNonNil               // NewTxInpoints(): empty but non-nil parents
	shapeOneParent
	shapeThreeParents
	shapeManyVouts       // one parent, 70000 vouts
	shapeManyParents     // 40 parents, above the in-place encoding limit
	shapeHugeParentCount // 3000 parents
	shapeZeroVoutCount   // a parent whose vout count is 0
	shapeNoVouts         // parents but no packed vouts (how many existing tests build inpoints)
	shapeInt16Max
	shapeInt16Min
	shapeNegativeIndex
	shapeTrailingWords    // a parent with a vout count of 1 and an extra word after it
	shapeSharedBufferWide // vouts sub-sliced from a shared buffer, count claims more words than len
	shapeShortVouts       // fewer packed words than the walk needs, but at least one per parent
	shapeSharedBufferOK   // well-formed vouts sub-sliced from a shared buffer (cap runs into the neighbour)
	shapeZeroParentsVouts // no parents but packed vouts: Serialize refuses it
	numShapes
)

// serializable reports whether TxInpoints.Serialize accepts p, which is what
// the stored wire bytes must agree with.
func serializable(p *subtreepkg.TxInpoints) bool {
	if p == nil {
		return false
	}

	_, err := p.Serialize()

	return err == nil
}

func buildInpoints(shape inpointsShape, seed uint64) *subtreepkg.TxInpoints {
	parent := func(k uint64) chainhash.Hash { return arenaHash(1<<40 + seed*7919 + k) }

	pack := func(n int, vouts func(k int) []uint32) *subtreepkg.TxInpoints {
		parents := make([]chainhash.Hash, n)

		var packed []uint32

		for k := range parents {
			parents[k] = parent(uint64(k))
			v := vouts(k)
			packed = append(packed, uint32(len(v)))
			packed = append(packed, v...)
		}

		p := subtreepkg.NewTxInpointsFromPacked(parents, packed)

		return &p
	}

	oneVout := func(k int) []uint32 { return []uint32{uint32(k) + 1} }

	switch shape {
	case shapeNilPointer:
		return nil
	case shapeZeroValue:
		return &subtreepkg.TxInpoints{}
	case shapeEmptyNonNil:
		p := subtreepkg.NewTxInpoints()
		return &p
	case shapeOneParent:
		return pack(1, oneVout)
	case shapeThreeParents:
		return pack(3, func(k int) []uint32 {
			if k%2 == 0 {
				return []uint32{1, uint32(k)}
			}

			return []uint32{2, uint32(k), uint32(k + 10), math.MaxUint32}
		})
	case shapeManyVouts:
		return pack(1, func(int) []uint32 {
			v := make([]uint32, 70000)
			for i := range v {
				v[i] = uint32(i)
			}

			return v
		})
	case shapeManyParents:
		return pack(40, oneVout)
	case shapeHugeParentCount:
		return pack(3000, oneVout)
	case shapeZeroVoutCount:
		return pack(2, func(k int) []uint32 {
			if k == 0 {
				return nil
			}

			return []uint32{5}
		})
	case shapeNoVouts:
		return &subtreepkg.TxInpoints{ParentTxHashes: []chainhash.Hash{parent(0)}}
	case shapeInt16Max:
		p := pack(1, oneVout)
		p.SubtreeIndex = math.MaxInt16

		return p
	case shapeInt16Min:
		p := pack(2, oneVout)
		p.SubtreeIndex = math.MinInt16

		return p
	case shapeNegativeIndex:
		p := pack(1, oneVout)
		p.SubtreeIndex = -1

		return p
	case shapeTrailingWords:
		p := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent(0)}, []uint32{1, 7, 9})
		return &p
	case shapeSharedBufferWide:
		// the tx's slice ends after 2 words, the next tx follows in the same buffer
		buf := []uint32{2, 7, 1, 5, 1, 6}
		p := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent(0)}, buf[0:2])

		return &p
	case shapeShortVouts:
		p := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent(0), parent(1)}, []uint32{1, 5})
		return &p
	case shapeSharedBufferOK:
		buf := []uint32{1, 7, 2, 5, 6, 1, 9}
		p := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{parent(0)}, buf[0:2])

		return &p
	case shapeZeroParentsVouts:
		p := subtreepkg.NewTxInpointsFromPacked(nil, []uint32{1, 2})
		return &p
	default:
		panic("unknown shape")
	}
}

func randomInpoints(r *rand.Rand, seed uint64) (*subtreepkg.TxInpoints, inpointsShape) {
	shape := inpointsShape(r.Intn(int(numShapes)))
	if shape == shapeZeroParentsVouts {
		shape = shapeOneParent // the stray vouts are not representable, see the dedicated test
	}

	// the huge shapes are expensive, keep them rare
	if (shape == shapeManyVouts || shape == shapeHugeParentCount) && r.Intn(20) != 0 {
		shape = shapeThreeParents
	}

	p := buildInpoints(shape, seed)
	if p != nil && r.Intn(3) == 0 {
		p.SubtreeIndex = int16(r.Intn(1 << 16)) //nolint:gosec // wraps on purpose, covering the whole int16 range
	}

	return p, shape
}

// TestSplitTxInpointsMap_RoundTripsEveryShape pins that Get returns exactly what
// was stored, for every shape of inpoints, through Set and SetIfNotExists.
func TestSplitTxInpointsMap_RoundTripsEveryShape(t *testing.T) {
	for _, buckets := range []uint16{1, 16, 16384} {
		m := NewSplitTxInpointsMap(buckets)

		for shape := inpointsShape(0); shape < numShapes; shape++ {
			want := buildInpoints(shape, uint64(shape))
			h := arenaHash(uint64(shape))

			if shape%2 == 0 {
				m.Set(h, want)
			} else {
				got, set := m.SetIfNotExists(h, want)
				require.True(t, set)
				require.Same(t, want, got, "a newly inserted value is returned as passed")
			}

			got, ok := m.Get(h)
			require.True(t, ok, "shape %d", shape)

			if want == nil {
				require.Nil(t, got, "a stored nil pointer is returned as nil, found")
			} else if shape == shapeZeroParentsVouts {
				// stray vouts without parents cannot be serialized and are not kept
				require.Empty(t, got.ParentTxHashes)
			} else {
				require.Equal(t, want, got, "shape %d buckets %d", shape, buckets)
			}

			require.True(t, m.Exists(h))
		}

		require.Equal(t, int(numShapes), m.Length())
	}
}

func TestSplitTxInpointsMap_SetIfNotExistsKeepsFirstAndReturnsIt(t *testing.T) {
	m := NewSplitTxInpointsMap(16)
	h := arenaHash(1)
	first := buildInpoints(shapeThreeParents, 1)
	second := buildInpoints(shapeOneParent, 2)

	_, set := m.SetIfNotExists(h, first)
	require.True(t, set)

	got, set := m.SetIfNotExists(h, second)
	require.False(t, set)
	require.Equal(t, first, got)

	got, _ = m.Get(h)
	require.Equal(t, first, got, "a failed SetIfNotExists must not change the stored value")
	require.Equal(t, 1, m.Length())
}

func TestSplitTxInpointsMap_SetOverwritesDeleteRemovesReinsert(t *testing.T) {
	m := NewSplitTxInpointsMap(16)
	h := arenaHash(1)

	for round := 0; round < 3; round++ {
		a := buildInpoints(shapeThreeParents, uint64(round))
		b := buildInpoints(shapeManyParents, uint64(round))

		m.Set(h, a)
		m.Set(h, b)

		got, ok := m.Get(h)
		require.True(t, ok)
		require.Equal(t, b, got, "Set overwrites")
		require.Equal(t, 1, m.Length())

		require.True(t, m.Delete(h))
		require.False(t, m.Delete(h))
		require.False(t, m.Exists(h))

		_, ok = m.Get(h)
		require.False(t, ok)

		_, set := m.SetIfNotExists(h, a)
		require.True(t, set, "a deleted key can be inserted again")

		got, _ = m.Get(h)
		require.Equal(t, a, got)
		require.True(t, m.Delete(h))
		require.Equal(t, 0, m.Length())
	}
}

// TestSplitTxInpointsMap_GetReturnsIndependentCopies pins that a returned value
// does not alias the arena: mutating it, or the value that was stored, must not
// change the map.
func TestSplitTxInpointsMap_GetReturnsIndependentCopies(t *testing.T) {
	m := NewSplitTxInpointsMap(16)
	h := arenaHash(1)
	stored := buildInpoints(shapeThreeParents, 1)
	pristine := buildInpoints(shapeThreeParents, 1)

	m.Set(h, stored)

	stored.ParentTxHashes[0] = chainhash.Hash{0xff}
	stored.SubtreeIndex = 99

	got, _ := m.Get(h)
	require.Equal(t, pristine, got, "mutating the stored value must not reach the map")

	got.ParentTxHashes[1] = chainhash.Hash{0xee}

	got2, _ := m.Get(h)
	require.Equal(t, pristine, got2, "mutating a returned value must not reach the map")
}

// TestSplitTxInpointsMap_AppendSerializedMatchesSerialize pins the stored wire
// bytes to TxInpoints.Serialize, including the error for inpoints that cannot
// be serialized.
func TestSplitTxInpointsMap_AppendSerializedMatchesSerialize(t *testing.T) {
	m := NewSplitTxInpointsMap(16)

	for shape := inpointsShape(0); shape < numShapes; shape++ {
		p := buildInpoints(shape, uint64(shape))
		h := arenaHash(uint64(shape))
		m.Set(h, p)

		got, found, err := m.AppendSerialized([]byte{0xEE}, h)
		require.True(t, found, "shape %d", shape)

		if !serializable(p) {
			require.Error(t, err, "shape %d", shape)
			require.Equal(t, []byte{0xEE}, got, "dst is returned untouched on error")

			continue
		}

		require.NoError(t, err, "shape %d", shape)

		want, err := p.Serialize()
		require.NoError(t, err)
		require.Equal(t, append([]byte{0xEE}, want...), got, "shape %d", shape)
	}

	got, found, err := m.AppendSerialized([]byte{1}, arenaHash(1<<50))
	require.False(t, found)
	require.NoError(t, err)
	require.Equal(t, []byte{1}, got)
}

// TestSplitTxInpointsMap_AppendMetaInpointsRefusesUnsetParents pins the rule
// subtreepkg.Meta.Serialize applies to every node but the first: parent tx
// hashes that were never set (nil) are an error, an empty non-nil slice is not.
func TestSplitTxInpointsMap_AppendMetaInpointsRefusesUnsetParents(t *testing.T) {
	m := NewSplitTxInpointsMap(16)

	for shape := inpointsShape(0); shape < numShapes; shape++ {
		p := buildInpoints(shape, uint64(shape))
		h := arenaHash(uint64(shape))
		m.Set(h, p)

		_, found, err := m.AppendMetaInpoints(nil, h)
		require.True(t, found)

		wantErr := p == nil || p.ParentTxHashes == nil || !serializable(p)
		if wantErr {
			require.Error(t, err, "shape %d", shape)
		} else {
			require.NoError(t, err, "shape %d", shape)
		}
	}
}

func TestSplitTxInpointsMap_MoveTo(t *testing.T) {
	src, dst := NewSplitTxInpointsMap(16), NewSplitTxInpointsMap(64)
	want := make(map[chainhash.Hash]*subtreepkg.TxInpoints)

	for i := uint64(0); i < 2000; i++ {
		p := buildInpoints(inpointsShape(i%uint64(numShapes)), i)
		if p == nil || i%uint64(numShapes) == uint64(shapeZeroParentsVouts) {
			p = buildInpoints(shapeOneParent, i)
		}

		src.Set(arenaHash(i), p)
		want[arenaHash(i)] = p
	}

	existing := buildInpoints(shapeOneParent, 777)
	dst.Set(arenaHash(3), existing)

	for i := uint64(0); i < 2000; i++ {
		moved, found := src.MoveTo(dst, arenaHash(i))
		require.True(t, found)
		require.Equal(t, i != 3, moved, "tx %d", i)
	}

	_, found := src.MoveTo(dst, arenaHash(1<<50))
	require.False(t, found)

	for i := uint64(0); i < 2000; i++ {
		got, ok := dst.Get(arenaHash(i))
		require.True(t, ok)

		if i == 3 {
			require.Equal(t, existing, got, "MoveTo must not overwrite")
		} else {
			require.Equal(t, want[arenaHash(i)], got, "tx %d", i)
		}
	}

	require.Equal(t, 2000, src.Length(), "MoveTo copies, the source keeps its entries")

	// moving into itself is a no-op that reports the hash as found and not moved
	moved, found := src.MoveTo(src, arenaHash(5))
	require.True(t, found)
	require.False(t, moved)
}

func TestSplitTxInpointsMap_MoveBucketFrom(t *testing.T) {
	const buckets = 32

	src, dst := NewSplitTxInpointsMap(buckets), NewSplitTxInpointsMap(buckets)
	byBucket := make(map[uint16][]chainhash.Hash)

	for i := uint64(0); i < 3000; i++ {
		h := arenaHash(i)
		src.Set(h, buildInpoints(shapeThreeParents, i))
		byBucket[dst.BucketFor(h)] = append(byBucket[dst.BucketFor(h)], h)
	}

	dst.Set(arenaHash(0), buildInpoints(shapeOneParent, 9))

	for b, hashes := range byBucket {
		inserted := make([]bool, len(hashes))
		missing := dst.MoveBucketFrom(src, b, hashes, inserted)
		require.Equal(t, -1, missing)

		for j, h := range hashes {
			require.Equal(t, h != arenaHash(0), inserted[j])
		}
	}

	require.Equal(t, 3000, dst.Length())

	// the first missing hash is reported by position, and the ones before it moved
	b := dst.BucketFor(arenaHash(1))
	absent := chainhash.Hash{}
	absent[0], absent[1] = arenaHash(1)[0], arenaHash(1)[1] // same bucket as tx 1
	absent[31] = 0xAB

	dst2 := NewSplitTxInpointsMap(buckets)
	inserted := make([]bool, 2)
	require.Equal(t, 1, dst2.MoveBucketFrom(src, b, []chainhash.Hash{arenaHash(1), absent}, inserted))
	require.True(t, inserted[0])
	require.True(t, dst2.Exists(arenaHash(1)))
}

func TestSplitTxInpointsMap_ClearReusesArenaWithoutLeakingOldBytes(t *testing.T) {
	m := NewSplitTxInpointsMap(4) // few buckets: every bucket sees every generation

	for gen := uint64(0); gen < 6; gen++ {
		// alternate large and small records so reused chunks hold stale bytes of other sizes
		for i := uint64(0); i < 3000; i++ {
			shape := shapeOneParent
			if (i+gen)%7 == 0 {
				shape = shapeManyParents
			}

			m.Set(arenaHash(i), buildInpoints(shape, gen*10_000+i))
		}

		for i := uint64(0); i < 3000; i++ {
			shape := shapeOneParent
			if (i+gen)%7 == 0 {
				shape = shapeManyParents
			}

			got, ok := m.Get(arenaHash(i))
			require.True(t, ok)
			require.Equal(t, buildInpoints(shape, gen*10_000+i), got, "gen %d tx %d", gen, i)
		}

		m.Clear()
		require.Equal(t, 0, m.Length())
		require.False(t, m.Exists(arenaHash(1)))

		_, ok := m.Get(arenaHash(1))
		require.False(t, ok, "a cleared record is gone")

		_, found, _ := m.AppendSerialized(nil, arenaHash(1))
		require.False(t, found)
	}
}

// TestSplitTxInpointsMap_MatchesReferenceModel drives random operations against
// the map and a plain Go map holding the same values, and requires the same
// answer from every operation.
func TestSplitTxInpointsMap_MatchesReferenceModel(t *testing.T) {
	for _, tc := range []struct {
		name    string
		buckets uint16
		keys    uint64
		hash    func(uint64) chainhash.Hash
	}{
		{"one bucket", 1, 300, arenaHash},
		{"spread", 257, 3000, arenaHash},
		{"all keys in one bucket", 16384, 400, func(i uint64) chainhash.Hash {
			h := arenaHash(i)
			h[0], h[1] = 0x12, 0x34 // identical bucket selector, the keys differ in the rest

			return h
		}},
		{"keys differing in the last byte only", 64, 256, func(i uint64) chainhash.Hash {
			return chainhash.Hash{31: byte(i)}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := rand.New(rand.NewSource(int64(tc.keys)))
			m := NewSplitTxInpointsMap(tc.buckets)
			model := make(map[chainhash.Hash]*subtreepkg.TxInpoints)

			for step := 0; step < 20_000; step++ {
				key := tc.hash(uint64(r.Intn(int(tc.keys))))
				p, _ := randomInpoints(r, uint64(step))

				switch op := r.Intn(100); {
				case op < 30:
					m.Set(key, p)
					model[key] = p
				case op < 55:
					got, set := m.SetIfNotExists(key, p)

					old, had := model[key]
					require.Equal(t, !had, set)

					if had {
						require.Equal(t, old, got)
					} else {
						require.Same(t, p, got)
						model[key] = p
					}
				case op < 70:
					_, had := model[key]
					require.Equal(t, had, m.Delete(key))
					delete(model, key)
				case op < 90:
					got, ok := m.Get(key)
					old, had := model[key]
					require.Equal(t, had, ok)

					if had {
						require.Equal(t, old, got)
					}
				case op < 99:
					_, had := model[key]
					require.Equal(t, had, m.Exists(key))
				default:
					m.Clear()
					clear(model)
				}

				if step%997 == 0 {
					require.Equal(t, len(model), m.Length())
				}
			}

			require.Equal(t, len(model), m.Length())

			for key, want := range model {
				got, ok := m.Get(key)
				require.True(t, ok)
				require.Equal(t, want, got)
			}
		})
	}
}

// TestSplitTxInpointsMap_ResultsIndependentOfBucketsAndOrder pins that the
// observable contents do not depend on the bucket count or the insertion order.
func TestSplitTxInpointsMap_ResultsIndependentOfBucketsAndOrder(t *testing.T) {
	const n = 2000

	keys := make([]uint64, n)
	for i := range keys {
		keys[i] = uint64(i)
	}

	var reference map[chainhash.Hash][]byte

	for _, buckets := range []uint16{1, 3, 64, 16384, 65535} {
		for seed := int64(0); seed < 3; seed++ {
			r := rand.New(rand.NewSource(seed))
			r.Shuffle(n, func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

			m := NewSplitTxInpointsMap(buckets)

			for _, k := range keys {
				m.Set(arenaHash(k), buildInpoints(shapeThreeParents, k))
			}

			got := make(map[chainhash.Hash][]byte, n)

			for k := uint64(0); k < n; k++ {
				b, found, err := m.AppendSerialized(nil, arenaHash(k))
				require.True(t, found)
				require.NoError(t, err)

				got[arenaHash(k)] = b
			}

			require.Equal(t, n, m.Length())

			if reference == nil {
				reference = got
			} else {
				require.Equal(t, reference, got, "buckets %d seed %d", buckets, seed)
			}
		}
	}
}

func TestSplitTxInpointsMap_BulkInsertsMatchSetIfNotExists(t *testing.T) {
	const n = 5000

	hashes := make([]chainhash.Hash, n)
	values := make([]*subtreepkg.TxInpoints, n)

	for i := range hashes {
		hashes[i] = arenaHash(uint64(i % 4000)) // 1000 duplicates
		values[i] = buildInpoints(shapeThreeParents, uint64(i))
	}

	bulk := NewSplitTxInpointsMap(64)
	wasSet := make([]bool, n)
	bulk.ParallelBulkSetIfNotExists(hashes, values, wasSet)

	seq := NewSplitTxInpointsMap(64)

	for i := range hashes {
		_, set := seq.SetIfNotExists(hashes[i], values[i])
		require.Equal(t, set, wasSet[i], "index %d", i)
	}

	for i := 0; i < 4000; i++ {
		a, _ := bulk.Get(arenaHash(uint64(i)))
		b, _ := seq.Get(arenaHash(uint64(i)))
		require.Equal(t, b, a)
	}

	// PutMultiBucketTxInpoints: same bucket, with a duplicate and a pre-existing key
	multi := NewSplitTxInpointsMap(64)

	var keys []chainhash.Hash

	var vals []*subtreepkg.TxInpoints

	bucket := multi.BucketFor(arenaHash(0))

	for i := uint64(0); len(keys) < 50; i++ {
		if multi.BucketFor(arenaHash(i)) == bucket {
			keys = append(keys, arenaHash(i))
			vals = append(vals, buildInpoints(shapeOneParent, i))
		}
	}

	keys = append(keys, keys[0])
	vals = append(vals, buildInpoints(shapeOneParent, 12345))

	got := multi.PutMultiBucketTxInpoints(bucket, keys, vals)
	require.Len(t, got, len(keys))

	for i := 0; i < 50; i++ {
		require.True(t, got[i])
	}

	require.False(t, got[50], "the duplicate in the batch is not inserted")

	first, _ := multi.Get(keys[0])
	require.Equal(t, vals[0], first)
}

// TestSplitTxInpointsMap_ConcurrentReadersNeverSeeAClearedRecord is the
// lifetime proof for the arena. Readers that the map does not synchronize with
// the owner (the subtree meta builder holds a captured half while the owner
// moves to the next block and clears the retired half) run against Clear and
// refill cycles. Every read must return either "not found" or exactly the
// record of the key it asked for, never bytes of another record, a torn mix, or
// a panic. Run with -race.
func TestSplitTxInpointsMap_ConcurrentReadersNeverSeeAClearedRecord(t *testing.T) {
	const (
		keys        = 4000
		generations = 40
	)

	m := NewSplitTxInpointsMap(8) // few buckets so records of different generations share chunks

	// the record of key i is a pure function of i, so any generation must return the same
	valueOf := func(i uint64) *subtreepkg.TxInpoints {
		if i%5 == 0 {
			return buildInpoints(shapeManyParents, i)
		}

		return buildInpoints(shapeThreeParents, i)
	}

	wire := func(i uint64) []byte {
		b, err := valueOf(i).Serialize()
		require.NoError(t, err)

		return b
	}

	var (
		stop       atomic.Bool
		reads      atomic.Int64
		wg         sync.WaitGroup
		dstMu      sync.Mutex
		failedOnce sync.Once
	)

	fail := func(msg string, args ...any) {
		failedOnce.Do(func() { t.Errorf(msg, args...) })
		stop.Store(true)
	}

	dst := NewSplitTxInpointsMap(8)

	for w := 0; w < 4; w++ {
		wg.Add(1)

		go func(w int) {
			defer wg.Done()

			r := rand.New(rand.NewSource(int64(w)))

			for !stop.Load() {
				i := uint64(r.Intn(keys))

				switch r.Intn(3) {
				case 0:
					if got, ok := m.Get(arenaHash(i)); ok {
						reads.Add(1)

						if want := valueOf(i); !equalInpoints(want, got) {
							fail("Get returned a foreign or torn record for key %d", i)
						}
					}
				case 1:
					if b, found, err := m.AppendMetaInpoints(nil, arenaHash(i)); found {
						reads.Add(1)

						if err != nil || string(b) != string(wire(i)) {
							fail("AppendMetaInpoints returned a foreign or torn record for key %d (err %v)", i, err)
						}
					}
				default:
					dstMu.Lock()
					dst.Clear()
					dstMu.Unlock()

					if moved, found := m.MoveTo(dst, arenaHash(i)); found && moved {
						reads.Add(1)

						if got, ok := dst.Get(arenaHash(i)); ok && !equalInpoints(valueOf(i), got) {
							fail("MoveTo copied a foreign or torn record for key %d", i)
						}
					}
				}
			}
		}(w)
	}

	for gen := 0; gen < generations; gen++ {
		for i := uint64(0); i < keys; i++ {
			m.Set(arenaHash(i), valueOf(i))
		}

		m.Clear()
	}

	stop.Store(true)
	wg.Wait()

	require.NotZero(t, reads.Load(), "the readers never hit a record, the test proves nothing")
}

func equalInpoints(a, b *subtreepkg.TxInpoints) bool {
	if a == nil || b == nil {
		return a == b
	}

	x, errX := a.Serialize()
	y, errY := b.Serialize()

	return errX == nil && errY == nil && string(x) == string(y) && a.SubtreeIndex == b.SubtreeIndex
}

// TestSplitTxInpointsMap_ConcurrentWritersAndReaders covers the ingest path:
// writers insert disjoint and overlapping keys while readers look them up.
func TestSplitTxInpointsMap_ConcurrentWritersAndReaders(t *testing.T) {
	m := NewSplitTxInpointsMap(256)

	var (
		wg       sync.WaitGroup
		inserted atomic.Int64
	)

	for w := uint64(0); w < 8; w++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for i := uint64(0); i < 5000; i++ { // every writer tries every key
				if _, set := m.SetIfNotExists(arenaHash(i), buildInpoints(shapeThreeParents, i)); set {
					inserted.Add(1)
				}

				if got, ok := m.Get(arenaHash(i)); ok {
					if !equalInpoints(buildInpoints(shapeThreeParents, i), got) {
						t.Errorf("wrong record for key %d", i)
						return
					}
				}
			}
		}()
	}

	wg.Wait()

	require.Equal(t, int64(5000), inserted.Load(), "exactly one writer wins each key")
	require.Equal(t, 5000, m.Length())
}

// FuzzSplitTxInpointsMapModel compares the map with a Go map under an operation
// sequence taken from the fuzz input.
func FuzzSplitTxInpointsMapModel(f *testing.F) {
	f.Add([]byte{0, 1, 3, 1, 2, 4, 2, 1, 0, 5, 0, 0, 6, 1, 1})
	f.Add([]byte{1, 0, 5, 1, 0, 6, 2, 0, 0, 3, 0, 0, 4, 0, 0})

	f.Fuzz(func(t *testing.T, data []byte) {
		m := NewSplitTxInpointsMap(3)
		model := make(map[chainhash.Hash]*subtreepkg.TxInpoints)

		for len(data) >= 3 {
			op, key, shape := data[0]%7, arenaHash(uint64(data[1]%16)), inpointsShape(int(data[2])%int(numShapes))
			data = data[3:]

			if shape == shapeManyVouts || shape == shapeHugeParentCount || shape == shapeZeroParentsVouts {
				shape = shapeThreeParents
			}

			p := buildInpoints(shape, uint64(len(data)))

			switch op {
			case 0:
				m.Set(key, p)
				model[key] = p
			case 1:
				_, set := m.SetIfNotExists(key, p)
				_, had := model[key]
				require.Equal(t, !had, set)

				if !had {
					model[key] = p
				}
			case 2:
				_, had := model[key]
				require.Equal(t, had, m.Delete(key))
				delete(model, key)
			case 3:
				got, ok := m.Get(key)
				want, had := model[key]
				require.Equal(t, had, ok)

				if had {
					require.Equal(t, want, got)
				}
			case 4:
				_, had := model[key]
				require.Equal(t, had, m.Exists(key))
			case 5:
				m.Clear()
				clear(model)
			case 6:
				dst := NewSplitTxInpointsMap(5)
				moved, found := m.MoveTo(dst, key)
				want, had := model[key]
				require.Equal(t, had, found)
				require.Equal(t, had, moved)

				if had {
					got, ok := dst.Get(key)
					require.True(t, ok)
					require.Equal(t, want, got)
				}
			}

			require.Equal(t, len(model), m.Length())
		}
	})
}

// TestSplitTxInpointsMap_EncodingMatchesSerializeOnMalformedPackedVouts pins the
// stored wire bytes to TxInpoints.Serialize for packed vouts that break the
// NewTxInpointsFromPacked contract: slices of a shared buffer whose cap runs
// into the next tx, trailing words, too few words, and vouts without parents.
// Whatever Serialize returns, bytes or error, is what AppendSerialized returns,
// and the neighbour's words never leak into the record.
func TestSplitTxInpointsMap_EncodingMatchesSerializeOnMalformedPackedVouts(t *testing.T) {
	for _, shape := range []inpointsShape{shapeTrailingWords, shapeSharedBufferWide, shapeShortVouts, shapeSharedBufferOK, shapeZeroParentsVouts, shapeNoVouts} {
		for _, parents := range []int{0, 1, 16, 17, 100} {
			m := NewSplitTxInpointsMap(4)
			h := arenaHash(uint64(shape))
			p := buildInpoints(shape, 1)

			if parents > 1 && len(p.ParentTxHashes) == 1 && shape != shapeNoVouts {
				// many parents, the same malformed words: pad the hashes
				for k := 1; k < parents; k++ {
					p.ParentTxHashes = append(p.ParentTxHashes, arenaHash(uint64(100+k)))
				}
			}

			m.Set(h, p)

			want, serErr := p.Serialize()
			got, found, err := m.AppendSerialized(nil, h)
			require.True(t, found)

			if serErr != nil {
				require.Error(t, err, "shape %d parents %d: Serialize fails so AppendSerialized must", shape, parents)
				continue
			}

			require.NoError(t, err, "shape %d parents %d", shape, parents)
			require.Equal(t, want, got, "shape %d parents %d", shape, parents)

			// and it reads back as what was stored
			back, ok := m.Get(h)
			require.True(t, ok)
			wantAgain, _ := back.Serialize()
			require.Equal(t, want, wantAgain, "shape %d parents %d", shape, parents)
		}
	}
}

// TestSplitTxInpointsMap_SharedBufferNeighbourDoesNotLeak stores a tx whose
// vouts are a slice of a buffer shared with the next tx, then changes the
// neighbour: the stored record must not move.
func TestSplitTxInpointsMap_SharedBufferNeighbourDoesNotLeak(t *testing.T) {
	m := NewSplitTxInpointsMap(4)
	buf := []uint32{2, 7, 1, 5, 1, 6}
	p := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{arenaHash(1)}, buf[0:2])

	m.Set(arenaHash(0), &p)

	before, _, err := m.AppendSerialized(nil, arenaHash(0))
	require.NoError(t, err)

	buf[2], buf[3] = 99, 98

	after, _, err := m.AppendSerialized(nil, arenaHash(0))
	require.NoError(t, err)
	require.Equal(t, before, after)

	want, err := p.Serialize()
	require.NoError(t, err)
	require.Equal(t, want, before[:len(want)], "the record is what Serialize wrote when it was stored")
}

// TestSplitTxInpointsMap_SetEncodesInPlace pins that a record is written straight
// into the arena whatever the parent count: no Serialize buffer per tx. Chunk
// growth is amortized: a 64-parent record is ~2.6 KB, so a 64 KiB chunk
// (txInpointsMaxChunk) holds ~25 of them and AllocsPerRun's integer average is
// 0, where a Serialize buffer per Set makes it at least 1.
func TestSplitTxInpointsMap_SetEncodesInPlace(t *testing.T) {
	for _, parents := range []int{1, 3, 64} {
		hashes := make([]chainhash.Hash, parents)
		vouts := make([]uint32, 0, 2*parents)

		for k := range hashes {
			hashes[k] = arenaHash(uint64(1000 + k))
			vouts = append(vouts, 1, uint32(k)) //nolint:gosec // test data
		}

		p := subtreepkg.NewTxInpointsFromPacked(hashes, vouts)
		m := NewSplitTxInpointsMap(1)
		h := arenaHash(1)

		m.Set(h, &p)

		allocs := testing.AllocsPerRun(1000, func() { m.Set(h, &p) })
		require.Zero(t, allocs, "parents %d", parents)

		want, err := p.Serialize()
		require.NoError(t, err)

		got, found, err := m.AppendSerialized(nil, h)
		require.True(t, found)
		require.NoError(t, err)
		require.Equal(t, want, got, "parents %d", parents)
	}
}

// TestSplitTxInpointsMap_ClearTrimsToTheLastFill pins that a half does not keep
// the arena of a past spike for ever: a big fill and Clear keeps what the fill
// used, a small fill and Clear after it drops the rest.
func TestSplitTxInpointsMap_ClearTrimsToTheLastFill(t *testing.T) {
	m := NewSplitTxInpointsMap(8)

	fill := func(from, to int) {
		for i := from; i < to; i++ {
			m.Set(arenaHash(uint64(i)), buildInpoints(shapeThreeParents, uint64(i)))
		}
	}

	fill(0, 20_000)
	big := m.retainedBytes()
	require.Greater(t, big, 1<<20)

	m.Clear()
	require.Equal(t, 0, m.usedBytes())
	require.GreaterOrEqual(t, m.retainedBytes(), big/2, "the generation that was just used is kept for the next fill")

	fill(0, 100)
	m.Clear()
	small := m.retainedBytes()
	require.Less(t, small, big/10, "chunks the last generation did not touch are released")

	// and the map still works after trimming, including a record bigger than any chunk
	m.Set(arenaHash(1), buildInpoints(shapeManyVouts, 1))
	got, ok := m.Get(arenaHash(1))
	require.True(t, ok)
	require.Equal(t, buildInpoints(shapeManyVouts, 1), got)

	m.Clear()
	m.Clear()

	for i := range m.buckets {
		for _, c := range m.buckets[i].chunks {
			require.LessOrEqual(t, cap(c), txInpointsMaxChunk, "an oversized single-record chunk is not kept")
		}
	}

	fill(0, 5000)

	for i := 0; i < 5000; i++ {
		got, ok := m.Get(arenaHash(uint64(i)))
		require.True(t, ok)
		require.Equal(t, buildInpoints(shapeThreeParents, uint64(i)), got)
	}
}

// TestReChainSubtrees_DoesNotGrowTheArena pins that rebuilding the chain re-uses
// the stored records, so the arena stays the size it was whatever is rechained.
func TestReChainSubtrees_DoesNotGrowTheArena(t *testing.T) {
	stp, _, newSubtreeChan := newDeferredDrainProcessor(t, nil)

	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	const txs = 200

	want := make(map[chainhash.Hash]*subtreepkg.TxInpoints)

	for i := uint64(0); i < txs; i++ {
		p := buildInpoints(shapeThreeParents, i)
		h := arenaHash(i)
		want[h] = p

		require.NoError(t, stp.addNode(subtreepkg.Node{Hash: h, Fee: 1, SizeInBytes: 250}, p, true))
	}

	require.Greater(t, len(stp.chainedSubtrees), 4)

	split := stp.currentTxMap.(*SplitTxInpointsMap)
	before := split.usedBytes()
	require.Positive(t, before)

	for _, from := range []int{3, 0, 1} {
		require.NoError(t, stp.reChainSubtrees(from))
		require.Equal(t, before, split.usedBytes(), "rechain from %d must not add records", from)
		require.Equal(t, txs, split.Length())
	}

	// the same txs in the same order are in the subtrees and the map still has their inpoints
	require.Len(t, subtreeHashes(stp), txs+1) // + the coinbase placeholder

	for h, p := range want {
		got, ok := split.Get(h)
		require.True(t, ok)
		require.Equal(t, p, got)
	}

	// K removals rechain K times: still nothing added
	for i := uint64(0); i < 10; i++ {
		require.NoError(t, stp.removeTxFromSubtrees(context.Background(), arenaHash(i*7)))
		require.Equal(t, before, split.usedBytes(), "removal %d must not add records", i)
	}

	require.Equal(t, txs-10, split.Length())
}

// TestMoveBucketFrom_FailureLeavesNothingBehindAfterSwapBack pins the rollback
// contract: a move that stops at a hash the source lacks may have inserted the
// hashes before it, and the reset path then drops the whole half.
func TestMoveBucketFrom_FailureLeavesNothingBehindAfterSwapBack(t *testing.T) {
	stp := newRemainderFanOutProcessor(t)

	src := stp.currentTxMap.(*SplitTxInpointsMap)
	for i := uint64(0); i < 50; i++ {
		src.Set(arenaHash(i), buildInpoints(shapeOneParent, i))
	}

	// resetSubtreeState's swap: the filled half becomes the shadow
	stp.currentTxMap, stp.currentTxMapShadow = stp.currentTxMapShadow, src

	nodes := []subtreepkg.Node{}
	for i := uint64(0); i < 50; i++ {
		nodes = append(nodes, subtreepkg.Node{Hash: arenaHash(i)})
	}

	nodes = append(nodes, subtreepkg.Node{Hash: arenaHash(1 << 50)}) // not in the source

	err := stp.parallelGetAndSetIfNotExists(nodes, src, 0, 4, make([]bool, len(nodes)))
	require.Error(t, err)

	stp.swapCurrentTxMapBack()

	require.Same(t, src, stp.currentTxMap, "the pre-reset half is current again")
	require.Equal(t, 50, src.Length())
	require.Zero(t, stp.currentTxMapShadow.Length(), "the half the failed move wrote into is dropped")
}

// TestWriteRecord_PanicsWhenInpointsChangedAfterSizing pins that a record whose
// inpoints changed between payloadOf and writeRecord is never stored silently.
func TestWriteRecord_PanicsWhenInpointsChangedAfterSizing(t *testing.T) {
	small := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{arenaHash(1)}, []uint32{1, 0})
	large := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{arenaHash(1), arenaHash(2)}, []uint32{1, 0, 2, 1, 2})
	invalid := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{arenaHash(1)}, nil)

	payload, flags := payloadOf(&small)

	for name, changed := range map[string]*subtreepkg.TxInpoints{"grown": &large, "invalid": &invalid} {
		rec := make([]byte, txInpointsRecordHeader+payload, 1024)
		require.Panics(t, func() { writeRecord(rec, changed, flags) }, name)
	}

	payload, flags = payloadOf(&large)
	rec := make([]byte, txInpointsRecordHeader+payload)
	require.Panics(t, func() { writeRecord(rec, &small, flags) }, "shrunk")

	rec = make([]byte, txInpointsRecordHeader+payload)
	require.NotPanics(t, func() { writeRecord(rec, &large, flags) })

	want, err := large.Serialize()
	require.NoError(t, err)
	require.Equal(t, want, rec[txInpointsRecordHeader:])
}

// A rechain over a tx the in-memory map no longer holds fails instead of adding
// a node with no inpoints; addNode's own check is what catches it.
func TestReChainSubtrees_InMemoryTxMissingFromMap(t *testing.T) {
	stp, _, newSubtreeChan := newDeferredDrainProcessor(t, nil)

	go func() {
		for req := range newSubtreeChan {
			if req.ErrChan != nil {
				req.ErrChan <- nil
			}
		}
	}()

	for i := uint64(0); i < 20; i++ {
		require.NoError(t, stp.addNode(subtreepkg.Node{Hash: arenaHash(i), Fee: 1, SizeInBytes: 250}, buildInpoints(shapeThreeParents, i), true))
	}

	split := stp.currentTxMap.(*SplitTxInpointsMap)
	require.True(t, split.Delete(arenaHash(5)))

	err := stp.reChainSubtrees(0)
	require.Error(t, err)
	require.Contains(t, err.Error(), arenaHash(5).String())
}

// A record whose inpoints change while it is stored panics, and block assembly
// recovers panics: the bucket lock must be released on the way out, or the
// rollback's Clear waits on it forever.
func TestSplitTxInpointsMap_PanicWhileStoringReleasesTheBucket(t *testing.T) {
	grow := func(p *subtreepkg.TxInpoints) {
		grown := subtreepkg.NewTxInpointsFromPacked(append(p.ParentTxHashes, arenaHash(1<<40)), []uint32{1, 0, 1, 0})
		*p = grown
	}

	inserts := map[string]func(m *SplitTxInpointsMap, h chainhash.Hash, p *subtreepkg.TxInpoints){
		"Set": func(m *SplitTxInpointsMap, h chainhash.Hash, p *subtreepkg.TxInpoints) { m.Set(h, p) },
		"SetIfNotExists": func(m *SplitTxInpointsMap, h chainhash.Hash, p *subtreepkg.TxInpoints) {
			m.SetIfNotExists(h, p)
		},
		"PutMultiBucketTxInpoints": func(m *SplitTxInpointsMap, h chainhash.Hash, p *subtreepkg.TxInpoints) {
			m.PutMultiBucketTxInpoints(m.BucketFor(h), []chainhash.Hash{h}, []*subtreepkg.TxInpoints{p})
		},
	}

	for name, insert := range inserts {
		t.Run(name, func(t *testing.T) {
			testHookAfterSizing = grow
			t.Cleanup(func() { testHookAfterSizing = nil })

			m := NewSplitTxInpointsMap(4)
			p := subtreepkg.NewTxInpointsFromPacked([]chainhash.Hash{arenaHash(7)}, []uint32{1, 0})

			require.Panics(t, func() { insert(m, arenaHash(1), &p) })

			testHookAfterSizing = nil

			cleared := make(chan struct{})

			go func() {
				m.Clear()
				close(cleared)
			}()

			select {
			case <-cleared:
			case <-time.After(5 * time.Second):
				t.Fatal("Clear is stuck on the bucket the panic left locked")
			}

			require.False(t, m.Exists(arenaHash(1)), "the record that panicked was not stored")
		})
	}
}
