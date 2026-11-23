package subtreeprocessor

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/util/lane"
	"github.com/stretchr/testify/require"
)

// At utxostore_laneMigration shadow block assembly keeps each pooled transaction's own lane
// position with its inpoints, so the lane bitmaps of every subtree it seals, and of every subtree
// it rebuilds, hold its members' positions. These tests pin that on both pool maps, and that a
// transaction kept without a position is stored exactly as without lanes.

var lanePositionSample = lane.Pack(7, 90_000)

func laneTestInpoints(seed uint64) *subtreepkg.TxInpoints {
	parents := []chainhash.Hash{arenaHash(1<<50 + seed), arenaHash(1<<51 + seed)}
	p := subtreepkg.NewTxInpointsFromPacked(parents, []uint32{1, 0, 2, 3, 4})

	return &p
}

// laneTestMaps is one of each pool map, built fresh by each call.
func laneTestMaps(t *testing.T) map[string]TxInpointsMap {
	t.Helper()

	disk, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = disk.Close() })

	return map[string]TxInpointsMap{"split": NewSplitTxInpointsMap(16), "disk": disk}
}

func TestTxMapKeepsLanePositionWithInpoints(t *testing.T) {
	for name := range laneTestMaps(t) {
		t.Run(name, func(t *testing.T) {
			m := laneTestMaps(t)[name]

			with, without, setAt := arenaHash(1), arenaHash(2), arenaHash(3)
			inpoints := laneTestInpoints(1)

			_, inserted := m.SetIfNotExistsAt(with, inpoints, lanePositionSample)
			require.True(t, inserted)

			_, inserted = m.SetIfNotExistsAt(without, inpoints, 0)
			require.True(t, inserted)

			m.SetAt(setAt, inpoints, lane.Pack(255, lane.MaxPosition))

			require.Equal(t, lanePositionSample, m.Position(with))
			require.Zero(t, m.Position(without), "a tx kept without a position has none")
			require.Equal(t, lane.Pack(255, lane.MaxPosition), m.Position(setAt), "every bit of the position comes back")
			require.Zero(t, m.Position(arenaHash(99)), "a tx the map does not hold has none")

			for _, hash := range []chainhash.Hash{with, without, setAt} {
				got, found := m.Get(hash)
				require.True(t, found)
				require.Equal(t, inpoints.ParentTxHashes, got.ParentTxHashes, "the position does not change the inpoints")

				want, err := inpoints.Serialize()
				require.NoError(t, err)

				gotBytes, err := got.Serialize()
				require.NoError(t, err)
				require.Equal(t, want, gotBytes)
			}

			_, inserted = m.SetIfNotExistsAt(with, inpoints, lane.Pack(9, 1))
			require.False(t, inserted, "a duplicate is not inserted")
			require.Equal(t, lanePositionSample, m.Position(with), "a duplicate keeps the position the tx has")
		})
	}
}

// TestSplitTxInpointsMapRecordWithoutPositionIsUnchanged: a record kept without a position is the
// record SetIfNotExists writes, byte for byte, so at off nothing in the map changes.
func TestSplitTxInpointsMapRecordWithoutPositionIsUnchanged(t *testing.T) {
	plain, positioned := NewSplitTxInpointsMap(1), NewSplitTxInpointsMap(1)
	hash := arenaHash(5)
	inpoints := laneTestInpoints(5)

	plain.SetIfNotExists(hash, inpoints)
	positioned.SetIfNotExistsAt(hash, inpoints, 0)

	plainRec, ok := plain.bucket(hash).copyRecord(nil, hash)
	require.True(t, ok)

	zeroRec, ok := positioned.bucket(hash).copyRecord(nil, hash)
	require.True(t, ok)
	require.Equal(t, plainRec, zeroRec)

	withPosition := NewSplitTxInpointsMap(1)
	withPosition.SetIfNotExistsAt(hash, inpoints, lanePositionSample)

	rec, ok := withPosition.bucket(hash).copyRecord(nil, hash)
	require.True(t, ok)
	require.Len(t, rec, len(plainRec)+txInpointsPositionSize, "a position costs eight bytes")
	require.Equal(t, plainRec[3:], rec[3:len(plainRec)], "the payload and its length are as without lanes")

	plainMeta, found, err := plain.AppendMetaInpoints(nil, hash)
	require.NoError(t, err)
	require.True(t, found)

	meta, position, found, err := withPosition.AppendMetaInpointsAt(nil, hash)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, plainMeta, meta, "the subtree meta bytes leave the position out")
	require.Equal(t, lanePositionSample, position)

	serialized, found, err := withPosition.AppendSerialized(nil, hash)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, plainMeta, serialized)
}

// TestDiskTxMapRecordWithoutPositionIsUnchanged: a record kept without a position starts with the
// record magic it always did, and one with a position with the positioned magic.
func TestDiskTxMapRecordWithoutPositionIsUnchanged(t *testing.T) {
	m, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
	require.NoError(t, err)

	defer func() { _ = m.Close() }()

	plain, positioned := arenaHash(6), arenaHash(7)
	inpoints := laneTestInpoints(6)

	m.SetIfNotExists(plain, inpoints)
	m.SetIfNotExistsAt(positioned, inpoints, lanePositionSample)

	want, err := inpoints.Serialize()
	require.NoError(t, err)

	for hash, magic := range map[chainhash.Hash]byte{plain: recordMagic, positioned: recordMagicPositioned} {
		s, g, entry, ok := m.pinEntry(hash)
		require.True(t, ok)

		payload, gotMagic, err := readRecord(g, g.logOf(hash), entryOffset(entry))
		unpin(s, g)
		require.NoError(t, err)
		require.Equal(t, magic, gotMagic)

		serialized, _ := splitPositioned(payload, gotMagic)
		require.Equal(t, want, serialized)
	}

	got, position, found, err := m.GetAtWithErr(positioned)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, lanePositionSample, position)
	require.Equal(t, inpoints.ParentTxHashes, got.ParentTxHashes)

	_, position, found, err = m.GetAtWithErr(plain)
	require.NoError(t, err)
	require.True(t, found)
	require.Zero(t, position)
}

// TestTxMapCopiesCarryLanePosition: the copies between the two halves of the double buffer, which
// move every pooled tx after a block, keep each tx's position.
func TestTxMapCopiesCarryLanePosition(t *testing.T) {
	hashes := []chainhash.Hash{arenaHash(10), arenaHash(11), arenaHash(12)}
	positions := []uint64{lane.Pack(3, 1), 0, lane.Pack(200, 77)}

	t.Run("split MoveTo and MoveBucketFrom", func(t *testing.T) {
		src := NewSplitTxInpointsMap(4)
		for i, hash := range hashes {
			src.SetAt(hash, laneTestInpoints(uint64(i)), positions[i])
		}

		dst := NewSplitTxInpointsMap(4)
		moved, found := src.MoveTo(dst, hashes[0])
		require.True(t, moved)
		require.True(t, found)
		require.Equal(t, positions[0], dst.Position(hashes[0]))

		for i, hash := range hashes[1:] {
			bucketDst := NewSplitTxInpointsMap(4)
			inserted := make([]bool, 1)
			require.Equal(t, -1, bucketDst.MoveBucketFrom(src, bucketDst.BucketFor(hash), []chainhash.Hash{hash}, inserted))
			require.True(t, inserted[0])
			require.Equal(t, positions[i+1], bucketDst.Position(hash))
		}
	})

	t.Run("split bulk inserts", func(t *testing.T) {
		inpoints := []*subtreepkg.TxInpoints{laneTestInpoints(0), laneTestInpoints(1), laneTestInpoints(2)}

		bulk := NewSplitTxInpointsMap(4)
		wasSet := make([]bool, len(hashes))
		bulk.ParallelBulkSetIfNotExistsAt(hashes, inpoints, positions, wasSet)

		for i, hash := range hashes {
			require.True(t, wasSet[i])
			require.Equal(t, positions[i], bulk.Position(hash))
		}

		bucketed := NewSplitTxInpointsMap(1)
		inserted := bucketed.PutMultiBucketTxInpointsAt(0, hashes, inpoints, positions)

		for i, hash := range hashes {
			require.True(t, inserted[i])
			require.Equal(t, positions[i], bucketed.Position(hash))
		}
	})

	t.Run("disk MoveFrom", func(t *testing.T) {
		src, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
		require.NoError(t, err)

		defer func() { _ = src.Close() }()

		dst, err := NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{t.TempDir()}})
		require.NoError(t, err)

		defer func() { _ = dst.Close() }()

		for i, hash := range hashes {
			src.SetAt(hash, laneTestInpoints(uint64(i)), positions[i])
		}

		wasSet := make([]bool, len(hashes))
		missing, err := dst.MoveFrom(src, hashes, wasSet)
		require.NoError(t, err)
		require.Equal(t, -1, missing)

		for i, hash := range hashes {
			require.True(t, wasSet[i])
			require.Equal(t, positions[i], dst.Position(hash))

			got, found := dst.Get(hash)
			require.True(t, found)
			require.Equal(t, laneTestInpoints(uint64(i)).ParentTxHashes, got.ParentTxHashes)
		}
	})
}

// BenchmarkTxMapLanePositionFootprint fills each pool map with a million txs, without and with a
// lane position each, and reports what the pool keeps per tx: heap bytes and, for the disk map,
// bytes written to its log files. The difference is the cost of keeping positions at
// utxostore_laneMigration shadow. Run with -benchtime=1x.
//
// The in-memory map's heap moves in steps, because each of its buckets grows its arena by doubling
// chunks: arenaUsedB/tx is what the records take, arenaCapB/tx what the chunks hold, so a fill
// level that tips a bucket into its next chunk shows more than the eight bytes a position adds.
func BenchmarkTxMapLanePositionFootprint(b *testing.B) {
	for _, total := range []int{1 << 20, 4 << 20} {
		for _, disk := range []bool{false, true} {
			for _, lanes := range []bool{false, true} {
				name := fmt.Sprintf("%dM/", total>>20) + map[bool]string{false: "split", true: "disk"}[disk] + map[bool]string{false: "/off", true: "/shadow"}[lanes]

				b.Run(name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						baseBytes, _ := heapStats()

						var (
							m    TxInpointsMap
							dm   *DiskTxMap
							fill func(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints, position uint64)
						)

						if disk {
							var err error

							dm, err = NewDiskTxMap(DiskTxMapOptions{BasePaths: []string{b.TempDir()}})
							require.NoError(b, err)

							m = dm
						} else {
							m = NewSplitTxInpointsMap(16384)
						}

						fill = func(hash chainhash.Hash, inpoints *subtreepkg.TxInpoints, position uint64) {
							m.SetIfNotExistsAt(hash, inpoints, position)
						}

						for j := 0; j < total; j++ {
							var position uint64
							if lanes {
								position = lane.Pack(uint8(j%17), uint64(j/17+1)) //nolint:gosec // j%17 < 17
							}

							fill(benchTxHash(j), benchTxInpoints(j), position)
						}

						bytes, _ := heapStats()
						b.ReportMetric(float64(bytes-baseBytes)/float64(total), "heapB/tx")

						if dm != nil {
							require.NoError(b, dm.Flush())
							b.ReportMetric(float64(dm.bytesWritten.Load())/float64(total), "diskB/tx")
							_ = dm.Close()
						}

						if split, ok := m.(*SplitTxInpointsMap); ok {
							b.ReportMetric(float64(split.usedBytes())/float64(total), "arenaUsedB/tx")
							b.ReportMetric(float64(split.retainedBytes())/float64(total), "arenaCapB/tx")
						}

						runtime.KeepAlive(m)
					}
				})
			}
		}
	}
}
