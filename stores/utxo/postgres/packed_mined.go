package postgres

import "encoding/binary"

// minedRecordSize is the byte width of one packed mined-block record in the
// txs.mined_info bytea: block_id int4 BE, height int4 BE, subtree_idx int4 BE.
// The three values that used to live in the parallel block_ids / block_heights /
// subtree_idxs INT[] columns are now a single fixed-stride bytea — 13 bytes for
// the 1-element common case (1-byte varlena header + 12) versus ~75 bytes for
// three single-element INT[] arrays (each array pays a 24-byte header). The
// column is deliberately UNINDEXED so the mine-path UPDATE stays HOT (see the
// txs DDL comment in schema.go).
const minedRecordSize = 12

// packMinedInfo encodes the parallel mined-block slices into the flat 12-byte-
// record bytea. It returns nil for an empty set so the caller binds SQL NULL
// (matching the pre-packing "no mined block" representation). subtree_idx is
// stored as an int4 bit-cast so a (theoretical) negative index round-trips.
func packMinedInfo(blockIDs, blockHeights []uint32, subtreeIdxs []int) []byte {
	n := len(blockIDs)
	if n == 0 {
		return nil
	}

	buf := make([]byte, n*minedRecordSize)
	for i := 0; i < n; i++ {
		off := i * minedRecordSize
		binary.BigEndian.PutUint32(buf[off:], blockIDs[i])

		var h uint32
		if i < len(blockHeights) {
			h = blockHeights[i]
		}
		binary.BigEndian.PutUint32(buf[off+4:], h)

		var si int
		if i < len(subtreeIdxs) {
			si = subtreeIdxs[i]
		}
		binary.BigEndian.PutUint32(buf[off+8:], uint32(int32(si)))
	}

	return buf
}

// decodeMinedInfo decodes the flat mined_info bytea back into the three parallel
// slices the Go API surface (meta.Data) exposes. A nil/empty/short-tail bytea
// yields nil slices. Any trailing bytes shorter than a full record are ignored
// (a malformed row degrades to "fewer records" rather than panicking).
func decodeMinedInfo(b []byte) (blockIDs, blockHeights []uint32, subtreeIdxs []int) {
	n := len(b) / minedRecordSize
	if n == 0 {
		return nil, nil, nil
	}

	blockIDs = make([]uint32, n)
	blockHeights = make([]uint32, n)
	subtreeIdxs = make([]int, n)
	for i := 0; i < n; i++ {
		off := i * minedRecordSize
		blockIDs[i] = binary.BigEndian.Uint32(b[off:])
		blockHeights[i] = binary.BigEndian.Uint32(b[off+4:])
		subtreeIdxs[i] = int(int32(binary.BigEndian.Uint32(b[off+8:])))
	}

	return blockIDs, blockHeights, subtreeIdxs
}

// decodeMinedBlockIDs decodes only the block_id field of each 12-byte record —
// the SetMinedMulti / UnsetMined result map and the unmined iterators need just
// the block-id list, not the parallel heights/subtree indexes.
func decodeMinedBlockIDs(b []byte) []uint32 {
	n := len(b) / minedRecordSize
	if n == 0 {
		return nil
	}

	ids := make([]uint32, n)
	for i := 0; i < n; i++ {
		ids[i] = binary.BigEndian.Uint32(b[i*minedRecordSize:])
	}

	return ids
}
