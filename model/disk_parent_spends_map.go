package model

import (
	"encoding/binary"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/mmaphash"
)

const dpsInpointKeySize = chainhash.HashSize + 4 // 32-byte hash + 4-byte index

// Compile-time check that DiskParentSpendsMap implements ParentSpendsMap.
var _ ParentSpendsMap = (*DiskParentSpendsMap)(nil)

// DiskParentSpendsMap tracks spent inpoints off-heap using one mmap
// open-addressing table per configured disk. It is ephemeral: created per
// block validation, Close()d (and its files reclaimed) when done.
type DiskParentSpendsMap struct {
	tables   []*mmaphash.Table
	numDisks int
}

// DiskParentSpendsMapOptions configures the DiskParentSpendsMap.
type DiskParentSpendsMapOptions struct {
	BasePaths      []string
	Prefix         string
	FilterCapacity uint // expected number of inpoints (named for source compatibility)
}

// NewDiskParentSpendsMap creates one mmap table per base path.
func NewDiskParentSpendsMap(opts DiskParentSpendsMapOptions) (*DiskParentSpendsMap, error) {
	if len(opts.BasePaths) == 0 {
		return nil, errors.NewProcessingError("DiskParentSpendsMap: at least one base path is required")
	}
	// The mmap table is fixed-capacity, so a zero FilterCapacity would build a
	// minimal table that overflows almost immediately. Fail fast instead.
	if opts.FilterCapacity == 0 {
		return nil, errors.NewProcessingError("DiskParentSpendsMap: FilterCapacity must be > 0")
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "disk-parentspends"
	}
	expected := uint64(opts.FilterCapacity)
	perDisk := expected/uint64(len(opts.BasePaths)) + 1

	m := &DiskParentSpendsMap{numDisks: len(opts.BasePaths)}
	for i, path := range opts.BasePaths {
		tbl, err := mmaphash.New(mmaphash.Options{
			Dir:       path,
			Prefix:    prefix,
			KeySize:   dpsInpointKeySize,
			ValueSize: 0,
			Expected:  perDisk,
		})
		if err != nil {
			for j := 0; j < i; j++ {
				_ = m.tables[j].Close()
			}
			return nil, errors.NewServiceError("DiskParentSpendsMap: failed to create table for disk %d (%s)", i, path, err)
		}
		m.tables = append(m.tables, tbl)
	}
	return m, nil
}

// inpointKey serializes an Inpoint to a fixed-size key array. Returning an
// array (not a slice) keeps it on the caller's stack — no per-call heap alloc.
func inpointKey(inpoint subtreepkg.Inpoint) [dpsInpointKeySize]byte {
	var key [dpsInpointKeySize]byte
	copy(key[:chainhash.HashSize], inpoint.Hash[:])
	binary.BigEndian.PutUint32(key[chainhash.HashSize:], inpoint.Index)
	return key
}

// SetIfNotExists returns inserted=true if the inpoint was newly recorded,
// inserted=false if it already existed. A non-nil error (e.g. the backing
// table is full) is a storage failure that must halt validation — it is never
// reclassified as a duplicate.
func (m *DiskParentSpendsMap) SetIfNotExists(inpoint subtreepkg.Inpoint) (bool, error) {
	key := inpointKey(inpoint)
	// Route to a disk using bytes disjoint from the table's bucket window
	// (key[0:8]) and segment window (key[8:16]) so disk selection does not
	// correlate with intra-table slot placement.
	disk := int(binary.LittleEndian.Uint16(key[16:18])) % m.numDisks
	_, inserted, err := m.tables[disk].Upsert(key[:], 0)
	if err != nil {
		return false, errors.NewStorageError("DiskParentSpendsMap: inpoint insert failed (table capacity exceeded?)", err)
	}
	return inserted, nil
}

// Close releases all tables.
func (m *DiskParentSpendsMap) Close() error {
	var lastErr error
	for _, tbl := range m.tables {
		if err := tbl.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Stats returns current metrics for prometheus reporters.
func (m *DiskParentSpendsMap) Stats() DiskMapStats {
	var entries int64
	for _, tbl := range m.tables {
		entries += int64(tbl.Len())
	}
	return DiskMapStats{
		Entries:          entries,
		FilterMemBytes:   0, // no in-RAM filter in the mmap implementation
		DiskBytesWritten: entries * int64(dpsInpointKeySize+1),
	}
}
