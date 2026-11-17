package model

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/stores/mmaphash"
)

const dtmKeySize = chainhash.HashSize // 32-byte tx hash
const dtmValueSize = 8

// Compile-time check that DiskTxMapUint64 implements txmap.TxMap.
var _ txmap.TxMap = (*DiskTxMapUint64)(nil)

// DiskTxMapUint64 is an off-heap chainhash.Hash -> uint64 map backed by one
// mmap open-addressing table per disk. Ephemeral: per block validation.
type DiskTxMapUint64 struct {
	tables   []*mmaphash.Table
	numDisks int
	frozen   atomic.Bool
}

// DiskTxMapUint64Options configures the DiskTxMapUint64.
type DiskTxMapUint64Options struct {
	BasePaths      []string
	Prefix         string
	FilterCapacity uint // expected number of transactions
}

// NewDiskTxMapUint64 creates one mmap table per base path.
func NewDiskTxMapUint64(opts DiskTxMapUint64Options) (*DiskTxMapUint64, error) {
	if len(opts.BasePaths) == 0 {
		return nil, errors.NewProcessingError("DiskTxMapUint64: at least one base path is required")
	}
	// A zero FilterCapacity is a misconfiguration: the table would start minimal
	// and grow repeatedly from nothing under load. Fail fast so sizing is explicit.
	if opts.FilterCapacity == 0 {
		return nil, errors.NewProcessingError("DiskTxMapUint64: FilterCapacity must be > 0")
	}
	prefix := opts.Prefix
	if prefix == "" {
		prefix = "disk-txmap"
	}
	perDisk := uint64(opts.FilterCapacity)/uint64(len(opts.BasePaths)) + 1

	m := &DiskTxMapUint64{numDisks: len(opts.BasePaths)}
	for i, path := range opts.BasePaths {
		tbl, err := mmaphash.New(mmaphash.Options{
			Dir:       path,
			Prefix:    prefix,
			KeySize:   dtmKeySize,
			ValueSize: dtmValueSize,
			Expected:  perDisk,
		})
		if err != nil {
			for j := 0; j < i; j++ {
				_ = m.tables[j].Close()
			}
			return nil, errors.NewServiceError("DiskTxMapUint64: failed to create table for disk %d (%s)", i, path, err)
		}
		m.tables = append(m.tables, tbl)
	}
	return m, nil
}

// tableFor routes a hash to its disk table using bytes disjoint from the
// table's bucket window (key[0:8]) and segment window (key[8:16]).
func (m *DiskTxMapUint64) tableFor(hash chainhash.Hash) *mmaphash.Table {
	disk := int(binary.LittleEndian.Uint16(hash[16:18])) % m.numDisks
	return m.tables[disk]
}

// Put inserts hash->value, returning errors.ErrTxExists if the hash already
// exists. A table-capacity failure returns a processing error (NOT ErrTxExists),
// so the caller halts instead of misreporting a duplicate.
func (m *DiskTxMapUint64) Put(hash chainhash.Hash, value uint64) error {
	if m.frozen.Load() {
		return txmap.ErrMapFrozen
	}

	_, inserted, err := m.tableFor(hash).Upsert(hash[:], value)
	if err != nil {
		return errors.NewProcessingError("DiskTxMapUint64: Put failed (table capacity exceeded?)", err)
	}
	if !inserted {
		return errors.ErrTxExists
	}
	return nil
}

// Get returns (value, found). The txmap.TxMap interface has no error return,
// so a Lookup error (only possible on an internal misuse such as a wrong key
// size, which cannot happen here since hash is always 32 bytes) is surfaced as
// a miss rather than silently returning a bogus value.
func (m *DiskTxMapUint64) Get(hash chainhash.Hash) (uint64, bool) {
	v, found, err := m.tableFor(hash).Lookup(hash[:])
	if err != nil {
		return 0, false
	}
	return v, found
}

// Exists reports whether the hash is present.
func (m *DiskTxMapUint64) Exists(hash chainhash.Hash) bool {
	_, found := m.Get(hash)
	return found
}

// Length returns the number of entries.
func (m *DiskTxMapUint64) Length() int {
	var n int64
	for _, tbl := range m.tables {
		n += tbl.Len()
	}
	return int(n)
}

// Flush is a no-op: the mmap table is synchronous (no write buffer).
func (m *DiskTxMapUint64) Flush() error { return nil }

// Close releases all tables.
func (m *DiskTxMapUint64) Close() error {
	var lastErr error
	for _, tbl := range m.tables {
		if err := tbl.Close(); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Stats returns current metrics.
func (m *DiskTxMapUint64) Stats() DiskMapStats {
	entries := int64(m.Length())
	return DiskMapStats{
		Entries:          entries,
		FilterMemBytes:   0,
		DiskBytesWritten: entries * int64(1+dtmKeySize+dtmValueSize),
	}
}

// DiskMapStats holds lightweight metrics for a disk-backed map.
type DiskMapStats struct {
	Entries          int64
	FilterMemBytes   int64
	DiskBytesWritten int64
}

// --- txmap.TxMap interface stubs (unused by block validation) ---

func (m *DiskTxMapUint64) Delete(_ chainhash.Hash) error {
	return errors.NewProcessingError("DiskTxMapUint64: Delete not supported")
}
func (m *DiskTxMapUint64) Keys() []chainhash.Hash { return nil }
func (m *DiskTxMapUint64) Set(_ chainhash.Hash, _ uint64) error {
	return errors.NewProcessingError("DiskTxMapUint64: Set not supported")
}
func (m *DiskTxMapUint64) SetIfExists(_ chainhash.Hash, _ uint64) (bool, error) {
	return false, errors.NewProcessingError("DiskTxMapUint64: SetIfExists not supported")
}
func (m *DiskTxMapUint64) SetIfNotExists(_ chainhash.Hash, _ uint64) (bool, error) {
	return false, errors.NewProcessingError("DiskTxMapUint64: SetIfNotExists not supported")
}
func (m *DiskTxMapUint64) PutMulti(_ []chainhash.Hash, _ uint64) error {
	return errors.NewProcessingError("DiskTxMapUint64: PutMulti not supported")
}
func (m *DiskTxMapUint64) Iter(_ func(hash chainhash.Hash, value uint64) bool) {
	// No-op: block validation only uses Put/Get/Exists/Length. Iter exists
	// solely to satisfy the txmap.TxMap interface and is intentionally empty.
}

// Freeze marks the map read-only, matching the txmap.TxMap contract: after
// Freeze, Put returns txmap.ErrMapFrozen. Block validation calls this once the
// duplicate-check write phase is done, before the read-only validation phase.
//
// It is a write guard only: the mmap table's reads are not made lock-free by
// Freeze (the in-memory split maps are the contended read path Freeze targets;
// the disk-backed map is the capacity fallback).
func (m *DiskTxMapUint64) Freeze() {
	m.frozen.Store(true)
}

// Clear lifts a Freeze so the map accepts writes again. It does NOT erase
// stored entries: DiskTxMapUint64 is ephemeral and single-use — block
// validation releases it with Close, never pooled reuse — so a
// content-clearing Clear (as the in-memory maps provide) is unnecessary. Clear
// exists to satisfy txmap.TxMap; do not rely on it to empty the map.
func (m *DiskTxMapUint64) Clear() {
	m.frozen.Store(false)
}
