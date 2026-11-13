package txmetacache

import (
	"encoding/binary"
	"math/bits"
	"sync/atomic"
	"unsafe"
)

// This file holds what LaneCache (lockfree_lanes.go) is built from: the record
// format, its checksum, and the index slot helpers.
//
// A record is [8B header][32B key][value, zero-padded to 8 bytes]. The header
// is [48-bit checksum | 16-bit value length]; the checksum covers key and
// value, so a reader that copied a record which was being overwritten, or
// which a late writer damaged, rejects the copy. A torn copy passes only if it
// hashes to the stored checksum, about one chance in 2^48.

const (
	lfHeaderBytes = 8
	lfKeyBytes    = 32
	lfSlotsPerSet = 8

	// The header word is [48-bit checksum | 16-bit value length].
	lfLenMask = 1<<16 - 1

	// lfBytesPerSlot sizes the index: one slot per 64 ring bytes, about twice
	// the number of records a ring holds (a typical record is 100-200 bytes),
	// so 8-way sets rarely have to evict a live entry.
	lfBytesPerSlot = 64

	// lfMaxCASRetries bounds an insert's slot-publish loop. Losing that many
	// races in a row means the set is extremely hot; the record is then left
	// unindexed, which a cache may do.
	lfMaxCASRetries = 16
)

// lfPrefetchGroup is how many lookups GetMulti and inserts SetMulti overlap.
// Each lookup is two dependent cache misses (index set, then record) on a large
// cache; touching a group's sets, then its records, before using them lets
// those misses overlap instead of being taken one after another.
const lfPrefetchGroup = 16

// lfPrefetchSink keeps the compiler from discarding prefetch loads.
var lfPrefetchSink atomic.Uint64

const maxInt = int(^uint(0) >> 1)

func nextPow2(v uint64) uint64 {
	if v <= 1 {
		return 1
	}

	return 1 << bits.Len64(v-1)
}

// fmix64 is the murmur3 64-bit finaliser.
func fmix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33

	return h
}

func slotAt(set unsafe.Pointer, i int) *uint64 {
	return (*uint64)(unsafe.Add(set, i*8))
}

// The record checksum folds the key words and the value words (the last one
// zero-padded) through checksumStep, one word at a time, so writer and reader
// compute it in the same pass that stores or copies the words. Each step is a
// bijection of the running hash for a fixed word and of the word for a fixed
// hash, so changing any single word always changes the result; the header
// keeps the top 48 bits.
const checksumInit = 0x9e3779b97f4a7c15

func checksumStep(h, w uint64) uint64 {
	return bits.RotateLeft64(h^(w*0xbf58476d1ce4e5b9), 31) * 0x94d049bb133111eb
}

func checksumFinal(h, valueLen uint64) uint64 {
	return fmix64(h^valueLen) &^ lfLenMask
}

func keyChecksum(key []byte) uint64 {
	h := uint64(checksumInit)
	for w := 0; w < 4; w++ {
		h = checksumStep(h, binary.LittleEndian.Uint64(key[w*8:]))
	}

	return h
}

func recordBytes(valueLen int) uint64 {
	return lfHeaderBytes + lfKeyBytes + (uint64(valueLen)+7)&^7 //nolint:gosec // valueLen < 2KB
}
