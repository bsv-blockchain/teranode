package model

import (
	"cmp"
	"context"
	"hash/maphash"
	"math"
	"slices"
	"sync/atomic"

	subtreepkg "github.com/bsv-blockchain/go-subtree"
	"golang.org/x/sync/errgroup"
)

// Duplicate input detection for block validation.
//
// Two transactions of a block must not spend the same outpoint, and one
// transaction must not spend an outpoint twice. Instead of a hash set holding
// every inpoint (36 byte key plus table overhead, ~110 bytes per input), every
// input is recorded as a short entry: a 64-bit seeded hash of the inpoint and
// the index of its transaction in the block, 12 bytes. Each run (one subtree,
// filled by one goroutine) groups its entries by the top bits of the hash. Once
// every run is in, each partition is sorted on its own and neighbours with equal
// hashes are compared against the real inpoints, which are fetched again only
// for those transactions. The hash decides where to look, never the verdict, so
// the result is exact for any hash, seed or collision.
//
// The reported transaction is the earliest one of the block that spends an
// outpoint already spent by an earlier transaction (or by an earlier input of
// itself): the minimum over all duplicate pairs of the later transaction of the
// pair. That is what a sequential scan of the block reports and it does not
// depend on scheduling, worker count, run layout or seed.

// dedupTxIndex is the type of the in-block transaction index stored in an entry.
// uint32 keeps an entry at 12 bytes; blocks of more than 2^32-1 transactions use
// uint64.
type dedupTxIndex interface{ ~uint32 | ~uint64 }

// inpointDedupHash overrides the seeded hash of the inpoint fingerprints. Nil
// (always, outside tests) means a random seed per block. Tests set it to force
// collisions.
var inpointDedupHash func(subtreepkg.Inpoint) uint64

// dedupMaxPartitionBits bounds the partition count at 4096; ~64K entries per
// partition keeps each sort in cache.
const dedupMaxPartitionBits = 12

// dedupEntry is a 64-bit hash split in two uint32s, so the narrow struct packs
// into 12 bytes with no padding.
type dedupEntry[T dedupTxIndex] struct {
	hi, lo uint32
	tx     T
}

func (e dedupEntry[T]) key() uint64 { return uint64(e.hi)<<32 | uint64(e.lo) }

// dedupRun holds the entries of one run grouped by partition: partition p is
// entries[offs[p]:offs[p+1]].
type dedupRun[T dedupTxIndex] struct {
	entries []dedupEntry[T]
	offs    []int
}

// inpointRunBuilder collects the inputs of one run. It is not safe for
// concurrent use; use one builder per goroutine.
type inpointRunBuilder interface {
	// addTx records the inputs of the transaction at index tx of the block
	addTx(tx uint64, inpoints []subtreepkg.Inpoint)
	// finish hands the run over to the dedup; the builder must not be used after
	finish()
}

// inpointDedupper finds the earliest transaction of a block that spends an
// inpoint spent before.
type inpointDedupper interface {
	// newRun returns the builder for run number run (each run number is used at
	// most once). capHint is the number of entries expected, 0 if unknown.
	newRun(run int, capHint int) inpointRunBuilder
	// detect checks every partition on up to workers goroutines. newResolver is
	// called once per worker and returns a function that gives the inpoints of a
	// transaction; it is only used for transactions whose input hashes collide.
	// It returns the earliest transaction with a duplicate input, if any.
	detect(ctx context.Context, workers int, newResolver func() func(tx uint64) ([]subtreepkg.Inpoint, error)) (uint64, bool, error)
}

type dedupImpl[T dedupTxIndex] struct {
	hash  func(subtreepkg.Inpoint) uint64
	pbits uint
	runs  []dedupRun[T]
}

// newInpointDedup prepares for numRuns runs, about expectedInputs inputs in total
// and transaction indexes below totalTxs. A nil hash uses a random seed; the seed
// only changes the layout, never the result.
func newInpointDedup(numRuns int, expectedInputs, totalTxs uint64, hash func(subtreepkg.Inpoint) uint64) inpointDedupper {
	if hash == nil {
		seed := maphash.MakeSeed()
		hash = func(in subtreepkg.Inpoint) uint64 { return maphash.Comparable(seed, in) }
	}

	// partition count only affects cache behaviour, so a wild estimate is harmless
	pbits := uint(0)
	for pbits < dedupMaxPartitionBits && expectedInputs>>pbits > 1<<16 {
		pbits++
	}

	if totalTxs <= math.MaxUint32 {
		return &dedupImpl[uint32]{hash: hash, pbits: pbits, runs: make([]dedupRun[uint32], numRuns)}
	}

	return &dedupImpl[uint64]{hash: hash, pbits: pbits, runs: make([]dedupRun[uint64], numRuns)}
}

func (d *dedupImpl[T]) partition(key uint64) int {
	if d.pbits == 0 {
		return 0
	}

	return int(key >> (64 - d.pbits)) //nolint:gosec // < 4096
}

type dedupBuilder[T dedupTxIndex] struct {
	d       *dedupImpl[T]
	run     int
	entries []dedupEntry[T]
}

func (d *dedupImpl[T]) newRun(run int, capHint int) inpointRunBuilder {
	return &dedupBuilder[T]{d: d, run: run, entries: make([]dedupEntry[T], 0, max(capHint, 0))}
}

func (rb *dedupBuilder[T]) addTx(tx uint64, inpoints []subtreepkg.Inpoint) {
	for _, in := range inpoints {
		k := rb.d.hash(in)
		rb.entries = append(rb.entries, dedupEntry[T]{hi: uint32(k >> 32), lo: uint32(k), tx: T(tx)}) //nolint:gosec // split of a uint64
	}
}

func (rb *dedupBuilder[T]) finish() {
	d := rb.d
	np := 1 << d.pbits
	offs := make([]int, np+1)

	for _, e := range rb.entries {
		offs[d.partition(e.key())+1]++
	}

	for p := 0; p < np; p++ {
		offs[p+1] += offs[p]
	}

	grouped := make([]dedupEntry[T], len(rb.entries))
	next := slices.Clone(offs[:np])

	for _, e := range rb.entries {
		p := d.partition(e.key())
		grouped[next[p]] = e
		next[p]++
	}

	d.runs[rb.run] = dedupRun[T]{entries: grouped, offs: offs}
	rb.entries = nil
}

func (d *dedupImpl[T]) detect(ctx context.Context, workers int, newResolver func() func(tx uint64) ([]subtreepkg.Inpoint, error)) (uint64, bool, error) {
	const none = ^uint64(0) // no block has 2^64-1 transactions

	var (
		earliest atomic.Uint64
		next     atomic.Int64
	)

	earliest.Store(none)

	partitions := 1 << d.pbits
	workers = max(1, min(workers, partitions))

	g, gCtx := errgroup.WithContext(ctx)

	for w := 0; w < workers; w++ {
		g.Go(func() error {
			var (
				buf     []dedupEntry[T]
				resolve func(tx uint64) ([]subtreepkg.Inpoint, error)
			)

			for {
				p := int(next.Add(1)) - 1
				if p >= partitions {
					return nil
				}

				if err := gCtx.Err(); err != nil {
					return err
				}

				buf = buf[:0]

				for r := range d.runs {
					run := &d.runs[r]
					if run.offs == nil {
						continue // run never finished: no entries
					}

					buf = append(buf, run.entries[run.offs[p]:run.offs[p+1]]...)
				}

				slices.SortFunc(buf, func(a, b dedupEntry[T]) int {
					if c := cmp.Compare(a.key(), b.key()); c != 0 {
						return c
					}

					return cmp.Compare(a.tx, b.tx)
				})

				for i := 0; i < len(buf); {
					j := i + 1
					for j < len(buf) && buf[j].key() == buf[i].key() {
						j++
					}

					// A group of equal hashes is a duplicate or a collision. Its
					// answer is at least its second smallest tx, so a group that
					// cannot beat what is already known is skipped; this only
					// prunes groups that could not change the minimum.
					if j-i >= 2 && uint64(buf[i+1].tx) < earliest.Load() {
						if resolve == nil {
							resolve = newResolver()
						}

						tx, found, err := d.verifyGroup(buf[i:j], resolve)
						if err != nil {
							return err
						}

						if found {
							for {
								cur := earliest.Load()
								if tx >= cur || earliest.CompareAndSwap(cur, tx) {
									break
								}
							}
						}
					}

					i = j
				}
			}
		})
	}

	if err := g.Wait(); err != nil {
		return 0, false, err
	}

	if e := earliest.Load(); e != none {
		return e, true, nil
	}

	return 0, false, nil
}

// verifyGroup settles a group of entries with equal hash, sorted by tx, against
// the real inpoints. It walks the transactions in ascending order and records
// every input that has the group's hash; the first one already recorded is a
// real duplicate, spent by an earlier input of this or an earlier transaction,
// and this transaction is the smallest later one of any duplicate pair of the
// group. Every transaction is resolved once, however large the group.
func (d *dedupImpl[T]) verifyGroup(group []dedupEntry[T], resolve func(tx uint64) ([]subtreepkg.Inpoint, error)) (uint64, bool, error) {
	seen := make(map[subtreepkg.Inpoint]struct{}, len(group))

	key := group[0].key()

	for k, e := range group {
		if k > 0 && e.tx == group[k-1].tx {
			continue // the transaction's inputs were all taken at its first entry
		}

		inpoints, err := resolve(uint64(e.tx))
		if err != nil {
			return 0, false, err
		}

		for _, in := range inpoints {
			if d.hash(in) != key {
				continue
			}

			if _, dup := seen[in]; dup {
				return uint64(e.tx), true, nil
			}

			seen[in] = struct{}{}
		}
	}

	return 0, false, nil
}
