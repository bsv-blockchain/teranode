package model

import (
	"io"
	"strings"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	subtreepkg "github.com/bsv-blockchain/go-subtree"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
)

// CheckSubtreeSlicesForDuplicateTxs is a MANDATORY consensus security floor —
// not an optimization. It enforces the CVE-2012-2459 duplicate-transaction
// check on any code path that holds the block's subtree slices in memory but
// does not go through Block.Valid (which runs the full pooled/disk-backed
// checkDuplicateTransactions). Every such path MUST call it before creating or
// spending UTXOs.
//
// WHY IT IS REQUIRED (do not remove or gate it on "the block is trusted"):
// the merkle root CANNOT detect a CVE-2012-2459 duplicate. The merkle tree's
// duplicate-last-node-when-odd rule means a block whose trailing transactions
// are duplicated in a specific pattern produces the SAME merkle root — and
// therefore the same block header and the same block hash — as the honest
// block. So CheckMerkleRoot passing does NOT imply the transaction list is
// duplicate-free; this scan is the only thing that catches it.
//
// WHY IT STILL APPLIES BELOW THE CHECKPOINT: "below checkpoint" trusts the
// chain of block HASHES (the header chain is checkpoint-anchored), NOT the
// block BODIES, which are still downloaded from untrusted peers. A CVE-2012-2459
// mutation produces a body that hashes to the same, trusted block hash while
// carrying a repeated transaction — so a below-checkpoint block being "safe"
// (its hash is on the trusted chain) does not make a peer-supplied body safe.
// Skipping this on a below-checkpoint fast path is a cheap, peer-triggerable
// sync-DoS / state-corruption vector, because the mutated body reuses the
// honest block's proof-of-work (no mining required).
//
// It scans every node hash across all subtree slices into a plain Go map,
// skipping the coinbase placeholder at position [0][0], and returns a
// BlockInvalidError on the first duplicate. It is intentionally lightweight —
// O(N) in the total transaction count with one map allocation — so it is cheap
// enough to run unconditionally on every slice-only path. Callers on the full
// validation path (Block.Valid) instead use the pooled/disk-backed
// checkDuplicateTransactions; this helper is the independent floor for the
// paths that skip it.
//
// This one-shot form suits a caller that holds the whole block's slices at once
// and has not yet touched the UTXO store. A caller that applies the block to the
// store in batches cannot satisfy the "before creating or spending" precondition
// with it, and should use DuplicateTxScanner instead.
func CheckSubtreeSlicesForDuplicateTxs(slices []*subtreepkg.Subtree) error {
	// Estimate total node count for the initial map capacity to avoid rehashing.
	totalNodes := 0
	for _, st := range slices {
		if st != nil {
			totalNodes += len(st.Nodes)
		}
	}

	seen := make(map[chainhash.Hash]struct{}, totalNodes)

	for subIdx, subtree := range slices {
		if subtree == nil {
			continue
		}
		for txIdx, node := range subtree.Nodes {
			// Skip the coinbase placeholder (all-0xFF hash) that occupies the
			// first position of the first subtree. It is not a real transaction
			// hash and must not be deduped against itself.
			if subIdx == 0 && txIdx == 0 && node.Hash.Equal(subtreepkg.CoinbasePlaceholderHashValue) {
				continue
			}

			if _, exists := seen[node.Hash]; exists {
				return errors.NewBlockInvalidError(
					"[CheckSubtreeSlicesForDuplicateTxs] block contains duplicate transaction %s (CVE-2012-2459)",
					node.Hash.String(),
				)
			}
			seen[node.Hash] = struct{}{}
		}
	}

	return nil
}

// DuplicateTxScanner is the incremental form of CheckSubtreeSlicesForDuplicateTxs,
// for paths that apply a block to the UTXO store in batches.
//
// The one-shot helper above is the wrong shape for such a path in two ways.
// It can only run once every batch has been applied, so a block rejected by it
// has already created — and, on default settings, locked — its UTXOs, with no
// unlock pass on the rejection path to clear them. And its plain Go map holds one
// entry per transaction for the whole block: hundreds of megabytes on a
// multi-million-transaction block, gigabytes at a hundred million, on precisely
// the path that exists to make large blocks fast.
//
// This type fixes both. Callers feed it each batch's transaction list BEFORE
// writing that batch, so a duplicate is rejected with nothing yet applied, and it
// draws its map from the same size-class pool or disk-backed table that
// Block.checkDuplicateTransactions uses on the full validation path, honouring the
// same DiskMapDirs setting.
//
// Feed it the transactions actually being applied to the store, not the subtree
// leaf hashes: on a retry the leaf list can be deserialized from a local .subtree
// file that differs from the peer-supplied body being written, and a check that
// scans a different list than the one applied is not a floor.
//
// It is not safe for concurrent use; scan a batch before fanning out its writes.
// Release must be called when the block is done, on every exit path.
type DuplicateTxScanner struct {
	txMap    txmap.TxMap
	pooled   *txmap.SplitSwissMapUint64 // non-nil only when txMap came from the size-class pool
	sizedFor uint64                     // the pool key Release must return the map under
	next     uint64
}

// NewDuplicateTxScanner returns a scanner sized for expectedTxs transactions.
// expectedTxs is only an allocation hint — a count above the largest pooled size
// class is clamped by txMapAllocHint rather than allocating unbounded, so it is
// safe to pass a peer-supplied transaction count. diskMapDirs mirrors
// settings.Block.DiskMapDirs: non-empty selects the disk-backed table.
func NewDuplicateTxScanner(expectedTxs uint64, diskMapDirs []string) (*DuplicateTxScanner, error) {
	s := &DuplicateTxScanner{sizedFor: expectedTxs}

	if len(diskMapDirs) > 0 {
		// The mmap-backed table rejects a zero capacity; an empty block clamps to 1.
		filterCapacity := expectedTxs
		if filterCapacity == 0 {
			filterCapacity = 1
		}

		diskMap, err := NewDiskTxMapUint64(DiskTxMapUint64Options{
			BasePaths:      diskMapDirs,
			Prefix:         "qv-dedup",
			FilterCapacity: uint(filterCapacity),
		})
		if err != nil {
			return nil, errors.NewProcessingError("[DuplicateTxScanner] failed to create disk tx map", err)
		}

		s.txMap = diskMap

		return s, nil
	}

	s.pooled = GetTxMap(expectedTxs)
	s.txMap = s.pooled

	return s, nil
}

// Add records one transaction hash, returning a BlockInvalidError if this block
// has already presented it. Put is mutually exclusive, so the duplicate is caught
// by the insert itself rather than a separate lookup.
func (s *DuplicateTxScanner) Add(hash chainhash.Hash) error {
	if err := s.txMap.Put(hash, s.next); err != nil {
		if errors.Is(err, errors.ErrTxExists) || strings.Contains(err.Error(), "hash already exists in map") {
			return errors.NewBlockInvalidError(
				"[DuplicateTxScanner] block contains duplicate transaction %s (CVE-2012-2459)",
				hash.String(),
			)
		}

		return errors.NewStorageError(
			"[DuplicateTxScanner] failed to record transaction %s",
			hash.String(), err,
		)
	}

	s.next++

	return nil
}

// Release returns the pooled map to its size class, or closes the disk-backed
// table. It is nil-safe and idempotent, so callers can defer it immediately after
// construction and let every exit path run it.
func (s *DuplicateTxScanner) Release() {
	if s == nil || s.txMap == nil {
		return
	}

	if s.pooled != nil {
		PutTxMap(s.pooled, s.sizedFor)
		s.pooled = nil
		s.txMap = nil

		return
	}

	if closer, ok := s.txMap.(io.Closer); ok {
		_ = closer.Close()
	}

	s.txMap = nil
}
