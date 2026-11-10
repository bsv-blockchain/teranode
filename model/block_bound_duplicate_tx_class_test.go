//go:build bvgaps

package model

import (
	"context"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	txmap "github.com/bsv-blockchain/go-tx-map"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/bsv-blockchain/teranode/util/test"
	"github.com/stretchr/testify/require"
)

// requireBoundDuplicateClassedInvalid asserts BV-BODY-004's class for a non-merkle-pattern
// duplicate in a bound body: rejected by the duplicate rule, ERR_BLOCK_INVALID, not corrupt.
func requireBoundDuplicateClassedInvalid(t *testing.T, err error) {
	t.Helper()

	require.Error(t, err)
	require.ErrorContains(t, err, "duplicate transaction", "the rejection must come from the duplicate rule")
	require.True(t, errors.Is(err, errors.ErrBlockInvalid),
		"BV-BODY-004: a duplicate in a bound body that is not the odd-node pattern must be ERR_BLOCK_INVALID, got: %v", err)
	require.False(t, errors.IsBlockCorrupt(err), "must not be ERR_BLOCK_CORRUPT")
}

// TestKnownDefect_BoundBodyDuplicateAcrossSubtrees_IsInvalidOnEveryLoadPath checks the class of a
// duplicate transaction in a bound body on each way the body is loaded.
//
// Spec: BV-BODY-004 (tracked as GAP-BV-051, bitcoin-sv/teranode#4984).
//
// Scenario: Given a block of two full 4-leaf subtrees whose last leaves are the same transaction X,
// and a header whose merkle root is computed over exactly that list, so the body is bound. Both
// subtrees have an even leaf count, so X is not the CVE-2012-2459 odd-node duplicate. When the body
// is loaded by getAndValidateSubtreesWithDedup, or validated by Block.Valid with the in-memory or the
// disk-backed txMap, Then it is rejected by the duplicate rule as ERR_BLOCK_INVALID, not
// ERR_BLOCK_CORRUPT.
//
// Today every path returns ERR_BLOCK_CORRUPT. These cases replace the CORRUPT assertions that
// TestGetAndValidateSubtreesWithDedup_DuplicateAcrossSubtrees and the bound cases of
// TestBlock_Valid_DuplicateAcrossLoadedSubtrees used to make. Their unbound counterparts stay
// CORRUPT and are still asserted there.
func TestKnownDefect_BoundBodyDuplicateAcrossSubtrees_IsInvalidOnEveryLoadPath(t *testing.T) {
	const (
		subtreeCount       = 2
		loadConcurrency    = 2
		validationTimeout  = 5 * time.Second
		chainHeaderIDCount = 11
	)

	t.Run("loading the subtrees with dedup", func(t *testing.T) {
		duplicate := randCorruptTestHash(t)
		block, _, _, store := dedupLoadFixture(t, &duplicate)

		defer block.releaseTxMap()

		_, err := block.getAndValidateSubtreesWithDedup(context.Background(), ulogger.TestLogger{}, store, loadConcurrency)

		requireBoundDuplicateClassedInvalid(t, err)
	})

	backings := []struct {
		name        string
		diskMapDirs func(t *testing.T) []string
	}{
		{"Block.Valid with the in-memory txMap", func(*testing.T) []string { return nil }},
		{"Block.Valid with the disk-backed txMap", func(t *testing.T) []string { return []string{t.TempDir()} }},
	}

	for _, backing := range backings {
		t.Run(backing.name, func(t *testing.T) {
			tSettings := test.CreateBaseTestSettings(t)
			tSettings.Block.DiskMapDirs = backing.diskMapDirs(t)

			duplicate := randCorruptTestHash(t)
			block, _, store := dedupLoadFixtureN(t, &duplicate, subtreeCount, nil)

			ctx, cancel := context.WithTimeout(context.Background(), validationTimeout)
			defer cancel()

			valid, err := block.Valid(ctx, ulogger.TestLogger{}, store, nil, txmap.NewSyncedMap[chainhash.Hash, []uint32](),
				regtestGenesisParentChain(t, block.Header), make([]uint32, chainHeaderIDCount), tSettings, nil)

			require.False(t, valid)
			requireBoundDuplicateClassedInvalid(t, err)
		})
	}
}
