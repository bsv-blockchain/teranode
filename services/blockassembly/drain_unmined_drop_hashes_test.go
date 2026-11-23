package blockassembly

import (
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
	"github.com/bsv-blockchain/teranode/services/blockassembly/subtreeprocessor"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// drainUnminedDropHashes backs both loadUnminedTransactions call sites
// (startup Start() and reset's postProcess): DrainQueue's own writes land
// after the caller's own end-of-load FlushDiskTxMapForLoad call, so they need
// a second flush-and-check of their own, or a trailing unflushed failure
// there surfaces later as a spurious "not found in currentTxMap" failure on
// an unrelated block instead.
func TestDrainUnminedDropHashes(t *testing.T) {
	dropHash := chainhash.HashH([]byte("cascaded-conflicting-child"))

	t.Run("nothing to drop: DrainQueue and the flush are both skipped", func(t *testing.T) {
		stp := &subtreeprocessor.MockSubtreeProcessor{}
		ba := &BlockAssembler{subtreeProcessor: stp}

		require.NoError(t, ba.drainUnminedDropHashes("test", false))
		stp.AssertNotCalled(t, "DrainQueue", mock.Anything)
		stp.AssertNotCalled(t, "FlushDiskTxMapForLoad", mock.Anything, mock.Anything)
	})

	t.Run("startup (isReload=false): a flush failure after DrainQueue fails the call", func(t *testing.T) {
		stp := &subtreeprocessor.MockSubtreeProcessor{}
		stp.On("DrainQueue", mock.MatchedBy(func(drop map[chainhash.Hash]struct{}) bool {
			_, ok := drop[dropHash]
			return len(drop) == 1 && ok
		})).Return()
		boom := errors.NewStorageError("disk tx map storage error")
		stp.On("FlushDiskTxMapForLoad", "drainQueue_startup", false).Return(boom)

		ba := &BlockAssembler{subtreeProcessor: stp, unminedDropHashes: map[chainhash.Hash]struct{}{dropHash: {}}}

		err := ba.drainUnminedDropHashes("drainQueue_startup", false)
		require.ErrorIs(t, err, boom, "the startup load must fail on a DrainQueue flush failure, same as the load rule")

		stp.AssertExpectations(t)
		require.Nil(t, ba.unminedDropHashes, "the accumulator must be cleared either way")
	})

	t.Run("reset reload (isReload=true): DrainQueue then a report-only flush, in order", func(t *testing.T) {
		stp := &subtreeprocessor.MockSubtreeProcessor{}
		var calls []string
		stp.On("DrainQueue", mock.Anything).Run(func(mock.Arguments) { calls = append(calls, "DrainQueue") }).Return()
		// FlushDiskTxMapForLoad(isReload=true) never returns a non-nil error
		// itself (see its own implementation: reportOrJoinDiskTxMapErr logs
		// and counts instead) - drainUnminedDropHashes has no swallowing
		// logic of its own, it just forwards whatever the call returns.
		stp.On("FlushDiskTxMapForLoad", "drainQueue_reset_reload", true).
			Run(func(mock.Arguments) { calls = append(calls, "FlushDiskTxMapForLoad") }).
			Return(nil)

		ba := &BlockAssembler{subtreeProcessor: stp, unminedDropHashes: map[chainhash.Hash]struct{}{dropHash: {}}}

		require.NoError(t, ba.drainUnminedDropHashes("drainQueue_reset_reload", true))

		stp.AssertExpectations(t)
		require.Equal(t, []string{"DrainQueue", "FlushDiskTxMapForLoad"}, calls,
			"the flush must run after DrainQueue, so it can see DrainQueue's own writes")
	})
}
