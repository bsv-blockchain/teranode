package netsync

import (
	"io"
	"testing"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/go-wire"
	"github.com/bsv-blockchain/teranode/ulogger"
	"github.com/stretchr/testify/require"
)

// TestInstallStreamingBlockPath_NilSettingsDoesNotPanic is FIX 4.
//
// installStreamingBlockPath used to read sm.settings.Legacy.PipelineReceive
// with no nil check, inside a function whose other guards (sm == nil,
// sm.blockPark == nil) are explicitly defensive. The reviewer measured the
// struct offsets: Settings.Legacy sits at 4728 and PipelineReceive at 5112,
// both past the 4096-byte guard page a Go process has mapped unreadable at
// address zero, so a nil settings pointer there was an unrecoverable hardware
// fault (SIGSEGV) rather than a recoverable nil-pointer panic —
// require.Panics could not even catch it. Streaming is now unconditional and
// installStreamingBlockPath no longer reads sm.settings at all, so that
// specific fault is gone with the field, but the function's job — installing
// the sink triple without touching sm.settings — is still worth pinning
// against nil settings regressing back in.
func TestInstallStreamingBlockPath_NilSettingsDoesNotPanic(t *testing.T) {
	sm := &SyncManager{
		logger: ulogger.TestLogger{},
		blockPark: &blockPark{
			logger:   ulogger.TestLogger{},
			entries:  make(map[chainhash.Hash]*parkedBlock),
			children: make(map[chainhash.Hash][]chainhash.Hash),
		},
		// settings intentionally left nil
	}

	var installed bool

	require.NotPanics(t, func() {
		sm.installStreamingBlockPath(func(
			sink func(chainhash.Hash, *wire.BlockHeader, io.Reader, int64) (bool, error),
			gate func(chainhash.Hash, *wire.BlockHeader) error,
			del func(chainhash.Hash, bool) error,
		) {
			installed = true
		})
	}, "a nil settings pointer must not be dereferenced past the struct's guard page")

	require.True(t, installed, "the sink triple must still be installed with nil settings")
}
