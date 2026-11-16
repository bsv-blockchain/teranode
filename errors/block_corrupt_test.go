package errors

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// TestNewBlockCorruptError checks the constructor produces the dedicated
// ERR_BLOCK_CORRUPT code and formats its message like its neighbours.
func TestNewBlockCorruptError(t *testing.T) {
	err := NewBlockCorruptError("corrupt body %s %d", "hash", 42)

	require.Equal(t, ERR_BLOCK_CORRUPT, err.Code(), "error code should be ERR_BLOCK_CORRUPT")
	require.Equal(t, "corrupt body hash 42", err.Message(), "error message should match")
	// String() is descriptor-driven; assert the enum name resolves so a hand-edited
	// generated descriptor (or a stale make gen) is caught here.
	require.Equal(t, "BLOCK_CORRUPT", ERR_BLOCK_CORRUPT.String())
}

// TestIsBlockCorrupt verifies the classifier is exclusive: a corrupt error must be
// distinguishable from block-invalid so it never lands on a poison path.
func TestIsBlockCorrupt(t *testing.T) {
	corrupt := NewBlockCorruptError("corrupt body")

	require.True(t, IsBlockCorrupt(corrupt))
	require.True(t, Is(corrupt, ErrBlockCorrupt))

	// Crucially it must NOT be seen as block-invalid: that is what keeps it off the
	// storeInvalidBlock / InvalidateBlock poison paths (bitcoin-sv/teranode#4692).
	require.False(t, Is(corrupt, ErrBlockInvalid), "corrupt must not match ErrBlockInvalid")
	require.False(t, IsTransientBlockIncomplete(corrupt))

	// Negatives.
	require.False(t, IsBlockCorrupt(nil))
	require.False(t, IsBlockCorrupt(NewBlockInvalidError("invalid")))
	require.False(t, Is(NewBlockInvalidError("invalid"), ErrBlockCorrupt))
}

// TestIsBlockCorruptThroughWrappers confirms IsBlockCorrupt walks the wrapped chain,
// so a corrupt cause is detected even when an outer error type shadows it. This is the
// shape the routing/classification code relies on.
func TestIsBlockCorruptThroughWrappers(t *testing.T) {
	corrupt := NewBlockCorruptError("corrupt body")

	// Wrapped by a block-invalid outer: IsBlockCorrupt must still see the cause, and
	// the outer must not flip a corrupt error into an invalid verdict on the Is check.
	wrappedByInvalid := NewBlockInvalidError("outer invalid", corrupt)
	require.True(t, IsBlockCorrupt(wrappedByInvalid), "corrupt cause must survive an invalid wrapper")

	// Wrapped by a processing error (the quick path must therefore return corrupt
	// directly, never NewProcessingError(corrupt), else this shadowing would hide it
	// from the ErrProcessing-first classifier — asserted true here only to document
	// that the chain walk itself finds it).
	wrappedByProcessing := NewProcessingError("outer processing", corrupt)
	require.True(t, IsBlockCorrupt(wrappedByProcessing))
}

// TestBlockCorruptGRPCSurvival verifies the corrupt classification survives a round
// trip through the gRPC wrap/unwrap boundary, since block delivery crosses services.
func TestBlockCorruptGRPCSurvival(t *testing.T) {
	corrupt := NewBlockCorruptError("corrupt body %s", "abc")

	wrapped := WrapGRPC(corrupt)
	require.Error(t, wrapped)

	unwrapped := UnwrapGRPC(wrapped)
	require.NotNil(t, unwrapped)
	require.True(t, IsBlockCorrupt(unwrapped), "corrupt must survive WrapGRPC/UnwrapGRPC")
	require.False(t, Is(unwrapped, ErrBlockInvalid))

	// And through the top-level Is which unwraps gRPC internally.
	require.True(t, Is(wrapped, ErrBlockCorrupt))
	require.False(t, Is(wrapped, ErrBlockInvalid))
}

// TestBlockCorruptSurvivesRealServerWrapper reproduces the exact shape the stateless
// block-validation RPC uses (bitcoin-sv/teranode#4692): the block.Valid corrupt error is re-wrapped
// in an OUTER corrupt error carrying RPC context and then sent through WrapGRPC. The
// classification must survive so the gRPC caller sees corrupt, not block-invalid.
func TestBlockCorruptSurvivesRealServerWrapper(t *testing.T) {
	inner := NewBlockCorruptError("[BLOCK] merkle root does not match")
	outer := NewBlockCorruptError("[ValidateBlock] block body is corrupt", inner)

	wrapped := WrapGRPC(outer)
	require.True(t, Is(wrapped, ErrBlockCorrupt))
	require.False(t, Is(wrapped, ErrBlockInvalid),
		"corrupt must NOT surface as block-invalid across the RPC boundary")

	unwrapped := UnwrapGRPC(wrapped)
	require.True(t, IsBlockCorrupt(unwrapped))
	require.False(t, Is(unwrapped, ErrBlockInvalid))

	// Counter-example documenting WHY the fix matters: had the RPC wrapped the corrupt
	// cause in NewBlockInvalidError (the pre-fix behaviour), errors.Is(ErrBlockInvalid)
	// would become true — the exact bug the corrupt outer wrapper above avoids.
	buggy := WrapGRPC(NewBlockInvalidError("block is not valid", inner))
	require.True(t, Is(buggy, ErrBlockInvalid),
		"wrapping a corrupt cause in block-invalid leaks it as block-invalid — do not do this")
}

// TestNewBlockCorruptErrorStripsWrappedInvalidCause verifies the wrapped-chain guard: even
// if a call site passes an ErrBlockInvalid-classified cause as the wrapped argument, the
// resulting error must classify as corrupt only, never also as invalid — otherwise a
// poisoning branch gated on errors.Is(_, ErrBlockInvalid) could fire for a corrupt body.
func TestNewBlockCorruptErrorStripsWrappedInvalidCause(t *testing.T) {
	cause := NewBlockInvalidError("cause")

	err := NewBlockCorruptError("msg", cause)

	require.True(t, IsBlockCorrupt(err))
	require.False(t, Is(err, ErrBlockInvalid), "guard must strip the collision, not just log it")
}

// TestBlockCorruptGRPCCodeDefault documents the intended mapping: like ERR_BLOCK_INVALID,
// ERR_BLOCK_CORRUPT has no explicit row in ErrorCodeToGRPCCode and falls to the
// codes.Internal default. No caller branches on the gRPC code; control flow keys on the
// reconstructed ERR code via IsBlockCorrupt.
func TestBlockCorruptGRPCCodeDefault(t *testing.T) {
	require.Equal(t, codes.Internal, ErrorCodeToGRPCCode(ERR_BLOCK_CORRUPT))
	// Parity with block-invalid, which also has no row.
	require.Equal(t, codes.Internal, ErrorCodeToGRPCCode(ERR_BLOCK_INVALID))
}
