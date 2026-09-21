package errors

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

// TestNewBlockPolicyDeclinedError checks the constructor produces the dedicated
// ERR_BLOCK_POLICY_DECLINED code and formats its message like its neighbours.
func TestNewBlockPolicyDeclinedError(t *testing.T) {
	err := NewBlockPolicyDeclinedError("block size %d exceeds excessiveblocksize %d", 5, 4)

	require.Equal(t, ERR_BLOCK_POLICY_DECLINED, err.Code(), "error code should be ERR_BLOCK_POLICY_DECLINED")
	require.Equal(t, "block size 5 exceeds excessiveblocksize 4", err.Message(), "error message should match")
	// String() is descriptor-driven; assert the enum name resolves so a hand-edited
	// generated descriptor (or a stale make gen) is caught here.
	require.Equal(t, "BLOCK_POLICY_DECLINED", ERR_BLOCK_POLICY_DECLINED.String())
	require.True(t, Is(err, ErrBlockPolicyDeclined), "the constructor must satisfy the sentinel")
}

// TestBlockPolicyDeclinedIsMutuallyExclusive is the load-bearing half of this contract. A local
// policy decline must be distinguishable from all three neighbouring block classifications
// (bitcoin-sv/teranode#4692):
//
//   - NOT ErrBlockError, the code it moved off. ERR_BLOCK_ERROR now solely carries the retryable
//     "given up waiting on previous blocks" timeout; a match here would let
//     releaseCatchupLock's narrowed wait-timeout case shadow the decline case, and would make
//     processCatchupChItem's terminal branch fire on a timeout that must keep retrying.
//   - NOT ErrBlockInvalid: a match re-opens every poisoning branch downstream (storeInvalidBlock /
//     InvalidateBlock), which is the fork-off-the-chain failure this work exists to remove.
//   - NOT ErrBlockCorrupt: a match would spend the corrupt re-download budget and earn the serving
//     peer a corrupt-body ban score for our own configuration.
func TestBlockPolicyDeclinedIsMutuallyExclusive(t *testing.T) {
	decline := NewBlockPolicyDeclinedError("block size exceeds excessiveblocksize (local policy)")

	require.True(t, Is(decline, ErrBlockPolicyDeclined))

	require.False(t, Is(decline, ErrBlockError), "a decline must not match the wait-timeout class it moved off")
	require.False(t, Is(decline, ErrBlockInvalid), "a decline must never reach the poison paths")
	require.False(t, Is(decline, ErrBlockCorrupt), "a decline must not spend the corrupt budget")
	require.False(t, IsBlockCorrupt(decline))
	require.False(t, IsTransientBlockIncomplete(decline))

	// And the converse, so the sentinel is not matching everything: neighbouring block errors are
	// not declines.
	require.False(t, Is(NewBlockError("given up waiting on previous blocks"), ErrBlockPolicyDeclined))
	require.False(t, Is(NewBlockInvalidError("invalid"), ErrBlockPolicyDeclined))
	require.False(t, Is(NewBlockCorruptError("corrupt body"), ErrBlockPolicyDeclined))
}

// TestBlockPolicyDeclinedThroughWrappers confirms the classification survives the wrapper shapes
// the catchup path actually builds, and — just as importantly — that the wrappers do not smuggle in
// a class the decline must never carry.
func TestBlockPolicyDeclinedThroughWrappers(t *testing.T) {
	decline := NewBlockPolicyDeclinedError("block size exceeds excessiveblocksize")

	wrappedByProcessing := NewProcessingError("outer processing", decline)
	require.True(t, Is(wrappedByProcessing, ErrBlockPolicyDeclined), "the decline must survive a processing wrapper")
	require.False(t, Is(wrappedByProcessing, ErrBlockInvalid))
	require.False(t, Is(wrappedByProcessing, ErrBlockCorrupt))

	// An outer BlockError wrapper DOES add ERR_BLOCK_ERROR to the chain — errors.Is walks it — which
	// is exactly why releaseCatchupLock's decline case must sit ABOVE its ErrBlockError case. The
	// decline itself still matches, which is what that ordering relies on.
	wrappedByBlockError := NewBlockError("outer block error", decline)
	require.True(t, Is(wrappedByBlockError, ErrBlockPolicyDeclined), "the decline must survive a block-error wrapper")
	require.False(t, Is(wrappedByBlockError, ErrBlockInvalid))
	require.False(t, Is(wrappedByBlockError, ErrBlockCorrupt))
}

// TestBlockPolicyDeclinedGRPCSurvival verifies the decline survives a round trip through the gRPC
// wrap/unwrap boundary. processCatchupChItem classifies errors that came back across the
// ValidateBlock RPC, so without this the terminal branch would never fire in production.
func TestBlockPolicyDeclinedGRPCSurvival(t *testing.T) {
	decline := NewBlockPolicyDeclinedError("block size %d exceeds excessiveblocksize", 5)

	wrapped := WrapGRPC(decline)
	require.Error(t, wrapped)

	unwrapped := UnwrapGRPC(wrapped)
	require.NotNil(t, unwrapped)
	require.True(t, Is(unwrapped, ErrBlockPolicyDeclined), "the decline must survive WrapGRPC/UnwrapGRPC")
	require.False(t, Is(unwrapped, ErrBlockInvalid))
	require.False(t, Is(unwrapped, ErrBlockError))

	// And through the top-level Is, which unwraps gRPC internally.
	require.True(t, Is(wrapped, ErrBlockPolicyDeclined))
	require.False(t, Is(wrapped, ErrBlockInvalid))
	require.False(t, Is(wrapped, ErrBlockError))
}

// TestBlockPolicyDeclinedGRPCCodeDefault documents the intended mapping: like ERR_BLOCK_INVALID and
// ERR_BLOCK_CORRUPT, ERR_BLOCK_POLICY_DECLINED has no explicit row in ErrorCodeToGRPCCode and falls
// to the codes.Internal default. No caller branches on the gRPC code; control flow keys on the
// reconstructed ERR code, so a new code must not silently acquire a transport-meaningful status.
func TestBlockPolicyDeclinedGRPCCodeDefault(t *testing.T) {
	require.Equal(t, codes.Internal, ErrorCodeToGRPCCode(ERR_BLOCK_POLICY_DECLINED))
	// Parity with the neighbours, which also have no row.
	require.Equal(t, codes.Internal, ErrorCodeToGRPCCode(ERR_BLOCK_CORRUPT))
	require.Equal(t, codes.Internal, ErrorCodeToGRPCCode(ERR_BLOCK_INVALID))
}
