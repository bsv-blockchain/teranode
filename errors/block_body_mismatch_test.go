package errors

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestBlockBodyMismatchIsAMarkerInsideTheInvalidVerdict pins the shape the
// legacy sink raises: the marker sits inside a BlockInvalidError, so the verdict
// still reads as ERR_BLOCK_INVALID outermost and IsBlockBodyMismatch still finds
// the marker through the chain. A plain invalid verdict does not carry it, and
// neither does a corrupt one.
func TestBlockBodyMismatchIsAMarkerInsideTheInvalidVerdict(t *testing.T) {
	marked := NewBlockInvalidError("merkle root %s does not match header's %s", "aa", "bb", ErrBlockBodyMismatch)

	require.Equal(t, ERR_BLOCK_INVALID, marked.Code(), "the outermost code is the verdict every reader already knows")
	require.True(t, Is(marked, ErrBlockInvalid))
	require.True(t, IsBlockBodyMismatch(marked), "the marker must be found through the chain")
	require.Contains(t, marked.Error(), "merkle root aa does not match header's bb")
	require.Equal(t, "BLOCK_BODY_MISMATCH", ERR_BLOCK_BODY_MISMATCH.String())

	require.False(t, IsBlockBodyMismatch(NewBlockInvalidError("declares more transactions than its body can hold")),
		"an invalid verdict without the marker is not a ban")
	require.False(t, IsBlockBodyMismatch(NewBlockCorruptError("the declared transactions used fewer bytes than declared")),
		"a corrupt delivery is never a ban")
	require.False(t, IsBlockBodyMismatch(nil))
}

// TestBlockBodyMismatchSurvivesAWrapperAndStaysOutOfTheMaliciousBucket covers the
// two things a consumer relies on: a processing wrapper above the verdict keeps
// the marker reachable, and the rendered code name contains none of the words
// IsMaliciousResponseError substring-matches, so the category stays "block"
// without a dedicated early-out (the trap ERR_BLOCK_CORRUPT fell into).
func TestBlockBodyMismatchSurvivesAWrapperAndStaysOutOfTheMaliciousBucket(t *testing.T) {
	marked := NewBlockInvalidError("block contains duplicate transaction", ErrBlockBodyMismatch)
	wrapped := NewProcessingError("streaming block: could not store the body", marked)

	require.True(t, IsBlockBodyMismatch(wrapped), "the marker must survive a processing wrapper")
	require.True(t, Is(wrapped, ErrBlockInvalid))

	require.False(t, IsMaliciousResponseError(NewBlockInvalidError("x", ErrBlockBodyMismatch)),
		"BLOCK_BODY_MISMATCH must not substring-match the malicious list")
	require.Equal(t, "block", errorCodeCategory(ERR_BLOCK_BODY_MISMATCH), "122 is in the second block decade")
}
