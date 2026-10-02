package peer

import (
	"testing"

	"github.com/bsv-blockchain/go-wire"
	"github.com/stretchr/testify/require"
)

// A reply queued behind blocks the peer is still sending is not a stall. A getdata can ask for
// several blocks, and when the first arrives the whole block-reply group is cleared, so the
// headers deadline counts down again while the peer sends the next one. At 1 GB and 5 to
// 10 MB/s that is longer than the 90 s headers budget: on 2026-09-24 mainnet disconnected peers
// for "headers timeout" while they were sending blocks, and lost the block one of them had just
// finished, which stopped the chain at 708,114.
func TestAReplyQueuedBehindAHealthyDownloadIsNotAStall(t *testing.T) {
	require.True(t, shouldDeferQueuedResponse(wire.CmdHeaders, true), "bytes are arriving at a healthy rate")
	require.False(t, shouldDeferQueuedResponse(wire.CmdHeaders, false), "nothing arriving is a stall")
	require.False(t, shouldDeferQueuedResponse(wire.CmdBlock, true), "block replies keep their own extension rule")
}
