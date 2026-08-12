package bsv

import (
	"encoding/json"
	"testing"

	"github.com/bsv-blockchain/teranode/daemon"
	"github.com/bsv-blockchain/teranode/test/utils/wirepeer"
	"github.com/stretchr/testify/require"
)

// commandLineInvalidBlockChain is how many blocks the port mines. Upstream mines
// 100 because it needs blocks deep enough to invalidate one 90 back and reindex;
// nothing here depends on depth, and invalidateblock and reconsiderblock are each
// several seconds, so the chain is kept short deliberately.
const commandLineInvalidBlockChain = 5

// upstreamUnknownBlockHash is the hash upstream passes to prove an unknown block is
// handled cleanly. Kept verbatim.
const upstreamUnknownBlockHash = "1000000000000000000000000000000000000000000000000000000000000000"

// TestBSVCommandLineInvalidBlock ports bsv-command-line-invalid-block.py.
//
// Upstream restarts the node seven times with different -invalidateblock command
// line arguments, checking each time where the tip lands. Teranode has no such
// startup option, so none of those seven scenarios can be set up as written.
//
// What is portable, and is the node behaviour those scenarios are really about, is
// the invalidate/reconsider pair reached through the RPCs upstream also calls:
// invalidating a block moves the tip below it, and reconsidering it puts the chain
// back. That is worth porting on its own account, because reconsiderblock is
// otherwise untested anywhere in this exercise - invalidateblock.py's port exercises
// invalidation and reorg but never the way back.
//
// The one divergence found is in the error contract rather than the chain handling:
// reconsidering an unknown block reports the wrong code and says far too much. See
// the reconsiderblock-error-contract gap.
func TestBSVCommandLineInvalidBlock(t *testing.T) {
	// A daemon per subtest, as upstream uses a node start per scenario. Not merely
	// tidiness: an earlier version shared one daemon and the second scenario failed
	// because the first had already invalidated and reconsidered the tip, leaving
	// state that stopped the chain coming fully back. Independent scenarios measure
	// the rule; shared ones measure the order they ran in.

	t.Run("invalidating the tip moves the chain back, and reconsidering restores it", func(t *testing.T) {
		td, tipHash, tipHeight := chainForInvalidation(t)
		defer td.Stop(t)

		// Upstream's "Invalidate tip" scenario, which invalidates via the command
		// line and then calls reconsiderblock over RPC. Only the first half changes.
		requireRPCOK(t, td, "invalidateblock", tipHash)
		requireChainSettlesAt(t, td, tipHeight-1,
			"invalidating the tip should leave the chain one block shorter")

		requireRPCOK(t, td, "reconsiderblock", tipHash)
		requireChainSettlesAt(t, td, tipHeight,
			"reconsidering the block should restore the original height")

		require.Equal(t, tipHash, bestBlockHash(t, td),
			"reconsidering the block should restore it as the tip")
	})

	t.Run("reconsidering the lowest of two invalidated blocks restores the whole chain", func(t *testing.T) {
		td, tipHash, tipHeight := chainForInvalidation(t)
		defer td.Stop(t)

		// Upstream's "Invalidate two blocks not at tip" scenario.
		lower := blockHashAtHeight(t, td, tipHeight-2)
		upper := blockHashAtHeight(t, td, tipHeight-1)

		requireRPCOK(t, td, "invalidateblock", upper)
		requireRPCOK(t, td, "invalidateblock", lower)
		requireChainSettlesAt(t, td, tipHeight-3, "the tip should sit below both invalidated blocks")

		// Reconsidering the lower one brings back everything above it, including the
		// separately-invalidated block. Worth asserting rather than assuming: a node
		// that cleared only the named block would stop one short.
		requireRPCOK(t, td, "reconsiderblock", lower)
		requireChainSettlesAt(t, td, tipHeight,
			"reconsidering the lowest invalidated block should restore the full chain")

		require.Equal(t, tipHash, bestBlockHash(t, td), "the original tip should be back")
	})

	t.Run("reconsidering the higher of two invalidated blocks fails", func(t *testing.T) {
		td, _, tipHeight := chainForInvalidation(t)
		defer td.Stop(t)

		// This is upstream's scenario as written: it invalidates blocks [-4] and [-3]
		// and then reconsiders [-3], the HIGHER of the two, expecting the chain to come
		// all the way back. bitcoin-sv can do that because Core's
		// ResetBlockFailureFlags clears the failure flag on ancestors as well as
		// descendants. Teranode refuses instead.
		lower := blockHashAtHeight(t, td, tipHeight-2)
		upper := blockHashAtHeight(t, td, tipHeight-1)

		requireRPCOK(t, td, "invalidateblock", upper)
		requireRPCOK(t, td, "invalidateblock", lower)
		requireChainSettlesAt(t, td, tipHeight-3, "the tip should sit below both invalidated blocks")

		resp, err := td.CallRPC(td.Ctx, "reconsiderblock", []any{upper})
		require.Error(t, err,
			"TRIPWIRE: reconsidering a block whose ancestor is still invalid now succeeds, which is "+
				"upstream's behaviour. Revisit the reconsiderblock-ignores-ancestors gap and assert "+
				"upstream's full restore instead")

		_, message := rpcError(t, resp)
		require.Contains(t, message, "parent block is invalid",
			"the refusal should name the invalid ancestor as the reason")

		t.Logf("reconsiderblock(upper) refused with: %s", message)

		// And the chain stays where it was, so the failed call changed nothing.
		requireChainSettlesAt(t, td, tipHeight-3, "a refused reconsider must leave the chain alone")
	})

	t.Run("reconsidering an unknown block fails, but with the wrong code and too much detail", func(t *testing.T) {
		td, _, _ := chainForInvalidation(t)
		defer td.Stop(t)

		// Upstream: assert_raises_rpc_error(-5, 'Block not found', reconsiderblock, hash).
		resp, err := td.CallRPC(td.Ctx, "reconsiderblock", []any{upstreamUnknownBlockHash})
		require.Error(t, err, "reconsidering a block the node has never seen must fail")

		code, message := rpcError(t, resp)

		t.Logf("reconsiderblock(unknown) -> code=%d message=%q", code, message)

		// The half that matches: it fails, and it says the block was not found.
		require.Contains(t, message, "BLOCK_NOT_FOUND",
			"the failure should be attributed to the block being unknown")

		// TRIPWIRE on the two halves that do not.
		require.EqualValues(t, -25, code,
			"TRIPWIRE: the error code changed. Upstream returns -5 for an unknown block; if Teranode "+
				"now does too, revisit the reconsiderblock-error-contract gap")
		require.Contains(t, message, "sql: no rows in result set",
			"TRIPWIRE: the RPC error no longer leaks the storage layer. That is the fix the "+
				"reconsiderblock-error-contract gap asks for - update it and drop this assertion")
	})
}

// chainForInvalidation starts a daemon with a short chain and returns it with its
// tip hash and height.
//
// P2P is enabled for speed, and the margin is large: reconsiderblock takes 9.77s
// without it and 2ms with it, measured. Same cause as the
// getpeerinfo-stalls-without-p2p-service gap - the RPC handler is given a P2P client
// whether or not the service is running and waits out its gRPC retries.
// invalidateblock is unaffected at 2ms either way, so the stall belongs to the
// handlers that touch that client rather than to invalidation itself.
func chainForInvalidation(t *testing.T) (*daemon.TestDaemon, string, uint32) {
	t.Helper()

	td := wirepeer.NewLegacyDaemonWithP2P(t)

	td.MineBlocks(t, commandLineInvalidBlockChain)

	tipHash := bestBlockHash(t, td)
	tipHeight := bestHeight(t, td)

	require.EqualValues(t, commandLineInvalidBlockChain, tipHeight,
		"should have mined %d blocks", commandLineInvalidBlockChain)

	return td, tipHash, tipHeight
}

// requireChainSettlesAt waits for the tip to reach a height and stay there.
//
// Polled rather than read once, because invalidateblock and reconsiderblock both
// return before the chain has finished moving. That is not a guess: the first
// version of this port read the height immediately and passed, but only because it
// was running without the P2P service, where reconsiderblock spends 9.77s waiting
// out gRPC retries and the asynchronous work finished inside that window. Enabling
// P2P cut the call to 2ms and the same assertion failed, one block short. The stall
// had been doing the waiting.
//
// tryBestHeight rather than bestHeight because a testify polling condition must not
// assert - see tryBestBlockHash.
func requireChainSettlesAt(t *testing.T, td *daemon.TestDaemon, want uint32, msg string) {
	t.Helper()

	require.Eventually(t, func() bool { return tryBestHeight(td) == want },
		rpcSettle, rpcPoll, "%s: wanted height %d, last saw %d", msg, want, tryBestHeight(td))
}

// requireRPCOK calls a single-argument RPC and requires it to succeed.
func requireRPCOK(t *testing.T, td *daemon.TestDaemon, method, arg string) {
	t.Helper()

	_, err := td.CallRPC(td.Ctx, method, []any{arg})
	require.NoError(t, err, "%s(%s)", method, arg)
}

// blockHashAtHeight reads the hash of the block at a given height.
func blockHashAtHeight(t *testing.T, td *daemon.TestDaemon, height uint32) string {
	t.Helper()

	block, err := td.BlockchainClient.GetBlockByHeight(td.Ctx, height)
	require.NoError(t, err, "fetch block at height %d", height)

	return block.Header.Hash().String()
}

// rpcError pulls the code and message out of a JSON-RPC error response.
//
// Read from the response body rather than from the Go error, because the Go error
// wraps the same text in several layers of its own and the code is what upstream
// asserts on.
func rpcError(t *testing.T, resp string) (int, string) {
	t.Helper()

	var envelope struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}

	require.NoError(t, json.Unmarshal([]byte(resp), &envelope), "decode RPC error from %s", resp)
	require.NotZero(t, envelope.Error.Code, "expected an error object in %s", resp)

	return envelope.Error.Code, envelope.Error.Message
}
