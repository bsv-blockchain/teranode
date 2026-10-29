package netsync

import (
	"strings"
	"testing"
	"time"

	"github.com/bsv-blockchain/teranode/errors"
	blockchain2 "github.com/bsv-blockchain/teranode/services/blockchain"
	"github.com/stretchr/testify/require"
)

func TestBlockRejectReason(t *testing.T) {
	gobdk := func(reason string) string { return "GoBDK fail to ValidateTransaction -> UNKNOWN (0): " + reason }

	for name, tc := range map[string]struct {
		err  error
		want string
	}{
		"nil":                          {nil, "block rejected"},
		"unclassified block verdict":   {errors.NewBlockInvalidError("this block is not one we can take"), "block rejected"},
		"not a verdict at all":         {errors.NewProcessingError("merkle root does not match header's"), "block rejected"},
		"gobdk duplicate inputs":       {errors.NewTxInvalidError(gobdk("bad-txns-inputs-duplicate")), "bad-txns-inputs-duplicate"},
		"gobdk reason inside a block":  {errors.NewBlockInvalidError("[ValidateBlock][x] block contains invalid transactions", errors.NewTxInvalidError(gobdk("bad-txns-in-belowout"))), "bad-txns-in-belowout"},
		"innermost reason wins":        {errors.NewBlockInvalidError("outer: bad-cb-amount", errors.NewTxInvalidError(gobdk("bad-txns-in-belowout"))), "bad-txns-in-belowout"},
		"a word in running text":       {errors.NewBlockInvalidError("saw bad-cb-amount in a field"), "block rejected"},
		"a word inside a longer token": {errors.NewTxInvalidError(gobdk("xbad-cb-amountx")), "block rejected"},
		"merkle root mismatch":         {errors.NewBlockInvalidError("merkle root does not match header's", errors.ErrBlockBodyMismatch), "bad-txnmrklroot"},
		"merkle root mismatch, hashes": {errors.NewBlockInvalidError("[pipelineBlockSink][x] merkle root " + strings.Repeat("ab", 32) + " does not match header's " + strings.Repeat("cd", 32)), "bad-txnmrklroot"},
		"merkle root mentioned":        {errors.NewBlockInvalidError("merkle root accumulated for subtree 3"), "block rejected"},
		"mutated merkle tree":          {errors.NewBlockInvalidError("[blockStreamBuilder] block contains duplicate transaction aa (CVE-2012-2459)", errors.ErrBlockBodyMismatch), "bad-txns-duplicate"},
		"a transaction that recurs":    {errors.NewBlockInvalidError("a second copy repeats a transaction"), "block rejected"},
		"duplicate inputs, our text":   {errors.NewBlockInvalidError("[validOrderAndBlessed][x][a:1]:2 transaction aa has duplicate inputs"), "bad-txns-inputs-duplicate"},
		"hash above target":            {errors.NewBlockInvalidError("[ValidateBlock][x] block does not meet target difficulty: boom"), "high-hash"},
		"wrong bits":                   {errors.NewBlockInvalidError("[ValidateBlock][x] block has incorrect difficulty bits: got 1, expected 2"), "bad-diffbits"},
		"no coinbase":                  {errors.NewBlockInvalidError("[BLOCK][x] missing coinbase transaction"), "bad-cb-missing"},
		"two coinbases":                {errors.NewBlockInvalidError("[BLOCK][x] multiple coinbase transactions"), "bad-tx-coinbase"},
		"invalid parent":               {errors.NewBlockInvalidError("[ValidateBlock][x] parent block is invalid"), "bad-prevblk"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, blockRejectReason(tc.err))
		})
	}
}

// The answer is only ever the closed vocabulary or the generic reason: error text, which can hold
// node-internal detail and peer-supplied data, is never passed on.
func TestBlockRejectReason_NeverLeaksErrorText(t *testing.T) {
	secret := "/var/lib/teranode/secret.dat 10.1.2.3 txid-deadbeef"
	got := blockRejectReason(errors.NewBlockInvalidError("failed to read " + secret))
	require.Equal(t, "block rejected", got)

	for _, err := range []error{
		errors.NewBlockInvalidError("merkle root " + secret),
		errors.NewTxInvalidError("x -> UNKNOWN (0): bad-txns-in-belowout " + secret),
	} {
		got := blockRejectReason(err)
		require.False(t, strings.Contains(got, "secret"), got)
		require.LessOrEqual(t, len(got), 111, "bitcoin-sv truncates a reject reason to 111 bytes")
	}
}

// Every string of the vocabulary survives a round trip, and all are lower-case dash words that fit
// the reference's 111-byte reject reason limit.
func TestBlockRejectReasons_Vocabulary(t *testing.T) {
	require.Len(t, blockRejectReasons, 34)

	for reason := range blockRejectReasons {
		require.LessOrEqual(t, len(reason), 111)
		require.Equal(t, reason, blockRejectReason(errors.NewTxInvalidError("GoBDK -> UNKNOWN (0): "+reason)))
	}
}

// The reject a peer gets for a block that block validation judged invalid names the reason.
func TestSyncManager_ARejectedParkedBlockTellsThePeerWhy(t *testing.T) {
	for name, tc := range map[string]struct {
		verdict error
		want    string
	}{
		"bad merkle root": {errors.NewBlockInvalidError("merkle root does not match header's", errors.ErrBlockBodyMismatch), "bad-txnmrklroot"},
		"unclassified":    {errors.NewBlockInvalidError("this block is not one we can take"), "block rejected"},
	} {
		t.Run(name, func(t *testing.T) {
			h := newParkWiringHarnessInState(t, true, blockchain2.FSMStateRUNNING)

			child := h.blocks[1].MsgBlock().BlockHash()
			h.validation.failOnce(child, tc.verdict)

			require.NoError(t, h.deliver(t, 1))
			h.sm.parkCommits = make(chan parkCommit, parkSweepRPCBudget)
			require.NoError(t, h.deliver(t, 0))
			h.drainOneParkCommit(t)

			require.True(t, WaitUntil(func() bool { return h.rec.wasRejected(child) }, 5*time.Second), "the peer must be told")
			require.Equal(t, tc.want, h.rec.rejectReason(child))
		})
	}
}
