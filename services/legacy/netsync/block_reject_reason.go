package netsync

import (
	"regexp"
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// defaultBlockRejectReason is what a peer is told when nothing more specific is known.
const defaultBlockRejectReason = "block rejected"

// blockRejectReasons is the closed vocabulary bitcoin-sv v1.2.3 puts in the reason of the
// reject it sends for an invalid block: the strings of its consensus checks that carry
// REJECT_INVALID (CheckBlock, ContextualCheckBlock, CheckTransaction and
// CheckTxInputs/ConnectBlock). Left out on purpose: bad-blk-ttor (reference code 0, which it
// sends no reject for), the mempool-only strings (bad-txns-too-many-sigops,
// bad-txns-nonstandard-inputs), and the checkpoint and version strings, whose reject codes
// are not REJECT_INVALID.
var blockRejectReasons = map[string]struct{}{
	"bad-blk-length": {}, "bad-blk-sigops": {}, "bad-cb-amount": {}, "bad-cb-height": {}, "bad-cb-length": {},
	"bad-cb-missing": {}, "bad-diffbits": {}, "bad-prevblk": {}, "bad-tx-coinbase": {}, "bad-txnmrklroot": {},
	"bad-txns-duplicate": {}, "bad-txns-fee-negative": {}, "bad-txns-fee-outofrange": {}, "bad-txn-sigops": {},
	"bad-txns-in-belowout": {}, "bad-txns-nonfinal": {}, "bad-txns-oversize": {}, "bad-txns-vin-empty": {},
	"bad-txns-vout-empty": {}, "bad-txns-vout-negative": {}, "bad-txns-vout-toolarge": {},
	"bad-txns-txouttotal-toolarge": {}, "bad-txns-vout-p2sh": {}, "bad-txns-prevout-null": {},
	"bad-txns-inputs-duplicate": {}, "bad-txns-inputs-missingorspent": {},
	"bad-txns-premature-spend-of-coinbase": {}, "bad-txns-inputvalues-outofrange": {},
	"bad-txns-premature-spend-of-confiscation": {}, "bad-txns-inputs-too-large": {}, "blk-bad-inputs": {},
	"high-hash": {}, "time-too-new": {}, "time-too-old": {},
}

// rejectReasonToken finds a bitcoin-sv reason where a validator reports one: as the whole text
// after a ": " (GoBDK says "... -> UNKNOWN (0): bad-txns-inputs-duplicate"), up to the end of the
// text or the next ": ". A word of the vocabulary that sits anywhere else in an error text is not
// a verdict and is not matched.
var rejectReasonToken = regexp.MustCompile(`: ([a-z][a-z0-9-]+)(?:$|[ :)])`)

// merkleMismatch matches Teranode's merkle root check failing, both forms of its message.
var merkleMismatch = regexp.MustCompile(`merkle root (?:[0-9a-f]{64} )?does not match header`)

// blockRejectPhrases maps what Teranode's own block checks say, where they have no bitcoin-sv
// string in them, to the string bitcoin-sv uses for the same condition. First match wins, so
// the more specific phrase goes first.
var blockRejectPhrases = []struct{ phrase, reason string }{
	{"has duplicate inputs", "bad-txns-inputs-duplicate"},
	{"CVE-2012-2459", "bad-txns-duplicate"},
	{"does not meet target difficulty", "high-hash"},
	{"incorrect difficulty bits", "bad-diffbits"},
	{"missing coinbase transaction", "bad-cb-missing"},
	{"multiple coinbase transactions", "bad-tx-coinbase"},
	{"parent block is invalid", "bad-prevblk"},
}

// blockRejectReason picks the reason of the reject that tells a peer why its block was refused.
// The answer is always one string of the closed vocabulary above or defaultBlockRejectReason,
// never the text of err, which can carry node-internal detail and peer-supplied data.
//
// Only an error that is a verdict on a block or a transaction is read at all: anything else keeps
// the generic reason. A bitcoin-sv string reported by a validator (GoBDK produces them verbatim)
// is taken from the innermost place it appears, the cause and not a wrapper's context. Teranode's
// own block checks have no such string; their fixed messages are matched only on a block verdict.
func blockRejectReason(err error) string {
	if err == nil {
		return defaultBlockRejectReason
	}

	blockVerdict := errors.Is(err, errors.ErrBlockInvalid)
	if !blockVerdict && !errors.Is(err, errors.ErrTxInvalid) {
		return defaultBlockRejectReason
	}

	text := err.Error()

	reported := rejectReasonToken.FindAllStringSubmatch(text, -1)
	for i := len(reported) - 1; i >= 0; i-- {
		if _, ok := blockRejectReasons[reported[i][1]]; ok {
			return reported[i][1]
		}
	}

	if !blockVerdict {
		return defaultBlockRejectReason
	}

	if merkleMismatch.MatchString(text) {
		return "bad-txnmrklroot"
	}

	for _, p := range blockRejectPhrases {
		if strings.Contains(text, p.phrase) {
			return p.reason
		}
	}

	return defaultBlockRejectReason
}
