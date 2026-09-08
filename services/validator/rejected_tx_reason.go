package validator

import (
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// maxRejectedTxReasonCodes caps how many codes from an error chain make it into
// the rejected-tx reason. Chains deeper than this carry no extra information
// worth sending to every peer.
const maxRejectedTxReasonCodes = 4

// rejectedTxReason reduces a validation error to a bounded reason string for
// the rejected-tx Kafka message, which the p2p service re-broadcasts to every
// mesh peer. The raw error text is unbounded and partly attacker-shaped (it can
// embed script and transaction details from the rejected input), so the
// network copy carries only the error codes along the chain (errors.CodeChain:
// outermost first, deduplicated, at most maxRejectedTxReasonCodes) joined by
// "/", e.g. "TX_INVALID/PROCESSING" (the enum names print without the ERR_
// prefix). An error carrying no teranode code at all reports TX_INVALID, the
// condition that led here.
func rejectedTxReason(err error) string {
	codes := errors.CodeChain(err, maxRejectedTxReasonCodes)
	if len(codes) == 0 {
		return errors.ERR_TX_INVALID.String()
	}

	names := make([]string, len(codes))
	for i, code := range codes {
		names[i] = code.String()
	}

	return strings.Join(names, "/")
}
