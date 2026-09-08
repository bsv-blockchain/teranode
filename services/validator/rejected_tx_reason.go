package validator

import (
	"strings"

	"github.com/bsv-blockchain/teranode/errors"
)

// maxRejectedTxReasonCodes caps how many codes from an error chain make it into
// the rejected-tx reason. Chains deeper than this carry no extra information
// worth sending to every peer.
const maxRejectedTxReasonCodes = 4

// rejectedTxReasonDetails is the closed set of rejection messages that may
// travel with the code list. Every entry is a fixed literal in this package's
// validation code (Bitcoin's standard bad-txns-* reject vocabulary, plus the
// few fixed verdicts of our own), matched by exact equality against each
// link's message, so the network copy can only ever carry one of these strings
// byte for byte and never text shaped by the rejected input. Messages with
// formatted values (input indexes, heights, times) and the BDK cause text are
// deliberately absent: they are what the code-only fallback is for.
//
// Adding an entry means asserting the literal is fixed and free of node or
// input state; the test pins that every entry passes that bar.
var rejectedTxReasonDetails = map[string]struct{}{
	// Consensus verdicts (ERR_TX_INVALID) from TxValidator.
	"bad-txns-vout-negative":                  {},
	"bad-txns-vout-toolarge":                  {},
	"bad-txns-txouttotal-toolarge":            {},
	"bad-txns-inputvalues-outofrange":         {},
	"bad-txns-in-belowout":                    {},
	"bad-txns-unconfirmed-input-in-block":     {},
	"coinbase transactions are not supported": {},
	// Policy verdicts (ERR_TX_POLICY, wrapped in ERR_TX_INVALID by the BDK
	// mapper) from TxValidator and the script verifier.
	"bad-txns-inputs-too-large":  {},
	"transaction fee is too low": {},
	errMsgPolicy:                 {},
	// Script verification failure: the outer literal is fixed, the BDK cause
	// text beneath it is not and is never carried.
	errMsgInvalidTx: {},
}

// rejectedTxReason reduces a validation error to a bounded reason string for
// the rejected-tx Kafka message, which the p2p service re-broadcasts to every
// mesh peer. The raw error text is unbounded and partly attacker-shaped (it can
// embed script and transaction details from the rejected input), so the
// network copy carries only the error codes along the chain (errors.CodeChain:
// outermost first, deduplicated, at most maxRejectedTxReasonCodes) joined by
// "/", e.g. "TX_INVALID/TX_POLICY" (the enum names print without the ERR_
// prefix), followed by ": <detail>" when a link's message is one of the fixed
// literals in rejectedTxReasonDetails, e.g. "TX_INVALID: bad-txns-in-belowout".
// The deepest matching literal wins, mirroring errors.DeepestPublicCause: the
// innermost verdict is the most specific one. An error carrying no teranode
// code at all reports TX_INVALID, the condition that led here.
func rejectedTxReason(err error) string {
	codes := errors.CodeChain(err, maxRejectedTxReasonCodes)

	var detail string

	errors.Walk(err, func(te *errors.Error) bool {
		if _, ok := rejectedTxReasonDetails[te.Message()]; ok {
			detail = te.Message()
		}

		return true
	})

	var b strings.Builder

	if len(codes) == 0 {
		b.WriteString(errors.ERR_TX_INVALID.String())
	}

	for i, code := range codes {
		if i > 0 {
			b.WriteByte('/')
		}

		b.WriteString(code.String())
	}

	if detail != "" {
		b.WriteString(": ")
		b.WriteString(detail)
	}

	return b.String()
}
