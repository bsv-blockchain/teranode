package subtreevalidation

import (
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/bsv-blockchain/go-bt/v2/chainhash"
	"github.com/bsv-blockchain/teranode/errors"
)

// txFailureTally counts the transactions a subtree or a block's levels failed on,
// by root cause, and keeps the first failed txid per cause as an example. It
// replaces one ERROR line per failed transaction with one summary per subtree:
// a 1,024-tx subtree built from already-mined transactions fails every input
// with the same spent-input cause, and the per-tx lines were a third of a
// node's log volume while carrying one fact.
//
// The cause is the code of the innermost *errors.Error in the chain. The outer
// codes are the wrappers every failure shares (TX_INVALID, PROCESSING); the
// innermost is what went wrong (UTXO_SPENT, UTXO_ERROR, SERVICE_UNAVAILABLE).
// This names one cause per transaction. A transaction with several failed
// inputs carries them as a joined chain of siblings, and the innermost is the
// last failed input: spent, spent, frozen reads UTXO_FROZEN, and a breaker
// error on the first of three inputs is not named. One label per transaction is
// a fair trade for a log summary; the first error's full chain is still
// returned to the caller.
type txFailureTally struct {
	mu     sync.Mutex
	causes map[string]*txFailureCause
	total  uint64
}

type txFailureCause struct {
	count   uint64
	example chainhash.Hash
}

// add records one failed transaction. A nil err counts under "unknown".
func (t *txFailureTally) add(txHash chainhash.Hash, err error) {
	cause := rootErrorCode(err)

	t.mu.Lock()
	defer t.mu.Unlock()

	if t.causes == nil {
		t.causes = make(map[string]*txFailureCause)
	}

	t.total++

	c, ok := t.causes[cause]
	if !ok {
		t.causes[cause] = &txFailureCause{count: 1, example: txHash}
		return
	}

	c.count++
}

// count returns the number of failed transactions recorded so far.
func (t *txFailureTally) count() uint64 {
	t.mu.Lock()
	defer t.mu.Unlock()

	return t.total
}

// String renders the tally as "CAUSE=count (e.g. txid), ..." with the largest
// cause first, and ties in code order so the text is stable.
func (t *txFailureTally) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(t.causes) == 0 {
		return "no failures"
	}

	codes := make([]string, 0, len(t.causes))
	for code := range t.causes {
		codes = append(codes, code)
	}

	sort.Slice(codes, func(i, j int) bool {
		ci, cj := t.causes[codes[i]], t.causes[codes[j]]
		if ci.count != cj.count {
			return ci.count > cj.count
		}

		return codes[i] < codes[j]
	})

	var b strings.Builder

	for i, code := range codes {
		if i > 0 {
			b.WriteString(", ")
		}

		c := t.causes[code]
		b.WriteString(code)
		b.WriteString("=")
		b.WriteString(strconv.FormatUint(c.count, 10))
		b.WriteString(" (e.g. ")
		b.WriteString(c.example.String())
		b.WriteString(")")
	}

	return b.String()
}

// rootCodeMaxDepth bounds the chain walk in rootErrorCode. The store and the
// validator cap a joined spend chain at 10 links plus the "... and N more"
// marker, so a real chain is far shorter; the bound guards a cycle or a
// pathological wrapper.
const rootCodeMaxDepth = 64

// rootErrorCode returns the name of the innermost coded *errors.Error in err's
// chain, or the name of ERR_UNKNOWN when the chain holds none.
//
// Two codes carry no information and are skipped: ERR_UNKNOWN, which the errors
// package puts on a plain Go error at the bottom of a chain, and ERR_ERROR, which
// errors.JoinCapped puts on its "... and N more errors" marker at the end of a
// capped chain. Without the second skip a transaction with more than 10 failed
// inputs would be counted as ERROR instead of by its cause.
//
// The walk calls errors.As once, to reach the first *errors.Error through any
// gRPC or foreign wrapper, and then follows Unwrap with type assertions. The
// package-level errors.As checks for a gRPC status on every call, which renders
// the chain; once per link made this quadratic in the chain length.
func rootErrorCode(err error) string {
	var e *errors.Error
	if !errors.As(err, &e) {
		return errors.ERR_UNKNOWN.String()
	}

	code := errors.ERR_UNKNOWN

	for depth := 0; e != nil && depth < rootCodeMaxDepth; depth++ {
		if c := e.Code(); c != errors.ERR_UNKNOWN && c != errors.ERR_ERROR {
			code = c
		}

		e = nextCodedError(e.Unwrap())
	}

	return code.String()
}

// nextCodedError returns the first *errors.Error at or below err, following
// single-error Unwrap through foreign wrappers, or nil when there is none.
func nextCodedError(err error) *errors.Error {
	for depth := 0; err != nil && depth < rootCodeMaxDepth; depth++ {
		if e, ok := err.(*errors.Error); ok {
			return e
		}

		wrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return nil
		}

		err = wrapper.Unwrap()
	}

	return nil
}
