package subtreevalidation

import (
	"sort"
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
// innermost is what went wrong (UTXO_SPENT, STORAGE_ERROR, SERVICE_UNAVAILABLE).
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
		b.WriteString(uitoa(c.count))
		b.WriteString(" (e.g. ")
		b.WriteString(c.example.String())
		b.WriteString(")")
	}

	return b.String()
}

// rootErrorCode returns the name of the innermost coded *errors.Error in err's
// chain, or the name of ERR_UNKNOWN when the chain holds none. A plain Go error
// at the bottom of the chain is wrapped by the errors package under ERR_UNKNOWN;
// that carries no information, so the coded error above it names the cause.
func rootErrorCode(err error) string {
	code := errors.ERR_UNKNOWN

	for err != nil {
		var e *errors.Error
		if !errors.As(err, &e) {
			break
		}

		if e.Code() != errors.ERR_UNKNOWN {
			code = e.Code()
		}

		err = e.Unwrap()
	}

	return code.String()
}

func uitoa(n uint64) string {
	if n == 0 {
		return "0"
	}

	var buf [20]byte
	i := len(buf)

	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	return string(buf[i:])
}
